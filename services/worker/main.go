package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	_ "github.com/lib/pq"
)

var db *sql.DB

type ClickEvent struct {
	ShortCode string `json:"short_code"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	Referer   string `json:"referer"`
	Timestamp string `json:"timestamp"`
}

func main() {
	log.Println("Starting analytics worker...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("Failed to initialise database connection")
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(5 * time.Minute)

	waitForDB()
	migrate()

	sqsQueue := os.Getenv("SQS_QUEUE_URL")
	if sqsQueue == "" {
		log.Fatal("SQS_QUEUE_URL is required")
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	// Initialise AWS SDK using the ECS task IAM role
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS configuration: %v", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)

	log.Println("SQS client initialised successfully")

	// Health check endpoint
	go func() {
		mux := http.NewServeMux()

		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			status := "ok"

			if err := db.Ping(); err != nil {
				status = "unhealthy"
				w.WriteHeader(http.StatusServiceUnavailable)
			}

			json.NewEncoder(w).Encode(map[string]string{
				"status":  status,
				"service": "worker",
			})
		})

		port := getEnv("HEALTH_PORT", "8090")
		log.Printf("Worker health check on :%s", port)

		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Printf("Health check server error: %v", err)
		}
	}()

	log.Println("Analytics worker started, polling SQS...")

	pollSQS(ctx, sqsClient, sqsQueue)

	log.Println("Worker stopped")
}

func migrate() {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS click_events (
			id SERIAL PRIMARY KEY,
			short_code VARCHAR(16) NOT NULL,
			ip_address VARCHAR(45),
			user_agent TEXT,
			referer TEXT,
			clicked_at TIMESTAMP NOT NULL,
			processed_at TIMESTAMP DEFAULT NOW()
		)`,

		`CREATE INDEX IF NOT EXISTS idx_click_events_short_code
		 ON click_events(short_code)`,

		`CREATE INDEX IF NOT EXISTS idx_click_events_clicked_at
		 ON click_events(clicked_at)`,

		`CREATE TABLE IF NOT EXISTS click_stats_hourly (
			short_code VARCHAR(16) NOT NULL,
			hour TIMESTAMP NOT NULL,
			clicks INTEGER DEFAULT 0,
			unique_ips INTEGER DEFAULT 0,
			PRIMARY KEY (short_code, hour)
		)`,

		`CREATE TABLE IF NOT EXISTS processed_sqs_messages (
			message_id TEXT PRIMARY KEY,
			processed_at TIMESTAMP DEFAULT NOW()
		)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
	}

	log.Println("Worker migrations complete")
}

func pollSQS(ctx context.Context, client *sqs.Client, queueURL string) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Receive messages using SQS long polling
		result, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
			VisibilityTimeout:   120,
		})

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf("Failed to receive SQS messages: %v", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}

			continue
		}

		if len(result.Messages) > 0 {
			log.Printf("Received %d SQS messages", len(result.Messages))
		}

		for _, msg := range result.Messages {
			if ctx.Err() != nil {
				return
			}

			err := processClickEvent(
				ctx,
				aws.ToString(msg.MessageId),
				aws.ToString(msg.Body),
			)

			if err != nil {
				log.Printf("Failed to process click event: %v", err)
				continue
			}

			// Delete message only after successful database processing
			_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})

			if err != nil {
				log.Printf("Failed to delete SQS message: %v", err)
				continue
			}

			log.Printf(
				"Successfully processed and deleted message: %s",
				aws.ToString(msg.MessageId),
			)
		}
	}
}

func processClickEvent(ctx context.Context, messageID, raw string) error {
	var event ClickEvent

	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return err
	}

	clickedAt, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		clickedAt = time.Now().UTC()
	}

	clickedAt = clickedAt.UTC()

	// Start database transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prevent duplicate processing of the same SQS message
	result, err := tx.ExecContext(ctx,
		`INSERT INTO processed_sqs_messages (message_id)
		 VALUES ($1)
		 ON CONFLICT DO NOTHING`,
		messageID,
	)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		log.Printf("Message already processed: %s", messageID)
		return tx.Commit()
	}

	// Insert raw click event
	_, err = tx.ExecContext(ctx,
		`INSERT INTO click_events
		 (short_code, ip_address, user_agent, referer, clicked_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		event.ShortCode,
		event.IP,
		event.UserAgent,
		event.Referer,
		clickedAt,
	)

	if err != nil {
		return err
	}

	// Update hourly analytics
	hour := clickedAt.Truncate(time.Hour)

	_, err = tx.ExecContext(ctx,
		`INSERT INTO click_stats_hourly
		 (short_code, hour, clicks, unique_ips)
		 VALUES ($1, $2, 1, 1)
		 ON CONFLICT (short_code, hour)
		 DO UPDATE SET clicks = click_stats_hourly.clicks + 1`,
		event.ShortCode,
		hour,
	)

	if err != nil {
		return err
	}

	// Commit both database operations
	return tx.Commit()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func waitForDB() {
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		err := db.PingContext(ctx)
		cancel()

		if err == nil {
			log.Println("Database connection successful")
			return
		}

		log.Printf("Waiting for database... (%d/30)", i+1)
		time.Sleep(time.Second)
	}

	log.Fatal("Database not ready after 30 attempts")
}