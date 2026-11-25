# Stage 1: Build

FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o worker .


# Stage 2: Runtime

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/worker .

EXPOSE 8090

CMD ["./worker"]