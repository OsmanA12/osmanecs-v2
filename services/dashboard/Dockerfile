
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod tidy && go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod -o dashboard .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/dashboard .

EXPOSE 8081

CMD ["./dashboard"]
