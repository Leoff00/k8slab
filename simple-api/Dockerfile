FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o main .


FROM alpine:3.23
WORKDIR /app
COPY --from=builder /app/main .
EXPOSE 8081
CMD ["./main"]