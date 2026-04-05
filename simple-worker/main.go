package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var live atomic.Bool
var ready atomic.Bool

func main() {
	live.Store(true)
	ready.Store(false)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rabbitURL := envOrDefault("RABBITMQ_URL", "amqp://lab:lab@127.0.0.1:5672/")
	queueName := envOrDefault("QUEUE_NAME", "simple-worker")
	exchangeName := envOrDefault("EXCHANGE_NAME", "lab.events")
	bindingKey := envOrDefault("BINDING_KEY", "labs.worker")
	consumerTag := envOrDefault("CONSUMER_TAG", "go-worker")

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		if !live.Load() {
			http.Error(w, "not live", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              ":8082",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	conn, ch, err := connectRabbit(ctx, rabbitURL, exchangeName, queueName, bindingKey)
	if err != nil {
		log.Fatalf("rabbit setup error: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName,
		consumerTag,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}

	ready.Store(true)
	log.Printf("worker ready: queue=%s exchange=%s binding=%s", queueName, exchangeName, bindingKey)

	go func() {
		<-ctx.Done()
		log.Println("shutdown signal received")
		ready.Store(false)
		_ = ch.Cancel(consumerTag, false)
		time.Sleep(5 * time.Second)
		live.Store(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopping")
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Println("rabbit consumer closed")
				live.Store(false)
				return
			}

			if err := handleMessage(msg.Body); err != nil {
				log.Printf("processing error: %v", err)
				_ = msg.Nack(false, true)
				continue
			}

			_ = msg.Ack(false)
		}
	}
}

func handleMessage(body []byte) error {
	log.Printf("processing: %s", string(body))
	time.Sleep(500 * time.Millisecond)
	return nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func connectRabbit(ctx context.Context, rabbitURL, exchangeName, queueName, bindingKey string) (*amqp.Connection, *amqp.Channel, error) {
	for {
		conn, err := amqp.Dial(rabbitURL)
		if err == nil {
			ch, channelErr := conn.Channel()
			if channelErr != nil {
				_ = conn.Close()
				err = channelErr
			} else {
				if setupErr := setupTopology(ch, exchangeName, queueName, bindingKey); setupErr != nil {
					_ = ch.Close()
					_ = conn.Close()
					err = setupErr
				} else {
					return conn, ch, nil
				}
			}
		}

		log.Printf("rabbit unavailable, retrying in 2s: %v", err)

		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func setupTopology(ch *amqp.Channel, exchangeName, queueName, bindingKey string) error {
	if err := ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.QueueBind(queueName, bindingKey, exchangeName, false, nil); err != nil {
		return err
	}

	return ch.Qos(1, 0, false)
}
