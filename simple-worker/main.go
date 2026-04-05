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

	rabbitURL := os.Getenv("RABBITMQ_URL")
	queueName := os.Getenv("QUEUE_NAME")

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

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit connect error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("rabbit channel error: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName,
		"go-worker",
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		ready.Store(false)
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
