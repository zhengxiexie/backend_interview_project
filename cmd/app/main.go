package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

func main() {
	log.Println("Starting Kafka producer/consumer application...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start Kafka container
	log.Println("Starting Kafka container programmatically...")
	kafkaManager, err := docker.NewKafkaManager()
	if err != nil {
		log.Fatalf("Failed to create Kafka manager: %v", err)
	}
	if err := kafkaManager.Start(ctx); err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}

	cfg := config.Load()

	if err := kafka.RecreateTopic(ctx, cfg.Kafka); err != nil {
		log.Printf("Warning: Failed to create topic with %d partitions: %v", cfg.Kafka.Partitions, err)
		panic(err)
	}

	// Create producer and consumer
	producer := kafka.NewProducer(cfg.Kafka.BrokerAddress, cfg.Kafka.Topic)
	consumer := kafka.NewConsumer(cfg.Kafka.BrokerAddress, cfg.Kafka.Topic, cfg.Kafka.GroupID)

	defer func() {
		log.Println("Closing producer and consumer...")
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
		kafkaManager.Close()
		log.Println("Application shutdown complete")
	}()

	var wg sync.WaitGroup

	// Start consumer in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Starting consumer...")
		if err := consumer.ConsumeMessages(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Starting producer...")
		if err := producer.ProduceMessages(ctx, cfg.App.NumMessages); err != nil {
			log.Printf("Producer error: %v", err)
		}
		log.Println("Producer finished sending messages")
		cancel()
	}()

	// Wait for interrupt signal or producer to finish
	select {
	case <-sigChan:
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	case <-ctx.Done():
		log.Println("Context done, shutting down...")
	}

	wg.Wait()
}
