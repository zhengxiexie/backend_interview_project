package config

import (
	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
	"fmt"
)

type Config struct {
	Kafka kafka.Config
	App   AppConfig
}

type KafkaConfig struct {
	BrokerAddress string
	Topic         string
	GroupID       string
}

type AppConfig struct {
	NumMessages int
}

func Load() *Config {
	return &Config{
		Kafka: kafka.Config{
			BrokerAddress:     fmt.Sprintf("127.0.0.1:%s", docker.KafkaPlaintextPort),
			ControllerAddress: fmt.Sprintf("127.0.0.1:%s", docker.KafkaControllerPort),
			Topic:             "test-topic",
			Partitions:        3,
			GroupID:           "test-consumer-group",
		},
		App: AppConfig{
			NumMessages: 10,
		},
	}
}
