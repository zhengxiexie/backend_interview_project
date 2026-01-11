package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

const (
	kafkaImage          = "confluentinc/cp-kafka:7.5.0"
	kafkaContainerName  = "kafka-kraft"
	KafkaPlaintextPort  = "29092"
	KafkaControllerPort = "29093"
)

type KafkaManager struct {
	client      *client.Client
	containerID string
}

func NewKafkaManager() (*KafkaManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &KafkaManager{
		client: cli,
	}, nil
}

// Start starts a Kafka container with KRaft mode
func (km *KafkaManager) Start(ctx context.Context) error {
	log.Println("Pulling Kafka image...")
	reader, err := km.client.ImagePull(ctx, kafkaImage, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	log.Println("Creating Kafka container...")

	config := &container.Config{
		Image: kafkaImage,
		Env: []string{
			"KAFKA_NODE_ID=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:29092",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:29093",
			"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093",
			"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_LOG_DIRS=/var/lib/kafka/data",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
			"CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
		},
		ExposedPorts: nat.PortSet{
			"9092/tcp":  struct{}{},
			"29092/tcp": struct{}{},
			"29093/tcp": struct{}{},
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"29092/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: KafkaPlaintextPort,
				},
			},
			"29093/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: KafkaControllerPort,
				},
			},
		},
		AutoRemove: true,
	}

	containers, err := km.client.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	for _, c := range containers {
		for _, name := range c.Names {
			if name == "/"+kafkaContainerName {
				log.Printf("Container %s already exists, removing it first...", kafkaContainerName)
				if err := km.client.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
					return fmt.Errorf("failed to remove existing container: %w", err)
				}
				break
			}
		}
	}

	resp, err := km.client.ContainerCreate(ctx, config, hostConfig, nil, nil, kafkaContainerName)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	km.containerID = resp.ID

	log.Println("Starting Kafka container...")
	if err := km.client.ContainerStart(ctx, km.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	log.Printf("Kafka container started with ID: %s", km.containerID[:12])

	// Wait for Kafka to be fully ready by checking logs
	if err := km.waitForKafkaStartup(ctx); err != nil {
		return fmt.Errorf("kafka failed to start: %w", err)
	}

	return nil
}

// waitForKafkaStartup waits for Kafka to complete startup by checking container logs
func (km *KafkaManager) waitForKafkaStartup(ctx context.Context) error {
	log.Println("Waiting for Kafka to complete startup (checking container logs)...")

	// Wait up to 60 seconds for Kafka to log that it's started
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case <-timeout:
			return fmt.Errorf("kafka did not start within 60 seconds")
		case <-ticker.C:
			// Check container logs for startup completion
			logs, err := km.client.ContainerLogs(ctx, km.containerID, container.LogsOptions{
				ShowStdout: true,
				ShowStderr: true,
				Tail:       "50",
			})
			if err != nil {
				log.Printf("Cannot read logs yet: %v", err)
				continue
			}

			// Read logs to check for startup message
			buf := make([]byte, 4096)
			n, _ := logs.Read(buf)
			logs.Close()

			logContent := string(buf[:n])

			// Look for the message that indicates Kafka is ready
			if strings.Contains(logContent, "Kafka Server started") {
				elapsed := time.Since(startTime)
				log.Printf("✓ Kafka is ready (startup took %v)", elapsed)
				return nil
			}

			log.Printf("Kafka still starting up... (%v elapsed)", time.Since(startTime).Round(time.Second))
		}
	}
}

func (km *KafkaManager) Close() error {
	if km.containerID == "" {
		return nil
	}

	log.Println("Stopping Kafka container...")

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	timeout := 10
	if km.client != nil {
		if err := km.client.ContainerStop(stopCtx, km.containerID, container.StopOptions{Timeout: &timeout}); err != nil {
			log.Printf("Warning: failed to stop container gracefully: %v", err)

			killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer killCancel()
			if killErr := km.client.ContainerKill(killCtx, km.containerID, "SIGKILL"); killErr != nil {
				log.Printf("Warning: failed to kill container: %v", killErr)
				return err
			}
			log.Printf("Container %s killed", km.containerID[:12])
		}
		log.Println("Kafka container stopped")

		// Close the Docker client AFTER stopping the container
		if err := km.client.Close(); err != nil {
			log.Printf("Warning: failed to close Docker client: %v", err)
		}
	}

	km.containerID = ""
	return nil
}
