package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
)

const (
	kafkaImagePrimary       = "confluentinc/cp-kafka:7.5.0"
	kafkaImageMirror        = "docker.1ms.run/confluentinc/cp-kafka:7.5.0"
	kafkaContainerName      = "kafka-kraft"
	KafkaPlaintextPort      = "29092"
	KafkaControllerPort     = "29093"
	imageInspectTimeout     = 3 * time.Second
	imagePullAttemptTimeout = 20 * time.Second
)

// KafkaManager owns the lifecycle of the single-broker Kafka test environment
// used by both the demo app and the e2e scenario suite.
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
	imageRef, err := km.ensureKafkaImage(ctx)
	if err != nil {
		return fmt.Errorf("ensure Kafka image: %w", err)
	}

	log.Println("Creating Kafka container...")

	config := &container.Config{
		Image: imageRef,
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
				if removeErr := km.client.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); removeErr != nil && !errdefs.IsNotFound(removeErr) {
					return fmt.Errorf("failed to remove existing container: %w", removeErr)
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

func (km *KafkaManager) ensureKafkaImage(ctx context.Context) (string, error) {
	for _, imageRef := range []string{kafkaImagePrimary, kafkaImageMirror} {
		inspectCtx, cancel := context.WithTimeout(ctx, imageInspectTimeout)
		_, _, err := km.client.ImageInspectWithRaw(inspectCtx, imageRef)
		cancel()
		if err == nil {
			log.Printf("Using local Kafka image %s", imageRef)
			return imageRef, nil
		}
	}

	var lastErr error
	for _, imageRef := range []string{kafkaImagePrimary, kafkaImageMirror} {
		pullCtx, cancel := context.WithTimeout(ctx, imagePullAttemptTimeout)
		log.Printf("Pulling Kafka image %s with timeout %s...", imageRef, imagePullAttemptTimeout)
		reader, err := km.client.ImagePull(pullCtx, imageRef, image.PullOptions{})
		if err != nil {
			cancel()
			lastErr = err
			log.Printf("Failed to pull %s: %v", imageRef, err)
			continue
		}
		_, _ = io.Copy(io.Discard, reader)
		reader.Close()
		cancel()
		return imageRef, nil
	}

	return "", lastErr
}

// waitForKafkaStartup waits for Kafka to complete startup using a two-phase
// approach: first it checks container logs for the startup marker, then it
// verifies the broker is actually accepting TCP connections. This layered
// strategy guards against both log-format changes across Kafka versions and
// situations where the process writes the marker before the listener is ready.
func (km *KafkaManager) waitForKafkaStartup(ctx context.Context) error {
	log.Println("Waiting for Kafka to complete startup (checking container logs + TCP probe)...")

	// Wait up to 60 seconds for Kafka to log that it's started
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case <-timeout:
			return fmt.Errorf("kafka did not start within 60 seconds")
		case <-ctx.Done():
			return ctx.Err()
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

			// Read all available log bytes so a verbose startup sequence
			// cannot push the readiness marker past a fixed-size buffer.
			logBytes, _ := io.ReadAll(logs)
			logs.Close()

			// Look for the message that indicates Kafka is ready
			if strings.Contains(string(logBytes), "Kafka Server started") {
				// Log marker found — now confirm the broker port is reachable.
				if err := tcpProbe("127.0.0.1:"+KafkaPlaintextPort, 2*time.Second); err != nil {
					log.Printf("Kafka log shows started but TCP probe failed: %v — retrying", err)
					continue
				}
				elapsed := time.Since(startTime)
				log.Printf("✓ Kafka is ready (startup took %v)", elapsed)
				return nil
			}

			log.Printf("Kafka still starting up... (%v elapsed)", time.Since(startTime).Round(time.Second))
		}
	}
}

// tcpProbe attempts a TCP dial to addr and closes the connection on success.
// It returns an error if the connection cannot be established within timeout.
func tcpProbe(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	return conn.Close()
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
