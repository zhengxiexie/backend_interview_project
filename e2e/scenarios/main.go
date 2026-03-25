package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	appinternal "backend_interview_project/internal/app"
	"backend_interview_project/internal/config"
	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

// scenarioRunner lets a scenario replace the default single-runtime execution
// path with a custom multi-phase experiment such as replay verification.
type scenarioRunner func(logsDir string, sc scenario) (scenarioResult, error)

// scenario describes one evidence-generating runtime experiment. The fields are
// intentionally explicit so the generated documentation can state exactly what a
// given run was supposed to prove.
type scenario struct {
	Name                     string
	Description              string
	NumMessages              int
	Kafka                    kafka.Config
	ExpectCatchUp            bool
	ExpectRetryLogs          bool
	ExpectGracefulDrainLogs  bool
	ExpectExhaustedRetryLogs bool
	ExpectDrainTimeoutLogs   bool
	ExpectCommitFailureLogs  bool
	ExpectRunError           bool
	ExpectOutOfOrderFrontier bool
	ReplayKey                string
	Runner                   scenarioRunner
}

// scenarioResult captures the durable artifacts from one scenario run: the
// scenario definition, the generated log file, the human summary, and the full
// in-memory log body used to render docs.
type scenarioResult struct {
	Scenario scenario
	LogPath  string
	Summary  string
	LogBody  string
}

func main() {
	base := config.Load()
	if err := runScenarios(base); err != nil {
		log.Fatalf("Scenario runner failed: %v", err)
	}
}

func runScenarios(base *config.Config) error {
	logsDir := filepath.Join("docs", "runtime-logs")
	if err := os.RemoveAll(logsDir); err != nil {
		return fmt.Errorf("reset logs dir: %w", err)
	}
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("create logs dir: %w", err)
	}

	scenarios := buildScenarios(base)
	results := make([]scenarioResult, 0, len(scenarios))
	for i, sc := range scenarios {
		printScenarioBanner(i+1, len(scenarios), sc)
		result, err := runScenario(logsDir, sc)
		if err != nil {
			return err
		}
		printScenarioResult(result)
		results = append(results, result)
	}

	if err := writeScenarioDoc(filepath.Join("docs", "runtime-scenarios.md"), results); err != nil {
		return err
	}
	return nil
}

func printScenarioBanner(idx, total int, sc scenario) {
	const width = 80
	divider := strings.Repeat("=", width)
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, divider)
	fmt.Fprintf(os.Stdout, "SCENARIO %d/%d: %s\n", idx, total, sc.Name)
	fmt.Fprintln(os.Stdout, sc.Description)
	fmt.Fprintln(os.Stdout, divider)
}

func printScenarioResult(result scenarioResult) {
	fmt.Fprintf(os.Stdout, "\u2713 DONE: %s \u2014 %s\n", result.Scenario.Name, result.Summary)
	fmt.Fprintln(os.Stdout)
}

func runScenario(logsDir string, sc scenario) (scenarioResult, error) {
	if sc.Runner != nil {
		return sc.Runner(logsDir, sc)
	}
	return executeRuntimeScenario(logsDir, sc)
}

func executeRuntimeScenario(logsDir string, sc scenario) (scenarioResult, error) {
	return captureScenarioRun(logsDir, sc, func() error {
		cfgCopy := &config.Config{
			Kafka: sc.Kafka,
			App: config.AppConfig{
				NumMessages: sc.NumMessages,
			},
		}
		return appinternal.NewRuntime(cfgCopy).Run(sc.Name)
	})
}

func executeRetryExhaustedReplayScenario(logsDir string, sc scenario) (scenarioResult, error) {
	return captureScenarioRun(logsDir, sc, func() error {
		return withManagedKafka(sc.Kafka, func(ctx context.Context, producer *kafka.Producer) error {
			// Phase 1 proves the message is left uncommitted after retries exhaust.
			// Phase 2 proves the exact same message can be replayed safely later.
			log.Println("Phase 1: produce messages and force retries to exhaust...")
			if err := producer.ProduceMessages(ctx, sc.NumMessages); err != nil {
				return err
			}

			processed1, err := runConsumerForDuration(sc.Kafka, 12*time.Second)
			if err != nil {
				return err
			}
			log.Printf("Phase 1 consumer processed %d messages before shutdown", processed1)

			log.Println("Phase 2: restart consumer without failure injection to verify replay...")
			phase2Cfg := sc.Kafka
			phase2Cfg.Simulation = kafka.SimulationConfig{}
			phase2Cfg.ProcessingDelay = 0
			phase2Cfg.PerKeyProcessingDelays = nil

			processed2, err := runConsumerUntilProcessed(phase2Cfg, 1, 5*time.Second)
			if err != nil {
				return err
			}
			log.Printf("Phase 2 consumer processed %d replayed messages", processed2)
			return nil
		})
	})
}

func executeShutdownWithUnfinishedGapScenario(logsDir string, sc scenario) (scenarioResult, error) {
	return captureScenarioRun(logsDir, sc, func() error {
		return withManagedKafka(sc.Kafka, func(ctx context.Context, producer *kafka.Producer) error {
			// Phase 1 intentionally cancels while work is still running so the suite
			// can prove a drain timeout leaves a replayable gap rather than an unsafe
			// early commit.
			log.Println("Phase 1: produce messages and cancel consumption before in-flight work finishes...")
			if err := producer.ProduceMessages(ctx, sc.NumMessages); err != nil {
				return err
			}

			processed1, err := runConsumerForDuration(sc.Kafka, 7*time.Second)
			if err != nil {
				return err
			}
			log.Printf("Phase 1 consumer processed %d messages before drain timeout", processed1)

			log.Println("Phase 2: restart consumer to replay the unfinished gap...")
			phase2Cfg := sc.Kafka
			phase2Cfg.ProcessingDelay = 0
			phase2Cfg.PerKeyProcessingDelays = nil

			processed2, err := runConsumerUntilProcessed(phase2Cfg, 1, 5*time.Second)
			if err != nil {
				return err
			}
			log.Printf("Phase 2 consumer processed %d replayed messages", processed2)
			return nil
		})
	})
}

func captureScenarioRun(logsDir string, sc scenario, run func() error) (scenarioResult, error) {
	logPath := filepath.Join(logsDir, sc.Name+".log")
	file, err := os.Create(logPath)
	if err != nil {
		return scenarioResult{}, fmt.Errorf("create scenario log %s: %w", sc.Name, err)
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := io.MultiWriter(&buf, file, os.Stdout)
	previousOutput := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(writer)
	log.SetFlags(log.Ldate | log.Ltime)
	defer func() {
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
	}()

	runErr := run()
	if sc.ExpectRunError {
		if runErr == nil {
			return scenarioResult{}, fmt.Errorf("scenario %s expected an error but completed successfully", sc.Name)
		}
		log.Printf("Observed expected scenario error: %v", runErr)
	} else if runErr != nil {
		return scenarioResult{}, fmt.Errorf("run scenario %s: %w", sc.Name, runErr)
	}

	logBody := buf.String()
	summary := summarizeScenario(sc, logBody, runErr)
	return scenarioResult{Scenario: sc, LogPath: logPath, Summary: summary, LogBody: logBody}, nil
}

func withManagedKafka(cfg kafka.Config, run func(ctx context.Context, producer *kafka.Producer) error) error {
	ctx := context.Background()
	log.Println("Starting Kafka container programmatically...")
	manager, err := docker.NewKafkaManager()
	if err != nil {
		return fmt.Errorf("create Kafka manager: %w", err)
	}
	defer func() {
		log.Println("Closing Kafka runtime...")
		if err := manager.Close(); err != nil {
			log.Printf("Error closing Kafka manager: %v", err)
		}
	}()

	if err := manager.Start(ctx); err != nil {
		return fmt.Errorf("start Kafka: %w", err)
	}
	if err := kafka.RecreateTopic(ctx, cfg); err != nil {
		return fmt.Errorf("recreate topic with %d partitions: %w", cfg.Partitions, err)
	}

	producer := kafka.NewProducer(cfg)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()

	return run(ctx, producer)
}

func runConsumerForDuration(cfg kafka.Config, duration time.Duration) (int64, error) {
	consumer := kafka.NewConsumer(cfg)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.ConsumeMessages(ctx)
	}()

	time.Sleep(duration)
	cancel()
	err := kafka.IgnoreContextCancellation(<-done)
	log.Printf("Consumer stopped after processing %d messages", consumer.ProcessedCount())
	return consumer.ProcessedCount(), err
}

func runConsumerUntilProcessed(cfg kafka.Config, expected int64, timeout time.Duration) (int64, error) {
	consumer := kafka.NewConsumer(cfg)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- consumer.ConsumeMessages(ctx)
	}()

	deadline := time.Now().Add(timeout)
	for {
		select {
		case err := <-done:
			return consumer.ProcessedCount(), kafka.IgnoreContextCancellation(err)
		default:
		}

		if consumer.ProcessedCount() >= expected {
			cancel()
			err := kafka.IgnoreContextCancellation(<-done)
			log.Printf("Consumer stopped after processing %d messages", consumer.ProcessedCount())
			return consumer.ProcessedCount(), err
		}
		if time.Now().After(deadline) {
			cancel()
			_ = kafka.IgnoreContextCancellation(<-done)
			return consumer.ProcessedCount(), fmt.Errorf("timed out waiting for processed count >= %d, got %d", expected, consumer.ProcessedCount())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func buildScenarios(base *config.Config) []scenario {
	baseKafka := base.Kafka

	baseline := baseKafka
	baseline.Topic = "scenario-baseline"
	baseline.GroupID = "scenario-baseline-group"
	baseline.ProcessingDelay = 0
	baseline.PerKeyProcessingDelays = nil

	retry := baseKafka
	retry.Topic = "scenario-retry"
	retry.GroupID = "scenario-retry-group"
	retry.RetryBaseDelay = 150 * time.Millisecond
	retry.RetryMaxDelay = 300 * time.Millisecond
	retry.Simulation = kafka.SimulationConfig{
		FailureKeys:     []string{"key-2", "key-7"},
		FailureAttempts: 1,
	}

	drain := baseKafka
	drain.Topic = "scenario-drain"
	drain.GroupID = "scenario-drain-group"
	drain.ProcessingDelay = 350 * time.Millisecond
	drain.PerKeyProcessingDelays = nil
	drain.DrainTimeout = 6 * time.Second
	drain.QuiescenceWindow = 150 * time.Millisecond

	outOfOrder := baseKafka
	outOfOrder.Topic = "scenario-out-of-order"
	outOfOrder.GroupID = "scenario-out-of-order-group"
	outOfOrder.WorkerCount = 3
	outOfOrder.ProcessingDelay = 0
	outOfOrder.PerKeyProcessingDelays = map[string]time.Duration{
		"key-0": 900 * time.Millisecond,
		"key-3": 50 * time.Millisecond,
		"key-5": 50 * time.Millisecond,
	}
	outOfOrder.DrainTimeout = 7 * time.Second

	retryExhausted := baseKafka
	retryExhausted.Topic = "scenario-retry-exhausted-replay"
	retryExhausted.GroupID = "scenario-retry-exhausted-replay-group"
	retryExhausted.MaxRetryAttempts = 2
	retryExhausted.WorkerCount = 1
	retryExhausted.QueueCapacity = 1
	retryExhausted.RetryBaseDelay = 100 * time.Millisecond
	retryExhausted.RetryMaxDelay = 200 * time.Millisecond
	retryExhausted.Simulation = kafka.SimulationConfig{
		FailureKeys:     []string{"key-0"},
		FailureAttempts: 5,
	}

	unfinishedGap := baseKafka
	unfinishedGap.Topic = "scenario-unfinished-gap-replay"
	unfinishedGap.GroupID = "scenario-unfinished-gap-replay-group"
	unfinishedGap.WorkerCount = 1
	unfinishedGap.ProcessingDelay = 10 * time.Second
	unfinishedGap.PerKeyProcessingDelays = nil
	unfinishedGap.DrainTimeout = 150 * time.Millisecond
	unfinishedGap.QuiescenceWindow = 50 * time.Millisecond

	commitFailure := baseKafka
	commitFailure.Topic = "scenario-commit-failure"
	commitFailure.GroupID = "scenario-commit-failure-group"
	commitFailure.Simulation = kafka.SimulationConfig{
		CommitFailureCount: 1,
	}

	return []scenario{
		{
			Name:                    "baseline-success",
			Description:             "Standard success path: all 10 messages are processed successfully and committed safely.",
			NumMessages:             10,
			Kafka:                   baseline,
			ExpectCatchUp:           true,
			ExpectGracefulDrainLogs: true,
		},
		{
			Name:                    "retry-and-recovery",
			Description:             "A subset of messages fails on the first processing attempt, then succeeds after backoff-based retry.",
			NumMessages:             10,
			Kafka:                   retry,
			ExpectCatchUp:           true,
			ExpectRetryLogs:         true,
			ExpectGracefulDrainLogs: true,
		},
		{
			Name:                    "graceful-drain",
			Description:             "Artificial processing delay shows that after the producer finishes, the consumer continues draining in-flight work before shutdown.",
			NumMessages:             10,
			Kafka:                   drain,
			ExpectCatchUp:           true,
			ExpectGracefulDrainLogs: true,
		},
		{
			Name:                     "out-of-order-frontier",
			Description:              "Same-partition messages finish out of order, but commit still waits for the earlier gap to close before advancing.",
			NumMessages:              6,
			Kafka:                    outOfOrder,
			ExpectCatchUp:            true,
			ExpectGracefulDrainLogs:  true,
			ExpectOutOfOrderFrontier: true,
		},
		{
			Name:                     "retry-exhausted-replay",
			Description:              "A message exhausts retries, remains uncommitted, and is then replayed successfully by a restarted consumer.",
			NumMessages:              1,
			Kafka:                    retryExhausted,
			ExpectExhaustedRetryLogs: true,
			ReplayKey:                "key-0",
			Runner:                   executeRetryExhaustedReplayScenario,
		},
		{
			Name:                   "shutdown-with-unfinished-gap",
			Description:            "Shutdown occurs while in-flight work is still unfinished, causing a drain timeout and later replay of the uncommitted message.",
			NumMessages:            1,
			Kafka:                  unfinishedGap,
			ExpectDrainTimeoutLogs: true,
			ReplayKey:              "key-0",
			Runner:                 executeShutdownWithUnfinishedGapScenario,
		},
		{
			Name:                    "commit-failure-stops-intake",
			Description:             "A simulated commit failure stops intake, triggers conservative draining, and surfaces an expected runtime error.",
			NumMessages:             10,
			Kafka:                   commitFailure,
			ExpectGracefulDrainLogs: true,
			ExpectCommitFailureLogs: true,
			ExpectRunError:          true,
		},
	}
}

func summarizeScenario(sc scenario, logBody string, runErr error) string {
	checks := make([]string, 0, 8)
	if sc.ExpectCatchUp && strings.Contains(logBody, "Consumer caught up:") {
		checks = append(checks, "consumer completed catch-up")
	}
	if sc.ExpectRetryLogs && strings.Contains(logBody, "retrying in") {
		checks = append(checks, "explicit retry/backoff logs appeared")
	}
	if sc.ExpectGracefulDrainLogs && strings.Contains(logBody, "All in-flight consumer work drained") {
		checks = append(checks, "graceful drain logs appeared")
	}
	if strings.Contains(logBody, "Committed partition") {
		checks = append(checks, "real commit logs appeared")
	}
	if sc.ExpectExhaustedRetryLogs && strings.Contains(logBody, "exhausted retries") {
		checks = append(checks, "retry exhaustion was observed")
	}
	if sc.ExpectDrainTimeoutLogs && strings.Contains(logBody, "Drain timeout reached") {
		checks = append(checks, "drain timeout logs appeared")
	}
	if sc.ExpectCommitFailureLogs && strings.Contains(logBody, "simulated commit failure") {
		checks = append(checks, "commit failure logs appeared")
	}
	if sc.ExpectRunError && runErr != nil {
		checks = append(checks, "expected scenario error was observed")
	}
	if sc.ReplayKey != "" && (strings.Count(logBody, "key="+sc.ReplayKey) >= 2 || strings.Contains(logBody, "replayed messages")) {
		checks = append(checks, "uncommitted message replay was observed")
	}
	if sc.ExpectOutOfOrderFrontier && detectOutOfOrderFrontier(logBody) {
		checks = append(checks, "out-of-order completion still committed only after the gap closed")
	}
	if len(checks) == 0 {
		return "This scenario ran, but none of the expected summary checks were matched."
	}
	return strings.Join(checks, "; ") + "."
}

func detectOutOfOrderFrontier(logBody string) bool {
	processedRe := regexp.MustCompile(`Processed partition (\d+) offset (\d+)`)
	committedRe := regexp.MustCompile(`Committed partition (\d+) through offset`)

	type processedEvent struct {
		lineIdx   int
		partition int
		offset    int
	}

	lines := strings.Split(logBody, "\n")
	var events []processedEvent
	// firstCommitLine[p] is the line index of the first commit log for partition p.
	firstCommitLine := map[int]int{}

	for i, line := range lines {
		if m := processedRe.FindStringSubmatch(line); m != nil {
			p, _ := strconv.Atoi(m[1])
			o, _ := strconv.Atoi(m[2])
			events = append(events, processedEvent{i, p, o})
		}
		if m := committedRe.FindStringSubmatch(line); m != nil {
			p, _ := strconv.Atoi(m[1])
			if _, seen := firstCommitLine[p]; !seen {
				firstCommitLine[p] = i
			}
		}
	}

	// For each partition track the running max offset seen in log order.
	// If a lower offset appears after a higher one (out-of-order completion)
	// and a commit for that partition followed both events, the frontier gate
	// is confirmed to have waited correctly.
	type partitionState struct {
		maxOffset  int
		maxLineIdx int
	}
	state := map[int]partitionState{}

	for _, ev := range events {
		ps, seen := state[ev.partition]
		if seen && ev.offset < ps.maxOffset {
			// Lower offset finished after a higher one: out-of-order confirmed.
			// Verify a commit for this partition appeared after this late event.
			if commitLine, ok := firstCommitLine[ev.partition]; ok && commitLine > ev.lineIdx {
				return true
			}
		}
		if !seen || ev.offset > ps.maxOffset {
			state[ev.partition] = partitionState{ev.offset, ev.lineIdx}
		}
	}
	return false
}

func writeScenarioDoc(enPath string, results []scenarioResult) error {
	if err := os.WriteFile(enPath, []byte(buildDoc(results)), 0o644); err != nil {
		return err
	}
	return nil
}

const flowzapInit = "%%{init: {'theme': 'base', 'themeVariables': {" +
	"'fontFamily': 'LXGWWenKaiMonoGB Nerd Font, monospace'," +
	"'fontSize': '14px','background': '#FFFFFF','mainBkg': '#FFFFFF'," +
	"'noteBkgColor': '#E8F5E9','noteTextColor': '#1B5E20','noteBorderColor': '#66BB6A'," +
	"'actorBkg': '#E3F2FD','actorBorder': '#1565C0','actorTextColor': '#0D47A1'," +
	"'actorLineColor': '#90CAF9','signalColor': '#37474F','signalTextColor': '#263238'," +
	"'labelBoxBkgColor': '#FFF3E0','labelBoxBorderColor': '#FB8C00','labelTextColor': '#E65100'," +
	"'loopTextColor': '#4A148C','activationBkgColor': '#E1F5FE','activationBorderColor': '#0288D1'," +
	"'sequenceNumberColor': '#FFFFFF','lineColor': '#78909C'}}}%%\n"

func buildDoc(results []scenarioResult) string {

	var b strings.Builder
	b.WriteString("# Runtime Scenarios and Real Log Evidence\n\n")
	b.WriteString("This document adds several representative runtime scenarios and includes real log excerpts generated by the current implementation. The goal is to provide concrete evidence that the solution satisfies the assignment requirements around concurrency, retries, offset commit timing, and graceful shutdown.\n\n")
	b.WriteString("## Summary\n\n")
	b.WriteString("- All scenarios were generated by running `go run ./e2e/scenarios` against the current repository.\n")
	b.WriteString("- The full raw logs are stored under `docs/runtime-logs/`.\n")
	b.WriteString("- This document only includes the most relevant excerpts that demonstrate the required behaviors.\n\n")

	for i, result := range results {
		fmt.Fprintf(&b, "## Scenario %d: %s\n\n", i+1, result.Scenario.Name)
		b.WriteString(result.Scenario.Description + "\n\n")
		b.WriteString("### What this proves\n\n")
		b.WriteString("- Verified outcomes: " + result.Summary + "\n")
		b.WriteString("- Full log: `" + filepath.ToSlash(result.LogPath) + "`\n\n")
		b.WriteString("### Scenario timeline\n\n")
		rawDiagram := buildScenarioDiagram(result)
		b.WriteString(strings.Replace(rawDiagram, "```mermaid\n", "```mermaid\n"+flowzapInit, 1))
		b.WriteString("\n\n")
		b.WriteString("### Key real log excerpts\n\n```text\n")
		b.WriteString(extractImportantLines(result.LogBody))
		b.WriteString("\n```\n\n")
		b.WriteString("### Mapping back to the assignment requirements\n\n")
		b.WriteString(explainScenario(result.Scenario.Name) + "\n\n")
	}

	return b.String()
}

func buildScenarioDiagram(result scenarioResult) string {
	logBody := result.LogBody
	switch result.Scenario.Name {
	case "baseline-success":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant CG as Consumer Group\n    participant WK as Worker Pool\n    participant ST as Partition State\n    participant CM as Commit Gate\n\n    RT->>PR: [%s] start producer pipeline\n    PR->>KF: [%s-%s] write 10 hash-partitioned messages\n    KF->>CG: [%s] fetch assigned partition messages\n    CG->>WK: dispatch jobs concurrently across P2, P0, and P1\n    WK->>ST: record successful completions by partition\n    ST->>CM: [%s / %s / %s] frontier advances when gaps close\n    CM->>KF: commit offsets only after contiguous success\n    RT->>CG: [%s] stop intake after catch-up\n    CG-->>RT: [%s] all in-flight work drained\n```",
			firstTimestampContaining(logBody, "Starting producer..."),
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Produced message 9"),
			firstTimestampContaining(logBody, "processing partition"),
			firstTimestampContaining(logBody, "Committed partition 2 through offset 3"),
			firstTimestampContaining(logBody, "Committed partition 0 through offset 2"),
			firstTimestampContaining(logBody, "Committed partition 1 through offset 2"),
			firstTimestampContaining(logBody, "Consumer caught up"),
			firstTimestampContaining(logBody, "All in-flight consumer work drained"),
		)
	case "retry-and-recovery":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant CG as Consumer Group\n    participant WK as Worker Pool\n    participant RP as Retry Policy\n    participant CM as Commit Gate\n\n    PR->>KF: [%s-%s] write 10 messages across 3 partitions\n    KF->>CG: [%s] deliver messages to workers\n    WK->>RP: [%s] key-2 fails on attempt 1\n    RP->>WK: bounded backoff then retry\n    WK->>RP: [%s] key-7 fails on attempt 1\n    RP->>WK: bounded backoff then retry\n    WK->>CM: [%s / %s] retried messages finally succeed\n    CM->>KF: [%s] frontier advances and commits resume\n    RT->>CG: [%s] catch-up complete, then graceful drain\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Produced message 9"),
			firstTimestampContaining(logBody, "processing partition"),
			firstTimestampContaining(logBody, "key=key-2 attempt=1"),
			firstTimestampContaining(logBody, "key=key-7 attempt=1"),
			firstTimestampContaining(logBody, "Processed partition 2 offset 0"),
			firstTimestampContaining(logBody, "Processed partition 2 offset 2"),
			firstTimestampContaining(logBody, "Committed partition 2 through offset 3"),
			firstTimestampContaining(logBody, "Consumer caught up"),
		)
	case "graceful-drain":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant CG as Consumer Group\n    participant WK as Worker Pool\n    participant CM as Commit Gate\n\n    PR->>KF: [%s-%s] produce 10 messages\n    PR-->>RT: [%s] producer finishes first\n    RT->>CG: open bounded catch-up window\n    CG->>WK: [%s] workers continue processing in-flight jobs\n    WK->>CM: [%s] successful completions keep advancing commits\n    RT->>CG: [%s] stop intake after catch-up\n    CG-->>RT: [%s] drain completes before exit\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Produced message 9"),
			firstTimestampContaining(logBody, "Producer finished sending messages"),
			firstTimestampContaining(logBody, "processing partition"),
			lastTimestampContaining(logBody, "Committed partition"),
			firstTimestampContaining(logBody, "Consumer caught up"),
			firstTimestampContaining(logBody, "All in-flight consumer work drained"),
		)
	case "out-of-order-frontier":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant WK1 as Worker offset 1/2\n    participant WK0 as Worker offset 0\n    participant ST as Partition 0 State\n    participant CM as Commit Gate\n\n    PR->>KF: [%s-%s] produce 6 messages\n    KF->>WK1: [%s] dispatch partition 0 offset 1\n    KF->>WK0: [%s] dispatch partition 0 offset 0\n    WK1->>ST: [%s / %s] offset 1 and 2 finish early\n    Note over ST: commit is still blocked by offset 0 gap\n    WK0->>ST: [%s] offset 0 finishes late\n    ST->>CM: [%s] contiguous frontier closes\n    CM->>KF: commit partition 0 through offset 2\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Produced message 5"),
			firstTimestampContaining(logBody, "partition 0 offset 1"),
			firstTimestampContaining(logBody, "partition 0 offset 0 attempt 1"),
			firstTimestampContaining(logBody, "Processed partition 0 offset 1"),
			firstTimestampContaining(logBody, "Processed partition 0 offset 2"),
			firstTimestampContaining(logBody, "Processed partition 0 offset 0"),
			firstTimestampContaining(logBody, "Committed partition 0 through offset 2"),
		)
	case "retry-exhausted-replay":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant C1 as Phase 1 Consumer\n    participant RP as Retry Policy\n    participant C2 as Replay Consumer\n    participant CM as Commit Gate\n\n    PR->>KF: [%s] produce key-0 once\n    KF->>C1: [%s] deliver offset 0 to phase 1 consumer\n    C1->>RP: [%s] attempt 1 fails\n    RP->>C1: [%s] retry with 100ms backoff\n    C1->>RP: [%s] attempt 2 fails\n    RP->>C1: retry with 200ms backoff\n    C1-->>KF: [%s] retries exhausted, leave offset uncommitted\n    RT->>C2: [%s] start replay consumer\n    C2->>CM: [%s] replayed offset 0 succeeds\n    CM->>KF: [%s] commit replayed message\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Starting consumer group \"scenario-retry-exhausted-replay-group\""),
			firstTimestampContaining(logBody, "attempt 1"),
			firstTimestampContaining(logBody, "retrying in 100ms"),
			firstTimestampContaining(logBody, "attempt 2"),
			firstTimestampContaining(logBody, "exhausted retries"),
			lastTimestampContaining(logBody, "Starting consumer group \"scenario-retry-exhausted-replay-group\""),
			firstTimestampContaining(logBody, "replayed messages"),
			firstTimestampContaining(logBody, "Committed partition 0 through offset 0"),
		)
	case "shutdown-with-unfinished-gap":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant C1 as Phase 1 Consumer\n    participant WK as In-flight Worker\n    participant C2 as Replay Consumer\n    participant CM as Commit Gate\n\n    PR->>KF: [%s] produce key-0 once\n    KF->>C1: [%s] start phase 1 consumer\n    C1->>WK: [%s] begin processing offset 0\n    RT->>C1: [%s] stop intake while work is still active\n    C1-->>RT: [%s] drain timeout with one in-flight job left\n    RT->>C2: [%s] restart consumer for replay\n    C2->>WK: [%s] replay offset 0 from Kafka\n    WK->>CM: offset 0 completes safely\n    CM->>KF: [%s] commit replayed message\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Starting consumer group \"scenario-unfinished-gap-replay-group\""),
			firstTimestampContaining(logBody, "processing partition 0 offset 0"),
			firstTimestampContaining(logBody, "Consumer intake stopped"),
			firstTimestampContaining(logBody, "Drain timeout reached"),
			lastTimestampContaining(logBody, "Starting consumer group \"scenario-unfinished-gap-replay-group\""),
			lastTimestampContaining(logBody, "processing partition 0 offset 0"),
			firstTimestampContaining(logBody, "Committed partition 0 through offset 0"),
		)
	case "commit-failure-stops-intake":
		return fmt.Sprintf("```mermaid\nsequenceDiagram\n    participant RT as Runtime\n    participant PR as Producer\n    participant KF as Kafka Topic\n    participant CG as Consumer Group\n    participant WK as Worker Pool\n    participant CM as Commit Gate\n\n    PR->>KF: [%s-%s] produce 10 messages\n    KF->>CG: [%s] start consumer group intake\n    CG->>WK: [%s] workers begin processing assigned partition messages\n    WK->>CM: try to commit a contiguous frontier\n    CM-->>RT: [%s] simulated commit failure surfaces\n    RT->>CG: [%s] stop intake immediately\n    CG-->>RT: [%s] drain remaining in-flight work\n    RT-->>RT: [%s] return expected runtime error\n```",
			firstTimestampContaining(logBody, "Produced message 0"),
			firstTimestampContaining(logBody, "Produced message 9"),
			firstTimestampContaining(logBody, "Starting consumer group \"scenario-commit-failure-group\""),
			firstTimestampContaining(logBody, "processing partition"),
			firstTimestampContaining(logBody, "simulated commit failure"),
			firstTimestampContaining(logBody, "Consumer intake stopped"),
			firstTimestampContaining(logBody, "All in-flight consumer work drained"),
			firstTimestampContaining(logBody, "Observed expected scenario error"),
		)

	default:
		return "```mermaid\nflowchart LR\n    A[Scenario log] --> B[No diagram template for this scenario yet]\n```"
	}
}

func firstTimestampContaining(logBody, keyword string) string {

	for _, line := range strings.Split(logBody, "\n") {
		if strings.Contains(line, keyword) {
			return extractLogTime(line)
		}
	}
	return "time N/A"
}

func lastTimestampContaining(logBody, keyword string) string {
	lines := strings.Split(logBody, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.Contains(lines[i], keyword) {
			return extractLogTime(lines[i])
		}
	}
	return "time N/A"
}

func extractLogTime(line string) string {
	if len(line) >= 19 && strings.Contains(line[:19], "/") {
		return line[11:19]
	}
	return "time N/A"
}

func extractImportantLines(logBody string) string {

	keywords := []string{
		"Phase 1",
		"Phase 2",
		"Starting consumer group",
		"Produced message",
		"processing partition",
		"simulated transient failure",
		"retrying in",
		"exhausted retries",
		"Processed partition",
		"Committed partition",
		"simulated commit failure",
		"Observed expected scenario error",
		"Consumer caught up",
		"Consumer intake stopped",
		"Drain timeout reached",
		"All in-flight consumer work drained",
		"Consumer stopped after processing",
	}

	lines := strings.Split(logBody, "\n")
	selected := make([]string, 0, len(lines))
	for _, line := range lines {
		for _, keyword := range keywords {
			if strings.Contains(line, keyword) {
				selected = append(selected, line)
				break
			}
		}
	}
	return strings.Join(selected, "\n")
}

func explainScenario(name string) string {
	switch name {
	case "baseline-success":
		return "This scenario demonstrates the normal success path: the consumer group fetches messages, processes them concurrently, advances a safe frontier per partition, and emits real commit logs. That directly supports the assignment's commit-after-success and at-least-once requirements."
	case "retry-and-recovery":
		return "This scenario injects one transient failure for selected keys and shows that the implementation does not commit those offsets immediately. Instead, it logs retry/backoff activity first, then advances the commit frontier only after the retried messages eventually succeed. That directly demonstrates explicit failure handling and success-gated offset commits."
	case "graceful-drain":
		return "This scenario adds processing delay to demonstrate that the process does not exit immediately after the producer finishes. Instead, it waits for the consumer to catch up, then stops intake and drains already in-flight work before shutdown. That directly supports the graceful shutdown requirement."
	case "out-of-order-frontier":
		return "This scenario deliberately creates out-of-order completion within the same partition by assigning different processing delays to different keys. The logs then show that commit does not advance until the earlier gap closes, which directly demonstrates same-partition concurrency without unsafe offset skipping."
	case "retry-exhausted-replay":
		return "This scenario first forces a message to exhaust its retries and remain uncommitted, then restarts consumption without failure injection on the same topic and group. That gives direct evidence for the repository's replay-based at-least-once story: duplicates are possible, message loss is not."
	case "shutdown-with-unfinished-gap":
		return "This scenario cancels consumption while in-flight work is still unfinished, forces a drain timeout, and then starts a second consumption phase on the same topic and group. It proves that shutdown stays conservative: unfinished work is not committed early, and the remaining message is replayed later."
	case "commit-failure-stops-intake":
		return "This scenario injects a commit failure and shows that the system stops intake, drains conservatively, and surfaces the error instead of continuing to fetch and process blindly."
	default:
		return "This scenario is intended to prove, using real logs, that the current implementation satisfies the key runtime requirements from the README."
	}
}
