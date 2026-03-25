package main

import (
	"log"

	"backend_interview_project/internal/app"
	"backend_interview_project/internal/config"
)

func main() {
	if err := app.NewRuntime(config.Load()).Run("default-run"); err != nil {
		log.Fatalf("Application run failed: %v", err)
	}
}
