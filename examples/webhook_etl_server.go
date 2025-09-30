package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/webhook"
)

func main() {
	// Load webhook configuration from file (supports dynamic parser and ETL loading)
	webhookConfig, err := webhook.LoadConfig("examples/cfg/webhook-config.json")
	if err != nil {
		log.Printf("Failed to load webhook config, using defaults: %v", err)
		webhookConfig = webhook.DefaultConfig()
	}

	// Create and start webhook server with dynamic loading
	server := webhook.NewWebhookServer(webhookConfig)

	fmt.Println("🚀 Starting Robust Webhook Server with ETL Integration")
	fmt.Println("📡 Webhook endpoint: http://localhost:8080/webhook")
	fmt.Println("🔍 Health check: http://localhost:8080/health")
	fmt.Println("📋 Supported data types: hl7, json, xml")
	fmt.Println("🔧 ETL Pipelines configured:")
	for name, pipeline := range webhookConfig.ETLPipelines {
		if pipeline.Enabled {
			fmt.Printf("   - %s: %s\n", name, pipeline.Description)
		}
	}
	fmt.Println("\n💡 Example webhook usage:")
	fmt.Println("   curl -X POST http://localhost:8080/webhook \\")
	fmt.Println("        -H 'Content-Type: application/json' \\")
	fmt.Println("        -d '{\"id\": 123, \"name\": \"John Doe\", \"email\": \"john@example.com\"}'")

	// Start the server (this will block)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start webhook server: %v", err)
	}
}
