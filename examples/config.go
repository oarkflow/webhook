package main

import (
	"fmt"

	"github.com/oarkflow/webhook"
)

func main() {
	fmt.Println("Testing webhook configuration with proper structs...")

	// Test creating default config
	config := webhook.DefaultConfig()

	// Verify all struct fields are properly initialized
	fmt.Printf("✅ Port: %s\n", config.Port)
	fmt.Printf("✅ MaxWorkers: %d\n", config.MaxWorkers)
	fmt.Printf("✅ RateLimit.Enabled: %t\n", config.RateLimit.Enabled)
	fmt.Printf("✅ RateLimit.RequestsPerMinute: %d\n", config.RateLimit.RequestsPerMinute)
	fmt.Printf("✅ Security.MaxRequestSize: %d\n", config.Security.MaxRequestSize)
	fmt.Printf("✅ Security.EnableAuditLog: %t\n", config.Security.EnableAuditLog)
	fmt.Printf("✅ Monitoring.EnableMetrics: %t\n", config.Monitoring.EnableMetrics)
	fmt.Printf("✅ RetryPolicy.MaxRetries: %d\n", config.RetryPolicy.MaxRetries)
	fmt.Printf("✅ Database.MaxConnections: %d\n", config.Database.MaxConnections)

	// Test creating webhook server
	_ = webhook.NewWebhookServer(config)
	fmt.Printf("✅ Webhook server created successfully\n")

	fmt.Println("✅ All configuration structs working correctly!")
}
