package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/oarkflow/webhook"
)

func main() {
	fmt.Println("=== Testing Production-Ready Webhook Features ===")

	// Create webhook server with production features
	config := webhook.DefaultConfig()
	config.Port = "8081"
	config.Security.MaxRequestSize = 1024 * 1024 // 1MB
	config.RateLimit.Enabled = true
	config.RateLimit.RequestsPerMinute = 100
	config.RateLimit.BurstSize = 10
	config.Monitoring.EnableMetrics = true
	config.Monitoring.MetricsPort = "9091"
	config.RetryPolicy.MaxRetries = 2
	config.Security.EnableAuditLog = true

	ws := webhook.NewWebhookServer(config)

	// Start server in background
	go func() {
		if err := ws.Start(); err != nil {
			fmt.Printf("❌ Server failed to start: %v\n", err)
		}
	}()

	// Wait for server to start
	time.Sleep(2 * time.Second)

	// Test 1: Basic webhook functionality
	fmt.Println("\n1. Testing basic webhook functionality...")
	testBasicWebhook()

	// Test 2: Rate limiting
	fmt.Println("\n2. Testing rate limiting...")
	testRateLimiting()

	// Test 3: Webhook status tracking
	fmt.Println("\n3. Testing webhook status tracking...")
	testWebhookStatus()

	// Test 4: Metrics endpoint
	fmt.Println("\n4. Testing metrics endpoint...")
	testMetricsEndpoint()

	// Test 5: Health check
	fmt.Println("\n5. Testing health check...")
	testHealthCheck()

	fmt.Println("\n=== Production Webhook Tests Completed ===")
}

func testBasicWebhook() {
	// Test JSON webhook
	jsonData := `{"id": 123, "name": "test", "active": true}`
	resp, err := http.Post("http://localhost:8081/webhook", "application/json", bytes.NewBuffer([]byte(jsonData)))
	if err != nil {
		fmt.Printf("❌ Basic webhook test failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("✅ Basic webhook response: %s (status: %d)\n", string(body), resp.StatusCode)
}

func testRateLimiting() {
	// Send multiple requests quickly to test rate limiting
	for i := 0; i < 15; i++ {
		jsonData := fmt.Sprintf(`{"id": %d, "name": "rate_test_%d"}`, i, i)
		resp, err := http.Post("http://localhost:8081/webhook", "application/json", bytes.NewBuffer([]byte(jsonData)))

		if err != nil {
			fmt.Printf("❌ Rate limiting test request %d failed: %v\n", i+1, err)
			continue
		}

		_, _ = ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			fmt.Printf("✅ Rate limiting working - request %d blocked (status: %d)\n", i+1, resp.StatusCode)
			break
		} else {
			fmt.Printf("✅ Request %d allowed (status: %d)\n", i+1, resp.StatusCode)
		}

		// Small delay between requests
		time.Sleep(100 * time.Millisecond)
	}
}

func testWebhookStatus() {
	// Send a webhook first
	jsonData := `{"id": 999, "name": "status_test", "email": "test@example.com"}`
	http.Post("http://localhost:8081/webhook", "application/json", bytes.NewBuffer([]byte(jsonData)))

	// Wait a moment for processing
	time.Sleep(500 * time.Millisecond)

	// Check webhook status
	resp, err := http.Get("http://localhost:8081/webhooks")
	if err != nil {
		fmt.Printf("❌ Webhook status test failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Printf("✅ Webhook status endpoint working\n")

		// Parse and display webhook statuses
		var statuses []*webhook.WebhookStatus
		json.Unmarshal(body, &statuses)
		fmt.Printf("✅ Found %d webhook statuses\n", len(statuses))
	} else {
		fmt.Printf("❌ Webhook status endpoint returned status: %d\n", resp.StatusCode)
	}
}

func testMetricsEndpoint() {
	resp, err := http.Get("http://localhost:8081/metrics")
	if err != nil {
		fmt.Printf("❌ Metrics endpoint test failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Printf("✅ Metrics endpoint working\n")

		// Parse metrics
		var metrics webhook.Metrics
		json.Unmarshal(body, &metrics)
		fmt.Printf("✅ Total requests: %d, Success: %d, Failed: %d\n",
			metrics.TotalRequests, metrics.SuccessfulRequests, metrics.FailedRequests)
	} else {
		fmt.Printf("❌ Metrics endpoint returned status: %d\n", resp.StatusCode)
	}
}

func testHealthCheck() {
	resp, err := http.Get("http://localhost:8081/health")
	if err != nil {
		fmt.Printf("❌ Health check test failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("✅ Health check response: %s (status: %d)\n", string(body), resp.StatusCode)
}
