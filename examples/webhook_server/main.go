package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/webhook"
)

func main() {
	ctx := context.Background()

	// Start a fake upstream that receives webhook deliveries.
	received := make(chan string, 16)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 4096)
		n, _ := r.Body.Read(body)
		received <- string(body[:n])
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	// Create the webhook server.
	wh := webhook.New(webhook.Config{
		WorkerPool: 2,
	})
	wh.Start()
	defer wh.Stop()

	// --- Server mode: receive webhooks from external services ---
	// Mount the admin API so subscribers can be managed and events replayed.
	app := fh.New()
	wh.Mount(app, "/webhooks")

	// Register the upstream endpoint as a subscriber.
	sub, err := wh.Subscribe(ctx, webhook.Subscription{
		URL:    upstream.URL,
		Secret: "whsec_example_secret",
		Events: []string{"order.created", "order.updated"},
		Headers: map[string]string{
			"X-Source": "fh-webhook-demo",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("subscribed:", sub.ID, "->", sub.URL)

	// Publish an event — it gets delivered to the upstream asynchronously.
	evtID, err := wh.Publish(ctx, webhook.Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 42, "total": 29.99}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("published event:", evtID)

	// Wait for the delivery to arrive at the upstream.
	select {
	case payload := <-received:
		fmt.Println("upstream received:", payload)
	case <-time.After(3 * time.Second):
		log.Fatal("timeout waiting for delivery")
	}

	// Publish a second event that matches no subscriber (filtered out).
	wh.Publish(ctx, webhook.Event{
		Type: "user.deleted",
		Data: json.RawMessage(`{"user_id": 7}`),
	})
	fmt.Println("published user.deleted (no matching subscriber)")

	// Replay the first event.
	if err := wh.ReplayEvent(ctx, evtID); err != nil {
		log.Fatal(err)
	}
	fmt.Println("replayed event:", evtID)

	// Pause and resume a subscription.
	if err := wh.PauseSubscription(ctx, sub.ID); err != nil {
		log.Fatal(err)
	}
	fmt.Println("paused:", sub.ID)
	if err := wh.ResumeSubscription(ctx, sub.ID); err != nil {
		log.Fatal(err)
	}
	fmt.Println("resumed:", sub.ID)

	// Rotate the subscriber secret.
	if err := wh.RotateSecret(ctx, sub.ID, "whsec_new_secret"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("rotated secret for:", sub.ID)

	// Start the admin server in the background.
	if err := app.Listen(":3001"); err != nil {
		log.Fatal(err)
	}
}
