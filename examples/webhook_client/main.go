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

	// Start a fake receiver that verifies the signature.
	secret := "whsec_shared_secret"
	received := make(chan string, 4)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 4096)
		n, _ := r.Body.Read(body)
		payload := body[:n]

		// Verify the signature server-side.
		sig := r.Header.Get("X-Webhook-Signature")
		if err := webhook.VerifyPayload([]byte(secret), payload, sig, "sha256"); err != nil {
			log.Println("signature verification failed:", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Println("  signature OK: alg=sha256")

		received <- string(payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// Create an fh HTTP client and wrap it with the webhook client.
	client := fh.NewClient(fh.ClientConfig{
		Timeout: 5 * time.Second,
	})
	defer client.Close()

	wc := webhook.NewWebhookClient(client)

	// --- Send a signed webhook ---
	evt := webhook.Event{
		ID:   "evt_custom_id",
		Type: "payment.completed",
		Data: json.RawMessage(`{"payment_id": "pi_123", "amount": 4999, "currency": "usd"}`),
	}
	res, err := wc.Send(ctx, evt, webhook.SendConfig{
		URL:    receiver.URL,
		Secret: secret,
		Headers: map[string]string{
			"X-Custom": "value123",
		},
	})
	if err != nil {
		log.Fatal("send failed:", err)
	}
	defer res.DrainAndClose()
	fmt.Println("send status:", res.StatusCode())

	// --- Use the convenience SendJSON helper ---
	res2, err := wc.SendJSON(ctx, receiver.URL, "user.signed_up",
		map[string]any{"user_id": 456, "email": "alice@example.com"},
		secret,
	)
	if err != nil {
		log.Fatal("send json failed:", err)
	}
	defer res2.DrainAndClose()
	fmt.Println("send json status:", res2.StatusCode())

	// Verify what the receiver got.
	for i := 0; i < 2; i++ {
		select {
		case payload := <-received:
			var evt webhook.Event
			json.Unmarshal([]byte(payload), &evt)
			fmt.Printf("receiver got event type=%q id=%q\n", evt.Type, evt.ID)
		case <-time.After(3 * time.Second):
			log.Fatal("timeout waiting for receiver")
		}
	}

	// --- Standalone signature verification ---
	payload := []byte(`{"test": "data"}`)
	sig := webhook.HMACSHA256().Sign([]byte(secret), payload)
	fmt.Println("\nstandalone verify:")
	fmt.Println("  sha256 match:", webhook.VerifySHA256([]byte(secret), payload, sig))
	fmt.Println("  sha384 match:", webhook.VerifySHA384([]byte(secret), payload, sig))
	fmt.Println("  wrong key  :", webhook.VerifySHA256([]byte("wrong"), payload, sig))
}
