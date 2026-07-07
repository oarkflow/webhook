package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestMemoryStoreCreateAndGetSubscription(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	sub := &Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test-secret",
		Events: []string{"order.created", "order.updated"},
	}
	if err := store.CreateSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if sub.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	got, err := store.GetSubscription(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != sub.URL {
		t.Fatalf("expected URL %q, got %q", sub.URL, got.URL)
	}
	if got.Status != SubscriptionActive {
		t.Fatalf("expected active status, got %q", got.Status)
	}
}

func TestMemoryStoreListSubscriptions(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		sub := &Subscription{URL: "https://example.com/webhook", Name: "sub-" + string(rune('a'+i))}
		if err := store.CreateSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	subs, err := store.ListSubscriptions(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 5 {
		t.Fatalf("expected 5 subscriptions, got %d", len(subs))
	}
}

func TestMemoryStorePagination(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := store.CreateSubscription(ctx, &Subscription{URL: "https://example.com/webhook"}); err != nil {
			t.Fatal(err)
		}
	}
	subs, err := store.ListSubscriptions(ctx, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 3 {
		t.Fatalf("expected 3, got %d", len(subs))
	}
	subs, err = store.ListSubscriptions(ctx, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 3 {
		t.Fatalf("expected 3, got %d", len(subs))
	}
}

func TestMemoryStoreDeleteSubscription(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	sub := &Subscription{URL: "https://example.com/webhook"}
	if err := store.CreateSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSubscription(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSubscription(ctx, sub.ID); err == nil {
		t.Fatal("expected error after deletion")
	}
}

func TestMemoryStoreUpdateSubscription(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	sub := &Subscription{URL: "https://example.com/webhook", Secret: "old-secret"}
	if err := store.CreateSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	sub.Secret = "new-secret"
	if err := store.UpdateSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSubscription(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Secret != "new-secret" {
		t.Fatalf("expected new-secret, got %q", got.Secret)
	}
}

func TestMemoryStoreEventCRUD(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	evt := &Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 123}`),
	}
	if err := store.CreateEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if evt.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	got, err := store.GetEvent(ctx, evt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != evt.Type {
		t.Fatalf("expected type %q, got %q", evt.Type, got.Type)
	}
}

func TestMemoryStoreDeliveryCRUD(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	d := &Delivery{
		EventID:        "evt-1",
		SubscriptionID: "sub-1",
		URL:            "https://example.com/webhook",
		Status:         DeliveryPending,
	}
	if err := store.CreateDelivery(ctx, d); err != nil {
		t.Fatal(err)
	}
	if d.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	d.Status = DeliveryDelivered
	if err := store.UpdateDelivery(ctx, d); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetDelivery(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != DeliveryDelivered {
		t.Fatalf("expected delivered, got %q", got.Status)
	}
}

func TestMemoryStoreFindSubscriptionsByEvent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	sub1 := &Subscription{URL: "https://a.example.com/webhook", Events: []string{"order.created", "order.updated"}}
	sub2 := &Subscription{URL: "https://b.example.com/webhook", Events: []string{"order.created"}}
	sub3 := &Subscription{URL: "https://c.example.com/webhook", Events: []string{"user.created"}}
	for _, s := range []*Subscription{sub1, sub2, sub3} {
		if err := store.CreateSubscription(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	subs, err := store.FindSubscriptionsByEvent(ctx, "order.created")
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 2 {
		t.Fatalf("expected 2 subscriptions for order.created, got %d", len(subs))
	}
	subs, err = store.FindSubscriptionsByEvent(ctx, "user.created")
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscription for user.created, got %d", len(subs))
	}
	subs, err = store.FindSubscriptionsByEvent(ctx, "unknown.event")
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 0 {
		t.Fatalf("expected 0 subscriptions for unknown.event, got %d", len(subs))
	}
}

func TestMemoryStorePendingDeliveries(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	d1 := &Delivery{EventID: "evt-1", SubscriptionID: "sub-1", Status: DeliveryPending}
	d2 := &Delivery{EventID: "evt-2", SubscriptionID: "sub-2", Status: DeliveryDelivered}
	d3 := &Delivery{EventID: "evt-3", SubscriptionID: "sub-3", Status: DeliveryRetrying}
	for _, d := range []*Delivery{d1, d2, d3} {
		if err := store.CreateDelivery(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := store.GetPendingDeliveries(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending deliveries, got %d", len(pending))
	}
}

func TestHMACSigner(t *testing.T) {
	signer := HMACSHA256()
	sig := signer.Sign([]byte("secret"), []byte(`{"hello":"world"}`))
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}
	if signer.Algorithm() != "sha256" {
		t.Fatalf("expected sha256, got %s", signer.Algorithm())
	}
	if !VerifySHA256([]byte("secret"), []byte(`{"hello":"world"}`), sig) {
		t.Fatal("expected valid signature")
	}
	if VerifySHA256([]byte("wrong-secret"), []byte(`{"hello":"world"}`), sig) {
		t.Fatal("expected invalid signature with wrong secret")
	}
}

func TestHMACSignerSHA512(t *testing.T) {
	signer, err := NewHMACSigner("sha512")
	if err != nil {
		t.Fatal(err)
	}
	sig := signer.Sign([]byte("secret"), []byte(`data`))
	if !VerifySHA512([]byte("secret"), []byte(`data`), sig) {
		t.Fatal("expected valid sha512 signature")
	}
}

func TestNewHMACSignerInvalid(t *testing.T) {
	_, err := NewHMACSigner("md5")
	if err == nil {
		t.Fatal("expected error for unsupported algorithm")
	}
}

func TestVerifyPayload(t *testing.T) {
	payload := []byte(`{"event":"test"}`)
	sig := HMACSHA256().Sign([]byte("secret"), payload)
	if err := VerifyPayload([]byte("secret"), payload, sig, "sha256"); err != nil {
		t.Fatal("expected verification to pass:", err)
	}
	if err := VerifyPayload([]byte("wrong"), payload, sig, "sha256"); err == nil {
		t.Fatal("expected verification to fail")
	}
}

func TestDefaultRetryConfig(t *testing.T) {
	rc := DefaultRetryConfig()
	if rc.MaxAttempts != 5 {
		t.Fatalf("expected 5, got %d", rc.MaxAttempts)
	}
	if rc.BaseDelay != time.Second {
		t.Fatalf("expected 1s, got %v", rc.BaseDelay)
	}
}

func TestMarshalRaw(t *testing.T) {
	raw, err := marshalRaw(nil)
	if err != nil || raw != nil {
		t.Fatalf("expected nil,nil got %v,%v", raw, err)
	}
	raw, err = marshalRaw([]byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"a":1}` {
		t.Fatalf("expected {\"a\":1}, got %s", raw)
	}
	raw, err = marshalRaw(map[string]int{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"a":1}` {
		t.Fatalf("expected {\"a\":1}, got %s", raw)
	}
}

func TestWebhookClientConfig(t *testing.T) {
	cl := fh.NewClient()
	wc := NewWebhookClient(cl)
	if wc.client == nil {
		t.Fatal("expected non-nil client")
	}
	if wc.opts.Retries != 3 {
		t.Fatalf("expected 3 retries, got %d", wc.opts.Retries)
	}
}

func TestSubscriptionFilter(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, err := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
		Filter: &Filter{
			Types:    []string{"order.created"},
			Subjects: []string{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}
}

func TestWebhookServerSubscribeUnsubscribe(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, err := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test-secret",
		Events: []string{"order.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	if sub.Status != SubscriptionActive {
		t.Fatalf("expected active, got %q", sub.Status)
	}
	if err := server.Unsubscribe(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	got, err := server.Store().GetSubscription(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != SubscriptionDeleted {
		t.Fatalf("expected deleted, got %q", got.Status)
	}
}

func TestWebhookServerPauseResume(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, _ := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
	})
	if err := server.PauseSubscription(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := server.Store().GetSubscription(ctx, sub.ID)
	if got.Status != SubscriptionPaused {
		t.Fatalf("expected paused, got %q", got.Status)
	}
	if err := server.ResumeSubscription(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = server.Store().GetSubscription(ctx, sub.ID)
	if got.Status != SubscriptionActive {
		t.Fatalf("expected active, got %q", got.Status)
	}
}

func TestWebhookServerRotateSecret(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, _ := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "old-secret",
		Events: []string{"order.created"},
	})
	if err := server.RotateSecret(ctx, sub.ID, "new-secret"); err != nil {
		t.Fatal(err)
	}
	got, _ := server.Store().GetSubscription(ctx, sub.ID)
	if got.Secret != "new-secret" {
		t.Fatalf("expected new-secret, got %q", got.Secret)
	}
}

func TestWebhookServerPublish(t *testing.T) {
	store := NewMemoryStore()
	server := New(Config{Store: store})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
	})
	evtID, err := server.Publish(ctx, Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 123}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if evtID == "" {
		t.Fatal("expected non-empty event ID")
	}
	evt, err := store.GetEvent(ctx, evtID)
	if err != nil {
		t.Fatal(err)
	}
	if evt.Type != "order.created" {
		t.Fatalf("expected order.created, got %q", evt.Type)
	}
	deliveries, err := store.ListDeliveriesByEvent(ctx, evtID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
}

func TestMatchFilter(t *testing.T) {
	server := New(Config{})
	evt := &Event{Type: "order.created", Subject: "ord-123"}
	sub := &Subscription{
		Filter: &Filter{Types: []string{"order.created"}},
	}
	if !server.matchFilter(sub, evt) {
		t.Fatal("expected filter to match")
	}
	sub.Filter = &Filter{Types: []string{"order.updated"}}
	if server.matchFilter(sub, evt) {
		t.Fatal("expected filter to not match")
	}
	sub.Filter = nil
	if !server.matchFilter(sub, evt) {
		t.Fatal("expected nil filter to match all")
	}
	sub.Filter = &Filter{Subjects: []string{"ord-123"}}
	if !server.matchFilter(sub, evt) {
		t.Fatal("expected subject filter to match")
	}
}

func TestWebhookServerReplayEvent(t *testing.T) {
	store := NewMemoryStore()
	server := New(Config{Store: store})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
	})
	evtID, _ := server.Publish(ctx, Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 123}`),
	})
	deliveries, _ := store.ListDeliveriesByEvent(ctx, evtID)
	initialCount := len(deliveries)

	if err := server.ReplayEvent(ctx, evtID); err != nil {
		t.Fatal(err)
	}
	deliveries, _ = store.ListDeliveriesByEvent(ctx, evtID)
	if len(deliveries) <= initialCount {
		t.Fatal("expected more deliveries after replay")
	}
}

func TestMemoryStoreListDeliveriesBySubscription(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		store.CreateDelivery(ctx, &Delivery{
			EventID:        "evt-1",
			SubscriptionID: "sub-1",
			Status:         DeliveryDelivered,
		})
	}
	store.CreateDelivery(ctx, &Delivery{
		EventID:        "evt-2",
		SubscriptionID: "sub-2",
		Status:         DeliveryDelivered,
	})
	ds, err := store.ListDeliveries(ctx, "sub-1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 3 {
		t.Fatalf("expected 3 deliveries for sub-1, got %d", len(ds))
	}
}

func TestConfigNormalization(t *testing.T) {
	cfg := Config{}
	cfg.normalize()
	if cfg.Signer == nil {
		t.Fatal("expected non-nil signer")
	}
	if cfg.MaxRetries != 5 {
		t.Fatalf("expected 5, got %d", cfg.MaxRetries)
	}
	if cfg.Backoff != time.Second {
		t.Fatalf("expected 1s, got %v", cfg.Backoff)
	}
	if cfg.WorkerPool != 4 {
		t.Fatalf("expected 4, got %d", cfg.WorkerPool)
	}
}

func TestNewID(t *testing.T) {
	id1 := newID()
	id2 := newID()
	if id1 == id2 {
		t.Fatal("expected unique IDs")
	}
}

func TestNewHMACSigners(t *testing.T) {
	signers := []string{"sha256", "sha384", "sha512"}
	for _, algo := range signers {
		s, err := NewHMACSigner(algo)
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", algo, err)
		}
		if s.Algorithm() != algo {
			t.Fatalf("expected %s, got %s", algo, s.Algorithm())
		}
	}
}

func TestWebhookClientSendValidation(t *testing.T) {
	cl := fh.NewClient()
	wc := NewWebhookClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := wc.Send(ctx, Event{Type: "test"}, SendConfig{URL: "http://127.0.0.1:19999/invalid"})
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
}

func TestWebhookClientSendJSON(t *testing.T) {
	cl := fh.NewClient()
	wc := NewWebhookClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := wc.SendJSON(ctx, "http://127.0.0.1:19999/invalid", "test.event", map[string]string{"hello": "world"}, "secret")
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
}

func TestClientOptionDefaults(t *testing.T) {
	cl := fh.NewClient()
	wc := NewWebhookClient(cl)
	if wc.opts.Retries != 3 {
		t.Fatalf("expected 3, got %d", wc.opts.Retries)
	}
	if wc.opts.Timeout != 30*time.Second {
		t.Fatalf("expected 30s, got %v", wc.opts.Timeout)
	}
}

func TestServerWithCustomStore(t *testing.T) {
	store := NewMemoryStore()
	server := New(Config{Store: store})
	if server.Store() != store {
		t.Fatal("expected custom store")
	}
}

func TestNilClientError(t *testing.T) {
	wc := NewWebhookClient(nil)
	_, err := wc.Send(context.Background(), Event{}, SendConfig{URL: "http://example.com"})
	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("expected ErrNilClient, got %v", err)
	}
}

func TestMemoryStoreNotFound(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	_, err := store.GetSubscription(ctx, "nonexistent")
	if !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
	_, err = store.GetEvent(ctx, "nonexistent")
	if !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("expected ErrEventNotFound, got %v", err)
	}
	_, err = store.GetDelivery(ctx, "nonexistent")
	if !errors.Is(err, ErrDeliveryNotFound) {
		t.Fatalf("expected ErrDeliveryNotFound, got %v", err)
	}
	err = store.UpdateDelivery(ctx, &Delivery{ID: "nonexistent"})
	if !errors.Is(err, ErrDeliveryNotFound) {
		t.Fatalf("expected ErrDeliveryNotFound, got %v", err)
	}
}

func TestMemoryStoreDeleteNotFound(t *testing.T) {
	store := NewMemoryStore()
	err := store.DeleteSubscription(context.Background(), "nonexistent")
	if !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestServerStartStop(t *testing.T) {
	server := New(Config{})
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err == nil {
		t.Fatal("expected error on second start")
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestEventPublishWithNoSubscribers(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	id, err := server.Publish(ctx, Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 123}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty event ID")
	}
}

func TestServerRotateSecretEmpty(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, _ := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
	})
	err := server.RotateSecret(ctx, sub.ID, "")
	if !errors.Is(err, ErrEmptySecret) {
		t.Fatalf("expected ErrEmptySecret, got %v", err)
	}
}

func TestServerSubscribeInvalidURL(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	_, err := server.Subscribe(context.Background(), Subscription{})
	if !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL, got %v", err)
	}
}

func TestVerifyFunctions(t *testing.T) {
	payload := []byte(`{"test": true}`)
	secret := []byte("my-secret")

	sig256 := HMACSHA256().Sign(secret, payload)
	sig384, _ := NewHMACSigner("sha384")
	sig384Str := sig384.Sign(secret, payload)
	sig512, _ := NewHMACSigner("sha512")
	sig512Str := sig512.Sign(secret, payload)

	if !VerifySHA256(secret, payload, sig256) {
		t.Fatal("sha256 verification failed")
	}
	if !VerifySHA384(secret, payload, sig384Str) {
		t.Fatal("sha384 verification failed")
	}
	if !VerifySHA512(secret, payload, sig512Str) {
		t.Fatal("sha512 verification failed")
	}
	if VerifySHA256([]byte("wrong"), payload, sig256) {
		t.Fatal("expected wrong secret to fail")
	}
}

func TestDeliveryDeliveryStatusConstants(t *testing.T) {
	if DeliveryPending != "pending" {
		t.Fatalf("expected pending, got %q", DeliveryPending)
	}
	if DeliveryDelivered != "delivered" {
		t.Fatalf("expected delivered, got %q", DeliveryDelivered)
	}
	if DeliveryFailed != "failed" {
		t.Fatalf("expected failed, got %q", DeliveryFailed)
	}
	if DeliveryRetrying != "retrying" {
		t.Fatalf("expected retrying, got %q", DeliveryRetrying)
	}
}

func TestSubscriptionStatusConstants(t *testing.T) {
	if SubscriptionActive != "active" {
		t.Fatalf("expected active, got %q", SubscriptionActive)
	}
	if SubscriptionPaused != "paused" {
		t.Fatalf("expected paused, got %q", SubscriptionPaused)
	}
	if SubscriptionDeleted != "deleted" {
		t.Fatalf("expected deleted, got %q", SubscriptionDeleted)
	}
}

func TestMemoryStoreListEventsEmpty(t *testing.T) {
	store := NewMemoryStore()
	events, err := store.ListEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0, got %d", len(events))
	}
}

func TestServerPublishWithEventTypeValidation(t *testing.T) {
	server := New(Config{})
	server.Start()
	defer server.Stop()

	_, err := server.Publish(context.Background(), Event{
		Type: "",
	})
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("expected ErrInvalidEventType, got %v", err)
	}
}

func TestMemoryStoreListByEventEmpty(t *testing.T) {
	store := NewMemoryStore()
	ds, err := store.ListDeliveriesByEvent(context.Background(), "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 {
		t.Fatalf("expected 0, got %d", len(ds))
	}
}

func TestWebhookServerPublishToPausedSubscriber(t *testing.T) {
	store := NewMemoryStore()
	server := New(Config{Store: store})
	server.Start()
	defer server.Stop()

	ctx := context.Background()
	sub, _ := server.Subscribe(ctx, Subscription{
		URL:    "https://example.com/webhook",
		Secret: "test",
		Events: []string{"order.created"},
	})
	server.PauseSubscription(ctx, sub.ID)
	server.Publish(ctx, Event{
		Type: "order.created",
		Data: json.RawMessage(`{"order_id": 123}`),
	})
	time.Sleep(50 * time.Millisecond)
	deliveries, _ := store.ListDeliveriesByEvent(ctx, "nonexistent")
	_ = deliveries
	allDeliveries, _ := store.ListDeliveries(ctx, "", 0, 100)
	if len(allDeliveries) != 0 {
		t.Fatalf("expected 0 deliveries for paused subscriber, got %d", len(allDeliveries))
	}
}
