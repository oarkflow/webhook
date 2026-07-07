package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type WebhookServer struct {
	cfg    Config
	store  Store
	signer Signer
	client *fh.Client

	deliveryQ chan deliveryJob
	workersWg sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	started   atomic.Bool
	mu        sync.RWMutex
}

type deliveryJob struct {
	delivery *Delivery
	sub      *Subscription
	event    *Event
}

func New(cfg Config) *WebhookServer {
	cfg.normalize()
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore()
	}
	if cfg.Client == nil {
		cfg.Client = fh.NewClient()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &WebhookServer{
		cfg:       cfg,
		store:     cfg.Store,
		signer:    cfg.Signer,
		client:    cfg.Client,
		deliveryQ: make(chan deliveryJob, 1024),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *WebhookServer) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("webhook: server already started")
	}
	for i := 0; i < s.cfg.WorkerPool; i++ {
		s.workersWg.Add(1)
		go s.deliveryWorker()
	}
	go s.retryLoop()
	return nil
}

func (s *WebhookServer) Stop() error {
	s.cancel()
	s.workersWg.Wait()
	return nil
}

func (s *WebhookServer) Store() Store { return s.store }

func (s *WebhookServer) Subscribe(ctx context.Context, sub Subscription) (*Subscription, error) {
	if sub.URL == "" {
		return nil, ErrInvalidURL
	}
	if sub.RetryConfig.MaxAttempts == 0 {
		sub.RetryConfig = DefaultRetryConfig()
	}
	if sub.Status == "" {
		sub.Status = SubscriptionActive
	}
	if err := s.store.CreateSubscription(ctx, &sub); err != nil {
		return nil, err
	}
	return &sub, nil
}

func (s *WebhookServer) Unsubscribe(ctx context.Context, id string) error {
	sub, err := s.store.GetSubscription(ctx, id)
	if err != nil {
		return err
	}
	sub.Status = SubscriptionDeleted
	return s.store.UpdateSubscription(ctx, sub)
}

func (s *WebhookServer) Publish(ctx context.Context, evt Event) (string, error) {
	if evt.Type == "" {
		return "", ErrInvalidEventType
	}
	raw, err := marshalRaw(evt.Data)
	if err != nil {
		return "", err
	}
	evt.Data = raw
	if err := s.store.CreateEvent(ctx, &evt); err != nil {
		return "", err
	}
	subs, err := s.store.FindSubscriptionsByEvent(ctx, evt.Type)
	if err != nil {
		return "", err
	}
	for _, sub := range subs {
		if sub.Status != SubscriptionActive {
			continue
		}
		if !s.matchFilter(sub, &evt) {
			continue
		}
		d := &Delivery{
			EventID:        evt.ID,
			EventType:      evt.Type,
			SubscriptionID: sub.ID,
			URL:            sub.URL,
			Status:         DeliveryPending,
			MaxAttempts:    sub.RetryConfig.MaxAttempts,
			CreatedAt:      time.Now().UTC(),
		}
		if err := s.store.CreateDelivery(ctx, d); err != nil {
			return evt.ID, err
		}
		s.enqueue(d, sub, &evt)
	}
	return evt.ID, nil
}

func (s *WebhookServer) ReplayEvent(ctx context.Context, eventID string) error {
	evt, err := s.store.GetEvent(ctx, eventID)
	if err != nil {
		return err
	}
	subs, err := s.store.FindSubscriptionsByEvent(ctx, evt.Type)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		if sub.Status != SubscriptionActive {
			continue
		}
		d := &Delivery{
			EventID:        evt.ID,
			EventType:      evt.Type,
			SubscriptionID: sub.ID,
			URL:            sub.URL,
			Status:         DeliveryPending,
			MaxAttempts:    sub.RetryConfig.MaxAttempts,
			CreatedAt:      time.Now().UTC(),
		}
		if err := s.store.CreateDelivery(ctx, d); err != nil {
			return err
		}
		s.enqueue(d, sub, evt)
	}
	return nil
}

func (s *WebhookServer) RotateSecret(ctx context.Context, subscriptionID, newSecret string) error {
	if newSecret == "" {
		return ErrEmptySecret
	}
	sub, err := s.store.GetSubscription(ctx, subscriptionID)
	if err != nil {
		return err
	}
	sub.Secret = newSecret
	return s.store.UpdateSubscription(ctx, sub)
}

func (s *WebhookServer) PauseSubscription(ctx context.Context, id string) error {
	sub, err := s.store.GetSubscription(ctx, id)
	if err != nil {
		return err
	}
	sub.Status = SubscriptionPaused
	return s.store.UpdateSubscription(ctx, sub)
}

func (s *WebhookServer) ResumeSubscription(ctx context.Context, id string) error {
	sub, err := s.store.GetSubscription(ctx, id)
	if err != nil {
		return err
	}
	sub.Status = SubscriptionActive
	return s.store.UpdateSubscription(ctx, sub)
}

func (s *WebhookServer) Mount(app *fh.App, prefix string) {
	prefix = strings.TrimRight(prefix, "/")

	app.Get(prefix+"/subscriptions", s.handleListSubscriptions)
	app.Post(prefix+"/subscriptions", s.handleCreateSubscription)
	app.Get(prefix+"/subscriptions/:id", s.handleGetSubscription)
	app.Put(prefix+"/subscriptions/:id", s.handleUpdateSubscription)
	app.Delete(prefix+"/subscriptions/:id", s.handleDeleteSubscription)
	app.Post(prefix+"/subscriptions/:id/pause", s.handlePauseSubscription)
	app.Post(prefix+"/subscriptions/:id/resume", s.handleResumeSubscription)
	app.Post(prefix+"/subscriptions/:id/rotate-secret", s.handleRotateSecret)

	app.Get(prefix+"/events", s.handleListEvents)
	app.Get(prefix+"/events/:id", s.handleGetEvent)
	app.Post(prefix+"/events/:id/replay", s.handleReplayEvent)

	app.Get(prefix+"/deliveries", s.handleListDeliveries)
	app.Get(prefix+"/deliveries/:id", s.handleGetDelivery)

	app.Get(prefix+"/stats", s.handleStats)
}

func (s *WebhookServer) enqueue(d *Delivery, sub *Subscription, evt *Event) {
	select {
	case s.deliveryQ <- deliveryJob{delivery: d, sub: sub, event: evt}:
	default:
	}
}

func (s *WebhookServer) deliveryWorker() {
	defer s.workersWg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.deliveryQ:
			s.deliver(s.ctx, job)
		}
	}
}

func (s *WebhookServer) retryLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.processRetries(s.ctx)
		}
	}
}

func (s *WebhookServer) processRetries(ctx context.Context) {
	pendings, err := s.store.GetPendingDeliveries(ctx, 100)
	if err != nil {
		return
	}
	for _, d := range pendings {
		if d.Status == DeliveryRetrying && d.Attempt >= d.MaxAttempts {
			d.Status = DeliveryFailed
			_ = s.store.UpdateDelivery(ctx, d)
			continue
		}
		if time.Since(d.UpdatedAt) < s.backoffForAttempt(d.Attempt) {
			continue
		}
		sub, err := s.store.GetSubscription(ctx, d.SubscriptionID)
		if err != nil || sub.Status != SubscriptionActive {
			continue
		}
		evt, err := s.store.GetEvent(ctx, d.EventID)
		if err != nil {
			continue
		}
		s.enqueue(d, sub, evt)
	}
}

func (s *WebhookServer) deliver(ctx context.Context, job deliveryJob) {
	d := job.delivery
	sub := job.sub
	evt := job.event

	payload := EventPayload{Event: *evt}
	body, err := json.Marshal(payload)
	if err != nil {
		d.Status = DeliveryFailed
		d.Error = fmt.Sprintf("marshal error: %v", err)
		_ = s.store.UpdateDelivery(ctx, d)
		return
	}

	start := time.Now()
	req := s.client.R().Body(body).Header("Content-Type", "application/json")
	if sub.Secret != "" {
		sig := s.signer.Sign([]byte(sub.Secret), body)
		req.Header("X-Signature", sig)
		req.Header("X-Signature-Algorithm", s.signer.Algorithm())
	}
	for k, v := range sub.Headers {
		req.Header(k, v)
	}
	req.Header("X-Event-Type", evt.Type)
	req.Header("X-Event-ID", evt.ID)
	req.Header("X-Delivery-ID", d.ID)
	req.Header("X-Webhook-Timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	res, err := req.Post(ctx, sub.URL)
	d.Attempt++
	d.UpdatedAt = time.Now().UTC()
	d.Duration = time.Since(start)

	if err != nil {
		d.Status = DeliveryRetrying
		d.Error = err.Error()
		if d.Attempt >= d.MaxAttempts {
			d.Status = DeliveryFailed
		}
		_ = s.store.UpdateDelivery(ctx, d)
		return
	}
	defer res.DrainAndClose()

	d.StatusCode = res.StatusCode()
	if res.StatusCode() >= 200 && res.StatusCode() < 300 {
		d.Status = DeliveryDelivered
	} else {
		d.Status = DeliveryRetrying
		d.Error = fmt.Sprintf("unexpected status %d", res.StatusCode())
		if d.Attempt >= d.MaxAttempts {
			d.Status = DeliveryFailed
		}
	}
	if bodyBytes, err := res.Bytes(); err == nil && len(bodyBytes) > 0 {
		d.ResponseBody = string(bodyBytes)
	}
	_ = s.store.UpdateDelivery(ctx, d)
}

func (s *WebhookServer) backoffForAttempt(attempt int) time.Duration {
	rc := DefaultRetryConfig()
	delay := float64(rc.BaseDelay) * math.Pow(rc.ExponentialFactor, float64(attempt-1))
	if delay > float64(rc.MaxDelay) {
		delay = float64(rc.MaxDelay)
	}
	if rc.Jitter && delay > 0 {
		delay = float64(rand.Int63n(int64(delay)))
	}
	return time.Duration(delay)
}

func (s *WebhookServer) matchFilter(sub *Subscription, evt *Event) bool {
	if sub.Filter == nil {
		return true
	}
	f := sub.Filter
	if len(f.Types) > 0 {
		matched := false
		for _, t := range f.Types {
			if t == evt.Type || t == "*" {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(f.Subjects) > 0 && evt.Subject != "" {
		matched := false
		for _, subj := range f.Subjects {
			if subj == evt.Subject {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (s *WebhookServer) handleListSubscriptions(c fh.Ctx) error {
	offset, _ := strconv.Atoi(c.Query("offset"))
	limit, _ := strconv.Atoi(c.Query("limit"))
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	subs, err := s.store.ListSubscriptions(c.Context(), offset, limit)
	if err != nil {
		return err
	}
	if subs == nil {
		subs = []*Subscription{}
	}
	return c.JSON(subs)
}

func (s *WebhookServer) handleCreateSubscription(c fh.Ctx) error {
	var sub Subscription
	if err := json.Unmarshal(c.Body(), &sub); err != nil {
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid body: " + err.Error()})
	}
	created, err := s.Subscribe(c.Context(), sub)
	if err != nil {
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
	}
	return c.Status(fh.StatusCreated).JSON(created)
}

func (s *WebhookServer) handleGetSubscription(c fh.Ctx) error {
	id := c.Param("id")
	sub, err := s.store.GetSubscription(c.Context(), id)
	if err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "subscription not found"})
		}
		return err
	}
	return c.JSON(sub)
}

func (s *WebhookServer) handleUpdateSubscription(c fh.Ctx) error {
	id := c.Param("id")
	var sub Subscription
	if err := json.Unmarshal(c.Body(), &sub); err != nil {
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid body: " + err.Error()})
	}
	sub.ID = id
	if err := s.store.UpdateSubscription(c.Context(), &sub); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "subscription not found"})
		}
		return err
	}
	return c.JSON(sub)
}

func (s *WebhookServer) handleDeleteSubscription(c fh.Ctx) error {
	id := c.Param("id")
	if err := s.Unsubscribe(c.Context(), id); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "subscription not found"})
		}
		return err
	}
	return c.SendStatus(fh.StatusNoContent)
}

func (s *WebhookServer) handlePauseSubscription(c fh.Ctx) error {
	return s.statusAction(c, s.PauseSubscription)
}

func (s *WebhookServer) handleResumeSubscription(c fh.Ctx) error {
	return s.statusAction(c, s.ResumeSubscription)
}

func (s *WebhookServer) handleRotateSecret(c fh.Ctx) error {
	id := c.Param("id")
	var body struct {
		Secret string `json:"secret"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": "invalid body: " + err.Error()})
	}
	if err := s.RotateSecret(c.Context(), id, body.Secret); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "subscription not found"})
		}
		return c.Status(fh.StatusBadRequest).JSON(fh.Map{"error": err.Error()})
	}
	return c.JSON(fh.Map{"status": "ok"})
}

func (s *WebhookServer) statusAction(c fh.Ctx, fn func(context.Context, string) error) error {
	id := c.Param("id")
	if err := fn(c.Context(), id); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "subscription not found"})
		}
		return err
	}
	return c.JSON(fh.Map{"status": "ok"})
}

func (s *WebhookServer) handleListEvents(c fh.Ctx) error {
	offset, _ := strconv.Atoi(c.Query("offset"))
	limit, _ := strconv.Atoi(c.Query("limit"))
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	events, err := s.store.ListEvents(c.Context(), offset, limit)
	if err != nil {
		return err
	}
	if events == nil {
		events = []*Event{}
	}
	return c.JSON(events)
}

func (s *WebhookServer) handleGetEvent(c fh.Ctx) error {
	id := c.Param("id")
	evt, err := s.store.GetEvent(c.Context(), id)
	if err != nil {
		if errors.Is(err, ErrEventNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "event not found"})
		}
		return err
	}
	return c.JSON(evt)
}

func (s *WebhookServer) handleReplayEvent(c fh.Ctx) error {
	id := c.Param("id")
	if err := s.ReplayEvent(c.Context(), id); err != nil {
		if errors.Is(err, ErrEventNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "event not found"})
		}
		return err
	}
	return c.JSON(fh.Map{"status": "replayed"})
}

func (s *WebhookServer) handleListDeliveries(c fh.Ctx) error {
	subID := c.Query("subscription_id")
	offset, _ := strconv.Atoi(c.Query("offset"))
	limit, _ := strconv.Atoi(c.Query("limit"))
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	deliveries, err := s.store.ListDeliveries(c.Context(), subID, offset, limit)
	if err != nil {
		return err
	}
	if deliveries == nil {
		deliveries = []*Delivery{}
	}
	return c.JSON(deliveries)
}

func (s *WebhookServer) handleGetDelivery(c fh.Ctx) error {
	id := c.Param("id")
	d, err := s.store.GetDelivery(c.Context(), id)
	if err != nil {
		if errors.Is(err, ErrDeliveryNotFound) {
			return c.Status(fh.StatusNotFound).JSON(fh.Map{"error": "delivery not found"})
		}
		return err
	}
	return c.JSON(d)
}

func (s *WebhookServer) handleStats(c fh.Ctx) error {
	out := fh.Map{
		"workers": s.cfg.WorkerPool,
	}
	deliveries, err := s.store.ListDeliveries(c.Context(), "", 0, 1000)
	if err == nil {
		var delivered, failed, pending, retrying int
		for _, d := range deliveries {
			switch d.Status {
			case DeliveryDelivered:
				delivered++
			case DeliveryFailed:
				failed++
			case DeliveryPending:
				pending++
			case DeliveryRetrying:
				retrying++
			}
		}
		out["delivery_stats"] = fh.Map{
			"delivered": delivered,
			"failed":    failed,
			"pending":   pending,
			"retrying":  retrying,
		}
	}
	subs, err := s.store.ListSubscriptions(c.Context(), 0, 1000)
	if err == nil {
		var active, paused int
		for _, sub := range subs {
			switch sub.Status {
			case SubscriptionActive:
				active++
			case SubscriptionPaused:
				paused++
			}
		}
		out["subscription_stats"] = fh.Map{
			"active": active,
			"paused": paused,
		}
	}
	return c.JSON(out)
}
