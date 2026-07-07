package webhook

import (
	"context"
	"sort"
	"sync"
	"time"
)

type MemoryStore struct {
	mu            sync.RWMutex
	subscriptions map[string]*Subscription
	events        map[string]*Event
	deliveries    map[string]*Delivery
	byEventType   map[string][]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		subscriptions: make(map[string]*Subscription),
		events:        make(map[string]*Event),
		deliveries:    make(map[string]*Delivery),
		byEventType:   make(map[string][]string),
	}
}

func (s *MemoryStore) CreateSubscription(ctx context.Context, sub *Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sub.ID == "" {
		sub.ID = newID()
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now().UTC()
	}
	if sub.UpdatedAt.IsZero() {
		sub.UpdatedAt = sub.CreatedAt
	}
	if sub.RetryConfig.MaxAttempts == 0 {
		sub.RetryConfig = DefaultRetryConfig()
	}
	if sub.Status == "" {
		sub.Status = SubscriptionActive
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *sub
	s.subscriptions[sub.ID] = &cp
	s.rebuildIndexLocked()
	return nil
}

func (s *MemoryStore) GetSubscription(ctx context.Context, id string) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[id]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	cp := *sub
	return &cp, nil
}

func (s *MemoryStore) ListSubscriptions(ctx context.Context, offset, limit int) ([]*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.subscriptions))
	for id := range s.subscriptions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if offset > len(ids) {
		offset = len(ids)
	}
	ids = ids[offset:]
	if limit <= 0 {
		limit = len(ids)
	}
	if limit > len(ids) {
		limit = len(ids)
	}
	ids = ids[:limit]
	out := make([]*Subscription, len(ids))
	for i, id := range ids {
		cp := *s.subscriptions[id]
		out[i] = &cp
	}
	return out, nil
}

func (s *MemoryStore) UpdateSubscription(ctx context.Context, sub *Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.subscriptions[sub.ID]
	if !ok {
		return ErrSubscriptionNotFound
	}
	sub.CreatedAt = existing.CreatedAt
	sub.UpdatedAt = time.Now().UTC()
	cp := *sub
	s.subscriptions[sub.ID] = &cp
	s.rebuildIndexLocked()
	return nil
}

func (s *MemoryStore) DeleteSubscription(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[id]; !ok {
		return ErrSubscriptionNotFound
	}
	delete(s.subscriptions, id)
	s.rebuildIndexLocked()
	return nil
}

func (s *MemoryStore) CreateEvent(ctx context.Context, evt *Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if evt.ID == "" {
		evt.ID = newID()
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *evt
	s.events[evt.ID] = &cp
	return nil
}

func (s *MemoryStore) GetEvent(ctx context.Context, id string) (*Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	evt, ok := s.events[id]
	if !ok {
		return nil, ErrEventNotFound
	}
	cp := *evt
	return &cp, nil
}

func (s *MemoryStore) ListEvents(ctx context.Context, offset, limit int) ([]*Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.events))
	for id := range s.events {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	if offset > len(ids) {
		offset = len(ids)
	}
	ids = ids[offset:]
	if limit <= 0 {
		limit = len(ids)
	}
	if limit > len(ids) {
		limit = len(ids)
	}
	ids = ids[:limit]
	out := make([]*Event, len(ids))
	for i, id := range ids {
		cp := *s.events[id]
		out[i] = &cp
	}
	return out, nil
}

func (s *MemoryStore) CreateDelivery(ctx context.Context, d *Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.ID == "" {
		d.ID = newID()
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = d.CreatedAt
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *d
	s.deliveries[d.ID] = &cp
	return nil
}

func (s *MemoryStore) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.deliveries[id]
	if !ok {
		return nil, ErrDeliveryNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *MemoryStore) UpdateDelivery(ctx context.Context, d *Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.deliveries[d.ID]
	if !ok {
		return ErrDeliveryNotFound
	}
	d.CreatedAt = existing.CreatedAt
	d.UpdatedAt = time.Now().UTC()
	cp := *d
	s.deliveries[d.ID] = &cp
	return nil
}

func (s *MemoryStore) ListDeliveries(ctx context.Context, subscriptionID string, offset, limit int) ([]*Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.deliveries))
	for id, d := range s.deliveries {
		if subscriptionID == "" || d.SubscriptionID == subscriptionID {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	if offset > len(ids) {
		offset = len(ids)
	}
	ids = ids[offset:]
	if limit <= 0 {
		limit = len(ids)
	}
	if limit > len(ids) {
		limit = len(ids)
	}
	ids = ids[:limit]
	out := make([]*Delivery, len(ids))
	for i, id := range ids {
		cp := *s.deliveries[id]
		out[i] = &cp
	}
	return out, nil
}

func (s *MemoryStore) ListDeliveriesByEvent(ctx context.Context, eventID string) ([]*Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Delivery
	for _, d := range s.deliveries {
		if d.EventID == eventID {
			cp := *d
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *MemoryStore) FindSubscriptionsByEvent(ctx context.Context, eventType string) ([]*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byEventType[eventType]
	if len(ids) == 0 {
		ids = s.byEventType["*"]
	}
	out := make([]*Subscription, 0, len(ids))
	for _, id := range ids {
		sub := s.subscriptions[id]
		if sub != nil && sub.Status == SubscriptionActive {
			cp := *sub
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *MemoryStore) GetPendingDeliveries(ctx context.Context, limit int) ([]*Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.deliveries))
	for id, d := range s.deliveries {
		if d.Status == DeliveryPending || d.Status == DeliveryRetrying {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if limit <= 0 {
		limit = len(ids)
	}
	if limit > len(ids) {
		limit = len(ids)
	}
	ids = ids[:limit]
	out := make([]*Delivery, len(ids))
	for i, id := range ids {
		cp := *s.deliveries[id]
		out[i] = &cp
	}
	return out, nil
}

func (s *MemoryStore) Close() error { return nil }

func (s *MemoryStore) rebuildIndexLocked() {
	s.byEventType = make(map[string][]string)
	for id, sub := range s.subscriptions {
		events := sub.Events
		if len(events) == 0 {
			events = []string{"*"}
		}
		for _, evt := range events {
			s.byEventType[evt] = append(s.byEventType[evt], id)
		}
	}
}


