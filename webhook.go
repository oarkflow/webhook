package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/oarkflow/fh"
)

var (
	ErrSubscriptionNotFound = errors.New("webhook: subscription not found")
	ErrEventNotFound        = errors.New("webhook: event not found")
	ErrDeliveryNotFound     = errors.New("webhook: delivery not found")
	ErrSubscriptionPaused   = errors.New("webhook: subscription is paused")
	ErrInvalidEventType     = errors.New("webhook: invalid event type")
	ErrInvalidURL           = errors.New("webhook: invalid URL")
	ErrEmptySecret          = errors.New("webhook: empty secret")
)

type SubscriptionStatus string

const (
	SubscriptionActive  SubscriptionStatus = "active"
	SubscriptionPaused  SubscriptionStatus = "paused"
	SubscriptionDeleted SubscriptionStatus = "deleted"
)

type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliveryDelivered DeliveryStatus = "delivered"
	DeliveryFailed    DeliveryStatus = "failed"
	DeliveryRetrying  DeliveryStatus = "retrying"
)

type RetryConfig struct {
	MaxAttempts       int           `json:"max_attempts"`
	BaseDelay         time.Duration `json:"base_delay"`
	MaxDelay          time.Duration `json:"max_delay"`
	ExponentialFactor float64       `json:"exponential_factor"`
	Jitter            bool          `json:"jitter"`
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 30 * time.Second, ExponentialFactor: 2.0, Jitter: true}
}

type Filter struct {
	Types    []string          `json:"types,omitempty"`
	Subjects []string          `json:"subjects,omitempty"`
	Payload  map[string]any    `json:"payload,omitempty"`
}

type Subscription struct {
	ID          string            `json:"id"`
	Name        string            `json:"name,omitempty"`
	URL         string            `json:"url"`
	Secret      string            `json:"secret,omitempty"`
	Events      []string          `json:"events,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Status      SubscriptionStatus `json:"status"`
	RetryConfig RetryConfig       `json:"retry_config"`
	Filter      *Filter           `json:"filter,omitempty"`
	RateLimit   int               `json:"rate_limit,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Subject   string          `json:"subject,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

type Delivery struct {
	ID             string         `json:"id"`
	EventID        string         `json:"event_id"`
	EventType      string         `json:"event_type"`
	SubscriptionID string         `json:"subscription_id"`
	URL            string         `json:"url"`
	Status         DeliveryStatus `json:"status"`
	Attempt        int            `json:"attempt"`
	MaxAttempts    int            `json:"max_attempts"`
	StatusCode     int            `json:"status_code,omitempty"`
	ResponseBody   string         `json:"response_body,omitempty"`
	Error          string         `json:"error,omitempty"`
	Duration       time.Duration  `json:"duration,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type EventPayload struct {
	Event
	Signature string `json:"signature,omitempty"`
}

type Signer interface {
	Sign(secret []byte, payload []byte) string
	Algorithm() string
}

type HMACSigner struct {
	hash func() hash.Hash
	name string
}

func NewHMACSigner(algorithm string) (*HMACSigner, error) {
	switch algorithm {
	case "sha256":
		return &HMACSigner{hash: sha256.New, name: "sha256"}, nil
	case "sha384":
		return &HMACSigner{hash: sha512.New384, name: "sha384"}, nil
	case "sha512":
		return &HMACSigner{hash: sha512.New, name: "sha512"}, nil
	default:
		return nil, fmt.Errorf("webhook: unsupported algorithm %q", algorithm)
	}
}

func HMACSHA256() *HMACSigner {
	return &HMACSigner{hash: sha256.New, name: "sha256"}
}

func (s *HMACSigner) Sign(secret []byte, payload []byte) string {
	mac := hmac.New(s.hash, secret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *HMACSigner) Algorithm() string { return s.name }

type Config struct {
	Client     *fh.Client
	Store      Store
	Signer     Signer
	MaxRetries int
	Backoff    time.Duration
	WorkerPool int
	EventTTL   time.Duration
}

func (c *Config) normalize() {
	if c.Signer == nil {
		c.Signer = HMACSHA256()
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.Backoff <= 0 {
		c.Backoff = time.Second
	}
	if c.WorkerPool <= 0 {
		c.WorkerPool = 4
	}
	if c.EventTTL <= 0 {
		c.EventTTL = 7 * 24 * time.Hour
	}
}

type Store interface {
	CreateSubscription(ctx context.Context, sub *Subscription) error
	GetSubscription(ctx context.Context, id string) (*Subscription, error)
	ListSubscriptions(ctx context.Context, offset, limit int) ([]*Subscription, error)
	UpdateSubscription(ctx context.Context, sub *Subscription) error
	DeleteSubscription(ctx context.Context, id string) error

	CreateEvent(ctx context.Context, evt *Event) error
	GetEvent(ctx context.Context, id string) (*Event, error)
	ListEvents(ctx context.Context, offset, limit int) ([]*Event, error)

	CreateDelivery(ctx context.Context, d *Delivery) error
	GetDelivery(ctx context.Context, id string) (*Delivery, error)
	UpdateDelivery(ctx context.Context, d *Delivery) error
	ListDeliveries(ctx context.Context, subscriptionID string, offset, limit int) ([]*Delivery, error)
	ListDeliveriesByEvent(ctx context.Context, eventID string) ([]*Delivery, error)

	FindSubscriptionsByEvent(ctx context.Context, eventType string) ([]*Subscription, error)
	GetPendingDeliveries(ctx context.Context, limit int) ([]*Delivery, error)

	Close() error
}

func newID() string {
	return fmt.Sprintf("wh_%x", time.Now().UnixNano())
}

func marshalRaw(v any) (json.RawMessage, error) {
	switch data := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return append([]byte(nil), data...), nil
	case json.RawMessage:
		return append([]byte(nil), data...), nil
	default:
		return json.Marshal(data)
	}
}
