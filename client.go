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
	"strconv"
	"time"

	"github.com/oarkflow/fh"
)

var (
	ErrClientWebhookFailed = errors.New("webhook: client delivery failed")
	ErrNilClient           = errors.New("webhook: nil client")
)

type ClientOption struct {
	HeaderPrefix string
	Signer       *HMACSigner
	Timeout      time.Duration
	Retries      int
}

type WebhookClient struct {
	client *fh.Client
	opts   ClientOption
}

func NewWebhookClient(cl *fh.Client, opts ...ClientOption) *WebhookClient {
	var o ClientOption
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Signer == nil {
		o.Signer = HMACSHA256()
	}
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	if o.Retries <= 0 {
		o.Retries = 3
	}
	if o.HeaderPrefix == "" {
		o.HeaderPrefix = "X-Webhook"
	}
	return &WebhookClient{client: cl, opts: o}
}

type SendConfig struct {
	URL     string
	Secret  string
	Headers map[string]string
	Retries int
	Timeout time.Duration
}

func (c *WebhookClient) Send(ctx context.Context, evt Event, cfg SendConfig) (*fh.Response, error) {
	if c.client == nil {
		return nil, ErrNilClient
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("webhook: marshal event: %w", err)
	}
	req := c.client.R().Bytes(body, "application/json")
	req.Header("X-Webhook-ID", evt.ID).
		Header("X-Webhook-Event", evt.Type).
		Header("X-Webhook-Timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	if cfg.Secret != "" {
		sig := signPayload([]byte(cfg.Secret), body, c.opts.Signer)
		req.Header("X-Webhook-Signature", sig).
			Header("X-Webhook-Signature-Algorithm", c.opts.Signer.Algorithm())
	}
	for k, v := range cfg.Headers {
		req.Header(k, v)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = c.opts.Timeout
	}
	req.Timeout(timeout)
	retries := cfg.Retries
	if retries <= 0 {
		retries = c.opts.Retries
	}
	if retries > 0 {
		var statuses []int
		for code := 429; code <= 429; code++ {
			statuses = append(statuses, code)
		}
		for code := 500; code <= 504; code++ {
			statuses = append(statuses, code)
		}
		statusMap := make(map[int]bool)
		for _, code := range statuses {
			statusMap[code] = true
		}
		req.Retry(fh.RetryPolicy{
			MaxAttempts:   retries,
			RetryStatuses: statusMap,
			RetryMethods:  map[string]bool{"POST": true, "PUT": true, "PATCH": true},
		})
	}
	res, err := req.Post(ctx, cfg.URL)
	if err != nil {
		return res, fmt.Errorf("%w: %w", ErrClientWebhookFailed, err)
	}
	if res.StatusCode() >= 400 {
		return res, fmt.Errorf("%w: status %d", ErrClientWebhookFailed, res.StatusCode())
	}
	return res, nil
}

func (c *WebhookClient) SendJSON(ctx context.Context, url, eventType string, data any, secret string) (*fh.Response, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	evt := Event{
		ID:        newID(),
		Type:      eventType,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}
	return c.Send(ctx, evt, SendConfig{URL: url, Secret: secret})
}

func signPayload(secret, payload []byte, s *HMACSigner) string {
	return s.Sign(secret, payload)
}

func VerifyPayload(secret, payload []byte, signature string, allowedAlgos ...string) error {
	for _, algo := range allowedAlgos {
		signer, err := NewHMACSigner(algo)
		if err != nil {
			continue
		}
		expected := signer.Sign(secret, payload)
		if hmac.Equal([]byte(expected), []byte(signature)) {
			return nil
		}
	}
	if len(allowedAlgos) == 0 {
		signer := HMACSHA256()
		expected := signer.Sign(secret, payload)
		if hmac.Equal([]byte(expected), []byte(signature)) {
			return nil
		}
	}
	return errors.New("webhook: signature verification failed")
}

func VerifySHA256(secret, payload []byte, signature string) bool {
	return verifyWithAlgo(secret, payload, signature, sha256.New)
}

func VerifySHA384(secret, payload []byte, signature string) bool {
	return verifyWithAlgo(secret, payload, signature, sha512.New384)
}

func VerifySHA512(secret, payload []byte, signature string) bool {
	return verifyWithAlgo(secret, payload, signature, sha512.New)
}

func verifyWithAlgo(secret, payload []byte, signature string, h func() hash.Hash) bool {
	mac := hmac.New(h, secret)
	mac.Write(payload)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}
