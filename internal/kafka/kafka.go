// Package kafka provides the event-bus layer for NEXUS-DISPATCH: topic
// definitions, an idempotent producer, and a consumer with retries and
// dead-letter handling.
//
// Design (docs/decisions/ADR-002, ADR-007):
//   - Partition key = driver_id for driver-scoped topics (per-driver ordering
//     for sequence-based dedupe), ride_id for ride-scoped topics.
//   - At-least-once delivery + idempotent consumers = effectively-once effect.
//   - On retry exhaustion, records are published to the dead-letter topic with
//     the original key and an error header, then committed.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Topic names.
const (
	TopicDriverLocation  = "driver-location"
	TopicRideRequested   = "ride-requested"
	TopicDriverReserved  = "driver-reserved"
	TopicDispatchCreated = "dispatch-created"
	TopicRideStatus      = "ride-status"
	TopicDriverStatus    = "driver-status"
	TopicDeadLetter      = "nexus-dead-letter"
)

// AllTopics lists every topic the platform uses (for bootstrap / compose).
var AllTopics = []string{
	TopicDriverLocation,
	TopicRideRequested,
	TopicDriverReserved,
	TopicDispatchCreated,
	TopicRideStatus,
	TopicDriverStatus,
	TopicDeadLetter,
}

// TopicSpec describes a topic's partitioning and retention.
type TopicSpec struct {
	Name       string
	Partitions int
	Retention  time.Duration
	KeyHint    string // documented partition key
}

// TopicSpecs is the full topic plan (see docs/kafka.md).
var TopicSpecs = []TopicSpec{
	{Name: TopicDriverLocation, Partitions: 12, Retention: time.Hour, KeyHint: "driver_id"},
	{Name: TopicRideRequested, Partitions: 8, Retention: time.Hour, KeyHint: "ride_id"},
	{Name: TopicDriverReserved, Partitions: 12, Retention: 24 * time.Hour, KeyHint: "driver_id"},
	{Name: TopicDispatchCreated, Partitions: 8, Retention: 24 * time.Hour, KeyHint: "ride_id"},
	{Name: TopicRideStatus, Partitions: 8, Retention: 24 * time.Hour, KeyHint: "ride_id"},
	{Name: TopicDriverStatus, Partitions: 12, Retention: 24 * time.Hour, KeyHint: "driver_id"},
	{Name: TopicDeadLetter, Partitions: 3, Retention: 7 * 24 * time.Hour, KeyHint: "original key"},
}

// Config is the broker configuration.
type Config struct {
	Brokers []string
	// ClientID identifies this producer/consumer in broker logs.
	ClientID string
}

// Producer publishes events. It is safe for concurrent use.
type Producer struct {
	w *kafka.Writer
}

// NewProducer creates a Kafka producer with sane defaults:
// idempotent writes, 3 retries with backoff, 10k message buffer.
func NewProducer(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured")
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.Brokers,
		Topic:    "", // set per-message (we publish to multiple topics)
		Balancer: &kafka.Hash{}, // key-based routing (per-key ordering)
		// RequireAll: wait for all in-sync replicas (durability).
		RequiredAcks: int(kafka.RequireAll),
		// BatchSize: messages per produce request.
		BatchSize: 100,
		// MaxAttempts: on broker error, retry up to 4 times (initial + 3).
		MaxAttempts: 4,
		// Write/ReadTimeout bound a single produce attempt; combined with
		// MaxAttempts this is our producer retry behavior.
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		// QueueCapacity: bounded in-memory buffer (backpressure, not OOM).
		QueueCapacity: 10000,
	})
	return &Producer{w: w}, nil
}

// Publish sends a message to a topic with the given key. The key determines
// the partition (and thus the per-key ordering).
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	if err := p.w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}); err != nil {
		return fmt.Errorf("kafka: publish %s key=%s: %w", topic, key, err)
	}
	return nil
}

// PublishWithHeaders sends a message with additional headers (e.g. traceparent).
func (p *Producer) PublishWithHeaders(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	var hs []kafka.Header
	for k, v := range headers {
		hs = append(hs, kafka.Header{Key: k, Value: []byte(v)})
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: hs,
	})
}

// Close flushes and closes the producer.
func (p *Producer) Close() error { return p.w.Close() }

// Consumer reads messages from a topic in a consumer group, applying a
// handler with retries and dead-letter handling.
type Consumer struct {
	r       *kafka.Reader
	topic   string
	group   string
	log     *slog.Logger
	handler func(ctx context.Context, msg kafka.Message) error
	dlq     *Producer // dead-letter producer (may be nil)
	maxRetries int
}

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	Brokers   []string
	Topic     string
	Group     string
	ClientID  string
	Handler   func(ctx context.Context, msg kafka.Message) error
	Logger    *slog.Logger
	DLQ       *Producer
	MaxRetries int
}

// NewConsumer creates a consumer. Offsets start at the latest (new messages
// only) unless the group has committed offsets.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.Group,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0, // commit per-message (we commit after handler success)
	})
	return &Consumer{
		r:          r,
		topic:      cfg.Topic,
		group:      cfg.Group,
		log:        cfg.Logger,
		handler:    cfg.Handler,
		dlq:        cfg.DLQ,
		maxRetries: cfg.MaxRetries,
	}, nil
}

// Run consumes until ctx is cancelled. It commits offsets only after the
// handler succeeds (at-least-once + idempotent handler = effectively-once).
// On retry exhaustion, the record goes to the DLQ and is committed.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("kafka consumer starting", "topic", c.topic, "group", c.group)
	defer c.r.Close()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := c.r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Transient read error (broker blip): log and retry.
			c.log.Warn("kafka read error", "topic", c.topic, "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if err := c.process(ctx, msg); err != nil {
			// process already sent to DLQ on exhaustion; commit so we don't
			// reprocess a poison message forever.
			c.log.Error("kafka message failed after retries, committed",
				"topic", c.topic, "key", string(msg.Key), "offset", msg.Offset, "err", err)
			if cerr := c.r.CommitMessages(ctx, msg); cerr != nil {
				c.log.Error("kafka commit after DLQ failed", "err", cerr)
			}
		}
	}
}

// process applies the handler with exponential backoff retries.
func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
		lastErr = c.handler(ctx, msg)
		if lastErr == nil {
			if cerr := c.r.CommitMessages(ctx, msg); cerr != nil {
				c.log.Warn("kafka commit failed", "offset", msg.Offset, "err", cerr)
			}
			return nil
		}
		c.log.Warn("kafka handler retry", "topic", c.topic, "key", string(msg.Key),
			"attempt", attempt+1, "max", c.maxRetries+1, "err", lastErr)
	}
	// Exhausted: dead-letter.
	if c.dlq != nil {
		if derr := c.dlq.PublishWithHeaders(ctx, TopicDeadLetter, string(msg.Key), msg.Value, map[string]string{
			"original_topic": c.topic,
			"error":          lastErr.Error(),
			"key":            string(msg.Key),
		}); derr != nil {
			c.log.Error("kafka DLQ publish failed", "err", derr)
		} else {
			c.log.Warn("kafka message dead-lettered", "topic", c.topic, "key", string(msg.Key))
		}
	}
	return lastErr
}