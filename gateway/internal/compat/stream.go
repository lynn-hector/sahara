package compat

import (
	"context"
	"log/slog"
	"time"

	eventv1 "github.com/sahara-ai/sahara/gen/sahara/event/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// RuntimeEvent is a parsed event from Redis Streams, ready for the handler to consume.
type RuntimeEvent struct {
	Type    eventv1.EventType
	Proto   *eventv1.AgentEvent
}

// EventReader subscribes to a session's Redis Stream and delivers parsed events
// through a Go channel. Used by the HTTP handler to bridge Redis Streams → HTTP response.
type EventReader struct {
	rdb        *redis.Client
	streamKey  string
	events     chan RuntimeEvent
	done       chan struct{}
}

// NewEventReader creates a reader for the given session's event stream.
func NewEventReader(rdb *redis.Client, sessionKey string) *EventReader {
	return &EventReader{
		rdb:       rdb,
		streamKey: "events:" + sessionKey,
		events:    make(chan RuntimeEvent, 64),
		done:      make(chan struct{}),
	}
}

// Events returns the channel of parsed events.
func (r *EventReader) Events() <-chan RuntimeEvent {
	return r.events
}

// Run polls the Redis Stream and sends events to the channel.
// Blocks until ctx is cancelled or Stop is called.
func (r *EventReader) Run(ctx context.Context) {
	defer close(r.events)

	lastID := "0"
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		default:
		}

		results, err := r.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{r.streamKey, lastID},
			Count:   50,
			Block:   500 * time.Millisecond,
		}).Result()

		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			slog.Error("compat event reader xread error", "err", err, "stream", r.streamKey)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for _, stream := range results {
			for _, msg := range stream.Messages {
				lastID = msg.ID

				dataStr, ok := msg.Values["data"]
				if !ok {
					continue
				}

				var raw []byte
				switch v := dataStr.(type) {
				case string:
					raw = []byte(v)
				case []byte:
					raw = v
				default:
					continue
				}

				var event eventv1.AgentEvent
				if err := proto.Unmarshal(raw, &event); err != nil {
					slog.Error("compat event unmarshal error", "err", err)
					continue
				}

				select {
				case r.events <- RuntimeEvent{Type: event.GetType(), Proto: &event}:
				case <-ctx.Done():
					return
				case <-r.done:
					return
				}
			}
		}
	}
}

// Stop signals the reader to stop polling.
func (r *EventReader) Stop() {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}
