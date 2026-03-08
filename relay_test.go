// MIT License
//
// Copyright (c) 2026 sparetimecoders

package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	outbox "github.com/sparetimecoders/go-messaging-outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProcessor struct {
	records []outbox.Record
	calls   atomic.Int32
}

func (m *mockProcessor) Process(_ context.Context, batchSize int, fn func([]outbox.Record) ([]string, error)) (int, error) {
	m.calls.Add(1)
	if len(m.records) == 0 {
		return 0, nil
	}

	batch := m.records
	if len(batch) > batchSize {
		batch = batch[:batchSize]
	}

	publishedIDs, err := fn(batch)
	if err != nil {
		return 0, err
	}

	// Remove published records
	remaining := make([]outbox.Record, 0)
	published := make(map[string]bool)
	for _, id := range publishedIDs {
		published[id] = true
	}
	for _, r := range m.records {
		if !published[r.ID] {
			remaining = append(remaining, r)
		}
	}
	m.records = remaining

	return len(publishedIDs), nil
}

type mockRawPublisher struct {
	published []outbox.Record
}

func (m *mockRawPublisher) PublishRaw(_ context.Context, routingKey string, payload []byte, headers map[string]string) error {
	m.published = append(m.published, outbox.Record{
		RoutingKey: routingKey,
		Payload:    payload,
		Headers:    headers,
	})
	return nil
}

func TestRelay_ProcessesBatch(t *testing.T) {
	store := &mockProcessor{
		records: []outbox.Record{
			{ID: "1", RoutingKey: "user.created", Payload: []byte(`{"id":1}`), Headers: map[string]string{"ce-id": "1"}},
			{ID: "2", RoutingKey: "user.updated", Payload: []byte(`{"id":2}`), Headers: map[string]string{"ce-id": "2"}},
		},
	}
	publisher := &mockRawPublisher{}

	relay := outbox.NewRelay(store, publisher, outbox.RelayConfig{
		PollInterval: 50 * time.Millisecond,
		BatchSize:    100,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = relay.Start(ctx)

	require.Len(t, publisher.published, 2)
	assert.Equal(t, "user.created", publisher.published[0].RoutingKey)
	assert.Equal(t, "user.updated", publisher.published[1].RoutingKey)
	assert.Empty(t, store.records)
}
