// MIT License
//
// Copyright (c) 2026 sparetimecoders

package outbox_test

import (
	"context"
	"testing"

	outbox "github.com/sparetimecoders/go-messaging-outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockInserter struct {
	inserted []outbox.Record
}

func (m *mockInserter) Insert(_ context.Context, record outbox.Record) error {
	m.inserted = append(m.inserted, record)
	return nil
}

func TestWriter_Write(t *testing.T) {
	store := &mockInserter{}
	writer := outbox.NewWriter("test-service")

	type payload struct {
		Name string `json:"name"`
	}

	err := writer.Write(context.Background(), store, "user.created", payload{Name: "alice"})
	require.NoError(t, err)

	require.Len(t, store.inserted, 1)
	record := store.inserted[0]
	assert.Equal(t, "user.created", record.RoutingKey)
	assert.JSONEq(t, `{"name":"alice"}`, string(record.Payload))
	assert.NotEmpty(t, record.ID)
	assert.Equal(t, "test-service", record.Headers["ce-source"])
	assert.Equal(t, "1.0", record.Headers["ce-specversion"])
	assert.Equal(t, "user.created", record.Headers["ce-type"])
	assert.NotEmpty(t, record.Headers["ce-id"])
	assert.NotEmpty(t, record.Headers["ce-time"])
	assert.Equal(t, "application/json", record.Headers["ce-datacontenttype"])
}

func TestWriter_Write_WithExtraHeaders(t *testing.T) {
	store := &mockInserter{}
	writer := outbox.NewWriter("test-service")

	extra := map[string]string{"ce-subject": "user/123"}
	err := writer.Write(context.Background(), store, "user.created", "payload", extra)
	require.NoError(t, err)

	record := store.inserted[0]
	assert.Equal(t, "user/123", record.Headers["ce-subject"])
}
