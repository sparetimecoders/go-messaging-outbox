// MIT License
//
// Copyright (c) 2026 sparetimecoders
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package outbox implements the transactional outbox pattern for reliable
// event publishing. Events are written to a database table within the same
// transaction as business data, then asynchronously relayed to a message
// broker by a background worker.
package outbox

import (
	"context"
	"time"
)

// Record represents a single outbox entry stored in the database.
type Record struct {
	ID         string
	EventType  string
	RoutingKey string
	Payload    []byte
	Headers    map[string]string
	CreatedAt  time.Time
}

// Inserter writes outbox records. Use this interface for the write path
// (within a caller-managed transaction).
type Inserter interface {
	// Insert adds a new outbox record. The caller is responsible for
	// executing this within the same database transaction as business writes.
	Insert(ctx context.Context, record Record) error
}

// Processor runs the relay read-publish-delete cycle.
type Processor interface {
	// Process runs fn within a transaction that:
	// 1. Acquires a leader lock (advisory lock or equivalent)
	// 2. Fetches up to batchSize unpublished records with FOR UPDATE SKIP LOCKED
	// 3. Passes them to fn for processing
	// 4. Deletes successfully processed records
	// 5. Commits the transaction
	//
	// fn receives the locked records and returns the IDs of records that were
	// successfully published. Only those IDs are deleted before commit.
	Process(ctx context.Context, batchSize int, fn func(records []Record) (publishedIDs []string, err error)) (int, error)
}

// RawPublisher publishes a pre-serialized message to the broker.
// Transport implementations (NATS, AMQP) provide adapters that satisfy this.
type RawPublisher interface {
	PublishRaw(ctx context.Context, routingKey string, payload []byte, headers map[string]string) error
}
