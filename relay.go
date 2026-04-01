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

package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RelayConfig configures the relay polling behavior.
type RelayConfig struct {
	// PollInterval is the delay between poll cycles when the previous batch
	// was not full. Defaults to 1 second.
	PollInterval time.Duration

	// BatchSize is the maximum number of events fetched per poll cycle.
	// Defaults to 100.
	BatchSize int
}

func (c RelayConfig) withDefaults() RelayConfig {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	return c
}

// Relay polls the outbox store for unpublished events and publishes them
// to the broker via a RawPublisher. After successful publication, records
// are deleted from the outbox within the same transaction.
type Relay struct {
	store     Processor
	publisher RawPublisher
	config    RelayConfig
	logger    *slog.Logger
}

// NewRelay creates a new Relay.
func NewRelay(store Processor, publisher RawPublisher, config RelayConfig, logger *slog.Logger) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		store:     store,
		publisher: publisher,
		config:    config.withDefaults(),
		logger:    logger,
	}
}

// Start runs the relay polling loop until ctx is cancelled.
func (r *Relay) Start(ctx context.Context) error {
	r.logger.Info("outbox relay started",
		"poll_interval", r.config.PollInterval,
		"batch_size", r.config.BatchSize,
	)
	defer r.logger.Info("outbox relay stopped")

	for {
		batchWasFull := r.processBatch(ctx)

		delay := r.config.PollInterval
		if batchWasFull {
			delay = 0
		}

		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
	}
}

func (r *Relay) processBatch(ctx context.Context) bool {
	published, err := r.store.Process(ctx, r.config.BatchSize, func(records []Record) ([]string, error) {
		var publishedIDs []string
		for _, record := range records {
			if ctx.Err() != nil {
				break
			}

			startTime := time.Now()
			if err := r.publisher.PublishRaw(ctx, record.RoutingKey, record.Payload, record.Headers); err != nil {
				elapsed := time.Since(startTime).Seconds()
				recordRelayPublished(record.RoutingKey, "error", elapsed)
				return nil, fmt.Errorf("outbox: publish event %s: %w", record.ID, err)
			}

			elapsed := time.Since(startTime).Seconds()
			recordRelayPublished(record.RoutingKey, "ok", elapsed)
			publishedIDs = append(publishedIDs, record.ID)
		}
		recordRelayBatchSize(len(publishedIDs))
		return publishedIDs, nil
	})
	if err != nil {
		r.logger.Error("outbox: process batch failed", "error", err)
		return false
	}

	return published >= r.config.BatchSize
}
