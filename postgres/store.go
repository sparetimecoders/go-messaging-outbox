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

// Package postgres provides a PostgreSQL implementation of outbox.Store.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	outbox "github.com/sparetimecoders/go-messaging-outbox"
)

//go:embed migrations/001_create_outbox.sql
var migrationSQL string

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithSkipMigrations disables automatic migration on NewStore.
func WithSkipMigrations() StoreOption {
	return func(s *Store) {
		s.skipMigrations = true
	}
}

// Store implements outbox.Store using PostgreSQL with pgx.
type Store struct {
	pool           *pgxpool.Pool
	skipMigrations bool
}

// NewStore creates a PostgreSQL outbox store. By default it runs the embedded
// migration to create the outbox table. Use WithSkipMigrations to disable this.
func NewStore(ctx context.Context, pool *pgxpool.Pool, opts ...StoreOption) (*Store, error) {
	s := &Store{pool: pool}
	for _, opt := range opts {
		opt(s)
	}
	if !s.skipMigrations {
		if _, err := pool.Exec(ctx, migrationSQL); err != nil {
			return nil, fmt.Errorf("outbox: run migration: %w", err)
		}
	}
	return s, nil
}

// Insert adds a new outbox record. For transactional writes alongside business
// data, use InsertTx to get a transaction-scoped inserter.
func (s *Store) Insert(ctx context.Context, record outbox.Record) error {
	return insertRecord(ctx, poolExec(s.pool), record)
}

// InsertTx returns a transaction-scoped inserter that writes to the given pgx.Tx.
// Use this to insert outbox records within the same transaction as business data.
func (s *Store) InsertTx(tx pgx.Tx) outbox.Inserter {
	return &txInserter{tx: tx}
}

// Process implements outbox.Store. It runs within a single transaction:
// acquire leader lock → fetch unpublished records → call fn → delete published → commit.
func (s *Store) Process(ctx context.Context, batchSize int, fn func([]outbox.Record) ([]string, error)) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	acquired, err := tryAdvisoryLock(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !acquired {
		return 0, nil
	}

	records, err := fetchAndLock(ctx, tx, batchSize)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	publishedIDs, err := fn(records)
	if err != nil {
		return 0, fmt.Errorf("outbox: process fn: %w", err)
	}

	if len(publishedIDs) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM messaging_outbox WHERE id = ANY($1)`, publishedIDs); err != nil {
			return 0, fmt.Errorf("outbox: delete published: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox: commit: %w", err)
	}

	return len(publishedIDs), nil
}

func insertRecord(ctx context.Context, execFn func(ctx context.Context, sql string, arguments ...any) error, record outbox.Record) error {
	headersJSON, err := json.Marshal(record.Headers)
	if err != nil {
		return fmt.Errorf("outbox: marshal headers: %w", err)
	}

	err = execFn(ctx,
		`INSERT INTO messaging_outbox (id, event_type, routing_key, payload, headers, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		record.ID, record.EventType, record.RoutingKey, record.Payload, headersJSON, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

func poolExec(pool *pgxpool.Pool) func(ctx context.Context, sql string, arguments ...any) error {
	return func(ctx context.Context, sql string, arguments ...any) error {
		_, err := pool.Exec(ctx, sql, arguments...)
		return err
	}
}

func txExec(tx pgx.Tx) func(ctx context.Context, sql string, arguments ...any) error {
	return func(ctx context.Context, sql string, arguments ...any) error {
		_, err := tx.Exec(ctx, sql, arguments...)
		return err
	}
}

func fetchAndLock(ctx context.Context, tx pgx.Tx, limit int) ([]outbox.Record, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, event_type, routing_key, payload, headers, created_at
		 FROM messaging_outbox
		 ORDER BY created_at ASC, id ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetch: %w", err)
	}
	defer rows.Close()

	var records []outbox.Record
	for rows.Next() {
		var r outbox.Record
		var headersJSON []byte
		if err := rows.Scan(&r.ID, &r.EventType, &r.RoutingKey, &r.Payload, &headersJSON, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if err := json.Unmarshal(headersJSON, &r.Headers); err != nil {
			return nil, fmt.Errorf("outbox: unmarshal headers: %w", err)
		}
		records = append(records, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: rows: %w", err)
	}

	return records, nil
}

func tryAdvisoryLock(ctx context.Context, tx pgx.Tx) (bool, error) {
	var acquired bool
	err := tx.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock(hashtext('messaging_outbox'))`,
	).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("outbox: advisory lock: %w", err)
	}
	return acquired, nil
}

// txInserter wraps a pgx.Tx and implements outbox.Inserter for use within a
// caller-managed transaction.
type txInserter struct {
	tx pgx.Tx
}

func (s *txInserter) Insert(ctx context.Context, record outbox.Record) error {
	return insertRecord(ctx, txExec(s.tx), record)
}
