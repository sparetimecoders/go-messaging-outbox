# go-messaging-outbox

<p align="center">
  <strong>Transactional outbox pattern for gomessaging -- reliable event publishing with PostgreSQL, CloudEvents, and Prometheus.</strong>
</p>

<p align="center">
  <a href="https://github.com/sparetimecoders/go-messaging-outbox/actions"><img alt="CI" src="https://github.com/sparetimecoders/go-messaging-outbox/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pkg.go.dev/github.com/sparetimecoders/go-messaging-outbox"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/sparetimecoders/go-messaging-outbox.svg"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
</p>

---

This package implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for the [gomessaging](https://github.com/sparetimecoders/messaging) framework. Events are written to a database table within the same transaction as business data, then asynchronously relayed to a message broker by a background worker. This guarantees at-least-once delivery without distributed transactions.

Transport adapters for the relay are provided by:
- [go-messaging-nats](https://github.com/sparetimecoders/go-messaging-nats) -- `nats.NewOutboxRawPublisher(publisher)`
- [go-messaging-amqp](https://github.com/sparetimecoders/go-messaging-amqp) -- `amqp.NewOutboxRawPublisher(publisher)`

## Installation

```sh
go get github.com/sparetimecoders/go-messaging-outbox
```

Requires Go 1.26+.

## How It Works

```
App Transaction                 Relay (background)              Broker
┌─────────────┐                ┌──────────────────┐           ┌────────┐
│ BEGIN        │                │ BEGIN             │           │        │
│ INSERT order │                │ Advisory lock     │           │        │
│ INSERT outbox├───────────────>│ SELECT FOR UPDATE │           │        │
│ COMMIT       │                │ PublishRaw ───────┼──────────>│ NATS / │
└─────────────┘                │ DELETE outbox     │           │ AMQP   │
                               │ COMMIT            │           │        │
                               └──────────────────┘           └────────┘
```

1. **Write path**: The application inserts an outbox record in the same transaction as business data using `Writer.Write()`.
2. **Relay**: A background `Relay` polls the outbox table, publishes each record via a `RawPublisher`, and deletes it -- all within a single transaction.
3. **Leader election**: A PostgreSQL advisory lock (`pg_try_advisory_xact_lock`) ensures only one relay instance processes at a time.
4. **Concurrency safety**: `SELECT ... FOR UPDATE SKIP LOCKED` prevents duplicate delivery across relay instances.
5. **Hard delete**: Published records are deleted immediately (no `published_at` column).

## Quick Start

```go
package main

import (
    "context"
    "log"
    "log/slog"
    "os/signal"
    "syscall"

    "github.com/jackc/pgx/v5/pgxpool"
    outbox "github.com/sparetimecoders/go-messaging-outbox"
    "github.com/sparetimecoders/go-messaging-outbox/postgres"
)

type OrderCreated struct {
    OrderID string `json:"order_id"`
    Amount  int    `json:"amount"`
}

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // Create the outbox store (runs migration by default)
    store, err := postgres.NewStore(ctx, pool)
    if err != nil {
        log.Fatal(err)
    }

    // Write an event within a business transaction
    writer := outbox.NewWriter("order-service")

    tx, err := pool.Begin(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Insert business data and outbox record in the same transaction
    _, _ = tx.Exec(ctx, "INSERT INTO orders (id, amount) VALUES ($1, $2)", "abc-123", 42)
    if err := writer.Write(ctx, store.InsertTx(tx), "Order.Created", OrderCreated{
        OrderID: "abc-123",
        Amount:  42,
    }); err != nil {
        _ = tx.Rollback(ctx)
        log.Fatal(err)
    }
    if err := tx.Commit(ctx); err != nil {
        log.Fatal(err)
    }

    // Start the relay (connects to your broker via a RawPublisher adapter)
    // relay := outbox.NewRelay(store, nats.NewOutboxRawPublisher(pub), outbox.RelayConfig{}, slog.Default())
    // relay.Start(ctx)
    _ = slog.Default() // placeholder
}
```

## Writing Events

Use `Writer` to create outbox records with [CloudEvents 1.0](https://cloudevents.io/) headers:

```go
writer := outbox.NewWriter("order-service")

tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

// Business write
tx.Exec(ctx, "INSERT INTO orders ...")

// Outbox write (same transaction)
err := writer.Write(ctx, store.InsertTx(tx), "Order.Created", OrderCreated{
    OrderID: "abc-123",
    Amount:  42,
})

tx.Commit(ctx)
```

### CloudEvents Headers

Every record includes these headers automatically:

| Header | Value |
|--------|-------|
| `ce-specversion` | `1.0` |
| `ce-type` | Routing key |
| `ce-source` | Service name |
| `ce-id` | UUID |
| `ce-time` | RFC 3339 timestamp |
| `ce-datacontenttype` | `application/json` |

Add custom headers as an optional variadic argument:

```go
writer.Write(ctx, inserter, "Order.Created", payload, map[string]string{
    "ce-subject": "orders/abc-123",
})
```

## Running the Relay

The relay polls the outbox table and publishes events to a message broker:

```go
relay := outbox.NewRelay(store, rawPublisher, outbox.RelayConfig{
    PollInterval: 500 * time.Millisecond, // default: 1s
    BatchSize:    200,                     // default: 100
}, slog.Default())

// Blocks until ctx is cancelled
err := relay.Start(ctx)
```

### Adaptive Polling

When a batch is full (published count >= batch size), the relay polls again immediately without waiting. When the batch is partial or empty, it waits for `PollInterval` before the next poll.

### Transport Adapters

The relay publishes via a `RawPublisher` interface. Transport-specific adapters are provided by the transport packages:

**NATS:**

```go
import nats "github.com/sparetimecoders/go-messaging-nats"

pub := nats.NewPublisher()
conn.Start(ctx, nats.EventStreamPublisher(pub))

rawPub := nats.NewOutboxRawPublisher(pub)
relay := outbox.NewRelay(store, rawPub, outbox.RelayConfig{}, logger)
```

**AMQP:**

```go
import "github.com/sparetimecoders/go-messaging-amqp"

pub := amqp.NewPublisher()
conn.Start(ctx, amqp.EventStreamPublisher(pub))

rawPub := amqp.NewOutboxRawPublisher(pub)
relay := outbox.NewRelay(store, rawPub, outbox.RelayConfig{}, logger)
```

## PostgreSQL Store

The `postgres` sub-package provides a production-ready store using [pgx](https://github.com/jackc/pgx).

```go
import "github.com/sparetimecoders/go-messaging-outbox/postgres"

store, err := postgres.NewStore(ctx, pool)
```

### Migrations

By default, `NewStore` runs an embedded migration that creates the `messaging_outbox` table and index. To manage migrations externally:

```go
store, err := postgres.NewStore(ctx, pool, postgres.WithSkipMigrations())
```

### Schema

```sql
CREATE TABLE IF NOT EXISTS messaging_outbox (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type  TEXT        NOT NULL,
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messaging_outbox_created_at
    ON messaging_outbox (created_at, id);
```

### Interfaces

The store exposes two separate interfaces to prevent misuse:

| Interface | Method | Purpose |
|-----------|--------|---------|
| `outbox.Inserter` | `Insert(ctx, record)` | Write path -- insert within a caller-managed transaction |
| `outbox.Processor` | `Process(ctx, batchSize, fn)` | Read path -- relay fetch-publish-delete cycle |

Use `store.InsertTx(tx)` to get a transaction-scoped `Inserter`. The `Store` itself implements `Processor` for use with the relay.

## Observability

### Metrics

Register Prometheus metrics once at startup:

```go
import "github.com/prometheus/client_golang/prometheus"

err := outbox.InitMetrics(prometheus.DefaultRegisterer)
```

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `outbox_events_written` | counter | `routing_key` | Events written to the outbox |
| `outbox_relay_published` | counter | `routing_key`, `result` | Events published by the relay (`ok` / `error`) |
| `outbox_relay_batch_size` | histogram | -- | Events processed per relay poll cycle |
| `outbox_relay_publish_duration_ms` | histogram | `routing_key`, `result` | Time to publish one event (ms) |

## Interfaces

### Core Types

```go
// Record represents a single outbox entry.
type Record struct {
    ID         string
    EventType  string
    RoutingKey string
    Payload    []byte
    Headers    map[string]string
    CreatedAt  time.Time
}

// Inserter writes outbox records within a caller-managed transaction.
type Inserter interface {
    Insert(ctx context.Context, record Record) error
}

// Processor runs the relay read-publish-delete cycle.
type Processor interface {
    Process(ctx context.Context, batchSize int,
        fn func(records []Record) (publishedIDs []string, err error)) (int, error)
}

// RawPublisher publishes a pre-serialized message to the broker.
type RawPublisher interface {
    PublishRaw(ctx context.Context, routingKey string, payload []byte,
        headers map[string]string) error
}
```

Implement `RawPublisher` to integrate with any message broker. Implement `Inserter` and `Processor` to use a different database backend.

## License

MIT
