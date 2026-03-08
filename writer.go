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
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sparetimecoders/messaging/specification/spec"
)

// Writer writes outbox records within a caller-managed transaction.
// It serializes the message and generates CloudEvents headers.
type Writer struct {
	serviceName string
}

// NewWriter creates a new outbox Writer that tags events with serviceName
// as the CloudEvents source.
func NewWriter(serviceName string) *Writer {
	return &Writer{serviceName: serviceName}
}

// Write serializes msg to JSON and inserts an outbox record via inserter.
// The caller MUST call this within the same database transaction as
// business writes to guarantee atomicity.
func (w *Writer) Write(ctx context.Context, inserter Inserter, routingKey string, msg any, headers ...map[string]string) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}

	now := time.Now().UTC()
	ceHeaders := map[string]string{
		spec.CESpecVersion:     spec.CESpecVersionValue,
		spec.CEType:            routingKey,
		spec.CESource:          w.serviceName,
		spec.CEID:              uuid.New().String(),
		spec.CETime:            now.Format(time.RFC3339),
		spec.CEDataContentType: "application/json",
	}
	for _, extra := range headers {
		for k, v := range extra {
			ceHeaders[k] = v
		}
	}

	record := Record{
		ID:         ceHeaders[spec.CEID],
		EventType:  routingKey,
		RoutingKey: routingKey,
		Payload:    payload,
		Headers:    ceHeaders,
		CreatedAt:  now,
	}

	if err := inserter.Insert(ctx, record); err != nil {
		return err
	}
	recordEventWritten(routingKey)
	return nil
}
