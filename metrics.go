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
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricRoutingKey = "routing_key"
	metricResult     = "result"
)

var (
	outboxWrittenCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "outbox_events_written",
			Help: "Count of events written to the outbox table",
		}, []string{metricRoutingKey},
	)

	outboxRelayPublishedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "outbox_relay_published",
			Help: "Count of events published by the relay",
		}, []string{metricRoutingKey, metricResult},
	)

	outboxRelayBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "outbox_relay_batch_size",
			Help:    "Number of events processed per relay poll cycle",
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100},
		},
	)

	outboxRelayPublishDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "outbox_relay_publish_duration_ms",
			Help:    "Milliseconds taken to publish one event from the relay",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}, []string{metricRoutingKey, metricResult},
	)
)

// InitMetrics registers all outbox Prometheus metrics with the given registerer.
// Call this once during application startup.
func InitMetrics(registerer prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		outboxWrittenCounter,
		outboxRelayPublishedCounter,
		outboxRelayBatchSize,
		outboxRelayPublishDuration,
	}
	for _, collector := range collectors {
		if mv, ok := collector.(metricResetter); ok {
			mv.Reset()
		}
		err := registerer.Register(collector)
		var are prometheus.AlreadyRegisteredError
		if err != nil && !errors.As(err, &are) {
			return err
		}
	}
	return nil
}

type metricResetter interface {
	Reset()
}

func recordEventWritten(routingKey string) {
	outboxWrittenCounter.WithLabelValues(routingKey).Inc()
}

func recordRelayPublished(routingKey, result string, durationMs int64) {
	outboxRelayPublishedCounter.WithLabelValues(routingKey, result).Inc()
	outboxRelayPublishDuration.WithLabelValues(routingKey, result).Observe(float64(durationMs))
}

func recordRelayBatchSize(size int) {
	outboxRelayBatchSize.Observe(float64(size))
}
