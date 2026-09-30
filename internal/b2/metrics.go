/*
Copyright 2026 Backblaze, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package b2

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "b2_operator_api_requests_total",
		Help: "B2 Native API requests by operation and HTTP status code (\"error\" for transport failures).",
	}, []string{"operation", "code"})

	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "b2_operator_api_request_duration_seconds",
		Help:    "Latency of B2 Native API requests by operation.",
		Buckets: prometheus.ExponentialBuckets(0.025, 2, 10),
	}, []string{"operation"})
)

// Collectors returns the client's Prometheus collectors for registration.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{requestsTotal, requestDuration}
}

func observe(op, code string, start time.Time) {
	requestsTotal.WithLabelValues(op, code).Inc()
	requestDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}
