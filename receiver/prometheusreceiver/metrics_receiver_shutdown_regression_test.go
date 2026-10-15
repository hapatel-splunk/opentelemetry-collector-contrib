// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusreceiver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/promslog"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
)

func TestReceiverShutdownCancelsBlockedMetricConsumption(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "# TYPE shutdown_test gauge\nshutdown_test 1")
	}))
	defer server.Close()

	cfg, err := promconfig.Load(fmt.Sprintf(`
scrape_configs:
- job_name: shutdown-test
  scrape_interval: 100ms
  static_configs:
  - targets: [%q]
`, strings.TrimPrefix(server.URL, "http://")), promslog.NewNopLogger())
	require.NoError(t, err)

	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce, canceledOnce sync.Once
	sink, err := consumer.NewMetrics(func(ctx context.Context, _ pmetric.Metrics) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			canceledOnce.Do(func() { close(canceled) })
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	require.NoError(t, err)

	receiver, err := newPrometheusReceiver(receivertest.NewNopSettings(metadata.Type),
		&Config{PrometheusConfig: (*PromConfig)(cfg), skipOffsetting: true}, sink)
	require.NoError(t, err)
	require.NoError(t, receiver.start(t.Context(), componenttest.NewNopHost(), prometheusComponentTestOptions{
		discovery: prometheusDiscoveryTestOptions{updatert: 50 * time.Millisecond},
		scrape:    prometheusScrapeTestOptions{discoveryReloadInterval: 50 * time.Millisecond},
	}))

	shutdownDone := make(chan struct{})
	var shutdownOnce sync.Once
	var shutdownErr error
	shutdown := func() {
		shutdownOnce.Do(func() {
			go func() {
				shutdownErr = receiver.Shutdown(t.Context())
				close(shutdownDone)
			}()
		})
	}
	t.Cleanup(func() {
		close(release)
		shutdown()
		select {
		case <-shutdownDone:
		case <-time.After(5 * time.Second):
			t.Error("receiver shutdown did not finish after releasing the consumer")
		}
	})

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not deliver scraped metrics to the blocking consumer")
	}
	shutdown()
	select {
	case <-shutdownDone:
		require.NoError(t, shutdownErr)
	case <-time.After(2 * time.Second):
		t.Fatal("receiver shutdown did not cancel blocked metric consumption")
	}
	select {
	case <-canceled:
	default:
		t.Fatal("consumer returned without receiver shutdown canceling its context")
	}
}
