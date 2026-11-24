// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkhecexporter

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
)

func TestDiscardOnStatusCodes(t *testing.T) {
	t.Parallel()

	// Update public documentation if the default classification changes.
	tests := []struct {
		name    string
		discard []int
		want    []int
	}{
		{name: "default", want: []int{400, 401, 403}},
		{name: "retry forbidden", discard: []int{400, 401}, want: []int{400, 401}},
		{name: "retry all", discard: []int{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, status := range []int{400, 401, 403, 429, 503} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(status)
					}))
					defer server.Close()

					endpoint, err := url.Parse(server.URL)
					require.NoError(t, err)
					worker := &defaultHecWorker{
						url: endpoint, client: server.Client(), logger: zap.NewNop(), discardOnStatusCodes: tt.discard,
					}
					err = worker.send(t.Context(), &cancellableBytesWriter{innerWriter: bytes.NewBufferString("data")}, nil)
					require.ErrorContains(t, err, http.StatusText(status))
					assert.Equal(t, slices.Contains(tt.want, status), consumererror.IsPermanent(err))
				})
			}
		})
	}
}

var errHecSendFailed = errors.New("hec send failed")

type mockHecWorker struct {
	failSend bool
}

func (m *mockHecWorker) send(_ context.Context, _ buffer, _ map[string]string) error {
	if m.failSend {
		return errHecSendFailed
	}
	return nil
}

var _ hecWorker = &mockHecWorker{}
