// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunkhecexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter"

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/goccy/go-json"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
)

var errUnsupportedEventValue = errors.New("unsupported value for event body")

const (
	// Keys are taken from https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/overview.md#trace-context-in-legacy-formats.
	// spanIDFieldKey is the key used in log event for the span id (if any).
	spanIDFieldKey = "span_id"
	// traceIDFieldKey is the key used in the log event for the trace id (if any).
	traceIDFieldKey = "trace_id"
)

func mapLogRecordToSplunkEvent(res pcommon.Resource, lr plog.LogRecord, config *Config) (*splunk.Event, error) {
	body := lr.Body().AsRaw()
	bodyStr, err := convertToEventString(body)
	if err != nil {
		return nil, err
	}

	host := unknownHostName
	source := config.Source
	sourcetype := config.SourceType
	index := config.Index
	fields := map[string]any{}
	sourceKey := config.HecToOtelAttrs.Source
	sourceTypeKey := config.HecToOtelAttrs.SourceType
	indexKey := config.HecToOtelAttrs.Index
	hostKey := config.HecToOtelAttrs.Host
	severityTextKey := config.HecFields.SeverityText
	severityNumberKey := config.HecFields.SeverityNumber
	if spanID := lr.SpanID(); !spanID.IsEmpty() {
		fields[spanIDFieldKey] = hex.EncodeToString(spanID[:])
	}
	if traceID := lr.TraceID(); !traceID.IsEmpty() {
		fields[traceIDFieldKey] = hex.EncodeToString(traceID[:])
	}
	if lr.SeverityText() != "" {
		fields[severityTextKey] = lr.SeverityText()
	}
	if lr.SeverityNumber() != plog.SeverityNumberUnspecified {
		fields[severityNumberKey] = lr.SeverityNumber()
	}

	res.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case hostKey:
			host = v.Str()
		case sourceKey:
			source = v.Str()
		case sourceTypeKey:
			sourcetype = v.Str()
		case indexKey:
			index = v.Str()
		case splunk.HecTokenLabel:
			// ignore
		default:
			mergeValue(fields, k, v.AsRaw())
		}
		return true
	})
	lr.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case hostKey:
			host = v.Str()
		case sourceKey:
			source = v.Str()
		case sourceTypeKey:
			sourcetype = v.Str()
		case indexKey:
			index = v.Str()
		case splunk.HecTokenLabel:
			// ignore
		default:
			mergeValue(fields, k, v.AsRaw())
		}
		return true
	})

	return &splunk.Event{
		Time:       nanoTimestampToEpochMilliseconds(lr.Timestamp()),
		Host:       host,
		Source:     source,
		SourceType: sourcetype,
		Index:      index,
		Event:      bodyStr,
		Fields:     fields,
	}, nil
}

// convertToEventString converts log body (any) to string for Event.Event.
// Returns error for nil, empty string, or unsupported values (Inf, NaN).
func convertToEventString(body any) (string, error) {
	if body == nil || body == "" {
		return "", errUnsupportedEventValue
	}
	switch v := body.(type) {
	case string:
		return v, nil
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return "", fmt.Errorf("%w: %v", errUnsupportedEventValue, v)
		}
		return fmt.Sprint(v), nil
	case map[string]any, []any:
		b, err := json.MarshalWithOption(v, json.DisableHTMLEscape())
		if err != nil {
			return "", fmt.Errorf("%w: %v", errUnsupportedEventValue, err)
		}
		return string(b), nil
	case int64, bool:
		return fmt.Sprint(v), nil
	default:
		return fmt.Sprint(body), nil
	}
}

// nanoTimestampToEpochMilliseconds transforms nanoseconds into <sec>.<ms>. For example, 1433188255.500 indicates 1433188255 seconds and 500 milliseconds after epoch.
func nanoTimestampToEpochMilliseconds(ts pcommon.Timestamp) float64 {
	return time.Duration(ts).Round(time.Millisecond).Seconds()
}

func mergeValue(dst map[string]any, k string, v any) {
	switch element := v.(type) {
	case []any:
		if isArrayFlat(element) {
			dst[k] = v
		} else {
			b, _ := json.MarshalWithOption(element, json.DisableHTMLEscape())
			dst[k] = string(b)
		}
	case map[string]any:
		flattenAndMergeMap(element, dst, k)
	default:
		dst[k] = v
	}
}

func isArrayFlat(array []any) bool {
	for _, v := range array {
		switch v.(type) {
		case []any, map[string]any:
			return false
		}
	}
	return true
}

func flattenAndMergeMap(src, dst map[string]any, key string) {
	for k, v := range src {
		current := fmt.Sprintf("%s.%s", key, k)
		switch element := v.(type) {
		case map[string]any:
			flattenAndMergeMap(element, dst, current)
		case []any:
			if isArrayFlat(element) {
				dst[current] = element
			} else {
				b, _ := json.MarshalWithOption(element, json.DisableHTMLEscape())
				dst[current] = string(b)
			}

		default:
			dst[current] = element
		}
	}
}
