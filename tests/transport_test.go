package tests

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/skarm/otelhttptrace"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTransportCreatesClientSpanAndComposesExistingHTTPTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	var existingDNSStart atomic.Int64
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			existingDNSStart.Add(1)
		},
	})

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("expected client trace in request context")
		}

		clientTrace.DNSStart(httptrace.DNSStartInfo{Host: "example.com"})
		clientTrace.DNSDone(httptrace.DNSDoneInfo{})
		clientTrace.ConnectStart("tcp", "example.com:80")
		clientTrace.ConnectDone("tcp", "example.com:80", nil)
		clientTrace.GotConn(httptrace.GotConnInfo{Reused: true, WasIdle: true})
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{})
		clientTrace.GotFirstResponseByte()

		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/widgets?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{
		TracerProvider: provider,
		Attributes:     []attribute.KeyValue{attribute.String("component", "test")},
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("span ended before response body close: got %d spans", len(spans))
	}

	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if got := existingDNSStart.Load(); got != 1 {
		t.Fatalf("existing httptrace DNSStart calls = %d, want 1", got)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}

	span := spans[0]
	if span.Name != "GET example.com" {
		t.Fatalf("span name = %q, want %q", span.Name, "GET example.com")
	}
	if span.SpanKind != trace.SpanKindClient {
		t.Fatalf("span kind = %v, want client", span.SpanKind)
	}

	attrs := attributeMap(span.Attributes)
	assertAttr(t, attrs, "component", "test")
	assertAttr(t, attrs, "http.request.method", "GET")
	assertAttr(t, attrs, "url.full", "http://example.com/widgets?q=1")
	assertAttr(t, attrs, "server.address", "example.com")
	assertAttr(t, attrs, "server.port", int64(80))
	assertAttr(t, attrs, "http.response.status_code", int64(http.StatusNoContent))
	assertAttr(t, attrs, "httptrace.connection.reused", true)
	assertAttr(t, attrs, "httptrace.connection.was_idle", true)

	for _, key := range []string{
		"httptrace.dns.duration_ns",
		"httptrace.connect.duration_ns",
		"httptrace.time_to_first_byte_ns",
	} {
		if _, ok := attrs[key]; !ok {
			t.Fatalf("missing span attribute %q", key)
		}
	}

	assertSpanEvents(t, span.Events,
		"httptrace.dns",
		"httptrace.connect",
		"httptrace.got_conn",
		"httptrace.wrote_request",
		"httptrace.got_first_response_byte",
	)
}

func TestTransportRecordsRoundTripError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	wantErr := errors.New("dial failed")
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: wantErr})
		return nil, wantErr
	})

	req, err := http.NewRequest(http.MethodPost, "https://api.example.com/upload", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = otelhttptrace.NewTransport(base, otelhttptrace.Config{
		TracerProvider: provider,
	}).RoundTrip(req)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Fatalf("span status = %v, want error", spans[0].Status.Code)
	}
}

func TestTransportEndsSpanImmediatelyForNoBodyResponse(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req, err := http.NewRequest(http.MethodGet, "http://example.com/widgets", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{
		TracerProvider: provider,
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Body != http.NoBody {
		t.Fatalf("response body = %T, want http.NoBody", res.Body)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
}

func TestNewClientTraceAddsHTTPTraceEventsToOtelHTTPSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		if trace == nil {
			t.Fatal("expected client trace in request context")
		}

		trace.DNSStart(httptrace.DNSStartInfo{Host: "example.com"})
		trace.DNSDone(httptrace.DNSDoneInfo{})
		trace.ConnectStart("tcp", "example.com:80")
		trace.ConnectDone("tcp", "example.com:80", nil)
		trace.GotConn(httptrace.GotConnInfo{Reused: false, WasIdle: false})
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		trace.GotFirstResponseByte()

		return &http.Response{
			StatusCode: http.StatusAccepted,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	transport := otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(provider),
		otelhttp.WithClientTrace(otelhttptrace.NewClientTrace),
	)

	req, err := http.NewRequest(http.MethodGet, "http://example.com/widgets", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}

	attrs := attributeMap(spans[0].Attributes)
	assertAttr(t, attrs, "httptrace.connection.reused", false)
	assertAttr(t, attrs, "httptrace.connection.was_idle", false)
	if _, ok := attrs["httptrace.time_to_first_byte_ns"]; !ok {
		t.Fatalf("missing time to first byte attribute")
	}

	assertSpanEvents(t, spans[0].Events,
		"httptrace.dns",
		"httptrace.connect",
		"httptrace.got_conn",
		"httptrace.wrote_request",
		"httptrace.got_first_response_byte",
	)
}

func TestTransportWithSpanFromContextAddsHTTPTraceEventsToOtelHTTPSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("expected client trace in request context")
		}

		clientTrace.ConnectStart("tcp", "example.com:80")
		clientTrace.ConnectDone("tcp", "example.com:80", nil)
		clientTrace.GotConn(httptrace.GotConnInfo{Reused: true, WasIdle: true})
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{})
		clientTrace.GotFirstResponseByte()

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	transport := otelhttp.NewTransport(
		otelhttptrace.NewTransport(base, otelhttptrace.Config{
			SpanFromContext: true,
		}),
		otelhttp.WithTracerProvider(provider),
	)

	req, err := http.NewRequest(http.MethodGet, "http://example.com/widgets", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Name != "HTTP GET" {
		t.Fatalf("span name = %q, want otelhttp client span", spans[0].Name)
	}
	if spans[0].SpanKind != trace.SpanKindClient {
		t.Fatalf("span kind = %v, want client", spans[0].SpanKind)
	}

	attrs := attributeMap(spans[0].Attributes)
	assertAttr(t, attrs, "httptrace.connection.reused", true)
	assertAttr(t, attrs, "httptrace.connection.was_idle", true)
	if _, ok := attrs["httptrace.connect.duration_ns"]; !ok {
		t.Fatalf("missing connect duration attribute")
	}

	assertSpanEvents(t, spans[0].Events,
		"httptrace.connect",
		"httptrace.got_conn",
		"httptrace.wrote_request",
		"httptrace.got_first_response_byte",
	)
}

func TestTransportPreservesReadWriteCloserResponseBody(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	body := &readWriteCloserBody{}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusSwitchingProtocols,
			Body:       body,
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req, err := http.NewRequest(http.MethodGet, "http://example.com/upgrade", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{
		TracerProvider: provider,
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	rwc, ok := res.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("response body type = %T, want io.ReadWriteCloser", res.Body)
	}
	if _, err := rwc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := body.written.String(); got != "ping" {
		t.Fatalf("written body = %q, want %q", got, "ping")
	}
	if err := rwc.Close(); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans after body close = %d, want 1", len(spans))
	}
}

func TestNewClientTraceSplitsNetworkPeerAddressAndPort(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("expected client trace in request context")
		}

		clientTrace.ConnectStart("tcp", "example.com:8443")
		clientTrace.ConnectDone("tcp", "example.com:8443", nil)

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	transport := otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(provider),
		otelhttp.WithClientTrace(otelhttptrace.NewClientTrace),
	)

	req, err := http.NewRequest(http.MethodGet, "https://example.com/widgets", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}

	var connectEventAttrs map[string]any
	for _, event := range spans[0].Events {
		if event.Name == "httptrace.connect" {
			connectEventAttrs = attributeMap(event.Attributes)
			break
		}
	}
	if connectEventAttrs == nil {
		t.Fatal("missing httptrace.connect event")
	}

	assertAttr(t, connectEventAttrs, "network.peer.address", "example.com")
	assertAttr(t, connectEventAttrs, "network.peer.port", int64(8443))
}

func TestTransportRecordsResponseBodyCloseError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	wantErr := errors.New("close failed")
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       closeErrBody{err: wantErr},
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req, err := http.NewRequest(http.MethodGet, "http://example.com/widgets", nil)
	if err != nil {
		t.Fatal(err)
	}

	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{
		TracerProvider: provider,
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v, want %v", err, wantErr)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Fatalf("span status = %v, want error", spans[0].Status.Code)
	}
	assertSpanEvents(t, spans[0].Events, "exception")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type readWriteCloserBody struct {
	strings.Reader
	written strings.Builder
}

func (b *readWriteCloserBody) Write(p []byte) (int, error) {
	return b.written.Write(p)
}

func (b *readWriteCloserBody) Close() error {
	return nil
}

type closeErrBody struct {
	err error
}

func (b closeErrBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (b closeErrBody) Close() error {
	return b.err
}

func attributeMap(attrs []attribute.KeyValue) map[string]any {
	out := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		switch attr.Value.Type() {
		case attribute.STRING:
			out[string(attr.Key)] = attr.Value.AsString()
		case attribute.INT64:
			out[string(attr.Key)] = attr.Value.AsInt64()
		case attribute.BOOL:
			out[string(attr.Key)] = attr.Value.AsBool()
		default:
			out[string(attr.Key)] = attr.Value.AsInterface()
		}
	}
	return out
}

func assertAttr(t *testing.T, attrs map[string]any, key string, want any) {
	t.Helper()
	if got, ok := attrs[key]; !ok || got != want {
		t.Fatalf("attribute %q = %v, %t; want %v", key, got, ok, want)
	}
}

func assertSpanEvents(t *testing.T, events []sdktrace.Event, want ...string) {
	t.Helper()

	eventNames := make(map[string]bool, len(events))
	for _, event := range events {
		eventNames[event.Name] = true
	}
	for _, name := range want {
		if !eventNames[name] {
			t.Fatalf("missing event %q in %#v", name, eventNames)
		}
	}
}
