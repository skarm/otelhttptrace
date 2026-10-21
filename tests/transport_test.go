package tests

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/skarm/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTransportCreatesClientSpanAndComposesExistingHTTPTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("expected client trace in request context")
		}

		clientTrace.DNSStart(httptrace.DNSStartInfo{Host: "example.com"})
		clientTrace.DNSDone(httptrace.DNSDoneInfo{})
		clientTrace.ConnectStart("tcp", "example.com:80")
		clientTrace.ConnectDone("tcp", "example.com:80", nil)
		clientTrace.GotConn(httptrace.GotConnInfo{Reused: false, WasIdle: false})
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{})
		clientTrace.GotFirstResponseByte()

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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSyncer(exporter))
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

func testProvider(t *testing.T, sampler sdktrace.Sampler) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	e := tracetest.NewInMemoryExporter()
	p := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSyncer(e))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, e
}

func request(t *testing.T, ctx context.Context, rawURL string) *http.Request {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func onlySpan(t *testing.T, e *tracetest.InMemoryExporter) tracetest.SpanStub {
	t.Helper()
	s := e.GetSpans()
	if len(s) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(s))
	}
	return s[0]
}

func noBodyTransport(code int) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: code, Body: http.NoBody, Request: r}, nil
	})
}

func TestRegressionURLCredentials(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	r := request(t, context.Background(), "https://user:password@example.com/a?q=a%2Bb&X-Amz-Credential=cred%2Fscope&X-Amz-Signature=one&X-Amz-Signature=two&X-Amz-Security-Token=secret&%61ccess_token=escaped&sig=signed#access_token=fragment")
	wantURL := r.URL.String()
	base := roundTripFunc(func(got *http.Request) (*http.Response, error) {
		if got.URL.String() != wantURL {
			t.Fatalf("wire URL changed: %s", got.URL)
		}
		return noBodyTransport(200).RoundTrip(got)
	})
	_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	attrs := attributeMap(onlySpan(t, e).Attributes)
	assertAttr(t, attrs, "url.full", "https://example.com/a?q=a%2Bb&X-Amz-Credential=REDACTED&X-Amz-Signature=REDACTED&X-Amz-Signature=REDACTED&X-Amz-Security-Token=REDACTED&%61ccess_token=REDACTED&sig=REDACTED")
	if r.URL.String() != wantURL {
		t.Fatalf("original URL changed: %s", r.URL)
	}
}

func TestRegressionRecoveredPhaseErrors(t *testing.T) {
	for _, mode := range []string{"standalone", "factory", "context"} {
		t.Run(mode, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			phaseErr := errors.New("recovered attempt")
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				h := httptrace.ContextClientTrace(r.Context())
				h.DNSDone(httptrace.DNSDoneInfo{Err: phaseErr})
				h.ConnectStart("tcp", "[::1]:80")
				h.ConnectDone("tcp", "[::1]:80", phaseErr)
				h.TLSHandshakeDone(tls.ConnectionState{}, phaseErr)
				h.WroteRequest(httptrace.WroteRequestInfo{Err: phaseErr})
				h.PutIdleConn(phaseErr)
				return noBodyTransport(200).RoundTrip(r)
			})
			var transport http.RoundTripper
			switch mode {
			case "standalone":
				transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
			case "factory":
				transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(p), otelhttp.WithClientTrace(otelhttptrace.NewClientTrace))
			case "context":
				transport = otelhttp.NewTransport(otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true}), otelhttp.WithTracerProvider(p))
			}
			res, err := transport.RoundTrip(request(t, context.Background(), "http://example.com"))
			if err != nil {
				t.Fatal(err)
			}
			if err = res.Body.Close(); err != nil {
				t.Fatal(err)
			}
			s := onlySpan(t, e)
			if s.Status.Code != codes.Unset {
				t.Fatalf("recovered attempt poisoned final status: %v", s.Status)
			}
			if _, ok := attributeMap(s.Attributes)["error.type"]; ok {
				t.Fatal("phase error leaked into span attributes")
			}
			for _, event := range s.Events {
				if event.Name == "exception" {
					t.Fatal("recovered attempt recorded exception")
				}
				if strings.HasPrefix(event.Name, "httptrace.") && event.Name != "httptrace.connect_start" {
					assertAttr(t, attributeMap(event.Attributes), "error.type", "*errors.errorString")
				}
			}
		})
	}
}

func TestRegressionTerminalError(t *testing.T) {
	for _, kind := range []string{"transport", "deadline", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			ctx := context.Background()
			wantErr := errors.New("terminal failure")
			wantType := "*errors.errorString"
			wantStatus := codes.Error
			if kind == "deadline" {
				wantErr, wantType = context.DeadlineExceeded, "timeout"
			}
			if kind == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantErr, wantStatus = context.Canceled, codes.Unset
			}
			base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				h := httptrace.ContextClientTrace(r.Context())
				h.WroteRequest(httptrace.WroteRequestInfo{Err: wantErr})
				return nil, wantErr
			})
			_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, ctx, "http://example.com"))
			if err != wantErr {
				t.Fatalf("error identity lost: %v", err)
			}
			s := onlySpan(t, e)
			if s.Status.Code != wantStatus {
				t.Fatalf("status = %v, want %v", s.Status, wantStatus)
			}
			exceptions := 0
			for _, event := range s.Events {
				if event.Name == "exception" {
					exceptions++
				}
			}
			if kind == "canceled" {
				if _, ok := attributeMap(s.Attributes)["error.type"]; ok || exceptions != 0 {
					t.Fatal("explicit cancellation classified as failure")
				}
			} else {
				assertAttr(t, attributeMap(s.Attributes), "error.type", wantType)
				if exceptions != 1 {
					t.Fatalf("exceptions = %d, want 1 terminal exception", exceptions)
				}
			}
		})
	}
}

type idleTransport struct{ count int }

func (b *idleTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return noBodyTransport(200).RoundTrip(r)
}

func (b *idleTransport) CloseIdleConnections() { b.count++ }

func TestRegressionCloseIdleConnections(t *testing.T) {
	b := &idleTransport{}
	c := &http.Client{Transport: otelhttptrace.NewTransport(b, otelhttptrace.Config{})}
	c.CloseIdleConnections()
	if b.count != 1 {
		t.Fatalf("base CloseIdleConnections calls = %d, want 1", b.count)
	}
	(&http.Client{Transport: otelhttptrace.NewTransport(noBodyTransport(200), otelhttptrace.Config{})}).CloseIdleConnections()
}

func TestRegressionRequestForms(t *testing.T) {
	for _, contextMode := range []bool{false, true} {
		for _, invalid := range []string{"nil", "nil-url"} {
			t.Run(strconv.FormatBool(contextMode)+"/"+invalid, func(t *testing.T) {
				base := roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("invalid request reached base"); return nil, nil })
				var r *http.Request
				var body *observedBody
				if invalid == "nil-url" {
					body = &observedBody{}
					r = &http.Request{Body: body}
				}
				res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: contextMode}).RoundTrip(r)
				if res != nil || err == nil {
					t.Fatalf("RoundTrip = %v, %v", res, err)
				}
				if body != nil && body.closed != 1 {
					t.Fatalf("invalid request body Close calls = %d, want 1", body.closed)
				}
			})
		}
	}
	t.Run("empty-method", func(t *testing.T) {
		p, e := testProvider(t, sdktrace.AlwaysSample())
		r := request(t, context.Background(), "http://example.com")
		r.Method = ""
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != "" {
				t.Fatal("instrumentation mutated request method")
			}
			return noBodyTransport(200).RoundTrip(r)
		})
		_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
		if err != nil {
			t.Fatal(err)
		}
		s := onlySpan(t, e)
		if s.Name != "GET example.com" {
			t.Fatalf("name = %q", s.Name)
		}
		assertAttr(t, attributeMap(s.Attributes), "http.request.method", "GET")
	})
}

func TestRegressionNonRecordingTraceComposition(t *testing.T) {
	p, _ := testProvider(t, sdktrace.NeverSample())
	dropCtx, drop := p.Tracer("test").Start(context.Background(), "drop")
	defer drop.End()
	rp, _ := testProvider(t, sdktrace.AlwaysSample())
	endedCtx, ended := rp.Tracer("test").Start(context.Background(), "ended")
	ended.End()
	for name, ctx := range map[string]context.Context{"absent": context.Background(), "drop": dropCtx, "ended": endedCtx} {
		t.Run(name, func(t *testing.T) {
			first := otelhttptrace.NewClientTrace(ctx)
			second := otelhttptrace.NewClientTrace(ctx)
			if first == nil || second == nil || first == second {
				t.Fatal("factory must return a fresh non-nil trace")
			}
			if first.DNSStart != nil || first.ConnectStart != nil || first.WroteRequest != nil {
				t.Fatal("non-recording trace unnecessarily installs hooks")
			}
			wantErr := errors.New("user hook error")
			calls := 0
			parent := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error { calls++; return wantErr }})
			composed := httptrace.WithClientTrace(parent, first)
			if err := httptrace.ContextClientTrace(composed).Got1xxResponse(103, nil); err != wantErr || calls != 1 {
				t.Fatalf("user callback lost: %v, calls=%d", err, calls)
			}
			if second.Got1xxResponse != nil {
				t.Fatal("composition mutated another factory result")
			}
		})
	}
}

type observedBody struct {
	readErr, closeErr error
	closed            int
}

func (b *observedBody) Read(p []byte) (int, error) { return copy(p, "abc"), b.readErr }

func (b *observedBody) Close() error {
	b.closed++
	if b.closed == 1 {
		return b.closeErr
	}
	return io.ErrClosedPipe
}

func TestRegressionBodyResultsAndRepeatedClose(t *testing.T) {
	for _, readErr := range []error{io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe} {
		for _, tc := range []struct {
			name string
			err  error
		}{{"nil", nil}, {"failure", errors.New("close failed")}, {"EOF", io.EOF}} {
			t.Run(readErr.Error()+"/Close-"+tc.name, func(t *testing.T) {
				closeErr := tc.err
				p, e := testProvider(t, sdktrace.AlwaysSample())
				body := &observedBody{readErr: readErr, closeErr: closeErr}
				base := roundTripFunc(func(r *http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })
				res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), "http://example.com"))
				if err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, 3)
				if n, err := res.Body.Read(buf); n != 3 || string(buf) != "abc" || err != readErr {
					t.Fatalf("Read = %d, %q, %v", n, buf, err)
				}
				if len(e.GetSpans()) != 0 {
					t.Fatal("span ended before Close")
				}
				if err = res.Body.Close(); err != closeErr {
					t.Fatalf("Close = %v, want %v", err, closeErr)
				}
				if err = res.Body.Close(); err != io.ErrClosedPipe || body.closed != 2 {
					t.Fatalf("repeated Close changed base contract: %v, %d", err, body.closed)
				}
				s := onlySpan(t, e)
				want := codes.Unset
				if readErr != io.EOF || closeErr != nil {
					want = codes.Error
				}
				if s.Status.Code != want {
					t.Fatalf("status = %v, want %v", s.Status.Code, want)
				}
			})
		}
	}
}

type blockingBody struct {
	reading, closed chan struct{}
	once            sync.Once
}

func (b *blockingBody) Read([]byte) (int, error) {
	close(b.reading)
	<-b.closed
	return 0, io.ErrUnexpectedEOF
}

func (b *blockingBody) Close() error { b.once.Do(func() { close(b.closed) }); return nil }

func TestRegressionConcurrentReadClose(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	b := &blockingBody{reading: make(chan struct{}), closed: make(chan struct{})}
	base := roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: b}, nil })
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), "http://example.com"))
	if err != nil {
		t.Fatal(err)
	}
	readDone, closeDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := res.Body.Read(make([]byte, 1)); readDone <- err }()
	<-b.reading
	go func() { closeDone <- res.Body.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked behind Read")
	}
	select {
	case err := <-readDone:
		if err != io.ErrUnexpectedEOF {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read was not unblocked by Close")
	}
	s := onlySpan(t, e)
	if s.Status.Code != codes.Error {
		t.Fatalf("in-flight Read error lost during Close: %v", s.Status)
	}
}

func TestNoopPropagationWithValidNonRecordingParent(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	r := request(t, trace.ContextWithSpanContext(context.Background(), sc), "http://example.com")
	r.Header = nil
	base := roundTripFunc(func(got *http.Request) (*http.Response, error) {
		actual := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(got.Header)))
		if actual.TraceID() != sc.TraceID() || actual.SpanID() != sc.SpanID() || actual.IsSampled() {
			t.Fatalf("non-recording context lost: %v", actual)
		}
		return noBodyTransport(204).RoundTrip(got)
	})
	_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: noop.NewTracerProvider(), Propagators: propagation.TraceContext{}}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	if r.Header != nil {
		t.Fatal("original nil Header map was changed")
	}
}

type countingPropagator struct {
	propagation.TraceContext
	calls int
}

func (p *countingPropagator) Inject(ctx context.Context, c propagation.TextMapCarrier) {
	p.calls++
	p.TraceContext.Inject(ctx, c)
}

func TestContextModeLeavesPropagationToOwner(t *testing.T) {
	p, _ := testProvider(t, sdktrace.AlwaysSample())
	ctx, span := p.Tracer("owner").Start(context.Background(), "owner")
	defer span.End()
	propagator := &countingPropagator{}
	r := request(t, ctx, "http://example.com")
	r.Header.Set("Traceparent", "owner-header")
	base := roundTripFunc(func(got *http.Request) (*http.Response, error) {
		if got.Header.Get("Traceparent") != "owner-header" {
			t.Fatal("context helper rewrote propagation")
		}
		return noBodyTransport(200).RoundTrip(got)
	})
	_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true, Propagators: propagator}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	if propagator.calls != 0 || !span.IsRecording() {
		t.Fatal("helper injected or ended externally owned span")
	}
}

type inspectSampler struct{ attrs []attribute.KeyValue }

func (s *inspectSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	s.attrs = append([]attribute.KeyValue(nil), p.Attributes...)
	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}

func (*inspectSampler) Description() string { return "inspect" }

func TestSamplerReceivesRequestAttributesAtStart(t *testing.T) {
	sampler := &inspectSampler{}
	p, e := testProvider(t, sampler)
	_, err := otelhttptrace.NewTransport(noBodyTransport(200), otelhttptrace.Config{TracerProvider: p, Attributes: []attribute.KeyValue{attribute.String("component", "sampler")}}).RoundTrip(request(t, context.Background(), "https://example.com:8443/path?sig=secret"))
	if err != nil {
		t.Fatal(err)
	}
	a := attributeMap(sampler.attrs)
	assertAttr(t, a, "server.port", int64(8443))
	assertAttr(t, a, "http.request.method", "GET")
	assertAttr(t, a, "component", "sampler")
	assertAttr(t, a, "url.full", "https://example.com:8443/path?sig=REDACTED")
	if len(e.GetSpans()) != 0 {
		t.Fatal("sampler Drop exported a span")
	}
}

func TestContextWithoutRecordingSpanPreservesUserTrace(t *testing.T) {
	for _, mode := range []string{"absent", "drop", "ended", "global-unset", "noop"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			if mode == "drop" || mode == "ended" {
				sampler := sdktrace.NeverSample()
				if mode == "ended" {
					sampler = sdktrace.AlwaysSample()
				}
				p, _ := testProvider(t, sampler)
				var span trace.Span
				ctx, span = p.Tracer("test").Start(ctx, "owner")
				span.End()
			}
			calls := 0
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { calls++ }})
			r := request(t, ctx, "http://example.com")
			base := roundTripFunc(func(got *http.Request) (*http.Response, error) {
				h := httptrace.ContextClientTrace(got.Context())
				if h == nil || h.GotConn == nil {
					t.Fatal("user trace lost")
				}
				h.GotConn(httptrace.GotConnInfo{})
				return noBodyTransport(200).RoundTrip(got)
			})
			cfg := otelhttptrace.Config{SpanFromContext: true}
			if mode == "global-unset" {
				cfg.SpanFromContext = false
			}
			if mode == "noop" {
				cfg.SpanFromContext = false
				cfg.TracerProvider = noop.NewTracerProvider()
			}
			_, err := otelhttptrace.NewTransport(base, cfg).RoundTrip(r)
			if err != nil || calls != 1 {
				t.Fatalf("RoundTrip = %v, user calls = %d", err, calls)
			}
		})
	}
}

func TestLateCallbacksDoNotChangeEndedSDKSpan(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	ctx, span := p.Tracer("test").Start(context.Background(), "owner")
	h := otelhttptrace.NewClientTrace(ctx)
	h.ConnectStart("tcp", "127.0.0.1:80")
	span.End()
	before := onlySpan(t, e)
	h.ConnectDone("tcp", "127.0.0.1:80", errors.New("late dial error"))
	h.DNSStart(httptrace.DNSStartInfo{Host: "late.test"})
	h.DNSDone(httptrace.DNSDoneInfo{})
	h.GotFirstResponseByte()
	after := onlySpan(t, e)
	if !reflect.DeepEqual(before, after) || span.IsRecording() {
		t.Fatal("late callbacks changed ended SDK span")
	}
	span.SetStatus(codes.Error, "late owner error")
	if !reflect.DeepEqual(before, onlySpan(t, e)) {
		t.Fatal("SDK changed an ended span")
	}
}

func TestSDKStatusPrecedence(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	_, s := p.Tracer("test").Start(context.Background(), "precedence")
	s.SetStatus(codes.Error, "failure")
	s.SetStatus(codes.Unset, "")
	s.End()
	if onlySpan(t, e).Status.Code != codes.Error {
		t.Fatal("unexpected SDK precedence")
	}
}

// W3C Trace Context 1 section 3.2.2.2: compare literal wire values rather than
// using the same propagator to encode and decode the result.
func TestStandardsTraceContextGoldenHeaders(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}
	state, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	for _, sampled := range []bool{false, true} {
		name, flags := "unsampled", trace.TraceFlags(0)
		want := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
		if sampled {
			name, flags = "sampled", trace.FlagsSampled
			want = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		}
		t.Run(name, func(t *testing.T) {
			sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: flags, TraceState: state})
			r := request(t, trace.ContextWithSpanContext(context.Background(), sc), "http://example.test")
			r.Header.Set("X-Keep", "value")
			base := roundTripFunc(func(got *http.Request) (*http.Response, error) {
				if got.Header.Get("Traceparent") != want || got.Header.Get("Tracestate") != "vendor=value" || got.Header.Get("X-Keep") != "value" {
					t.Fatalf("wire propagation = %v", got.Header)
				}
				return noBodyTransport(204).RoundTrip(got)
			})
			_, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: noop.NewTracerProvider(), Propagators: propagation.TraceContext{}}).RoundTrip(r)
			if err != nil || r.Header.Get("Traceparent") != "" || r.Header.Get("Tracestate") != "" {
				t.Fatalf("RoundTrip = %v, original headers = %v", err, r.Header)
			}
		})
	}
}

func TestCancellationDoesNotHideTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"canceled", fmt.Errorf("operation: %w", context.Canceled), codes.Unset},
		{"net-error-canceled", &net.OpError{Op: "read", Net: "tcp", Err: context.Canceled}, codes.Unset},
		{"joined-canceled", errors.Join(context.Canceled, fmt.Errorf("attempt: %w", context.Canceled)), codes.Unset},
		{"deadline", errors.Join(context.Canceled, context.DeadlineExceeded), codes.Error},
		{"network-timeout", errors.Join(context.Canceled, &net.DNSError{Err: "i/o timeout", IsTimeout: true}), codes.Error},
		{"network-timeout-after-net-error", errors.Join(&net.OpError{Op: "read", Net: "tcp", Err: context.Canceled}, &net.DNSError{Err: "i/o timeout", IsTimeout: true}), codes.Error},
		{"wrapped-network-timeout", &net.OpError{Op: "read", Net: "tcp", Err: fmt.Errorf("attempts: %w", errors.Join(context.Canceled, &net.DNSError{Err: "i/o timeout", IsTimeout: true}))}, codes.Error},
	} {
		for _, operation := range []string{"round-trip", "read", "write", "close"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				p, e := testProvider(t, sdktrace.AlwaysSample())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				base := roundTripFunc(func(*http.Request) (*http.Response, error) {
					cancel()
					if operation == "round-trip" {
						return nil, tc.err
					}
					var body io.ReadCloser
					switch operation {
					case "read":
						body = &observedBody{readErr: tc.err}
					case "write":
						body = &writeErrorBody{err: tc.err}
					case "close":
						body = &observedBody{closeErr: tc.err}
					}
					return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
				})
				res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, ctx, "http://example.com"))
				if operation == "round-trip" {
					if err != tc.err {
						t.Fatalf("RoundTrip error = %v, want original error %v", err, tc.err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if operation == "read" {
						if n, err := res.Body.Read(make([]byte, 3)); n != 3 || err != tc.err {
							t.Fatalf("Read = %d, %v", n, err)
						}
					}
					if operation == "write" {
						if n, err := res.Body.(io.Writer).Write([]byte("ping")); n != 2 || err != tc.err {
							t.Fatalf("Write = %d, %v", n, err)
						}
					}
					closeErr := res.Body.Close()
					if operation == "close" && closeErr != tc.err || operation != "close" && closeErr != nil {
						t.Fatalf("Close = %v", closeErr)
					}
				}
				s := onlySpan(t, e)
				if s.Status.Code != tc.want {
					t.Fatalf("status = %v, want %v", s.Status.Code, tc.want)
				}
				exceptions := 0
				for _, event := range s.Events {
					if event.Name == "exception" {
						exceptions++
					}
				}
				if tc.want == codes.Error {
					assertAttr(t, attributeMap(s.Attributes), "error.type", "timeout")
					if exceptions != 1 {
						t.Fatalf("exception count = %d, want 1", exceptions)
					}
				} else if _, ok := attributeMap(s.Attributes)["error.type"]; ok || exceptions != 0 {
					t.Fatal("caller cancellation recorded as an error")
				}
			})
		}
	}
}
