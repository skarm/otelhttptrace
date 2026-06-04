package otelhttptrace

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type recordingProvider struct {
	noop.TracerProvider
	tracer *recordingTracer
}

func newRecordingProvider() *recordingProvider {
	p := &recordingProvider{}
	p.tracer = &recordingTracer{provider: p}
	return p
}

func (p *recordingProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return p.tracer
}

func (p *recordingProvider) lastSpan() *recordingSpan {
	p.tracer.mu.Lock()
	defer p.tracer.mu.Unlock()

	if len(p.tracer.spans) == 0 {
		return nil
	}
	return p.tracer.spans[len(p.tracer.spans)-1]
}

func (p *recordingProvider) startCount() int {
	p.tracer.mu.Lock()
	defer p.tracer.mu.Unlock()

	return len(p.tracer.spans)
}

type recordingTracer struct {
	noop.Tracer
	provider trace.TracerProvider

	mu    sync.Mutex
	spans []*recordingSpan
}

func (t *recordingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	span := newRecordingSpan(name)
	span.provider = t.provider
	span.attributes = append(span.attributes, cfg.Attributes()...)
	span.kind = cfg.SpanKind()

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()

	return trace.ContextWithSpan(ctx, span), span
}

type recordingSpan struct {
	noop.Span

	mu             sync.Mutex
	name           string
	kind           trace.SpanKind
	provider       trace.TracerProvider
	attributes     []attribute.KeyValue
	events         []recordingEvent
	recordedErrors []error
	statusCode     codes.Code
	statusDesc     string
	ended          bool
}

type recordingEvent struct {
	name       string
	attrs      []attribute.KeyValue
	stackTrace bool
}

func newRecordingSpan(name string) *recordingSpan {
	return &recordingSpan{
		name:     name,
		provider: noop.NewTracerProvider(),
	}
}

func (s *recordingSpan) End(...trace.SpanEndOption) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ended = true
}

func (s *recordingSpan) AddEvent(name string, opts ...trace.EventOption) {
	cfg := trace.NewEventConfig(opts...)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, recordingEvent{
		name:       name,
		attrs:      append([]attribute.KeyValue(nil), cfg.Attributes()...),
		stackTrace: cfg.StackTrace(),
	})
}

func (s *recordingSpan) IsRecording() bool {
	return true
}

func (s *recordingSpan) RecordError(err error, opts ...trace.EventOption) {
	if err == nil {
		return
	}
	cfg := trace.NewEventConfig(opts...)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.recordedErrors = append(s.recordedErrors, err)
	s.events = append(s.events, recordingEvent{
		name:       "exception",
		attrs:      append([]attribute.KeyValue(nil), cfg.Attributes()...),
		stackTrace: cfg.StackTrace(),
	})
}

func (s *recordingSpan) SetStatus(code codes.Code, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.statusCode = code
	s.statusDesc = description
}

func (s *recordingSpan) SetName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.name = name
}

func (s *recordingSpan) SetAttributes(kv ...attribute.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, next := range kv {
		replaced := false
		for i, existing := range s.attributes {
			if existing.Key == next.Key {
				s.attributes[i] = next
				replaced = true
				break
			}
		}
		if !replaced {
			s.attributes = append(s.attributes, next)
		}
	}
}

func (s *recordingSpan) TracerProvider() trace.TracerProvider {
	return s.provider
}

func (s *recordingSpan) hasEvent(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range s.events {
		if event.name == name {
			return true
		}
	}
	return false
}

func (s *recordingSpan) eventAttrs(name string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range s.events {
		if event.name == name {
			return attrMap(event.attrs)
		}
	}
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type stringAddr string

func (a stringAddr) Network() string { return "test" }
func (a stringAddr) String() string  { return string(a) }

type stubConn struct {
	remote net.Addr
}

func (c stubConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c stubConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c stubConn) Close() error                     { return nil }
func (c stubConn) LocalAddr() net.Addr              { return stringAddr("local") }
func (c stubConn) RemoteAddr() net.Addr             { return c.remote }
func (c stubConn) SetDeadline(time.Time) error      { return nil }
func (c stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c stubConn) SetWriteDeadline(time.Time) error { return nil }

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

func mustRequest(t *testing.T, method, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func attrMap(attrs []attribute.KeyValue) map[string]any {
	out := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		switch attr.Value.Type() {
		case attribute.STRING:
			out[string(attr.Key)] = attr.Value.AsString()
		case attribute.INT64:
			out[string(attr.Key)] = attr.Value.AsInt64()
		case attribute.BOOL:
			out[string(attr.Key)] = attr.Value.AsBool()
		case attribute.STRINGSLICE:
			out[string(attr.Key)] = attr.Value.AsStringSlice()
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
