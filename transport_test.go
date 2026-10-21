package otelhttptrace

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestNewTransportUsesResolvedConfig(t *testing.T) {
	provider := newRecordingProvider()
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	res, err := NewTransport(base, Config{
		TracerProvider: provider,
		Attributes:     []attribute.KeyValue{attribute.String("component", "unit")},
		SpanNameFormatter: func(*http.Request) string {
			return "custom span"
		},
	}).RoundTrip(mustRequest(t, http.MethodGet, "http://example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Body != nil {
		if err := res.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}

	span := provider.lastSpan()
	if span == nil {
		t.Fatal("missing span")
	}
	if span.name != "custom span" {
		t.Fatalf("span name = %q, want custom span", span.name)
	}
	if got := attrMap(span.attributes)["component"]; got != "unit" {
		t.Fatalf("span component attr = %v, want unit", got)
	}
}

func TestNewTransportUsesDefaultBaseWhenNil(t *testing.T) {
	transport := NewTransport(nil, Config{})

	if transport.base == nil {
		t.Fatal("base transport is nil")
	}
}

func TestTransportRejectsNilRequest(t *testing.T) {
	res, err := NewTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("base transport should not be called")
		return nil, nil
	}), Config{}).RoundTrip(nil)

	if res != nil {
		if res.Body != nil {
			if closeErr := res.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		t.Fatalf("response = %v, want nil", res)
	}
	if err == nil || err.Error() != "otelhttptrace: nil request" {
		t.Fatalf("error = %v, want nil request error", err)
	}
}

func TestTransportCreatesSpanRecordsTraceAndEndsOnBodyClose(t *testing.T) {
	provider := newRecordingProvider()
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("missing client trace")
		}

		clientTrace.DNSStart(httptrace.DNSStartInfo{Host: "example.com"})
		clientTrace.DNSDone(httptrace.DNSDoneInfo{Addrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}})
		clientTrace.GetConn("example.com:8080")
		clientTrace.ConnectStart("tcp", "example.com:8080")
		clientTrace.ConnectDone("tcp", "example.com:8080", nil)
		clientTrace.TLSHandshakeStart()
		clientTrace.TLSHandshakeDone(tls.ConnectionState{Version: tls.VersionTLS13}, nil)
		clientTrace.GotConn(httptrace.GotConnInfo{Reused: true, WasIdle: true, IdleTime: 5})
		clientTrace.WroteHeaders()
		clientTrace.Wait100Continue()
		clientTrace.Got100Continue()
		if err := clientTrace.Got1xxResponse(103, nil); err != nil {
			t.Fatal(err)
		}
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{})
		clientTrace.GotFirstResponseByte()
		clientTrace.PutIdleConn(nil)

		return &http.Response{
			StatusCode: http.StatusAccepted,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req := mustRequest(t, http.MethodGet, "https://example.com:8080/widgets?q=1")
	res, err := NewTransport(base, Config{
		TracerProvider: provider,
		Attributes:     []attribute.KeyValue{attribute.String("component", "unit")},
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	span := provider.lastSpan()
	if span == nil {
		t.Fatal("missing span")
	}
	if span.name != "GET example.com" {
		t.Fatalf("span name = %q, want GET example.com", span.name)
	}
	if span.ended {
		t.Fatal("span ended before response body close")
	}

	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !span.ended {
		t.Fatal("span did not end after response body close")
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if span.endCount != 1 {
		t.Fatalf("End calls = %d, want 1", span.endCount)
	}

	attrs := attrMap(span.attributes)
	assertAttr(t, attrs, "component", "unit")
	assertAttr(t, attrs, "http.request.method", "GET")
	assertAttr(t, attrs, "url.full", "https://example.com:8080/widgets?q=1")
	assertAttr(t, attrs, "url.scheme", "https")
	assertAttr(t, attrs, "server.address", "example.com")
	assertAttr(t, attrs, "server.port", int64(8080))
	assertAttr(t, attrs, "http.response.status_code", int64(http.StatusAccepted))
	assertAttr(t, attrs, "httptrace.connection.reused", true)
	assertAttr(t, attrs, "httptrace.connection.was_idle", true)

	for _, key := range []string{
		"httptrace.dns.duration_ns",
		"httptrace.get_conn.duration_ns",
		"httptrace.connect.duration_ns",
		"httptrace.tls_handshake.duration_ns",
		"httptrace.time_to_first_byte_ns",
	} {
		if _, ok := attrs[key]; !ok {
			t.Fatalf("missing span attribute %q", key)
		}
	}

	for _, name := range []string{
		"httptrace.dns_start",
		"httptrace.dns",
		"httptrace.get_conn_start",
		"httptrace.connect_start",
		"httptrace.connect",
		"httptrace.tls_handshake_start",
		"httptrace.tls_handshake",
		"httptrace.got_conn",
		"httptrace.wrote_headers",
		"httptrace.wait_100_continue",
		"httptrace.got_100_continue",
		"httptrace.got_1xx_response",
		"httptrace.wrote_request",
		"httptrace.got_first_response_byte",
		"httptrace.put_idle_conn",
	} {
		if !span.hasEvent(name) {
			t.Fatalf("missing event %q", name)
		}
	}
}

func TestTransportEndsSpanImmediatelyForNilAndNoBodyResponses(t *testing.T) {
	tests := []struct {
		name string
		body io.ReadCloser
	}{
		{name: "nil body", body: nil},
		{name: "http NoBody", body: http.NoBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := newRecordingProvider()
			base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       tt.body,
					Header:     make(http.Header),
					Request:    req,
				}, nil
			})

			res, err := NewTransport(base, Config{TracerProvider: provider}).RoundTrip(mustRequest(t, http.MethodGet, "http://example.com"))
			if err != nil {
				t.Fatal(err)
			}
			if res.Body != tt.body {
				t.Fatalf("response body = %T, want original body %T", res.Body, tt.body)
			}
			if res.Body != nil {
				if err := res.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if span := provider.lastSpan(); span == nil || !span.ended {
				t.Fatalf("span ended = %v, want true", span != nil && span.ended)
			}
		})
	}
}

func TestTransportRecordsRoundTripErrorAndEndsSpan(t *testing.T) {
	provider := newRecordingProvider()
	wantErr := errors.New("dial failed")
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{Err: wantErr})
		return nil, wantErr
	})

	res, err := NewTransport(base, Config{TracerProvider: provider}).RoundTrip(mustRequest(t, http.MethodPost, "https://api.example.com/upload"))
	if res != nil && res.Body != nil {
		if closeErr := res.Body.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}

	span := provider.lastSpan()
	if span == nil {
		t.Fatal("missing span")
	}
	if !span.ended {
		t.Fatal("span did not end on RoundTrip error")
	}
	if span.statusCode != codes.Error {
		t.Fatalf("status = %v, want error", span.statusCode)
	}
	if len(span.recordedErrors) == 0 {
		t.Fatal("expected recorded error")
	}
	if !span.hasEvent("httptrace.wrote_request") {
		t.Fatal("missing wrote_request event")
	}
}

func TestTransportRecordsResponseBodyCloseError(t *testing.T) {
	provider := newRecordingProvider()
	wantErr := errors.New("close failed")
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       closeErrBody{err: wantErr},
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	res, err := NewTransport(base, Config{TracerProvider: provider}).RoundTrip(mustRequest(t, http.MethodGet, "http://example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("close error = %v, want %v", err, wantErr)
	}

	span := provider.lastSpan()
	if span == nil {
		t.Fatal("missing span")
	}
	if !span.ended {
		t.Fatal("span did not end after response body close error")
	}
	if span.statusCode != codes.Error {
		t.Fatalf("status = %v, want error", span.statusCode)
	}
	if len(span.recordedErrors) != 1 {
		t.Fatalf("recorded errors = %d, want 1", len(span.recordedErrors))
	}
}

func TestTransportEndsSpanWhenRoundTripReturnsNilResponseWithoutError(t *testing.T) {
	provider := newRecordingProvider()
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, nil
	})

	res, err := NewTransport(base, Config{TracerProvider: provider}).RoundTrip(mustRequest(t, http.MethodGet, "http://example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		if res.Body != nil {
			if closeErr := res.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		t.Fatalf("response = %v, want nil", res)
	}
	if span := provider.lastSpan(); span == nil || !span.ended {
		t.Fatalf("span ended = %v, want true", span != nil && span.ended)
	}
}

func TestTransportSpanFromContextRecordsOnActiveSpanWithoutOwningIt(t *testing.T) {
	provider := newRecordingProvider()
	activeSpan := newRecordingSpan("active")
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		clientTrace := httptrace.ContextClientTrace(req.Context())
		if clientTrace == nil {
			t.Fatal("missing client trace")
		}
		clientTrace.ConnectStart("tcp", "example.com:443")
		clientTrace.ConnectDone("tcp", "example.com:443", nil)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	req := mustRequest(t, http.MethodGet, "https://example.com")
	req = req.WithContext(trace.ContextWithSpan(req.Context(), activeSpan))

	res, err := NewTransport(base, Config{
		TracerProvider:  provider,
		SpanFromContext: true,
	}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}

	if provider.startCount() != 0 {
		t.Fatalf("started spans = %d, want 0", provider.startCount())
	}
	if activeSpan.ended {
		t.Fatal("active span was ended by SpanFromContext transport")
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if activeSpan.ended {
		t.Fatal("active span was ended after response body close")
	}
	if !activeSpan.hasEvent("httptrace.connect") {
		t.Fatal("missing connect event on active span")
	}

	attrs := activeSpan.eventAttrs("httptrace.connect")
	assertAttr(t, attrs, "network.peer.address", "example.com")
	assertAttr(t, attrs, "network.peer.port", int64(443))
}

func TestRecordSpanErrorIgnoresNil(t *testing.T) {
	span := newRecordingSpan("trace")

	recordSpanError(span, nil)

	if span.statusCode != codes.Unset {
		t.Fatalf("status = %v, want unset", span.statusCode)
	}
	if len(span.recordedErrors) != 0 {
		t.Fatalf("recorded errors = %d, want 0", len(span.recordedErrors))
	}
}
