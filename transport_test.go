package otelhttptrace

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
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
		clientTrace.ConnectStart("tcp", "example.com:8080")
		clientTrace.ConnectDone("tcp", "example.com:8080", nil)
		clientTrace.TLSHandshakeStart()
		clientTrace.TLSHandshakeDone(tls.ConnectionState{Version: tls.VersionTLS13}, nil)
		clientTrace.GotConn(httptrace.GotConnInfo{Reused: true, WasIdle: true, IdleTime: 5})
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
		"httptrace.connect_start",
		"httptrace.connect",
		"httptrace.tls_handshake_start",
		"httptrace.tls_handshake",
		"httptrace.got_conn",
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

func TestTraceRecorderRecordsErrorsAndPhaseAttributes(t *testing.T) {
	span := newRecordingSpan("trace")
	recorder := newTraceRecorder(span)
	wantErr := errors.New("phase failed")

	recorder.markDNSDone(httptrace.DNSDoneInfo{Err: wantErr})
	recorder.markConnectDone("tcp", "example.com:443", wantErr)
	recorder.markTLSDone(tls.ConnectionState{Version: tls.VersionTLS12}, wantErr)
	recorder.markPutIdleConn(wantErr)

	if span.statusCode != codes.Error {
		t.Fatalf("status = %v, want error", span.statusCode)
	}
	if len(span.recordedErrors) != 4 {
		t.Fatalf("recorded errors = %d, want 4", len(span.recordedErrors))
	}
	if !span.hasEvent("httptrace.dns") || !span.hasEvent("httptrace.connect") || !span.hasEvent("httptrace.tls_handshake") || !span.hasEvent("httptrace.put_idle_conn") {
		t.Fatal("missing error phase event")
	}

	tlsAttrs := span.eventAttrs("httptrace.tls_handshake")
	assertAttr(t, tlsAttrs, "tls.protocol.version", "1.2")
}

func TestTraceRecorderGotConnWithoutOptionalConnectionDetails(t *testing.T) {
	span := newRecordingSpan("trace")
	recorder := newTraceRecorder(span)

	recorder.markGotConn(httptrace.GotConnInfo{})

	attrs := span.eventAttrs("httptrace.got_conn")
	assertAttr(t, attrs, "httptrace.connection.reused", false)
	assertAttr(t, attrs, "httptrace.connection.was_idle", false)
	if _, ok := attrs["httptrace.connection.idle_ns"]; ok {
		t.Fatal("unexpected idle duration attribute")
	}
	if _, ok := attrs["httptrace.connection.remote.address"]; ok {
		t.Fatal("unexpected remote address attribute")
	}
}

func TestTraceRecorderGotConnRecordsRemoteAddressFromConnection(t *testing.T) {
	span := newRecordingSpan("trace")
	recorder := newTraceRecorder(span)

	recorder.markGotConn(httptrace.GotConnInfo{
		Conn: stubConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}},
	})

	attrs := span.eventAttrs("httptrace.got_conn")
	assertAttr(t, attrs, "httptrace.connection.remote.address", "127.0.0.1")
	assertAttr(t, attrs, "httptrace.connection.remote.port", int64(443))
}

func TestRequestPortAndAttributes(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want int
	}{
		{name: "explicit valid", url: "http://example.com:8080/path", want: 8080},
		{name: "explicit invalid", url: "http://example.com:bad/path", want: 0},
		{name: "explicit invalid ipv6", url: "http://[::1]:bad/path", want: 0},
		{name: "explicit empty", url: "http://example.com:/path", want: 0},
		{name: "explicit overflow", url: "http://example.com:999999999999999999999999999999999999/path", want: 0},
		{name: "http default", url: "http://example.com/path", want: 80},
		{name: "https default", url: "https://example.com/path", want: 443},
		{name: "https ipv6 default", url: "https://[::1]/path", want: 443},
		{name: "malformed ipv6", url: "http://[::1/path", want: 80},
		{name: "malformed ipv6 suffix", url: "http://[::1]x/path", want: 80},
		{name: "unbracketed ipv6", url: "http://::1/path", want: 80},
		{name: "unknown scheme", url: "ftp://example.com/path", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			if tt.name == "explicit invalid" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "example.com:bad", Path: "/path"}}
			} else if tt.name == "explicit invalid ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1]:bad", Path: "/path"}}
			} else if tt.name == "explicit empty" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "example.com:", Path: "/path"}}
			} else if tt.name == "malformed ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1", Path: "/path"}}
			} else if tt.name == "malformed ipv6 suffix" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1]x", Path: "/path"}}
			} else if tt.name == "unbracketed ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "::1", Path: "/path"}}
			} else {
				req = mustRequest(t, http.MethodGet, tt.url)
			}
			if got := requestPort(req); got != tt.want {
				t.Fatalf("requestPort = %d, want %d", got, tt.want)
			}
		})
	}

	attrs := attrMap(requestAttributes(mustRequest(t, http.MethodPut, "https://example.com:9443/a?b=c"), []attribute.KeyValue{
		attribute.String("component", "unit"),
	}))
	assertAttr(t, attrs, "component", "unit")
	assertAttr(t, attrs, "http.request.method", "PUT")
	assertAttr(t, attrs, "url.full", "https://example.com:9443/a?b=c")
	assertAttr(t, attrs, "url.scheme", "https")
	assertAttr(t, attrs, "server.address", "example.com")
	assertAttr(t, attrs, "server.port", int64(9443))
}

func TestDefaultSpanNameFallsBackToURLHost(t *testing.T) {
	req := &http.Request{Method: http.MethodGet, URL: &url.URL{Scheme: "http", Host: "example.com:bad"}}

	if got := defaultSpanName(req); got != "GET example.com:bad" {
		t.Fatalf("default span name = %q, want %q", got, "GET example.com:bad")
	}
}

func TestNetworkAndRemoteAddressAttributes(t *testing.T) {
	attrs := attrMap(networkPeerAttributes("example.com:8443"))
	assertAttr(t, attrs, "network.peer.address", "example.com")
	assertAttr(t, attrs, "network.peer.port", int64(8443))

	attrs = attrMap(networkPeerAttributes("example.com"))
	assertAttr(t, attrs, "network.peer.address", "example.com")

	attrs = attrMap(remoteAddrAttributes(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9090}))
	assertAttr(t, attrs, "httptrace.connection.remote.address", "127.0.0.1")
	assertAttr(t, attrs, "httptrace.connection.remote.port", int64(9090))

	attrs = attrMap(remoteAddrAttributes(stringAddr("unix:/tmp/socket")))
	assertAttr(t, attrs, "httptrace.connection.remote.address", "unix:/tmp/socket")

	if attrs := remoteAddrAttributes(nil); attrs != nil {
		t.Fatalf("nil remote attrs = %v, want nil", attrs)
	}
}

func TestSpanBodyEndsOnceAndPreservesReadWriteCloser(t *testing.T) {
	var endCount int
	body := newSpanBody(io.NopCloser(strings.NewReader("ok")), func(error) {
		endCount++
	})

	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if endCount != 1 {
		t.Fatalf("end calls = %d, want 1", endCount)
	}

	readWrite := &readWriteCloserBody{}
	wrapped := newSpanBody(readWrite, func(error) {})
	rwc, ok := wrapped.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("wrapped body = %T, want io.ReadWriteCloser", wrapped)
	}
	if _, err := rwc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := readWrite.written.String(); got != "ping" {
		t.Fatalf("written = %q, want ping", got)
	}
	if err := rwc.Close(); err != nil {
		t.Fatal(err)
	}
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

func TestTLSVersion(t *testing.T) {
	tests := []struct {
		version uint16
		want    string
	}{
		{version: tls.VersionTLS10, want: "1.0"},
		{version: tls.VersionTLS11, want: "1.1"},
		{version: tls.VersionTLS12, want: "1.2"},
		{version: tls.VersionTLS13, want: "1.3"},
		{version: 0x9999, want: "0x9999"},
	}

	for _, tt := range tests {
		if got := tlsVersion(tt.version); got != tt.want {
			t.Fatalf("tlsVersion(%x) = %q, want %q", tt.version, got, tt.want)
		}
	}
}
