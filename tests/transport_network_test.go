package tests

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/skarm/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// dualStackResolver serves only the two DNS records used by this test, on a
// local UDP socket. No system hosts file, external DNS or timing sleep is used.
func dualStackResolver(t *testing.T) *net.Resolver {
	t.Helper()
	c, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1500)
		for {
			n, peer, err := c.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 17 {
				continue
			}
			end := 12
			for end < n && buf[end] != 0 {
				end += int(buf[end]) + 1
			}
			end += 5 // zero label, QTYPE, QCLASS
			if end > n {
				continue
			}
			qtype := binary.BigEndian.Uint16(buf[end-4 : end-2])
			var ip net.IP
			if qtype == 1 {
				ip = net.IPv4(127, 0, 0, 1).To4()
			}
			if qtype == 28 {
				ip = net.IPv6loopback.To16()
			}
			response := append([]byte(nil), buf[:end]...)
			binary.BigEndian.PutUint16(response[2:4], 0x8180)
			binary.BigEndian.PutUint16(response[4:6], 1)
			for i := 6; i < 12; i++ {
				response[i] = 0
			}
			if ip != nil {
				binary.BigEndian.PutUint16(response[6:8], 1)
				response = append(response, 0xc0, 0x0c, byte(qtype>>8), byte(qtype), 0, 1, 0, 0, 0, 0, 0, byte(len(ip)))
				response = append(response, ip...)
			}
			_, _ = c.WriteTo(response, peer)
		}
	}()
	t.Cleanup(func() { _ = c.Close(); <-done })
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp4", c.LocalAddr().String())
	}}
}

var errFallbackIPv6 = errors.New("test: reject IPv6 dial to exercise fallback")

func fallbackDialer(resolver *net.Resolver) *net.Dialer {
	return &net.Dialer{
		Resolver: resolver,
		// An IPv4 server does not reserve the same port on IPv6. Force the
		// first failure even if another listener occupies that IPv6 port.
		// The real Dialer still runs DNS, trace hooks and IPv4 fallback.
		Control: func(network, _ string, _ syscall.RawConn) error {
			if network == "tcp6" {
				return errFallbackIPv6
			}
			return nil
		},
	}
}

func TestFallbackDialerRejectsListeningIPv6(t *testing.T) {
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	c, err := fallbackDialer(nil).DialContext(context.Background(), "tcp6", l.Addr().String())
	if c != nil {
		_ = c.Close()
	}
	if !errors.Is(err, errFallbackIPv6) {
		t.Fatalf("IPv6 dial = %v, want controlled fallback failure even when the port is listening", err)
	}
}

func TestRegressionRealDualStackFallback(t *testing.T) {
	resolver := dualStackResolver(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	if srv.Listener.Addr().(*net.TCPAddr).IP.To4() == nil {
		t.Fatal("test requires IPv4 httptest listener")
	}
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"standalone", "factory", "context"} {
		t.Run(mode, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			dialer := fallbackDialer(resolver)
			base := &http.Transport{DialContext: dialer.DialContext}
			defer base.CloseIdleConnections()
			var transport http.RoundTripper
			switch mode {
			case "standalone":
				transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
			case "factory":
				transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(p), otelhttp.WithClientTrace(otelhttptrace.NewClientTrace))
			case "context":
				transport = otelhttp.NewTransport(otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true}), otelhttp.WithTracerProvider(p))
			}
			var failed, connected atomic.Int64
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{ConnectDone: func(_, _ string, err error) {
				if err != nil {
					if !errors.Is(err, errFallbackIPv6) {
						t.Errorf("unexpected dial failure: %v", err)
					}
					failed.Add(1)
				} else {
					connected.Add(1)
				}
			}})
			res, err := transport.RoundTrip(request(t, ctx, "http://dualstack.test:"+port))
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(res.Body)
			if err != nil || string(data) != "ok" {
				t.Fatalf("body = %q, %v", data, err)
			}
			if err = res.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if failed.Load() < 1 || connected.Load() != 1 {
				t.Fatalf("not a recovered fallback: failed=%d connected=%d", failed.Load(), connected.Load())
			}
			s := onlySpan(t, e)
			if s.Status.Code != codes.Unset {
				t.Fatalf("fallback poisoned span: %v", s.Status)
			}
			for _, event := range s.Events {
				if event.Name == "exception" {
					t.Fatal("recovered fallback emitted exception")
				}
			}
		})
	}
}

type failingWriteConn struct {
	net.Conn
	fail atomic.Bool
	err  error
}

func (c *failingWriteConn) Write(p []byte) (int, error) {
	if c.fail.CompareAndSwap(true, false) {
		return 0, c.err
	}
	return c.Conn.Write(p)
}

func TestRegressionRealRecoveredWrite(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	var first *failingWriteConn
	var dials atomic.Int64
	wantErr := errors.New("injected write failure on reused TCP connection")
	base := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		wrapped := &failingWriteConn{Conn: c, err: wantErr}
		if dials.Add(1) == 1 {
			first = wrapped
		}
		return wrapped, nil
	}}
	defer base.CloseIdleConnections()
	var writeErrors atomic.Int64
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err != nil {
			writeErrors.Add(1)
		}
	}})
	transport := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
	for i := range 2 {
		r := request(t, ctx, srv.URL)
		if i == 1 {
			// Force a flush inside Request.write. A small header block can fail
			// only on the later buffered Flush, after WroteRequest(nil) ran.
			r.Header.Set("X-Pad", strings.Repeat("x", 16384))
		}
		res, err := transport.RoundTrip(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.Copy(io.Discard, res.Body); err != nil {
			t.Fatal(err)
		}
		if err = res.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first.fail.Store(true)
		}
	}
	if writeErrors.Load() != 1 || dials.Load() != 2 {
		t.Fatalf("write retry not exercised: errors=%d dials=%d", writeErrors.Load(), dials.Load())
	}
	spans := e.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("spans = %d", len(spans))
	}
	if spans[1].Status.Code != codes.Unset {
		t.Fatalf("successful retry status = %v", spans[1].Status)
	}
}

func TestRegressionRealUpgradeBody(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			if err := c.Close(); err != nil {
				t.Errorf("close upgraded connection: %v", err)
			}
		}()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		if err = rw.Flush(); err != nil {
			t.Error(err)
			return
		}
		var buf [4]byte
		if _, err = io.ReadFull(rw, buf[:]); err != nil {
			t.Error(err)
			return
		}
		if string(buf[:]) != "ping" {
			t.Errorf("upgrade write = %q", buf)
		}
		_, _ = c.Write([]byte("pong"))
	}))
	defer srv.Close()
	base := &http.Transport{}
	defer base.CloseIdleConnections()
	r := request(t, context.Background(), srv.URL)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "test")
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	rwc, ok := res.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("upgrade body = %T", res.Body)
	}
	if _, err = rwc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var buf [4]byte
	if _, err = io.ReadFull(rwc, buf[:]); err != nil || string(buf[:]) != "pong" {
		t.Fatalf("upgrade Read = %q, %v", buf, err)
	}
	if err = rwc.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if onlySpan(t, e).Status.Code != codes.Unset {
		t.Fatal("successful upgrade recorded an error")
	}
}

func TestRegressionUserInformationalResponseError(t *testing.T) {
	for _, mode := range []string{"standalone", "factory", "context"} {
		t.Run(mode, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(103); w.WriteHeader(200) }))
			defer srv.Close()
			base := &http.Transport{}
			defer base.CloseIdleConnections()
			var transport http.RoundTripper
			switch mode {
			case "standalone":
				transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
			case "factory":
				transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(p), otelhttp.WithClientTrace(otelhttptrace.NewClientTrace))
			case "context":
				transport = otelhttp.NewTransport(otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true}), otelhttp.WithTracerProvider(p))
			}
			wantErr := errors.New("user rejected informational response")
			var calls atomic.Int64
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
				calls.Add(1)
				if code != 103 {
					t.Errorf("code = %d", code)
				}
				return wantErr
			}})
			_, err := transport.RoundTrip(request(t, ctx, srv.URL))
			if !errors.Is(err, wantErr) || calls.Load() != 1 {
				t.Fatalf("user error lost: %v, calls=%d", err, calls.Load())
			}
			if onlySpan(t, e).Status.Code != codes.Error {
				t.Fatal("terminal hook error not recorded by span owner")
			}
		})
	}
}

func TestRegressionRealDialError(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	base := &http.Transport{}
	defer base.CloseIdleConnections()
	_, err = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), "http://"+addr))
	if _, ok := errors.AsType[*net.OpError](err); !ok {
		t.Fatalf("expected real dial failure, got %v", err)
	}
	s := onlySpan(t, e)
	if s.Status.Code != codes.Error {
		t.Fatalf("status = %v", s.Status)
	}
	assertAttr(t, attributeMap(s.Attributes), "error.type", "*net.OpError")
	exceptions := 0
	for _, event := range s.Events {
		if event.Name == "exception" {
			exceptions++
		}
	}
	if exceptions != 1 {
		t.Fatalf("exceptions = %d, want 1", exceptions)
	}
}

type writeErrorBody struct {
	readWriteCloserBody
	err error
}

func (b *writeErrorBody) Write(p []byte) (int, error) { return len(p) / 2, b.err }

func TestRegressionUpgradeWriteError(t *testing.T) {
	for _, wantErr := range []error{errors.New("upgrade write failed"), io.EOF} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			body := &writeErrorBody{err: wantErr}
			base := roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 101, Body: body}, nil })
			res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), "http://example.com"))
			if err != nil {
				t.Fatal(err)
			}
			if n, err := res.Body.(io.Writer).Write([]byte("ping")); n != 2 || err != wantErr {
				t.Fatalf("Write = %d, %v", n, err)
			}
			if err = res.Body.Close(); err != nil {
				t.Fatal(err)
			}
			s := onlySpan(t, e)
			if s.Status.Code != codes.Error {
				t.Fatalf("Write failure lost: %v", s.Status)
			}
			assertAttr(t, attributeMap(s.Attributes), "error.type", "*errors.errorString")
		})
	}
}

func TestRegressionCopyReportsBodyError(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	body := &observedBody{readErr: io.ErrUnexpectedEOF}
	base := roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), "http://example.com"))
	if err != nil {
		t.Fatal(err)
	}
	var dst strings.Builder
	if n, err := io.Copy(&dst, res.Body); n != 3 || err != io.ErrUnexpectedEOF || dst.String() != "abc" {
		t.Fatalf("Copy = %d, %q, %v", n, dst.String(), err)
	}
	if err = res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if onlySpan(t, e).Status.Code != codes.Error {
		t.Fatal("io.Copy lost body failure")
	}
}

func TestRegressionStandalonePropagation(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	for _, recording := range []bool{true, false} {
		t.Run(strconv.FormatBool(recording), func(t *testing.T) {
			sampler := sdktrace.NeverSample()
			if recording {
				sampler = sdktrace.AlwaysSample()
			}
			p, e := testProvider(t, sampler)
			receivedContext := make(chan trace.SpanContext, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedContext <- trace.SpanContextFromContext(propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header)))
				w.WriteHeader(204)
			}))
			defer srv.Close()
			base := &http.Transport{}
			defer base.CloseIdleConnections()
			ctx, parent := p.Tracer("test").Start(context.Background(), "parent")
			defer parent.End()
			r := request(t, ctx, srv.URL)
			r.Header.Set("Traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
			original := r.Header.Clone()
			var sent trace.SpanContext
			wrapped := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				sent = trace.SpanContextFromContext(r.Context())
				return base.RoundTrip(r)
			})
			res, err := otelhttptrace.NewTransport(wrapped, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
			if err != nil {
				t.Fatal(err)
			}
			if err = res.Body.Close(); err != nil {
				t.Fatal(err)
			}
			received := <-receivedContext
			if !received.IsValid() || received.TraceID() != sent.TraceID() || received.SpanID() != sent.SpanID() || received.IsSampled() != recording {
				t.Fatalf("received context %v, want created client context %v (recording=%v)", received, sent, recording)
			}
			if sent.SpanID() == parent.SpanContext().SpanID() {
				t.Fatal("propagated parent instead of new client span")
			}
			if !reflect.DeepEqual(r.Header, original) {
				t.Fatalf("original headers changed: %v", r.Header)
			}
			if recording {
				onlySpan(t, e)
			} else if len(e.GetSpans()) != 0 {
				t.Fatal("Drop exported a span")
			}
		})
	}
}

func TestRegressionHTTPStatus(t *testing.T) {
	for _, code := range []int{200, 204, 302, 400, 404, 429, 500, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
			defer srv.Close()
			base := &http.Transport{}
			defer base.CloseIdleConnections()
			r := request(t, context.Background(), srv.URL)
			res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != code {
				t.Fatalf("status code = %d", res.StatusCode)
			}
			if err = res.Body.Close(); err != nil {
				t.Fatal(err)
			}
			s := onlySpan(t, e)
			want := codes.Unset
			if code >= 400 {
				want = codes.Error
			}
			if s.Status.Code != want || s.Status.Description != "" {
				t.Fatalf("span status = %v, want %v with empty description", s.Status, want)
			}
			a := attributeMap(s.Attributes)
			assertAttr(t, a, "http.response.status_code", int64(code))
			if code >= 400 {
				assertAttr(t, a, "error.type", strconv.Itoa(code))
			} else if _, ok := a["error.type"]; ok {
				t.Fatalf("successful request has error.type: %v", a)
			}
			for _, event := range s.Events {
				if event.Name == "exception" {
					t.Fatal("HTTP status created a Go exception")
				}
			}
		})
	}
}

func TestRegressionTruncatedBody(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "20")
		_, _ = io.WriteString(w, "short")
	}))
	defer srv.Close()
	base := &http.Transport{}
	defer base.CloseIdleConnections()
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(res.Body)
	if string(data) != "short" || err != io.ErrUnexpectedEOF {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
	if len(e.GetSpans()) != 0 {
		t.Fatal("Close-owned span ended during Read")
	}
	if err = res.Body.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	s := onlySpan(t, e)
	if s.Status.Code != codes.Error {
		t.Fatalf("span status = %v, lost unexpected EOF", s.Status)
	}
	assertAttr(t, attributeMap(s.Attributes), "error.type", "*errors.errorString")
	assertSpanEvents(t, s.Events, "exception")
}

func TestRegressionIdlePoolFullIsSuccessful(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer srv.Close()
	base := &http.Transport{MaxIdleConnsPerHost: 1}
	defer base.CloseIdleConnections()
	transport := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
	var idleErrors atomic.Int64
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{PutIdleConn: func(err error) {
		if err != nil {
			idleErrors.Add(1)
		}
	}})
	// Keep the first response unread, forcing a second TCP connection. Drain it
	// first to fill the one-entry pool before returning the original connection.
	first, err := transport.RoundTrip(request(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := first.Body.Close(); err != nil {
			t.Errorf("close first response body: %v", err)
		}
	}()
	second, err := transport.RoundTrip(request(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	for _, res := range []*http.Response{second, first} {
		if _, err = io.Copy(io.Discard, res.Body); err != nil {
			t.Fatal(err)
		}
		if err = res.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if idleErrors.Load() != 1 {
		t.Fatalf("idle pool errors = %d, want 1", idleErrors.Load())
	}
	spans := e.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("spans = %d", len(spans))
	}
	for _, s := range spans {
		if s.Status.Code != codes.Unset {
			t.Fatalf("idle pool rejection poisoned successful span: %v", s.Status)
		}
	}
}

func TestRegressionLoopbackProtocolsAndReuse(t *testing.T) {
	for _, protocol := range []string{"http1", "https1", "http2"} {
		t.Run(protocol, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
			if protocol == "http1" {
				srv.Start()
			} else {
				srv.EnableHTTP2 = protocol == "http2"
				srv.StartTLS()
			}
			defer srv.Close()
			base := srv.Client().Transport.(*http.Transport).Clone()
			defer base.CloseIdleConnections()
			checkedBase := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				res, err := base.RoundTrip(r)
				if err == nil {
					_, writerTo := res.Body.(io.WriterTo)
					t.Logf("stdlib %s body %T: WriterTo=%t", protocol, res.Body, writerTo)
				}
				return res, err
			})
			transport := otelhttptrace.NewTransport(checkedBase, otelhttptrace.Config{TracerProvider: p})
			for range 2 {
				res, err := transport.RoundTrip(request(t, context.Background(), srv.URL))
				if err != nil {
					t.Fatal(err)
				}
				wantProto := 1
				if protocol == "http2" {
					wantProto = 2
				}
				if res.ProtoMajor != wantProto {
					t.Fatalf("protocol = %q", res.Proto)
				}
				data, err := io.ReadAll(res.Body)
				if err != nil || string(data) != "ok" {
					t.Fatalf("body = %q, %v", data, err)
				}
				if err = res.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			spans := e.GetSpans()
			if len(spans) != 2 {
				t.Fatalf("spans = %d", len(spans))
			}
			assertAttr(t, attributeMap(spans[0].Attributes), "httptrace.connection.reused", false)
			assertAttr(t, attributeMap(spans[1].Attributes), "httptrace.connection.reused", true)
			for _, s := range spans {
				if s.Status.Code != codes.Unset {
					t.Fatalf("status = %v", s.Status)
				}
				a := attributeMap(s.Attributes)
				if duration, ok := a["httptrace.time_to_first_byte_ns"].(int64); !ok || duration < 0 {
					t.Fatalf("TTFB = %v", a)
				}
			}
			if protocol != "http1" {
				if _, ok := attributeMap(spans[0].Attributes)["httptrace.tls_handshake.duration_ns"]; !ok {
					t.Fatal("TLS duration missing")
				}
			}
		})
	}
}

func TestURLSanitizationDoesNotChangeWireRequest(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	type received struct{ query, user, password string }
	got := make(chan received, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		got <- received{r.URL.RawQuery, u, p}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	base := &http.Transport{}
	defer base.CloseIdleConnections()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("user", "password")
	u.RawQuery = "q=one%2Btwo&X-Amz-Signature=one&X-Amz-Signature=two&X-Amz-Credential=key%2Fscope&X-Amz-Security-Token=secret"
	r := request(t, context.Background(), u.String())
	c := &http.Client{Transport: otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})}
	res, err := c.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	wire := <-got
	if wire != (received{u.RawQuery, "user", "password"}) {
		t.Fatalf("wire request changed: %#v", wire)
	}
	if r.URL.String() != u.String() {
		t.Fatal("original request URL mutated")
	}
	full := attributeMap(onlySpan(t, e).Attributes)["url.full"].(string)
	if strings.Contains(full, "user") || strings.Contains(full, "password") || strings.Contains(full, "=one&") || strings.Contains(full, "=two&") || strings.Contains(full, "key%2Fscope") || strings.Contains(full, "=secret") {
		t.Fatalf("credentials leaked: %s", full)
	}
}

func TestStandardsEmptyPortMatchesHTTPDial(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "example.test" {
			t.Errorf("wire Host = %q", r.Host)
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	var dialAddress string
	base := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialAddress = addr
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}}
	defer base.CloseIdleConnections()
	r := request(t, context.Background(), "http://example.test/path")
	r.URL.Host = "example.test:"
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if dialAddress != "example.test:80" || r.URL.Host != "example.test:" {
		t.Fatalf("dial = %q, original authority = %q", dialAddress, r.URL.Host)
	}
	assertAttr(t, attributeMap(onlySpan(t, e).Attributes), "server.port", int64(80))
}

func TestStandardsTCP4HookRecordsTCPTransport(t *testing.T) {
	p, e := testProvider(t, sdktrace.AlwaysSample())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	base := &http.Transport{DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp4", addr)
	}}
	defer base.CloseIdleConnections()
	res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, context.Background(), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	closeErr := res.Body.Close()
	if err != nil || closeErr != nil || string(body) != "ok" {
		t.Fatalf("body = %q, read = %v, close = %v", body, err, closeErr)
	}
	events := onlySpan(t, e).Events
	for _, name := range []string{"httptrace.connect_start", "httptrace.connect"} {
		found := false
		for _, event := range events {
			if event.Name == name {
				found = true
				assertAttr(t, attributeMap(event.Attributes), "network.transport", "tcp")
			}
		}
		if !found {
			t.Fatalf("missing real dial event %q", name)
		}
	}
}
