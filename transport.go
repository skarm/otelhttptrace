package otelhttptrace

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const tracerName = "github.com/skarm/otelhttptrace"

var clientSpanStartOption = trace.WithSpanKind(trace.SpanKindClient)

const (
	dnsDurationKey             = attribute.Key("httptrace.dns.duration_ns")
	connectDurationKey         = attribute.Key("httptrace.connect.duration_ns")
	tlsHandshakeDurationKey    = attribute.Key("httptrace.tls_handshake.duration_ns")
	timeToFirstByteKey         = attribute.Key("httptrace.time_to_first_byte_ns")
	connectionReusedKey        = attribute.Key("httptrace.connection.reused")
	connectionWasIdleKey       = attribute.Key("httptrace.connection.was_idle")
	connectionIdleDurationKey  = attribute.Key("httptrace.connection.idle_ns")
	connectionRemoteAddressKey = attribute.Key("httptrace.connection.remote.address")
	connectionRemotePortKey    = attribute.Key("httptrace.connection.remote.port")
)

// Transport instruments outgoing HTTP requests with OpenTelemetry spans and
// net/http/httptrace timing events.
//
// A Transport may be used concurrently when its base RoundTripper and
// SpanNameFormatter are safe for concurrent use.
type Transport struct {
	base              http.RoundTripper
	tracer            trace.Tracer
	attrs             []attribute.KeyValue
	spanNameFormatter SpanNameFormatter
	spanFromContext   bool
	noopTracer        bool
}

var _ http.RoundTripper = (*Transport)(nil)

// NewTransport returns a Transport that records httptrace phases as span events
// and attributes.
//
// If base is nil, http.DefaultTransport is used. By default the returned
// Transport creates a client span for each request and ends it when the
// response body is closed, or immediately when RoundTrip returns an error or a
// response without a body. If cfg.SpanFromContext is true, it records on the
// request's active span instead and does not create or end spans.
func NewTransport(base http.RoundTripper, cfg Config) *Transport {
	cfg = cfg.Resolve()
	if base == nil {
		base = http.DefaultTransport
	}

	return &Transport{
		base:              base,
		tracer:            cfg.TracerProvider.Tracer(tracerName),
		attrs:             cfg.Attributes,
		spanNameFormatter: cfg.SpanNameFormatter,
		spanFromContext:   cfg.SpanFromContext,
		noopTracer:        isNoopTracerProvider(cfg.TracerProvider),
	}
}

// RoundTrip executes req through the base RoundTripper while recording
// httptrace events on the configured span.
//
// RoundTrip returns an error for a nil request. In standalone mode, errors from
// the base RoundTripper and response body Close are recorded on the span and the
// original errors are returned unchanged.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("otelhttptrace: nil request")
	}

	if t.spanFromContext {
		ctx := httptrace.WithClientTrace(req.Context(), NewClientTrace(req.Context()))
		return t.base.RoundTrip(req.WithContext(ctx))
	}

	var (
		ctx  context.Context
		span trace.Span
	)

	if t.noopTracer {
		ctx, span = t.tracer.Start(req.Context(), t.spanNameFormatter(req), clientSpanStartOption)
	} else {
		attrs := requestAttributes(req, t.attrs)
		ctx, span = t.tracer.Start(req.Context(), t.spanNameFormatter(req), clientSpanStartOption, trace.WithAttributes(attrs...))
	}

	if !span.IsRecording() {
		req = req.WithContext(ctx)
		res, err := t.base.RoundTrip(req)
		span.End()
		return res, err
	}

	ctx = httptrace.WithClientTrace(ctx, NewClientTrace(ctx))
	req = req.WithContext(ctx)

	res, err := t.base.RoundTrip(req)
	if err != nil {
		recordSpanError(span, err)
		span.End()
		return res, err
	}

	if res != nil {
		span.SetAttributes(attribute.Int("http.response.status_code", res.StatusCode))

		if res.Body != nil && res.Body != http.NoBody {
			res.Body = newSpanBody(res.Body, func(err error) {
				if err != nil {
					recordSpanError(span, err)
				}
				span.End()
			})

			return res, nil
		}
	}

	span.End()
	return res, nil
}

func isNoopTracerProvider(provider trace.TracerProvider) bool {
	_, ok := provider.(noop.TracerProvider)
	return ok
}

type traceRecorder struct {
	span trace.Span

	startNS        int64
	dnsStartNS     atomic.Int64
	connectStartNS atomic.Int64
	tlsStartNS     atomic.Int64
}

func newTraceRecorder(span trace.Span) *traceRecorder {
	return &traceRecorder{
		span:    span,
		startNS: time.Now().UnixNano(),
	}
}

// NewClientTrace returns an httptrace.ClientTrace that records HTTP client
// phase events and timing attributes on the span active in ctx.
//
// It is intended for use with instrumentation that already owns the HTTP client
// span, such as otelhttp.WithClientTrace. If ctx contains no recording span,
// the returned trace is still valid but records no telemetry.
func NewClientTrace(ctx context.Context) *httptrace.ClientTrace {
	return newTraceRecorder(trace.SpanFromContext(ctx)).clientTrace()
}

func (r *traceRecorder) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			r.markDNSStart(info.Host)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			r.markDNSDone(info)
		},
		ConnectStart: func(network, addr string) {
			r.markConnectStart(network, addr)
		},
		ConnectDone: func(network, addr string, err error) {
			r.markConnectDone(network, addr, err)
		},
		TLSHandshakeStart: func() {
			r.markTLSStart()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			r.markTLSDone(state, err)
		},
		GotConn: func(info httptrace.GotConnInfo) {
			r.markGotConn(info)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			r.markWroteRequest(info.Err)
		},
		GotFirstResponseByte: func() {
			r.markGotFirstResponseByte()
		},
		PutIdleConn: func(err error) {
			r.markPutIdleConn(err)
		},
	}
}

func (r *traceRecorder) markDNSStart(host string) {
	now := time.Now()
	r.dnsStartNS.Store(now.UnixNano())

	r.span.AddEvent("httptrace.dns_start",
		trace.WithTimestamp(now),
		trace.WithAttributes(attribute.String("server.address", host)),
	)
}

func (r *traceRecorder) markDNSDone(info httptrace.DNSDoneInfo) {
	var attrs []attribute.KeyValue
	now := time.Now()

	if startNS := r.dnsStartNS.Load(); startNS != 0 {
		duration := now.Sub(time.Unix(0, startNS))
		attrs = append(attrs, dnsDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(dnsDurationKey.Int64(duration.Nanoseconds()))
	}

	if info.Err != nil {
		attrs = append(attrs, attribute.String("error.type", fmt.Sprintf("%T", info.Err)))
		recordSpanError(r.span, info.Err)
	}
	if len(info.Addrs) > 0 {
		addrs := make([]string, 0, len(info.Addrs))
		for _, addr := range info.Addrs {
			addrs = append(addrs, addr.String())
		}
		attrs = append(attrs, attribute.StringSlice("httptrace.dns.addresses", addrs))
	}

	r.span.AddEvent("httptrace.dns", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markConnectStart(network, addr string) {
	now := time.Now()
	r.connectStartNS.Store(now.UnixNano())

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, attribute.String("network.transport", network))
	attrs = append(attrs, networkPeerAttributes(addr)...)

	r.span.AddEvent("httptrace.connect_start",
		trace.WithTimestamp(now),
		trace.WithAttributes(attrs...),
	)
}

func (r *traceRecorder) markConnectDone(network, addr string, err error) {
	now := time.Now()
	attrs := []attribute.KeyValue{
		attribute.String("network.transport", network),
	}
	attrs = append(attrs, networkPeerAttributes(addr)...)

	if startNS := r.connectStartNS.Load(); startNS != 0 {
		duration := now.Sub(time.Unix(0, startNS))
		attrs = append(attrs, connectDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(connectDurationKey.Int64(duration.Nanoseconds()))
	}

	if err != nil {
		attrs = append(attrs, attribute.String("error.type", fmt.Sprintf("%T", err)))
		recordSpanError(r.span, err)
	}

	r.span.AddEvent("httptrace.connect", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markTLSStart() {
	now := time.Now()
	r.tlsStartNS.Store(now.UnixNano())

	r.span.AddEvent("httptrace.tls_handshake_start", trace.WithTimestamp(now))
}

func (r *traceRecorder) markTLSDone(state tls.ConnectionState, err error) {
	var attrs []attribute.KeyValue
	now := time.Now()

	if startNS := r.tlsStartNS.Load(); startNS != 0 {
		duration := now.Sub(time.Unix(0, startNS))
		attrs = append(attrs, tlsHandshakeDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(tlsHandshakeDurationKey.Int64(duration.Nanoseconds()))
	}

	if state.Version != 0 {
		attrs = append(attrs, attribute.String("tls.protocol.version", tlsVersion(state.Version)))
	}

	if err != nil {
		attrs = append(attrs, attribute.String("error.type", fmt.Sprintf("%T", err)))
		recordSpanError(r.span, err)
	}

	r.span.AddEvent("httptrace.tls_handshake", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markGotConn(info httptrace.GotConnInfo) {
	now := time.Now()
	attrs := []attribute.KeyValue{
		connectionReusedKey.Bool(info.Reused),
		connectionWasIdleKey.Bool(info.WasIdle),
	}

	if info.IdleTime > 0 {
		attrs = append(attrs, connectionIdleDurationKey.Int64(info.IdleTime.Nanoseconds()))
	}

	if info.Conn != nil {
		attrs = append(attrs, remoteAddrAttributes(info.Conn.RemoteAddr())...)
	}

	r.span.SetAttributes(attrs...)
	r.span.AddEvent("httptrace.got_conn", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markWroteRequest(err error) {
	var attrs []attribute.KeyValue
	now := time.Now()

	if err != nil {
		attrs = append(attrs, attribute.String("error.type", fmt.Sprintf("%T", err)))
		recordSpanError(r.span, err)
	}

	r.span.AddEvent("httptrace.wrote_request", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markGotFirstResponseByte() {
	now := time.Now()
	duration := now.Sub(time.Unix(0, r.startNS))

	attr := timeToFirstByteKey.Int64(duration.Nanoseconds())
	r.span.SetAttributes(attr)
	r.span.AddEvent("httptrace.got_first_response_byte", trace.WithTimestamp(now), trace.WithAttributes(attr))
}

func (r *traceRecorder) markPutIdleConn(err error) {
	var attrs []attribute.KeyValue
	now := time.Now()

	if err != nil {
		attrs = append(attrs, attribute.String("error.type", fmt.Sprintf("%T", err)))
		recordSpanError(r.span, err)
	}

	r.span.AddEvent("httptrace.put_idle_conn", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

type spanBody struct {
	io.ReadCloser
	once sync.Once
	end  func(error)
}

type spanReadWriteBody struct {
	io.ReadWriteCloser
	once sync.Once
	end  func(error)
}

func newSpanBody(body io.ReadCloser, end func(error)) io.ReadCloser {
	if rwc, ok := body.(io.ReadWriteCloser); ok {
		return &spanReadWriteBody{
			ReadWriteCloser: rwc,
			end:             end,
		}
	}

	return &spanBody{
		ReadCloser: body,
		end:        end,
	}
}

func (b *spanBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		b.end(err)
	})
	return err
}

func (b *spanReadWriteBody) Close() error {
	err := b.ReadWriteCloser.Close()
	b.once.Do(func() {
		b.end(err)
	})
	return err
}

func requestAttributes(req *http.Request, extra []attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(extra)+5)
	attrs = append(attrs, extra...)
	attrs = append(attrs,
		attribute.String("http.request.method", req.Method),
		attribute.String("url.full", req.URL.String()),
		attribute.String("url.scheme", req.URL.Scheme),
	)

	if host := req.URL.Hostname(); host != "" {
		attrs = append(attrs, attribute.String("server.address", host))
	}

	if port := requestPort(req); port > 0 {
		attrs = append(attrs, attribute.Int("server.port", port))
	}

	return attrs
}

func requestPort(req *http.Request) int {
	if req == nil || req.URL == nil {
		return 0
	}

	if port, ok := explicitPort(req.URL.Host); ok {
		n, err := strconv.Atoi(port)
		if err != nil {
			return 0
		}
		return n
	}

	switch req.URL.Scheme {
	case "http":
		return 80
	case "https":
		return 443
	default:
		return 0
	}
}

func explicitPort(host string) (string, bool) {
	if host == "" {
		return "", false
	}
	if host[0] == '[' {
		end := strings.IndexByte(host, ']')
		if end >= 0 && end+1 < len(host) && host[end+1] == ':' {
			return host[end+2:], true
		}
		return "", false
	}
	first := strings.IndexByte(host, ':')
	if first >= 0 && first == strings.LastIndexByte(host, ':') {
		return host[first+1:], true
	}
	return "", false
}

func networkPeerAttributes(addr string) []attribute.KeyValue {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return []attribute.KeyValue{
			attribute.String("network.peer.address", addr),
		}
	}

	attrs := []attribute.KeyValue{
		attribute.String("network.peer.address", host),
	}

	if n, err := strconv.Atoi(port); err == nil {
		attrs = append(attrs, attribute.Int("network.peer.port", n))
	}

	return attrs
}

func remoteAddrAttributes(addr net.Addr) []attribute.KeyValue {
	if addr == nil {
		return nil
	}

	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return []attribute.KeyValue{
			connectionRemoteAddressKey.String(tcpAddr.IP.String()),
			connectionRemotePortKey.Int(tcpAddr.Port),
		}
	}

	return []attribute.KeyValue{
		connectionRemoteAddressKey.String(addr.String()),
	}
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func tlsVersion(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	default:
		return fmt.Sprintf("0x%x", version)
	}
}
