package otelhttptrace

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http/httptrace"
	"net/textproto"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	dnsDurationKey             = attribute.Key("httptrace.dns.duration_ns")
	dnsAddressesKey            = attribute.Key("httptrace.dns.addresses")
	getConnDurationKey         = attribute.Key("httptrace.get_conn.duration_ns")
	connectDurationKey         = attribute.Key("httptrace.connect.duration_ns")
	tlsHandshakeDurationKey    = attribute.Key("httptrace.tls_handshake.duration_ns")
	timeToFirstByteKey         = attribute.Key("httptrace.time_to_first_byte_ns")
	connectionReusedKey        = attribute.Key("httptrace.connection.reused")
	connectionWasIdleKey       = attribute.Key("httptrace.connection.was_idle")
	connectionIdleDurationKey  = attribute.Key("httptrace.connection.idle_ns")
	connectionRemoteAddressKey = attribute.Key("httptrace.connection.remote.address")
	connectionRemotePortKey    = attribute.Key("httptrace.connection.remote.port")
)

type traceRecorder struct {
	span trace.Span
	now  func() time.Time

	start      time.Time
	mu         sync.Mutex
	dns        phaseStart
	getConn    phaseStart
	connectMap map[string]phaseStart
	tls        phaseStart
}

// The hooks do not identify concurrent operations with the same target. Keep
// only outstanding state and omit durations for an overlapping group, rather
// than claiming that a completion belongs to an arbitrarily chosen start.
// All access is protected by traceRecorder.mu.
type phaseStart struct {
	start     time.Time
	pending   int
	ambiguous bool
}

func (p *phaseStart) begin(now time.Time) {
	if p.pending == 0 {
		p.start = now
	} else {
		p.ambiguous = true
	}
	p.pending++
}

func (p *phaseStart) finish() time.Time {
	if p.pending == 0 {
		return time.Time{}
	}
	p.pending--
	var start time.Time
	if !p.ambiguous {
		start = p.start
	}
	if p.pending == 0 {
		*p = phaseStart{}
	}
	return start
}

func newTraceRecorder(span trace.Span) *traceRecorder {
	return newTraceRecorderWithClock(span, time.Now)
}

func newTraceRecorderWithClock(span trace.Span, now func() time.Time) *traceRecorder {
	if now == nil {
		now = time.Now
	}
	return &traceRecorder{
		span:  span,
		now:   now,
		start: now(),
	}
}

// NewClientTrace returns an httptrace.ClientTrace that records HTTP client
// phase events and timing attributes on the span active in ctx.
//
// It is intended for use with instrumentation that already owns the HTTP client
// span, such as otelhttp.WithClientTrace. If ctx contains no recording span,
// the returned trace is still valid but records no telemetry.
func NewClientTrace(ctx context.Context) *httptrace.ClientTrace {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		// WithClientTrace rejects nil and mutates its argument during composition.
		return &httptrace.ClientTrace{}
	}
	return newTraceRecorder(span).clientTrace()
}

func (r *traceRecorder) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			r.markGetConnStart(hostPort)
		},
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
		WroteHeaders: func() {
			r.markWroteHeaders()
		},
		Wait100Continue: func() {
			r.markWait100Continue()
		},
		Got100Continue: func() {
			r.markGot100Continue()
		},
		Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
			return r.markGot1xxResponse(code, header)
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
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.dns.begin(now)
	r.mu.Unlock()

	r.span.AddEvent("httptrace.dns_start",
		trace.WithTimestamp(now),
		trace.WithAttributes(semconv.ServerAddress(host)),
	)
}

func (r *traceRecorder) markDNSDone(info httptrace.DNSDoneInfo) {
	if !r.span.IsRecording() {
		return
	}
	var attrs []attribute.KeyValue
	now := r.now()

	if start := r.popDNSStart(); !start.IsZero() {
		duration := now.Sub(start)
		attrs = append(attrs, dnsDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(dnsDurationKey.Int64(duration.Nanoseconds()))
	}

	if info.Err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", info.Err)))
	}
	if len(info.Addrs) > 0 {
		addrs := make([]string, 0, len(info.Addrs))
		for _, addr := range info.Addrs {
			addrs = append(addrs, addr.String())
		}
		attrs = append(attrs, dnsAddressesKey.StringSlice(addrs))
	}

	r.span.AddEvent("httptrace.dns", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) popDNSStart() time.Time {
	r.mu.Lock()
	start := r.dns.finish()
	r.mu.Unlock()
	return start
}

func (r *traceRecorder) markGetConnStart(hostPort string) {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.getConn.begin(now)
	r.mu.Unlock()

	r.span.AddEvent("httptrace.get_conn_start",
		trace.WithTimestamp(now),
		trace.WithAttributes(hostPortAttributes(nil, hostPort, semconv.ServerAddressKey, semconv.ServerPortKey)...),
	)
}

func (r *traceRecorder) popGetConnStart() time.Time {
	r.mu.Lock()
	start := r.getConn.finish()
	r.mu.Unlock()
	return start
}

func (r *traceRecorder) markConnectStart(network, addr string) {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	r.mu.Lock()
	if r.connectMap == nil {
		r.connectMap = make(map[string]phaseStart)
	}
	key := connectKey(network, addr)
	phase := r.connectMap[key]
	phase.begin(now)
	r.connectMap[key] = phase
	r.mu.Unlock()

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, semconv.NetworkTransportKey.String(networkTransport(network)))
	attrs = networkPeerAttributes(attrs, addr)

	r.span.AddEvent("httptrace.connect_start",
		trace.WithTimestamp(now),
		trace.WithAttributes(attrs...),
	)
}

func (r *traceRecorder) markConnectDone(network, addr string, err error) {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	attrs := make([]attribute.KeyValue, 1, 5)
	attrs[0] = semconv.NetworkTransportKey.String(networkTransport(network))
	attrs = networkPeerAttributes(attrs, addr)

	if start := r.popConnectStart(network, addr); !start.IsZero() {
		duration := now.Sub(start)
		attrs = append(attrs, connectDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(connectDurationKey.Int64(duration.Nanoseconds()))
	}

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
	}

	r.span.AddEvent("httptrace.connect", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) popConnectStart(network, addr string) time.Time {
	r.mu.Lock()
	key := connectKey(network, addr)
	phase := r.connectMap[key]
	start := phase.finish()
	if phase.pending == 0 {
		delete(r.connectMap, key)
	} else {
		r.connectMap[key] = phase
	}
	r.mu.Unlock()
	return start
}

func connectKey(network, addr string) string {
	return network + "\x00" + addr
}

func networkTransport(network string) string {
	// Go's network names also encode address family or socket mode. Semconv
	// network.transport describes the protocol; attempt keys keep the Go name.
	switch network {
	case "tcp4", "tcp6":
		return "tcp"
	case "udp4", "udp6":
		return "udp"
	case "unixgram", "unixpacket":
		return "unix"
	default:
		return network
	}
}

func (r *traceRecorder) markTLSStart() {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.tls.begin(now)
	r.mu.Unlock()

	r.span.AddEvent("httptrace.tls_handshake_start", trace.WithTimestamp(now))
}

func (r *traceRecorder) markTLSDone(state tls.ConnectionState, err error) {
	if !r.span.IsRecording() {
		return
	}
	var attrs []attribute.KeyValue
	now := r.now()

	r.mu.Lock()
	start := r.tls.finish()
	r.mu.Unlock()
	if !start.IsZero() {
		duration := now.Sub(start)
		attrs = append(attrs, tlsHandshakeDurationKey.Int64(duration.Nanoseconds()))
		r.span.SetAttributes(tlsHandshakeDurationKey.Int64(duration.Nanoseconds()))
	}

	if state.Version != 0 {
		attrs = append(attrs, semconv.TLSProtocolVersion(tlsVersion(state.Version)))
	}

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
	}

	r.span.AddEvent("httptrace.tls_handshake", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markGotConn(info httptrace.GotConnInfo) {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	start := r.popGetConnStart()
	var remoteAttrs []attribute.KeyValue
	if info.Conn != nil {
		remoteAttrs = remoteAddrAttributes(info.Conn.RemoteAddr())
	}
	capacity := 2 + len(remoteAttrs)
	if !start.IsZero() {
		capacity++
	}
	if info.IdleTime > 0 {
		capacity++
	}
	attrs := make([]attribute.KeyValue, 0, capacity)
	attrs = append(attrs,
		connectionReusedKey.Bool(info.Reused),
		connectionWasIdleKey.Bool(info.WasIdle),
	)

	if !start.IsZero() {
		duration := now.Sub(start)
		attrs = append(attrs, getConnDurationKey.Int64(duration.Nanoseconds()))
	}

	if info.IdleTime > 0 {
		attrs = append(attrs, connectionIdleDurationKey.Int64(info.IdleTime.Nanoseconds()))
	}

	attrs = append(attrs, remoteAttrs...)

	r.span.SetAttributes(attrs...)
	r.span.AddEvent("httptrace.got_conn", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markWroteHeaders() {
	if !r.span.IsRecording() {
		return
	}
	r.span.AddEvent("httptrace.wrote_headers", trace.WithTimestamp(r.now()))
}

func (r *traceRecorder) markWait100Continue() {
	if !r.span.IsRecording() {
		return
	}
	r.span.AddEvent("httptrace.wait_100_continue", trace.WithTimestamp(r.now()))
}

func (r *traceRecorder) markGot100Continue() {
	if !r.span.IsRecording() {
		return
	}
	r.span.AddEvent("httptrace.got_100_continue", trace.WithTimestamp(r.now()))
}

func (r *traceRecorder) markGot1xxResponse(code int, _ textproto.MIMEHeader) error {
	if !r.span.IsRecording() {
		return nil
	}
	r.span.AddEvent("httptrace.got_1xx_response",
		trace.WithTimestamp(r.now()),
		trace.WithAttributes(semconv.HTTPResponseStatusCode(code)),
	)
	return nil
}

func (r *traceRecorder) markWroteRequest(err error) {
	if !r.span.IsRecording() {
		return
	}
	var attrs []attribute.KeyValue
	now := r.now()

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
	}

	r.span.AddEvent("httptrace.wrote_request", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func (r *traceRecorder) markGotFirstResponseByte() {
	if !r.span.IsRecording() {
		return
	}
	now := r.now()
	duration := now.Sub(r.start)

	attr := timeToFirstByteKey.Int64(duration.Nanoseconds())
	r.span.SetAttributes(attr)
	r.span.AddEvent("httptrace.got_first_response_byte", trace.WithTimestamp(now), trace.WithAttributes(attr))
}

func (r *traceRecorder) markPutIdleConn(err error) {
	if !r.span.IsRecording() {
		return
	}
	var attrs []attribute.KeyValue
	now := r.now()

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err)))
	}

	r.span.AddEvent("httptrace.put_idle_conn", trace.WithTimestamp(now), trace.WithAttributes(attrs...))
}

func networkPeerAttributes(dst []attribute.KeyValue, addr string) []attribute.KeyValue {
	return hostPortAttributes(dst, addr, semconv.NetworkPeerAddressKey, semconv.NetworkPeerPortKey)
}

func hostPortAttributes(dst []attribute.KeyValue, hostPort string, addressKey, portKey attribute.Key) []attribute.KeyValue {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return append(dst, addressKey.String(hostPort))
	}

	if n, err := strconv.Atoi(port); err == nil {
		return append(dst, addressKey.String(host), portKey.Int(n))
	}
	return append(dst, addressKey.String(host))
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
