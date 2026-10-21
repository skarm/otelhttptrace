package otelhttptrace

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
)

func TestTraceRecorderOmitsOverlappingDNSAndMatchesDistinctConnectTargets(t *testing.T) {
	span := newRecordingSpan("trace")
	clock := steppedClock(
		time.Unix(0, 0),
		10*time.Millisecond,
		20*time.Millisecond,
		50*time.Millisecond,
		80*time.Millisecond,
		90*time.Millisecond,
		100*time.Millisecond,
		150*time.Millisecond,
		180*time.Millisecond,
	)
	recorder := newTraceRecorderWithClock(span, clock)

	recorder.markDNSStart("a.example.com")
	recorder.markDNSStart("b.example.com")
	recorder.markDNSDone(httptrace.DNSDoneInfo{
		Addrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}},
	})
	recorder.markDNSDone(httptrace.DNSDoneInfo{
		Addrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.2")}},
	})

	recorder.markConnectStart("tcp", "127.0.0.1:443")
	recorder.markConnectStart("tcp", "[::1]:443")
	recorder.markConnectDone("tcp", "127.0.0.1:443", nil)
	recorder.markConnectDone("tcp", "[::1]:443", nil)

	events := span.eventsByName("httptrace.dns")
	if len(events) != 2 {
		t.Fatalf("dns events = %d, want 2", len(events))
	}
	for _, event := range events {
		if _, ok := attrMap(event.attrs)["httptrace.dns.duration_ns"]; ok {
			t.Fatal("DNS completions have no host or operation ID; overlapping durations must be omitted")
		}
	}

	events = span.eventsByName("httptrace.connect")
	if len(events) != 2 {
		t.Fatalf("connect events = %d, want 2", len(events))
	}
	assertAttr(t, attrMap(events[0].attrs), "network.peer.address", "127.0.0.1")
	assertAttr(t, attrMap(events[0].attrs), "httptrace.connect.duration_ns", int64(60*time.Millisecond))
	assertAttr(t, attrMap(events[1].attrs), "network.peer.address", "::1")
	assertAttr(t, attrMap(events[1].attrs), "httptrace.connect.duration_ns", int64(80*time.Millisecond))
}

func TestTraceRecorderMatchesManyConnectDurationsOutOfOrder(t *testing.T) {
	span := newRecordingSpan("trace")
	clock := steppedClock(
		time.Unix(0, 0),
		10*time.Millisecond,
		20*time.Millisecond,
		30*time.Millisecond,
		40*time.Millisecond,
		100*time.Millisecond,
		130*time.Millisecond,
		180*time.Millisecond,
		220*time.Millisecond,
	)
	recorder := newTraceRecorderWithClock(span, clock)

	recorder.markConnectStart("tcp", "10.0.0.1:443")
	recorder.markConnectStart("tcp", "10.0.0.2:443")
	recorder.markConnectStart("tcp", "[::1]:443")
	recorder.markConnectStart("tcp", "example.com:443")

	recorder.markConnectDone("tcp", "[::1]:443", nil)
	recorder.markConnectDone("tcp", "10.0.0.1:443", nil)
	recorder.markConnectDone("tcp", "example.com:443", nil)
	recorder.markConnectDone("tcp", "10.0.0.2:443", nil)

	want := map[string]int64{
		"10.0.0.1":    int64(120 * time.Millisecond),
		"10.0.0.2":    int64(200 * time.Millisecond),
		"::1":         int64(70 * time.Millisecond),
		"example.com": int64(140 * time.Millisecond),
	}

	events := span.eventsByName("httptrace.connect")
	if len(events) != len(want) {
		t.Fatalf("connect events = %d, want %d", len(events), len(want))
	}
	for _, event := range events {
		attrs := attrMap(event.attrs)
		host, ok := attrs["network.peer.address"].(string)
		if !ok {
			t.Fatalf("connect event attrs missing network.peer.address: %v", attrs)
		}
		assertAttr(t, attrs, "httptrace.connect.duration_ns", want[host])
		delete(want, host)
	}
	if len(want) != 0 {
		t.Fatalf("missing connect events for hosts: %v", want)
	}
}

func TestClientTraceCallbacksCanRunConcurrently(t *testing.T) {
	const attempts = 64

	span := newRecordingSpan("trace")
	clientTrace := newTraceRecorder(span).clientTrace()

	runConcurrent := func(fn func(int)) {
		t.Helper()

		var wg sync.WaitGroup
		for i := range attempts {
			wg.Go(func() {
				fn(i)
			})
		}
		wg.Wait()
	}

	runConcurrent(func(i int) {
		clientTrace.DNSStart(httptrace.DNSStartInfo{Host: connectTestAddr(i)})
	})
	runConcurrent(func(i int) {
		clientTrace.DNSDone(httptrace.DNSDoneInfo{
			Addrs: []net.IPAddr{{IP: net.IPv4(127, 0, 0, byte(i+1))}},
		})
	})

	runConcurrent(func(i int) {
		clientTrace.GetConn(connectTestAddr(i))
	})
	runConcurrent(func(i int) {
		clientTrace.ConnectStart("tcp", connectTestAddr(i))
	})
	runConcurrent(func(i int) {
		clientTrace.ConnectDone("tcp", connectTestAddr(i), nil)
	})
	runConcurrent(func(i int) {
		clientTrace.GotConn(httptrace.GotConnInfo{
			Conn: stubConn{remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, byte(i+1)), Port: 443}},
		})
	})
	runConcurrent(func(int) { clientTrace.TLSHandshakeStart() })
	runConcurrent(func(int) { clientTrace.TLSHandshakeDone(tls.ConnectionState{}, nil) })

	runConcurrent(func(int) {
		clientTrace.WroteHeaders()
		clientTrace.Wait100Continue()
		clientTrace.Got100Continue()
		if err := clientTrace.Got1xxResponse(http.StatusEarlyHints, nil); err != nil {
			t.Errorf("Got1xxResponse error = %v", err)
		}
		clientTrace.WroteRequest(httptrace.WroteRequestInfo{})
		clientTrace.GotFirstResponseByte()
		clientTrace.PutIdleConn(nil)
	})

	assertEventCount(t, span, "httptrace.dns_start", attempts)
	assertEventCount(t, span, "httptrace.dns", attempts)
	assertEventCount(t, span, "httptrace.get_conn_start", attempts)
	assertEventCount(t, span, "httptrace.connect_start", attempts)
	assertEventCount(t, span, "httptrace.connect", attempts)
	assertEventCount(t, span, "httptrace.got_conn", attempts)
	assertEventCount(t, span, "httptrace.tls_handshake_start", attempts)
	assertEventCount(t, span, "httptrace.tls_handshake", attempts)
	assertEventCount(t, span, "httptrace.wrote_headers", attempts)
	assertEventCount(t, span, "httptrace.wait_100_continue", attempts)
	assertEventCount(t, span, "httptrace.got_100_continue", attempts)
	assertEventCount(t, span, "httptrace.got_1xx_response", attempts)
	assertEventCount(t, span, "httptrace.wrote_request", attempts)
	assertEventCount(t, span, "httptrace.got_first_response_byte", attempts)
	assertEventCount(t, span, "httptrace.put_idle_conn", attempts)

	for _, event := range span.eventsByName("httptrace.connect") {
		if _, ok := attrMap(event.attrs)["httptrace.connect.duration_ns"]; !ok {
			t.Fatalf("connect event missing duration: %v", event.attrs)
		}
	}
	for _, event := range span.eventsByName("httptrace.dns") {
		if _, ok := attrMap(event.attrs)["httptrace.dns.duration_ns"]; ok {
			t.Fatalf("ambiguous DNS event claimed a duration: %v", event.attrs)
		}
	}
	for _, event := range span.eventsByName("httptrace.tls_handshake") {
		if _, ok := attrMap(event.attrs)["httptrace.tls_handshake.duration_ns"]; ok {
			t.Fatalf("ambiguous TLS event claimed a duration: %v", event.attrs)
		}
	}
}

func TestTraceRecorderRecordsPhaseErrorsWithoutSettingOperationStatus(t *testing.T) {
	span := newRecordingSpan("trace")
	recorder := newTraceRecorder(span)
	wantErr := errors.New("phase failed")

	recorder.markDNSDone(httptrace.DNSDoneInfo{Err: wantErr})
	recorder.markConnectDone("tcp", "example.com:443", wantErr)
	recorder.markTLSDone(tls.ConnectionState{Version: tls.VersionTLS12}, wantErr)
	recorder.markPutIdleConn(wantErr)

	if span.statusCode != codes.Unset {
		t.Fatalf("phase error changed operation status = %v", span.statusCode)
	}
	if len(span.recordedErrors) != 0 {
		t.Fatalf("phase errors created exceptions = %d, want 0", len(span.recordedErrors))
	}
	if !span.hasEvent("httptrace.dns") || !span.hasEvent("httptrace.connect") || !span.hasEvent("httptrace.tls_handshake") || !span.hasEvent("httptrace.put_idle_conn") {
		t.Fatal("missing error phase event")
	}

	tlsAttrs := span.eventAttrs("httptrace.tls_handshake")
	assertAttr(t, tlsAttrs, "tls.protocol.version", "1.2")
	for _, name := range []string{"httptrace.dns", "httptrace.connect", "httptrace.tls_handshake", "httptrace.put_idle_conn"} {
		assertAttr(t, span.eventAttrs(name), "error.type", "*errors.errorString")
	}
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

func TestNetworkAndRemoteAddressAttributes(t *testing.T) {
	attrs := attrMap(networkPeerAttributes(nil, "example.com:8443"))
	assertAttr(t, attrs, "network.peer.address", "example.com")
	assertAttr(t, attrs, "network.peer.port", int64(8443))

	attrs = attrMap(networkPeerAttributes(nil, "example.com"))
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

// Semconv 1.43.0 network.transport requires a well-known value when it applies.
func TestClientTraceNetworkTransportUsesProtocolNames(t *testing.T) {
	for _, tc := range []struct{ network, transport string }{
		{"tcp", "tcp"}, {"tcp4", "tcp"}, {"tcp6", "tcp"},
		{"udp", "udp"}, {"udp4", "udp"}, {"udp6", "udp"},
		{"unix", "unix"}, {"unixgram", "unix"}, {"unixpacket", "unix"},
		{"pipe", "pipe"}, {"quic", "quic"}, {"custom", "custom"},
	} {
		t.Run(tc.network, func(t *testing.T) {
			span := newRecordingSpan("network")
			r := newTraceRecorder(span)
			r.markConnectStart(tc.network, "127.0.0.1:80")
			r.markConnectDone(tc.network, "127.0.0.1:80", nil)
			for _, name := range []string{"httptrace.connect_start", "httptrace.connect"} {
				assertAttr(t, span.eventAttrs(name), "network.transport", tc.transport)
			}
		})
	}
}

func TestClientTraceKeepsNetworkIdentityWhenMatchingAttempts(t *testing.T) {
	span := newRecordingSpan("network")
	r := newTraceRecorderWithClock(span, steppedClock(time.Now(), time.Millisecond, 2*time.Millisecond, 5*time.Millisecond, 9*time.Millisecond))
	r.markConnectStart("tcp4", "example.com:80")
	r.markConnectStart("tcp6", "example.com:80")
	r.markConnectDone("tcp6", "example.com:80", nil)
	r.markConnectDone("tcp4", "example.com:80", nil)
	events := span.eventsByName("httptrace.connect")
	assertAttr(t, attrMap(events[0].attrs), string(connectDurationKey), int64(3*time.Millisecond))
	assertAttr(t, attrMap(events[1].attrs), string(connectDurationKey), int64(8*time.Millisecond))
}

func TestTraceRecorderDoesNotInventOverlappingConnectDurations(t *testing.T) {
	span := newRecordingSpan("trace")
	r := newTraceRecorderWithClock(span, steppedClock(time.Now(), time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, 4*time.Millisecond, 5*time.Millisecond, 9*time.Millisecond))
	r.markConnectStart("tcp", "127.0.0.1:443")
	r.markConnectStart("tcp", "127.0.0.1:443")
	r.markConnectDone("tcp", "127.0.0.1:443", nil)
	r.markConnectDone("tcp", "127.0.0.1:443", nil)
	for _, event := range span.eventsByName("httptrace.connect") {
		if _, ok := attrMap(event.attrs)[string(connectDurationKey)]; ok {
			t.Fatal("same-target overlapping attempts have no identity; duration must be omitted")
		}
	}
	// A later sequential retry is unambiguous again.
	r.markConnectStart("tcp", "127.0.0.1:443")
	r.markConnectDone("tcp", "127.0.0.1:443", nil)
	events := span.eventsByName("httptrace.connect")
	assertAttr(t, attrMap(events[2].attrs), string(connectDurationKey), int64(4*time.Millisecond))
}

func TestTraceRecorderClockPerPhase(t *testing.T) {
	for _, phase := range []string{"dns", "get_conn", "connect", "tls_handshake", "time_to_first_byte"} {
		t.Run(phase, func(t *testing.T) {
			span := newRecordingSpan("clock")
			r := newTraceRecorderWithClock(span, steppedClock(time.Now(), 2*time.Millisecond, 5*time.Millisecond))
			key := "httptrace." + phase + ".duration_ns"
			want := int64(3 * time.Millisecond)
			switch phase {
			case "dns":
				r.markDNSStart("host")
				r.markDNSDone(httptrace.DNSDoneInfo{})
			case "get_conn":
				r.markGetConnStart("host:80")
				r.markGotConn(httptrace.GotConnInfo{})
			case "connect":
				r.markConnectStart("tcp", "host:80")
				r.markConnectDone("tcp", "host:80", nil)
			case "tls_handshake":
				r.markTLSStart()
				r.markTLSDone(tls.ConnectionState{}, nil)
			case "time_to_first_byte":
				r.markGotFirstResponseByte()
				key = "httptrace.time_to_first_byte_ns"
				want = int64(2 * time.Millisecond)
			}
			assertAttr(t, attrMap(span.attributes), key, want)
		})
	}
}

func TestTraceRecorderConsumesTLSStart(t *testing.T) {
	span := newRecordingSpan("trace")
	r := newTraceRecorderWithClock(span, steppedClock(time.Now(), time.Millisecond, 4*time.Millisecond, 9*time.Millisecond))
	r.markTLSStart()
	r.markTLSDone(tls.ConnectionState{}, nil)
	r.markTLSDone(tls.ConnectionState{}, nil)
	events := span.eventsByName("httptrace.tls_handshake")
	assertAttr(t, attrMap(events[0].attrs), string(tlsHandshakeDurationKey), int64(3*time.Millisecond))
	if _, ok := attrMap(events[1].attrs)[string(tlsHandshakeDurationKey)]; ok {
		t.Fatal("duplicate TLS done reused consumed start")
	}
}

func TestTraceRecorderEpochStartIsNotMissing(t *testing.T) {
	span := newRecordingSpan("trace")
	r := newTraceRecorderWithClock(span, steppedClock(time.Unix(0, -1), time.Nanosecond, 11*time.Nanosecond))
	r.markDNSStart("example.com")
	r.markDNSDone(httptrace.DNSDoneInfo{})
	assertAttr(t, span.eventAttrs("httptrace.dns"), string(dnsDurationKey), int64(10))
}

func TestAmbiguousPhasesRecoverAfterDrain(t *testing.T) {
	for _, phase := range []string{"dns", "get_conn", "tls_handshake"} {
		t.Run(phase, func(t *testing.T) {
			span := newRecordingSpan("trace")
			r := newTraceRecorderWithClock(span, steppedClock(time.Now(), time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, 4*time.Millisecond, 5*time.Millisecond, 9*time.Millisecond))
			var begin, done func()
			eventName := "httptrace." + phase
			key := "httptrace." + phase + ".duration_ns"
			switch phase {
			case "dns":
				begin = func() { r.markDNSStart("host") }
				done = func() { r.markDNSDone(httptrace.DNSDoneInfo{}) }
			case "get_conn":
				begin = func() { r.markGetConnStart("host:80") }
				done = func() { r.markGotConn(httptrace.GotConnInfo{}) }
				eventName = "httptrace.got_conn"
			case "tls_handshake":
				begin = r.markTLSStart
				done = func() { r.markTLSDone(tls.ConnectionState{}, nil) }
			}
			begin()
			begin()
			done()
			done()
			for _, event := range span.eventsByName(eventName) {
				if _, ok := attrMap(event.attrs)[key]; ok {
					t.Fatal("ambiguous phase reported a duration")
				}
			}
			begin()
			done()
			events := span.eventsByName(eventName)
			assertAttr(t, attrMap(events[2].attrs), key, int64(4*time.Millisecond))
		})
	}
}
