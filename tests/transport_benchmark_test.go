package tests

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"

	"github.com/skarm/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type auditBody struct{}

func (auditBody) Read([]byte) (int, error) { return 0, io.EOF }
func (auditBody) Close() error             { return nil }

type auditBase struct {
	phases string
	body   bool
}

func BenchmarkAuditLoopbackReuse(b *testing.B) {
	for _, mode := range []string{"base", "drop", "recording", "context-absent", "otelhttp-drop-hooks", "otelhttp-recording-hooks"} {
		b.Run(mode, func(b *testing.B) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
			defer srv.Close()
			base := &http.Transport{}
			defer base.CloseIdleConnections()
			sampler := sdktrace.NeverSample()
			if mode == "recording" || mode == "otelhttp-recording-hooks" {
				sampler = sdktrace.AlwaysSample()
			}
			p := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
			b.Cleanup(func() {
				if err := p.Shutdown(context.Background()); err != nil {
					b.Errorf("shutdown tracer provider: %v", err)
				}
			})
			var transport http.RoundTripper = base
			switch mode {
			case "drop", "recording":
				transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p})
			case "context-absent":
				transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true})
			case "otelhttp-drop-hooks", "otelhttp-recording-hooks":
				transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(p), otelhttp.WithClientTrace(otelhttptrace.NewClientTrace))
			}
			r, err := http.NewRequest(http.MethodGet, srv.URL, nil)
			if err != nil {
				b.Fatal(err)
			}
			// Warm up the pool outside the measured loop.
			res, err := transport.RoundTrip(r)
			if err != nil {
				b.Fatal(err)
			}
			_, readErr := io.Copy(io.Discard, res.Body)
			closeErr := res.Body.Close()
			if readErr != nil {
				b.Fatalf("read response during warmup: %v", readErr)
			}
			if closeErr != nil {
				b.Fatalf("close response during warmup: %v", closeErr)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				res, err := transport.RoundTrip(r)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = io.Copy(io.Discard, res.Body); err != nil {
					b.Fatal(err)
				}
				if err = res.Body.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func (b auditBase) RoundTrip(r *http.Request) (*http.Response, error) {
	if h := httptrace.ContextClientTrace(r.Context()); h != nil {
		if b.phases == "reused" || b.phases == "phases" {
			if h.GetConn != nil {
				h.GetConn("example.com:443")
			}
			if b.phases == "phases" {
				if h.DNSStart != nil {
					h.DNSStart(httptrace.DNSStartInfo{Host: "example.com"})
				}
				if h.DNSDone != nil {
					h.DNSDone(httptrace.DNSDoneInfo{})
				}
				if h.ConnectStart != nil {
					h.ConnectStart("tcp", "127.0.0.1:443")
				}
				if h.ConnectDone != nil {
					h.ConnectDone("tcp", "127.0.0.1:443", nil)
				}
				if h.TLSHandshakeStart != nil {
					h.TLSHandshakeStart()
				}
				if h.TLSHandshakeDone != nil {
					h.TLSHandshakeDone(tls.ConnectionState{Version: tls.VersionTLS13}, nil)
				}
			}
			if h.GotConn != nil {
				h.GotConn(httptrace.GotConnInfo{Reused: b.phases == "reused"})
			}
			if h.WroteRequest != nil {
				h.WroteRequest(httptrace.WroteRequestInfo{})
			}
			if h.GotFirstResponseByte != nil {
				h.GotFirstResponseByte()
			}
		}
	}
	var body io.ReadCloser = http.NoBody
	if b.body {
		body = auditBody{}
	}
	return &http.Response{StatusCode: 200, Body: body, Request: r}, nil
}

// These benchmarks contain no network I/O or retaining exporter. Each recording
// request gets a new SDK span; no ever-growing span/event slice is measured.
func BenchmarkAuditRoundTrip(b *testing.B) {
	for _, phases := range []string{"none", "reused", "phases", "body"} {
		for _, mode := range []string{"base", "noop", "global-unset", "drop", "recording", "context-absent", "context-drop", "context-recording", "otelhttp-drop", "otelhttp-drop-hooks", "otelhttp-recording", "otelhttp-recording-hooks"} {
			b.Run(phases+"/"+mode, func(b *testing.B) {
				base := auditBase{phases: phases, body: phases == "body"}
				var transport http.RoundTripper = base
				sampler := sdktrace.NeverSample()
				if mode == "recording" || mode == "context-recording" || mode == "otelhttp-recording" || mode == "otelhttp-recording-hooks" {
					sampler = sdktrace.AlwaysSample()
				}
				provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
				b.Cleanup(func() {
					if err := provider.Shutdown(context.Background()); err != nil {
						b.Errorf("shutdown tracer provider: %v", err)
					}
				})
				switch mode {
				case "noop":
					transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: noop.NewTracerProvider()})
				case "global-unset":
					transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{})
				case "drop", "recording":
					transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: provider})
				case "context-absent", "context-drop", "context-recording":
					transport = otelhttptrace.NewTransport(base, otelhttptrace.Config{SpanFromContext: true})
				case "otelhttp-drop", "otelhttp-recording":
					transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(provider))
				case "otelhttp-drop-hooks", "otelhttp-recording-hooks":
					transport = otelhttp.NewTransport(base, otelhttp.WithTracerProvider(provider), otelhttp.WithClientTrace(otelhttptrace.NewClientTrace))
				}
				r, err := http.NewRequest(http.MethodGet, "https://example.com/path?q=1", nil)
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				tracer := provider.Tracer("benchmark-owner")
				var buf [32]byte
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					req := r
					var end func()
					if mode == "context-drop" || mode == "context-recording" {
						c, span := tracer.Start(ctx, "owner")
						req = r.WithContext(c)
						end = func() { span.End() }
					}
					res, err := transport.RoundTrip(req)
					if err != nil {
						b.Fatal(err)
					}
					if phases == "body" {
						if _, err = res.Body.Read(buf[:]); err != io.EOF {
							b.Fatal(err)
						}
					}
					if err = res.Body.Close(); err != nil {
						b.Fatal(err)
					}
					if end != nil {
						end()
					}
				}
			})
		}
	}
}

func BenchmarkAuditNewClientTrace(b *testing.B) {
	for _, mode := range []string{"absent", "drop", "ended", "recording"} {
		b.Run(mode, func(b *testing.B) {
			sampler := sdktrace.AlwaysSample()
			if mode == "drop" {
				sampler = sdktrace.NeverSample()
			}
			p := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
			b.Cleanup(func() {
				if err := p.Shutdown(context.Background()); err != nil {
					b.Errorf("shutdown tracer provider: %v", err)
				}
			})
			ctx := context.Background()
			if mode != "absent" {
				var spanEnd func()
				c, span := p.Tracer("bench").Start(ctx, "parent")
				ctx, spanEnd = c, func() { span.End() }
				defer spanEnd()
				if mode == "ended" {
					spanEnd()
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				h := otelhttptrace.NewClientTrace(ctx)
				if h == nil {
					b.Fatal("nil trace")
				}
			}
		})
	}
}

func BenchmarkPerformanceParallelRoundTrip(b *testing.B) {
	for _, mode := range []string{"base", "noop", "drop", "recording", "recording-phases"} {
		b.Run(mode, func(b *testing.B) {
			sampler := sdktrace.NeverSample()
			if mode == "recording" || mode == "recording-phases" {
				sampler = sdktrace.AlwaysSample()
			}
			provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
			b.Cleanup(func() {
				if err := provider.Shutdown(context.Background()); err != nil {
					b.Errorf("shutdown tracer provider: %v", err)
				}
			})
			base := auditBase{}
			if mode == "recording-phases" {
				base.phases = "phases"
			}
			var transport http.RoundTripper = base
			if mode != "base" {
				cfg := otelhttptrace.Config{TracerProvider: provider}
				if mode == "noop" {
					cfg.TracerProvider = noop.NewTracerProvider()
				}
				transport = otelhttptrace.NewTransport(base, cfg)
			}
			req, err := http.NewRequest(http.MethodGet, "https://example.com/path?q=value", nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					res, err := transport.RoundTrip(req)
					if err != nil {
						b.Error(err)
						return
					}
					if err := res.Body.Close(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

type performancePayloadBody struct{ remaining int }

func (p *performancePayloadBody) Read(buf []byte) (int, error) {
	n := min(len(buf), p.remaining)
	clear(buf[:n])
	p.remaining -= n
	if p.remaining == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (*performancePayloadBody) Close() error { return nil }

func BenchmarkPerformanceStreamingBody(b *testing.B) {
	const size = 1024 * 1024
	for _, chunk := range []int{256, 32768} {
		name := "chunk256"
		if chunk == 32768 {
			name = "chunk32K"
		}
		b.Run(name, func(b *testing.B) {
			for _, mode := range []string{"base", "recording"} {
				b.Run(mode, func(b *testing.B) {
					provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
					b.Cleanup(func() {
						if err := provider.Shutdown(context.Background()); err != nil {
							b.Errorf("shutdown tracer provider: %v", err)
						}
					})
					var transport http.RoundTripper = roundTripFunc(func(req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: &performancePayloadBody{remaining: size}, Request: req}, nil
					})
					if mode == "recording" {
						transport = otelhttptrace.NewTransport(transport, otelhttptrace.Config{TracerProvider: provider})
					}
					req, err := http.NewRequest(http.MethodGet, "http://example.com/body", nil)
					if err != nil {
						b.Fatal(err)
					}
					buf := make([]byte, chunk)
					b.ReportAllocs()
					b.SetBytes(size)
					b.ResetTimer()
					for b.Loop() {
						res, err := transport.RoundTrip(req)
						if err != nil {
							b.Fatal(err)
						}
						total := 0
						for {
							n, err := res.Body.Read(buf)
							total += n
							if err == io.EOF {
								break
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						if total != size {
							b.Fatalf("body bytes = %d, want %d", total, size)
						}
						if err := res.Body.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
