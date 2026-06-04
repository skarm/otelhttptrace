package otelhttptrace_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/skarm/otelhttptrace"

	"go.opentelemetry.io/otel/trace/noop"
)

type benchmarkRoundTripper struct{}

type benchmarkPortCase struct {
	name string
	req  *http.Request
}

var benchmarkRoundTripResponse *http.Response

func (benchmarkRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Body:       http.NoBody,
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func BenchmarkRoundTripperBaseline(b *testing.B) {
	benchmarkPortRoundTrips(b, func() http.RoundTripper {
		return benchmarkRoundTripper{}
	})
}

func BenchmarkTransportRoundTripPortAttributes(b *testing.B) {
	benchmarkPortRoundTrips(b, func() http.RoundTripper {
		return otelhttptrace.NewTransport(benchmarkRoundTripper{}, otelhttptrace.Config{
			TracerProvider: noop.NewTracerProvider(),
		})
	})
}

func benchmarkPortRoundTrips(b *testing.B, newRoundTripper func() http.RoundTripper) {
	for _, tt := range benchmarkPortCases(b) {
		b.Run(tt.name, func(b *testing.B) {
			roundTripper := newRoundTripper()
			var res *http.Response

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var err error
				res, err = roundTripper.RoundTrip(tt.req)
				if err != nil {
					b.Fatal(err)
				}
				if err := res.Body.Close(); err != nil {
					b.Fatal(err)
				}
			}

			benchmarkRoundTripResponse = res
		})
	}
}

func benchmarkPortCases(tb testing.TB) []benchmarkPortCase {
	return []benchmarkPortCase{
		{name: "explicit_port", req: benchmarkRequest(tb, "http://example.com:8080/path")},
		{name: "implicit_http_port", req: benchmarkRequest(tb, "http://example.com/path")},
		{name: "implicit_https_port", req: benchmarkRequest(tb, "https://example.com/path")},
		{name: "invalid_explicit_port", req: benchmarkManualRequest("http", "example.com:bad", "/path")},
		{name: "malformed_bracketed_ipv6", req: benchmarkManualRequest("http", "[::1", "/path")},
		{name: "unbracketed_ipv6", req: benchmarkManualRequest("http", "::1", "/path")},
	}
}

func benchmarkRequest(tb testing.TB, rawURL string) *http.Request {
	tb.Helper()

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		tb.Fatal(err)
	}

	return req
}

func benchmarkManualRequest(scheme, host, path string) *http.Request {
	return &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: scheme,
			Host:   host,
			Path:   path,
		},
		Header: make(http.Header),
	}
}
