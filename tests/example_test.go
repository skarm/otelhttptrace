package tests

import (
	"net/http"

	"github.com/skarm/otelhttptrace"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func Example_withOtelHTTPClientTrace() {
	client := &http.Client{
		Transport: otelhttp.NewTransport(
			http.DefaultTransport,
			otelhttp.WithClientTrace(otelhttptrace.NewClientTrace),
		),
	}

	_ = client
}

func Example_withOtelHTTPBaseTransport() {
	client := &http.Client{
		Transport: otelhttp.NewTransport(
			otelhttptrace.NewTransport(http.DefaultTransport, otelhttptrace.Config{
				SpanFromContext: true,
			}),
		),
	}

	_ = client
}
