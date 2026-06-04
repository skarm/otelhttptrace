package otelhttptrace

import (
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SpanNameFormatter formats the name of a client span for an outgoing request.
type SpanNameFormatter func(*http.Request) string

// Config configures HTTP client tracing.
//
// The zero value is valid. Call Resolve to apply defaults explicitly, or pass
// the config to NewTransport and it will be resolved before use.
type Config struct {
	// TracerProvider creates standalone client spans.
	// If nil, the global OpenTelemetry tracer provider is used.
	TracerProvider trace.TracerProvider

	// Attributes are added to every standalone client span.
	Attributes []attribute.KeyValue

	// SpanNameFormatter formats standalone client span names.
	// If nil, spans are named "METHOD host".
	SpanNameFormatter SpanNameFormatter

	// SpanFromContext records httptrace events on the span already active in the
	// request context instead of starting and ending a standalone client span.
	// Use this when another instrumentation layer, such as otelhttp.Transport,
	// owns the HTTP client span.
	SpanFromContext bool
}

// Resolve returns a copy of c with defaults applied.
//
// Resolve also copies Attributes so later mutations to the original slice do
// not affect the resolved configuration.
func (c Config) Resolve() Config {
	if c.TracerProvider == nil {
		c.TracerProvider = otel.GetTracerProvider()
	}
	if c.SpanNameFormatter == nil {
		c.SpanNameFormatter = defaultSpanName
	}
	if c.Attributes != nil {
		c.Attributes = append([]attribute.KeyValue(nil), c.Attributes...)
	}
	return c
}

func defaultSpanName(req *http.Request) string {
	host := req.URL.Hostname()
	if host == "" {
		host = req.URL.Host
	}
	return req.Method + " " + host
}
