# otelhttptrace

OpenTelemetry instrumentation for outgoing `net/http` requests. Records DNS,
connection acquisition, TCP connect, TLS handshake, connection reuse, request
write, and time-to-first-byte data using `net/http/httptrace`.

Requires Go 1.26 or later.

## Usage

Import `github.com/skarm/otelhttptrace` and wrap the client's transport:

```go
client := &http.Client{
	Transport: otelhttptrace.NewTransport(http.DefaultTransport, otelhttptrace.Config{}),
}
```

The zero-value `Config` uses the global OpenTelemetry tracer provider and
propagator. Configure them in your application, or set `Config.TracerProvider`
and `Config.Propagators` explicitly. `Config.Attributes` adds span attributes;
`Config.SpanNameFormatter` overrides the default `METHOD host` name. A nil base
transport uses `http.DefaultTransport`.

Each request creates a client span. Always close the response body to finish
the span; reading to EOF alone does not finish it. Transport errors and
responses without a body finish the span immediately.

## Usage with otelhttp

With `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`, use
`WithClientTrace` to record phase data on its HTTP client span:

```go
client := &http.Client{
	Transport: otelhttp.NewTransport(
		http.DefaultTransport,
		otelhttp.WithClientTrace(otelhttptrace.NewClientTrace),
	),
}
```

Alternatively, pass `otelhttptrace.NewTransport` with
`otelhttptrace.Config{SpanFromContext: true}` as the base transport to
`otelhttp.NewTransport`. In both cases, `otelhttp` creates and ends the span
and handles propagation.

## Telemetry

Standard attribute keys come from `go.opentelemetry.io/otel/semconv/v1.43.0`.
Phase events and timing attributes under `httptrace.*` are package extensions;
durations use nanoseconds. Ambiguous overlapping phases retain their events
but omit durations.

Standalone spans mark HTTP 4xx/5xx responses and terminal transport or body
errors as `Error`. Explicit request cancellation alone does not set `Error`;
errors that also include a timeout do. Phase failures are diagnostic and may
be followed by a successful request.

`url.full` omits userinfo and fragments and redacts known credential query
parameters, such as `access_token`, `api_key`, and `X-Amz-Signature`. Other query
values remain visible. Redaction affects telemetry only; the request URL is
unchanged.

## Tests and benchmarks

The root package and SDK/`otelhttp` integration tests in `tests/` are separate
Go modules. Run tests in both:

```sh
go test -race ./...
(cd tests && go test -race ./...)
```

Run benchmarks:

```sh
go test -run '^$' -bench . -benchmem
(cd tests && go test -run '^$' -bench . -benchmem)
```
