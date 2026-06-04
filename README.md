# otelhttptrace

OpenTelemetry instrumentation for outgoing `net/http` requests with
`net/http/httptrace` phase timings.

The package can work in two modes:

- standalone: wrap an `http.RoundTripper`, start a client span for each request,
  and record DNS, TCP connect, TLS handshake, connection reuse, request write,
  and time-to-first-byte data as span events and attributes;
- with `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`: record
  the same `httptrace` data on the span created by `otelhttp`.

## Usage

```go
client := &http.Client{
    Transport: otelhttptrace.NewTransport(http.DefaultTransport, otelhttptrace.Config{}),
}

resp, err := client.Get("https://example.com")
if err != nil {
    return err
}
defer resp.Body.Close()
```

Use `Config` to provide a non-global tracer provider, add default attributes,
or customize span names:

```go
client := &http.Client{
    Transport: otelhttptrace.NewTransport(
        nil,
        otelhttptrace.Config{
            TracerProvider: provider,
            Attributes: []attribute.KeyValue{
                attribute.String("service.component", "api-client"),
            },
            SpanNameFormatter: func(req *http.Request) string {
                return req.Method + " " + req.URL.Hostname()
            },
        },
    ),
}
```

Spans end when the response body is closed. This keeps the span duration aligned
with the full response consumption instead of only the header round trip.

## Usage with otelhttp

The preferred integration is to let `otelhttp` own the HTTP client span and pass
this package's client trace factory to `otelhttp.WithClientTrace`:

```go
client := &http.Client{
    Transport: otelhttp.NewTransport(
        http.DefaultTransport,
        otelhttp.WithClientTrace(otelhttptrace.NewClientTrace),
    ),
}
```

If you need this package to sit in the `RoundTripper` chain, set
`Config.SpanFromContext` and make it the base transport for `otelhttp`:

```go
client := &http.Client{
    Transport: otelhttp.NewTransport(
        otelhttptrace.NewTransport(
            http.DefaultTransport,
            otelhttptrace.Config{SpanFromContext: true},
        ),
    ),
}
```

In this mode `otelhttptrace` does not create or end a span. It only records
`httptrace` events and timing attributes on the span already active in the
request context.
