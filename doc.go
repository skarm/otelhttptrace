// Package otelhttptrace instruments outgoing net/http requests with
// OpenTelemetry spans enriched by net/http/httptrace timing events.
//
// The package can either wrap an http.RoundTripper and create one client span
// per request, or provide an httptrace.ClientTrace factory for instrumentation
// that already owns the HTTP client span. In the standalone mode, construct an
// http.Client with NewTransport and close each response body to finish the
// created span.
package otelhttptrace
