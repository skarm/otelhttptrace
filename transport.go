package otelhttptrace

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const tracerName = "github.com/skarm/otelhttptrace"

var clientSpanStartOption = trace.WithSpanKind(trace.SpanKindClient)

// Transport instruments outgoing HTTP requests with OpenTelemetry spans and
// net/http/httptrace timing events.
//
// A Transport may be used concurrently when its base RoundTripper and
// SpanNameFormatter and propagator are safe for concurrent use.
type Transport struct {
	base              http.RoundTripper
	tracer            trace.Tracer
	attrs             []attribute.KeyValue
	spanNameFormatter SpanNameFormatter
	spanFromContext   bool
	noopTracer        bool
	propagators       propagation.TextMapPropagator
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
		propagators:       cfg.Propagators,
	}
}

// RoundTrip executes req through the base RoundTripper while recording
// httptrace events on the configured span.
//
// RoundTrip returns an error for a nil request or URL. In standalone mode,
// terminal transport and body errors are recorded on the span. Response body
// Close ends the span, after any in-flight Read or Write returns. All base
// transport and body results are returned unchanged.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("otelhttptrace: nil request")
	}
	if req.URL == nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, errors.New("otelhttptrace: nil request URL")
	}

	if t.spanFromContext {
		if !trace.SpanFromContext(req.Context()).IsRecording() {
			return t.base.RoundTrip(req)
		}
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

	recording := span.IsRecording()
	if recording {
		ctx = httptrace.WithClientTrace(ctx, NewClientTrace(ctx))
	}

	// WithContext is a shallow copy. Clone headers before any propagator writes
	// to them; neither sampling nor IsRecording determines propagation.
	req = req.WithContext(ctx)
	if len(t.propagators.Fields()) > 0 {
		req.Header = req.Header.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		t.propagators.Inject(ctx, propagation.HeaderCarrier(req.Header))
	}

	if !recording {
		res, err := t.base.RoundTrip(req)
		span.End()
		return res, err
	}

	res, err := t.base.RoundTrip(req)
	if err != nil {
		recordOperationError(req.Context(), span, err)
		span.End()
		return res, err
	}

	if res != nil {
		span.SetAttributes(semconv.HTTPResponseStatusCode(res.StatusCode))
		if res.StatusCode >= 400 || res.StatusCode < 100 {
			span.SetAttributes(semconv.ErrorTypeKey.String(strconv.Itoa(res.StatusCode)))
			span.SetStatus(codes.Error, "")
		}

		if res.Body != nil && res.Body != http.NoBody {
			res.Body = newSpanBody(res.Body, func(err error) {
				if err != nil {
					recordOperationError(req.Context(), span, err)
				}
				span.End()
			})

			return res, nil
		}
	}

	span.End()
	return res, nil
}

// CloseIdleConnections forwards explicit idle connection cleanup when the base
// transport supports it, including when called through http.Client.
func (t *Transport) CloseIdleConnections() {
	if base, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		base.CloseIdleConnections()
	}
}

func isNoopTracerProvider(provider trace.TracerProvider) bool {
	_, ok := provider.(noop.TracerProvider)
	return ok
}

func requestAttributes(req *http.Request, extra []attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(extra)+5)
	attrs = append(attrs, extra...)
	attrs = append(attrs,
		semconv.HTTPRequestMethodKey.String(requestMethod(req)),
		semconv.URLFull(redactedURLString(req.URL)),
		semconv.URLScheme(req.URL.Scheme),
	)

	if host := req.URL.Hostname(); host != "" {
		attrs = append(attrs, semconv.ServerAddress(host))
	}

	if port := requestPort(req); port > 0 {
		attrs = append(attrs, semconv.ServerPort(port))
	}

	return attrs
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetAttributes(semconv.ErrorTypeKey.String(errorType(err)))
	span.SetStatus(codes.Error, err.Error())
}

func recordOperationError(ctx context.Context, span trace.Span, err error) {
	// Only suppress a cancellation when both the operation's error and its
	// context confirm caller cancellation. A deadline or timeout is a failure.
	if errors.Is(ctx.Err(), context.Canceled) && isCancellationOnly(err) && !isTimeout(err) {
		return
	}
	recordSpanError(span, err)
}

// A joined error is caller cancellation only when every cause is cancellation.
// errors.Is alone would also match a join containing an independent failure.
func isCancellationOnly(err error) bool {
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			return isCancellationOnly(cause)
		}
	case interface{ Unwrap() []error }:
		found := false
		for _, cause := range wrapped.Unwrap() {
			if cause != nil {
				found = true
				if !isCancellationOnly(cause) {
					return false
				}
			}
		}
		if found {
			return true
		}
	}
	return errors.Is(err, context.Canceled)
}

func errorType(err error) string {
	if isTimeout(err) {
		return "timeout"
	}
	return fmt.Sprintf("%T", err)
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout net.Error
	if !errors.As(err, &timeout) {
		return false
	}
	if timeout.Timeout() {
		return true
	}
	// The first net.Error may be a non-timeout wrapper or an earlier failed
	// attempt. Check the remaining causes before suppressing a cancellation.
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return isTimeout(wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		if slices.ContainsFunc(wrapped.Unwrap(), isTimeout) {
			return true
		}
	}
	return false
}
