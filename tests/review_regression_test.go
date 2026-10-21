package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/skarm/otelhttptrace"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestCancellationDoesNotHideIndependentFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"joined", errors.Join(context.Canceled, io.ErrUnexpectedEOF)},
		{"wrapped-join", fmt.Errorf("request: %w", errors.Join(context.Canceled, io.ErrClosedPipe))},
		{"network-wrapper", &net.OpError{Op: "read", Net: "tcp", Err: errors.Join(context.Canceled, io.ErrUnexpectedEOF)}},
	} {
		for _, operation := range []string{"round-trip", "read", "write", "close"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				p, e := testProvider(t, sdktrace.AlwaysSample())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				base := roundTripFunc(func(*http.Request) (*http.Response, error) {
					cancel()
					if operation == "round-trip" {
						return nil, tc.err
					}
					body := &operationErrorBody{}
					switch operation {
					case "read":
						body.readErr = tc.err
					case "write":
						body.writeErr = tc.err
					case "close":
						body.closeErr = tc.err
					}
					return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
				})
				res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, ctx, "http://example.test"))
				if operation == "round-trip" {
					if err != tc.err {
						t.Fatalf("RoundTrip error = %v, want original %v", err, tc.err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					switch operation {
					case "read":
						if n, err := res.Body.Read(make([]byte, 3)); n != 3 || err != tc.err {
							t.Fatalf("Read = %d, %v", n, err)
						}
					case "write":
						if n, err := res.Body.(io.Writer).Write([]byte("abc")); n != 3 || err != tc.err {
							t.Fatalf("Write = %d, %v", n, err)
						}
					}
					if err := res.Body.Close(); operation == "close" && err != tc.err || operation != "close" && err != nil {
						t.Fatalf("Close = %v", err)
					}
				}
				s := onlySpan(t, e)
				if s.Status.Code != codes.Error {
					t.Fatalf("independent failure hidden by cancellation: status = %v", s.Status)
				}
				assertSpanEvents(t, s.Events, "exception")
			})
		}
	}
}

type operationErrorBody struct {
	readErr, writeErr, closeErr error
}

func (b *operationErrorBody) Read(p []byte) (int, error)  { return copy(p, "abc"), b.readErr }
func (b *operationErrorBody) Write(p []byte) (int, error) { return len(p), b.writeErr }
func (b *operationErrorBody) Close() error                { return b.closeErr }

func TestBodyEarlierErrorDoesNotHideLaterFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first error
		last  error
	}{
		{"cancellation-then-failure", context.Canceled, io.ErrUnexpectedEOF},
		{"cancellation-then-timeout", context.Canceled, context.DeadlineExceeded},
		{"failure-then-timeout", io.ErrUnexpectedEOF, context.DeadlineExceeded},
	} {
		for _, operation := range []string{"read", "write"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				p, e := testProvider(t, sdktrace.AlwaysSample())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				body := &operationErrorBody{closeErr: tc.last}
				if operation == "read" {
					body.readErr = tc.first
				} else {
					body.writeErr = tc.first
				}
				base := roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
				})
				res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, ctx, "http://example.test"))
				if err != nil {
					t.Fatal(err)
				}
				cancel()
				if operation == "read" {
					if n, err := res.Body.Read(make([]byte, 3)); n != 3 || err != tc.first {
						t.Fatalf("Read = %d, %v", n, err)
					}
				} else if n, err := res.Body.(io.Writer).Write([]byte("abc")); n != 3 || err != tc.first {
					t.Fatalf("Write = %d, %v", n, err)
				}
				if len(e.GetSpans()) != 0 {
					t.Fatal("body error ended span before Close")
				}
				if err := res.Body.Close(); err != tc.last {
					t.Fatalf("Close = %v, want original %v", err, tc.last)
				}
				s := onlySpan(t, e)
				if s.Status.Code != codes.Error {
					t.Fatalf("later failure lost: status = %v", s.Status)
				}
				if tc.last == context.DeadlineExceeded {
					assertAttr(t, attributeMap(s.Attributes), "error.type", "timeout")
				}
				assertSpanEvents(t, s.Events, "exception")
			})
		}
	}
}

type pendingOperationBody struct {
	started, finish chan struct{}
}

func (b *pendingOperationBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.finish
	return 0, context.Canceled
}

func (b *pendingOperationBody) Write([]byte) (int, error) {
	return b.Read(nil)
}

func (*pendingOperationBody) Close() error { return context.DeadlineExceeded }

func TestBodyConcurrentCloseKeepsTimeoutAndWaitsForOperation(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			p, e := testProvider(t, sdktrace.AlwaysSample())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &pendingOperationBody{started: make(chan struct{}), finish: make(chan struct{})}
			defer close(body.finish)
			base := roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
			})
			res, err := otelhttptrace.NewTransport(base, otelhttptrace.Config{TracerProvider: p}).RoundTrip(request(t, ctx, "http://example.test"))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				if operation == "read" {
					_, err = res.Body.Read(make([]byte, 1))
				} else {
					_, err = res.Body.(io.Writer).Write([]byte("a"))
				}
				done <- err
			}()
			select {
			case <-body.started:
			case <-time.After(5 * time.Second):
				t.Fatal("body operation did not start")
			}
			cancel()
			if err := res.Body.Close(); err != context.DeadlineExceeded {
				t.Fatalf("Close = %v", err)
			}
			if len(e.GetSpans()) != 0 {
				t.Fatal("Close ended span while body operation was still in flight")
			}
			body.finish <- struct{}{}
			select {
			case err := <-done:
				if err != context.Canceled {
					t.Fatalf("body operation = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("body operation did not finish")
			}
			s := onlySpan(t, e)
			if s.Status.Code != codes.Error {
				t.Fatalf("concurrent cancellation hid Close timeout: %v", s.Status)
			}
			assertAttr(t, attributeMap(s.Attributes), "error.type", "timeout")
		})
	}
}
