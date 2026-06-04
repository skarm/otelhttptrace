package otelhttptrace_test

import (
	"net/http"

	"github.com/skarm/otelhttptrace"
)

func ExampleNewTransport() {
	client := &http.Client{
		Transport: otelhttptrace.NewTransport(http.DefaultTransport, otelhttptrace.Config{}),
	}

	_ = client
}
