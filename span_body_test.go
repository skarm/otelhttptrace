package otelhttptrace

import (
	"io"
	"strings"
	"testing"
)

func TestSpanBodyEndsOnceAndPreservesReadWriteCloser(t *testing.T) {
	var endCount int
	body := newSpanBody(io.NopCloser(strings.NewReader("ok")), func(error) {
		endCount++
	})

	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if endCount != 1 {
		t.Fatalf("end calls = %d, want 1", endCount)
	}

	readWrite := &readWriteCloserBody{}
	wrapped := newSpanBody(readWrite, func(error) {})
	rwc, ok := wrapped.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("wrapped body = %T, want io.ReadWriteCloser", wrapped)
	}
	if _, err := rwc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := readWrite.written.String(); got != "ping" {
		t.Fatalf("written = %q, want ping", got)
	}
	if err := rwc.Close(); err != nil {
		t.Fatal(err)
	}
}
