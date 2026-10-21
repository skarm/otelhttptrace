package otelhttptrace

import (
	"io"
	"sync"
)

type spanBody struct {
	io.ReadCloser
	mu          sync.Mutex
	active      int
	closed      bool
	ended       bool
	err         error
	errPriority int
	end         func(error)
}

type spanReadWriteBody struct {
	*spanBody
	writer io.Writer
}

func newSpanBody(body io.ReadCloser, end func(error)) io.ReadCloser {
	b := &spanBody{ReadCloser: body, end: end}
	if rwc, ok := body.(io.ReadWriteCloser); ok {
		return &spanReadWriteBody{spanBody: b, writer: rwc}
	}
	return b
}

func (b *spanBody) begin() bool {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return false
	}
	b.active++
	b.mu.Unlock()
	return true
}

func (b *spanBody) complete(err error, closeDone bool) {
	priority := 0
	if err != nil {
		// Keep one representative error with bounded storage. A timeout must
		// survive earlier failures, and cancellation must not hide a failure.
		priority = 1
		if isTimeout(err) {
			priority = 2
		} else if isCancellationOnly(err) {
			priority = 0
		}
	}
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return
	}
	if err != nil && (b.err == nil || priority > b.errPriority) {
		b.err = err
		b.errPriority = priority
	}
	if closeDone {
		b.closed = true
	} else {
		b.active--
	}
	finish := b.closed && b.active == 0
	if finish {
		b.ended = true
	}
	err = b.err
	b.mu.Unlock()
	if finish {
		b.end(err)
	}
}

func (b *spanBody) Read(p []byte) (int, error) {
	track := b.begin()
	// Never hold the state mutex across I/O: Close must be able to unblock Read.
	n, err := b.ReadCloser.Read(p)
	if track {
		readErr := err
		if readErr == io.EOF {
			readErr = nil
		}
		b.complete(readErr, false)
	}
	return n, err
}

func (b *spanBody) Close() error {
	// Every call reaches the base body, preserving its repeated-Close contract.
	err := b.ReadCloser.Close()
	b.complete(err, true)
	return err
}

func (b *spanReadWriteBody) Write(p []byte) (int, error) {
	track := b.begin()
	n, err := b.writer.Write(p)
	if track {
		b.complete(err, false)
	}
	return n, err
}
