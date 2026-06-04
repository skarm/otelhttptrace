package otelhttptrace

import (
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestConfigResolveAppliesDefaultsAndCopiesAttributes(t *testing.T) {
	attrs := []attribute.KeyValue{attribute.String("component", "unit")}
	cfg := Config{Attributes: attrs}.Resolve()

	if cfg.TracerProvider == nil {
		t.Fatal("resolved tracer provider is nil")
	}
	if cfg.SpanNameFormatter == nil {
		t.Fatal("resolved span name formatter is nil")
	}
	if got := cfg.SpanNameFormatter(mustRequest(t, http.MethodPost, "https://api.example.com/items")); got != "POST api.example.com" {
		t.Fatalf("default span name = %q, want %q", got, "POST api.example.com")
	}

	attrs[0] = attribute.String("component", "mutated")
	if got := attrMap(cfg.Attributes)["component"]; got != "unit" {
		t.Fatalf("resolved attributes were not copied: component = %v", got)
	}
}
