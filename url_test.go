package otelhttptrace

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestRequestPortAndAttributes(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want int
	}{
		{name: "explicit valid", url: "http://example.com:8080/path", want: 8080},
		{name: "explicit invalid", url: "http://example.com:bad/path", want: 0},
		{name: "explicit invalid ipv6", url: "http://[::1]:bad/path", want: 0},
		{name: "explicit empty", url: "http://example.com:/path", want: 80},
		{name: "explicit overflow", url: "http://example.com:999999999999999999999999999999999999/path", want: 0},
		{name: "http default", url: "http://example.com/path", want: 80},
		{name: "https default", url: "https://example.com/path", want: 443},
		{name: "https ipv6 default", url: "https://[::1]/path", want: 443},
		{name: "malformed ipv6", url: "http://[::1/path", want: 80},
		{name: "malformed ipv6 suffix", url: "http://[::1]x/path", want: 80},
		{name: "unbracketed ipv6", url: "http://::1/path", want: 80},
		{name: "unknown scheme", url: "ftp://example.com/path", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			if tt.name == "explicit invalid" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "example.com:bad", Path: "/path"}}
			} else if tt.name == "explicit invalid ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1]:bad", Path: "/path"}}
			} else if tt.name == "explicit empty" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "example.com:", Path: "/path"}}
			} else if tt.name == "malformed ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1", Path: "/path"}}
			} else if tt.name == "malformed ipv6 suffix" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "[::1]x", Path: "/path"}}
			} else if tt.name == "unbracketed ipv6" {
				req = &http.Request{URL: &url.URL{Scheme: "http", Host: "::1", Path: "/path"}}
			} else {
				req = mustRequest(t, http.MethodGet, tt.url)
			}
			if got := requestPort(req); got != tt.want {
				t.Fatalf("requestPort = %d, want %d", got, tt.want)
			}
		})
	}

	attrs := attrMap(requestAttributes(mustRequest(t, http.MethodPut, "https://example.com:9443/a?b=c"), []attribute.KeyValue{
		attribute.String("component", "unit"),
	}))
	assertAttr(t, attrs, "component", "unit")
	assertAttr(t, attrs, "http.request.method", "PUT")
	assertAttr(t, attrs, "url.full", "https://example.com:9443/a?b=c")
	assertAttr(t, attrs, "url.scheme", "https")
	assertAttr(t, attrs, "server.address", "example.com")
	assertAttr(t, attrs, "server.port", int64(9443))

	reqWithUserinfo := mustRequest(t, http.MethodGet, "https://user:secret@example.com/private?q=1")
	attrs = attrMap(requestAttributes(reqWithUserinfo, nil))
	assertAttr(t, attrs, "url.full", "https://example.com/private?q=1")
	if got := reqWithUserinfo.URL.User.String(); got != "user:secret" {
		t.Fatalf("request URL userinfo = %q, want original userinfo", got)
	}
}

func TestURLRedactionOpaqueAuthority(t *testing.T) {
	u := &url.URL{Scheme: "http", Opaque: "//user:secret@example.com/path?token=secret&q=1"}
	if got := redactedURLString(u); got != "http://example.com/path?token=REDACTED&q=1" {
		t.Fatalf("opaque URL leaked credentials: %q", got)
	}
	if u.Opaque != "//user:secret@example.com/path?token=secret&q=1" {
		t.Fatal("opaque request URL mutated")
	}
	u.Opaque = "unknown:secret"
	if got := redactedURLString(u); got != "http:REDACTED" {
		t.Fatalf("unstructured opaque URL = %q", got)
	}
}

func TestURLRedactionPreservesQueryEncoding(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"?safe=a%2Bb&token=x&token=y&empty=&flag", "?safe=a%2Bb&token=REDACTED&token=REDACTED&empty=&flag"},
		{"?%58%2dAmz%2dSignature=a%2Bb&safe=one+two", "?%58%2dAmz%2dSignature=REDACTED&safe=one+two"},
		{"?safe=1;client_secret=bad&after=%2F", "?safe=1;client_secret=REDACTED&after=%2F"},
		{"?bad%zz=secret&safe=ok", "?bad%zz=REDACTED&safe=ok"},
		{"?signature&sig=&q=1&&", "?signature=REDACTED&sig=REDACTED&q=1&&"},
		{"?ordinary=one&ordinary=two", "?ordinary=one&ordinary=two"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			r := mustRequest(t, "GET", "https://user@example.com/path"+tc.raw)
			original := r.URL.String()
			if got := redactedURLString(r.URL); got != "https://example.com/path"+tc.want {
				t.Fatalf("redacted URL = %q", got)
			}
			if r.URL.String() != original {
				t.Fatal("URL mutated")
			}
		})
	}
}

// RFC 9110 sections 4.2.1 and 4.2.2 give an empty port the scheme default.
func TestHTTPEmptyPortUsesSchemeDefault(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		port int64
	}{
		{"http://example.com:/path", 80},
		{"https://example.com:/path", 443},
		{"http://[::1]:/path", 80},
		{"https://[::1]:/path", 443},
		{"http://127.0.0.1:/path", 80},
		{"https://127.0.0.1:/path", 443},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			u, err := url.Parse(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			// NewRequest removes empty ports; RoundTrip also accepts requests
			// constructed directly with a valid net/url.URL.
			req := &http.Request{Method: http.MethodGet, URL: u}
			original := req.URL.String()
			assertAttr(t, attrMap(requestAttributes(req, nil)), "server.port", tc.port)
			if req.URL.String() != original {
				t.Fatal("telemetry changed request URL")
			}
		})
	}
}

func FuzzURLRedactionPreservesSafeQuery(f *testing.F) {
	f.Add("secret", "a+b&c;%2F")
	f.Add("", "")
	f.Add("пароль", "значение")
	f.Add("\xff", "\x00\xff")
	f.Fuzz(func(t *testing.T, secret, safe string) {
		u := &url.URL{
			Scheme:   "https",
			Host:     "example.com",
			Path:     "/path",
			User:     url.UserPassword("user", secret),
			RawQuery: "token=" + url.QueryEscape(secret) + "&safe=" + url.QueryEscape(safe),
			Fragment: secret,
		}
		original := u.String()
		got := redactedURLString(u)
		want := "https://example.com/path?token=REDACTED&safe=" + url.QueryEscape(safe)
		if got != want || u.String() != original {
			t.Fatalf("redaction did not preserve safe query or original URL: %q", got)
		}
		parsed, err := url.Parse(got)
		if err != nil || parsed.User != nil || parsed.Fragment != "" || redactedURLString(parsed) != got {
			t.Fatalf("redacted URL is not credential-free and idempotent: %q, %v", got, err)
		}
	})
}

var benchmarkRedactedURL string

func BenchmarkURLRedaction(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"empty", ""},
		{"ordinary", "q=one%2Btwo&lang=en"},
		{"signed", "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2Fscope&X-Amz-Signature=signature&X-Amz-Security-Token=token&q=value"},
		{"large-safe", "q=" + strings.Repeat("a", 16384)},
		{"large-redacted", "token=" + strings.Repeat("a", 16384)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			u := &url.URL{Scheme: "https", Host: "example.com", Path: "/path", RawQuery: tc.query}
			b.ReportAllocs()
			for b.Loop() {
				benchmarkRedactedURL = redactedURLString(u)
			}
		})
	}
}
