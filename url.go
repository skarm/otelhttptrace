package otelhttptrace

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func redactedURLString(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	if c.Opaque != "" {
		// Manually constructed HTTP URLs can put the authority (and userinfo)
		// in Opaque. Parse hierarchical forms before scrubbing. Unknown opaque
		// syntax has no safely separable credential boundary.
		if parsed, err := url.Parse(u.String()); err == nil && parsed.Opaque == "" {
			c = *parsed
		} else {
			c.Opaque = "REDACTED"
		}
	}
	c.User = nil
	c.RawQuery = redactQuery(c.RawQuery)
	// Fragments are not sent over HTTP and may contain OAuth credentials.
	c.Fragment, c.RawFragment = "", ""
	return c.String()
}

func requestPort(req *http.Request) int {
	if req == nil || req.URL == nil {
		return 0
	}

	// An empty port is valid URI syntax and uses the HTTP scheme default,
	// just like an absent port (RFC 9110 sections 4.2.1 and 4.2.2).
	if port, ok := explicitPort(req.URL.Host); ok && port != "" {
		n, err := strconv.Atoi(port)
		if err != nil {
			return 0
		}
		return n
	}

	switch req.URL.Scheme {
	case "http":
		return 80
	case "https":
		return 443
	default:
		return 0
	}
}

func explicitPort(host string) (string, bool) {
	if host == "" {
		return "", false
	}
	if host[0] == '[' {
		end := strings.IndexByte(host, ']')
		if end >= 0 && end+1 < len(host) && host[end+1] == ':' {
			return host[end+2:], true
		}
		return "", false
	}
	first := strings.IndexByte(host, ':')
	if first >= 0 && first == strings.LastIndexByte(host, ':') {
		return host[first+1:], true
	}
	return "", false
}

// redactQuery preserves key spelling, order, duplicates and the encoding of
// non-sensitive values. Parsing the whole query with URL.Query would silently
// drop malformed parameters. Treat legacy semicolons as separators too.
func redactQuery(raw string) string {
	var out strings.Builder
	copied := 0
	for start := 0; start < len(raw); {
		end := strings.IndexAny(raw[start:], "&;")
		if end < 0 {
			end = len(raw)
		} else {
			end += start
		}
		part := raw[start:end]
		key, _, _ := strings.Cut(part, "=")
		decoded, err := url.QueryUnescape(key)
		if err != nil || sensitiveQueryKey(decoded) {
			out.WriteString(raw[copied:start])
			out.WriteString(key)
			out.WriteString("=REDACTED")
			copied = end
		}
		start = end + 1
	}
	if copied == 0 {
		return raw
	}
	out.WriteString(raw[copied:])
	return out.String()
}

func sensitiveQueryKey(key string) bool {
	switch strings.ToLower(key) {
	case "awsaccesskeyid", "signature", "sig",
		"x-amz-credential", "x-amz-signature", "x-amz-security-token",
		"x-goog-credential", "x-goog-signature", "x-goog-security-token",
		"access_token", "refresh_token", "id_token", "token",
		"api_key", "apikey", "password", "passwd", "secret", "client_secret",
		"credential", "credentials", "authorization":
		return true
	default:
		return false
	}
}
