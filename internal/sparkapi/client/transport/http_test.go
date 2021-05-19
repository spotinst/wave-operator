package transport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHttpClientConstruction(t *testing.T) {
	host := "test"
	port := "6060"

	t.Run("SetsTimeoutToDefaultValue", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port)
		assert.Equal(tt, 5*time.Second, t.client.Timeout)
	})
	t.Run("ConfiguresTimeout", func(tt *testing.T) {
		timeout := 30 * time.Hour
		t := NewHTTPClientTransport(host, port, WithTimeout(timeout))
		assert.Equal(tt, timeout, t.client.Timeout)
	})
}
