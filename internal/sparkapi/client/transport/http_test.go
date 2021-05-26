package transport

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transportTestFunc func(req *http.Request) (*http.Response, error)

func (f transportTestFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHttpClientConstruction(t *testing.T) {
	host := "test"
	port := "6060"

	t.Run("SetsTimeoutToDefaultValue", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port)
		assert.Equal(tt, 15*time.Second, t.client.Timeout)
	})
	t.Run("ConfiguresTimeout", func(tt *testing.T) {
		timeout := 30 * time.Hour
		t := NewHTTPClientTransport(host, port, WithTimeout(timeout))
		assert.Equal(tt, timeout, t.client.Timeout)
	})
	t.Run("ConfiguresTransport", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTransport(http.DefaultTransport))
		assert.Equal(tt, http.DefaultTransport, t.client.Transport)
	})
}

func TestHttpClientGet(t *testing.T) {
	host := "test-get"
	port := "6060"

	t.Run("GetsPathSuccessfully", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTransport(transportTestFunc(func(req *http.Request) (*http.Response, error) {
			assert.Equal(tt, fmt.Sprintf("http://%s:%s/test/stuff", host, port), req.URL.String())
			return &http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(bytes.NewBufferString("Success")),
			}, nil
		})))

		body, err := t.Get("test/stuff")
		require.NoError(tt, err)
		assert.Equal(tt, "Success", string(body))

	})
	t.Run("ReturnsServiceUnavailableOnConnectionError", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTransport(transportTestFunc(func(req *http.Request) (*http.Response, error) {
			return nil, &net.OpError{}
		})))

		_, err := t.Get("fails-connection")
		require.Error(tt, err)
		assert.ErrorAs(tt, err, &ServiceUnavailableError{})
	})
	t.Run("ReturnsServiceUnavailableOnEOFError", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTransport(transportTestFunc(func(req *http.Request) (*http.Response, error) {
			return nil, io.EOF
		})))

		_, err := t.Get("eof-error")
		require.Error(tt, err)
		assert.ErrorAs(tt, err, &ServiceUnavailableError{})
	})
	t.Run("ReturnsServiceUnavailableOnTimeout", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTimeout(1*time.Microsecond), WithTransport(transportTestFunc(func(req *http.Request) (*http.Response, error) {
			time.Sleep(5 * time.Microsecond)
			return nil, nil
		})))

		_, err := t.Get("times-out")
		require.Error(tt, err)
		assert.ErrorAs(tt, err, &ServiceUnavailableError{})
	})
	t.Run("ReturnsNotFoundErrorWhenResponseIs404", func(tt *testing.T) {
		t := NewHTTPClientTransport(host, port, WithTransport(transportTestFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNotFound,
			}, nil
		})))

		_, err := t.Get("not-found")
		require.Error(tt, err)
		assert.ErrorAs(tt, err, &NotFoundError{})
	})
}
