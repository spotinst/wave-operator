package transport

type Client interface {
	Get(path string) ([]byte, error)
}
