package admission

import (
	"net/http"
)

type SparkDriverPodHandler struct{}

func (s SparkDriverPodHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	panic("implement me")
}
