package admission

import (
	"net/http"
)

type SparkConfigMapHandler struct{}

func (s SparkConfigMapHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	panic("implement me")
}
