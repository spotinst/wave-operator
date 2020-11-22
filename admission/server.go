package admission

import (
	"context"
	"crypto/tls"
	"fmt"
	"html"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
)

type AdmissionController struct {
	log logr.Logger
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello %q", html.EscapeString(r.URL.Path))
}

func NewAdmissionController(log logr.Logger) *AdmissionController {
	return &AdmissionController{log}
}

func (ac *AdmissionController) Start(ctx context.Context) error {
	var err error

	var podHandler = &SparkDriverPodHandler{}
	var configmapHandler = &SparkConfigMapHandler{}

	var conf *tls.Config = nil
	certDir := "/etc/webhook/certs"
	fileinfo, err := os.Stat(certDir)
	if err != nil {
		return err
	}
	ac.log.Info("loading webhook certs", "directory", fileinfo.Name())
	cert, err := tls.LoadX509KeyPair(filepath.Join(certDir, "tls.crt"), filepath.Join(certDir, "tls.key"))
	if err != nil {
		conf = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRoot)
	mux.Handle("/mutate/pod", podHandler)
	mux.Handle("/mutate/configmap", configmapHandler)

	srv := &http.Server{
		Addr:      "0.0.0.0:9443", // TODO configure port
		Handler:   mux,
		TLSConfig: conf,
	}
	go func() {
		ac.log.Info("starting admission controller", "address", srv.Addr)
		if err := srv.ListenAndServeTLS(
			filepath.Join(certDir, "tls.crt"), filepath.Join(certDir, "tls.key"),
		); err != nil && err != http.ErrServerClosed {
			ac.log.Info("listen", "error", err)
			return
		}
	}()

	<-ctx.Done()

	ac.log.Info("shutting down admission controller")

	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		cancel()
	}()

	if err = srv.Shutdown(ctxShutDown); err != nil {
		ac.log.Info("admission controller shutdown failed", "error", err)
		return err
	}

	ac.log.Info("admission controller exited properly")
	return nil
}
