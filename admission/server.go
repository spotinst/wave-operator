package admission

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
)

type AdmissionController struct {
	log logr.Logger
}

type Mutator func(admissionSpec *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error)

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Wave Mutating Admissison Webhook")
}

func NewAdmissionController(log logr.Logger) *AdmissionController {
	return &AdmissionController{log}
}

func (ac *AdmissionController) GetHandlerFunc(mutate Mutator) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			ac.log.Error(err, "reading webhook request body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		review := &admissionv1.AdmissionReview{}
		err = json.Unmarshal(body, review)
		if err != nil {
			ac.log.Error(err, "deserializing webhook request body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		response, err := mutate(review.Request)
		if err != nil {
			ac.log.Error(err, "mutating webhook request", "name", review.Request.Name, "kind", review.Request.Kind)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		review.Response = response

		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(review); err != nil {
			ac.log.Error(err, "failed to encode response body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (ac *AdmissionController) Start(ctx context.Context) error {
	var err error

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
	mux.HandleFunc("/mutate/pod", ac.GetHandlerFunc(MutatePod))
	mux.HandleFunc("/mutate/configmap", ac.GetHandlerFunc(MutateConfigMap))

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
