package admission

import (
	"context"
	"net/http"
	"time"

	"github.com/go-logr/logr"
)

type AdmissionController struct {
	log logr.Logger
}

func NewAdmissionController(log logr.Logger) *AdmissionController {
	return &AdmissionController{log}
}

func (ac *AdmissionController) Start(ctx context.Context) error {

	var err error
	srv := &http.Server{
		Addr:    "0.0.0.0:8443",
		Handler: nil,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
