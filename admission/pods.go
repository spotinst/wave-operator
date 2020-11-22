package admission

import (
	"github.com/go-logr/logr"
	"github.com/spotinst/wave-operator/cloudstorage"
	admissionv1 "k8s.io/api/admission/v1"
)

func MutatePod(info cloudstorage.CloudStorageProvider, log logr.Logger, req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {

	// needs:
	//  logger
	//  storage information to inject

	// patchType := admissionv1.PatchTypeJSONPatch
	resp := &admissionv1.AdmissionResponse{
		UID:              req.UID,
		Allowed:          true,
		Result:           nil,
		Patch:            nil,
		PatchType:        nil,
		AuditAnnotations: nil,
		Warnings:         nil,
	}

	return resp, nil
}
