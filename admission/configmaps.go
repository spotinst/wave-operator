package admission

import (
	admissionv1 "k8s.io/api/admission/v1"
)

func MutateConfigMap(req *admissionv1.AdmissionRequest) (*admissionv1.AdmissionResponse, error) {
	//patchType := admissionv1.PatchTypeJSONPatch
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
	// return nil, fmt.Errorf("no configmaps mutated today")
}
