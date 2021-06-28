package config

import (
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	"github.com/spotinst/wave-operator/internal/ocean"
)

type Credentials struct {
	Account string
	Token   string
}

func getCredentials(cm *corev1.ConfigMap, secret *corev1.Secret, log logr.Logger) (Credentials, error) {
	tokenGetter := configValGetter{
		log:            log,
		envVar:         envVarToken,
		fallbackEnvVar: envVarTokenLegacy,
		fallback: func() string {
			return ocean.GetToken(secret, cm)
		},
		required:     true,
		defaultValue: "",
	}

	accountGetter := configValGetter{
		log:            log,
		envVar:         envVarAccount,
		fallbackEnvVar: envVarAccountLegacy,
		fallback: func() string {
			return ocean.GetAccount(secret, cm)
		},
		required:     true,
		defaultValue: "",
	}

	token, err := tokenGetter.get()
	if err != nil {
		return Credentials{}, fmt.Errorf("could not get token, %w", err)
	}

	account, err := accountGetter.get()
	if err != nil {
		return Credentials{}, fmt.Errorf("could not get account, %w", err)
	}

	return Credentials{
		Account: account,
		Token:   token,
	}, nil
}
