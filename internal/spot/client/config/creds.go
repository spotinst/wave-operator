package config

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"

	"github.com/go-logr/logr"
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
		fallback: func() (string, error) {
			token, err := ocean.GetToken(secret, cm)
			if err != nil {
				return "", err
			}
			return token, nil
		},
		required:     true,
		defaultValue: "",
	}

	accountGetter := configValGetter{
		log:            log,
		envVar:         envVarAccount,
		fallbackEnvVar: envVarAccountLegacy,
		fallback: func() (string, error) {
			account, err := ocean.GetAccount(secret, cm)
			if err != nil {
				return "", err
			}
			return account, nil
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
