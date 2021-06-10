package config

import (
	"fmt"
	"os"
)

const (
	envVarToken         = "SPOTINST_TOKEN"
	envVarTokenLegacy   = "SPOTINST_TOKEN_LEGACY"
	envVarAccount       = "SPOTINST_ACCOUNT"
	envVarAccountLegacy = "SPOTINST_ACCOUNT_LEGACY"
)

type Credentials struct {
	Account string
	Token   string
}

func GetCredentials() (Credentials, error) {
	token := os.Getenv(envVarToken)
	if token == "" {
		token = os.Getenv(envVarTokenLegacy)
	}
	if token == "" {
		return Credentials{}, fmt.Errorf("could not get token, env var %q not set", envVarToken)
	}
	account := os.Getenv(envVarAccount)
	if account == "" {
		account = os.Getenv(envVarAccountLegacy)
	}
	if account == "" {
		return Credentials{}, fmt.Errorf("could not get account, env var %q not set", envVarAccount)
	}
	return Credentials{
		Account: account,
		Token:   token,
	}, nil
}
