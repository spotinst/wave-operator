package config

import (
	"fmt"
	"os"
)

const (
	envVarToken   = "SPOTINST_TOKEN"
	envVarAccount = "SPOTINST_ACCOUNT"
)

type Credentials struct {
	Account string
	Token   string
}

func GetCredentials() (Credentials, error) {
	token := os.Getenv(envVarToken)
	if token == "" {
		return Credentials{}, fmt.Errorf("could not get token, env var %q not set", envVarToken)
	}
	account := os.Getenv(envVarAccount)
	if account == "" {
		return Credentials{}, fmt.Errorf("could not get account, env var %q not set", envVarAccount)
	}
	return Credentials{
		Account: account,
		Token:   token,
	}, nil
}
