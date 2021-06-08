package spot

import (
	"fmt"
	"os"
)

const (
	envVarToken   = "SPOTINST_TOKEN"
	envVarAccount = "SPOTINST_ACCOUNT"
)

type credentials struct {
	Account string
	Token   string
}

func getCredentials() (credentials, error) {
	token := os.Getenv(envVarToken)
	if token == "" {
		return credentials{}, fmt.Errorf("could not get token, env var %q not set", envVarToken)
	}
	account := os.Getenv(envVarAccount)
	if account == "" {
		return credentials{}, fmt.Errorf("could not get account, env var %q not set", envVarAccount)
	}
	return credentials{
		Account: account,
		Token:   token,
	}, nil
}
