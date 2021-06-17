//go:generate mockgen -destination=mock_client/wave_mock.go . WaveClient

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"

	"google.golang.org/protobuf/proto"

	sparkpb "github.com/spotinst/wave-operator/api/proto/spark/v1"
	"github.com/spotinst/wave-operator/api/v1alpha1"
)

const (
	contentTypeProtobuf = "application/x-protobuf"
)

var ErrUpdatingApplication = errors.New("spot: unable to update application")

type WaveClient interface {
	ApplicationGetter
	ApplicationSaver
}

type ApplicationGetter interface {
	GetSparkApplication(ctx context.Context, ID string) (string, error)
}

type ApplicationSaver interface {
	SaveSparkApplication(app *v1alpha1.SparkApplication) error
}

func (c *Client) GetSparkApplication(ctx context.Context, ID string) (string, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("/wave/spark/application/%s", ID))
	if err != nil {
		return "", err
	}

	body, _ := httputil.DumpResponse(resp, true)
	return string(body), nil
}

func (c *Client) SaveSparkApplication(app *v1alpha1.SparkApplication) error {
	c.logger.Info("Persisting spark application",
		"id", app.Spec.ApplicationID,
		"name", app.Spec.ApplicationName,
		"heritage", app.Spec.Heritage,
		"revision", app.ResourceVersion)

	appBody, err := json.Marshal(app)
	if err != nil {
		return err
	}
	sparkAppBody := string(appBody)

	topology := sparkpb.BigDataSparkApplicationsTopology{
		SparkApplications: []*sparkpb.BigDataSparkApplication{
			{
				SparkApplication: &sparkAppBody,
			},
		},
	}

	body, err := proto.Marshal(&topology)
	if err != nil {
		return err
	}

	payload := bytes.NewBuffer(body)
	req, err := http.NewRequest(http.MethodPost, "mcs/kubernetes/topology/bigdata/spark/application", payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentTypeProtobuf)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return ErrUpdatingApplication
	}

	return nil
}
