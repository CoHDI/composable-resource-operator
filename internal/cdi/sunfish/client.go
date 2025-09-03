/**
 * (C) Copyright IBM Corp. 2024.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sunfish

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
)

var setupLog = ctrl.Log.WithName("sunfish_client")

type ProcessorType string

const (
	TypeGpu = "GPU"
)

type SupportedGPU string

const (
	V100    = "Tesla-V100-PCIE-16GB"
	A10040G = "NVIDIA-A100-PCIE-40GB"
	A10080G = "NVIDIA-A100-80GB-PCIe"
)

type CompositionRequest struct {
	Name  string     `json:"Name"`
	Procs Processors `json:"Processors"`
}

type Processors struct {
	Members []ProcessorRequest `json:"Members"`
}

type ProcessorRequest struct {
	RequestCount int64  `json:"@Redfish.RequestCount"`
	ProcType     string `json:"ProcessorType"`
	Model        string `json:"Model"`
}

type SunfishClient struct {
	compositionServiceEndpoint string
}

func NewSunfishClient() *SunfishClient {
	endpoint := "composition-service.cro-system.svc.cluster.local:5060"
	if ep := os.Getenv("SUNFISH_ENDPOINT"); ep != "" {
		endpoint = ep
	}

	return &SunfishClient{compositionServiceEndpoint: endpoint}
}

// TODO: Discuss with the community and change the returned deviceid according to the actual situation of sunfish.
func (s *SunfishClient) sendPatchRequest(cr CompositionRequest) (string, string, error) {
	crJson, _ := json.Marshal(cr)
	setupLog.Info("requesting resources attachment", "json_request", string(crJson))

	bodyReader := bytes.NewReader(crJson)

	req, err := http.NewRequest("PATCH", "http://"+s.compositionServiceEndpoint+"/redfish/v1/Systems/System", bodyReader)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return "", "", err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return "", "", fmt.Errorf("http returned code %d", response.StatusCode)
	}

	return "", "", nil
}

func (s *SunfishClient) AddResource(instance *v1alpha1.ComposableResource) (string, string, error) {
	pr := ProcessorRequest{}

	switch instance.Spec.Model {
	case V100, A10080G, A10040G:
		pr.Model = instance.Spec.Model
		pr.ProcType = TypeGpu
		// TODO: need to fix
		pr.RequestCount = 1
	}

	prs := Processors{Members: []ProcessorRequest{pr}}

	cr := CompositionRequest{Name: instance.Spec.TargetNode, Procs: prs}

	return s.sendPatchRequest(cr)
}

func (s *SunfishClient) RemoveResource(instance *v1alpha1.ComposableResource) error {
	pr := ProcessorRequest{}
	switch instance.Spec.Model {
	case V100, A10080G, A10040G:
		pr.Model = instance.Spec.Model
		pr.ProcType = TypeGpu
		// TODO: need to fix
		pr.RequestCount = 0
	}

	prs := Processors{Members: []ProcessorRequest{pr}}

	cr := CompositionRequest{Name: instance.Spec.TargetNode, Procs: prs}

	_, _, err := s.sendPatchRequest(cr)
	return err
}

func (s *SunfishClient) CheckResource(instance *v1alpha1.ComposableResource) error {
	return nil
}

func (s *SunfishClient) GetResources() (deviceInfoList []cdi.DeviceInfo, err error) {
	return nil, nil
}
