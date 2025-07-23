package fm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	machinev1beta1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
	"github.com/IBM/composable-resource-operator/internal/cdi/fti"
	ftifmapi "github.com/IBM/composable-resource-operator/internal/cdi/fti/fm/api"
)

var clientLog = ctrl.Log.WithName("fti_fm_client")

type FTIClient struct {
	compositionServiceEndpoint string
	tenantID                   string
	clusterID                  string
	ctx                        context.Context
	client                     client.Client
	clientSet                  *kubernetes.Clientset
	token                      *fti.CachedToken
}

func NewFTIClient(ctx context.Context, client client.Client, clientSet *kubernetes.Clientset) *FTIClient {
	endpoint := os.Getenv("FTI_CDI_ENDPOINT")
	tenantID := os.Getenv("FTI_CDI_TENANT_ID")
	clusterID := os.Getenv("FTI_CDI_CLUSTER_ID")

	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}

	return &FTIClient{
		compositionServiceEndpoint: endpoint,
		tenantID:                   tenantID,
		clusterID:                  clusterID,
		ctx:                        ctx,
		client:                     client,
		clientSet:                  clientSet,
		token:                      fti.NewCachedToken(clientSet, endpoint),
	}
}

func (f *FTIClient) AddResource(instance *v1alpha1.ComposableResource) (deviceID string, CDIDeviceID string, err error) {
	clientLog.Info("start adding resource", "ComposableResource", instance.Name)

	machineID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node machineUUID from cluster", "ComposableResource", instance.Name)
		return "", "", err
	}

	token, err := f.token.GetToken()
	if err != nil {
		clientLog.Error(err, "failed to get authentication token for CM scaleup", "ComposableResource", instance.Name)
		return "", "", err
	}

	body := ftifmapi.ScaleUpBody{
		Tenants: ftifmapi.ScaleUpTenants{
			TenantUUID: f.tenantID,
			Machines: []ftifmapi.ScaleUpMachineItem{
				{
					MachineUUID: machineID,
					Resources: []ftifmapi.ScaleUpResourceItem{
						{
							ResourceSpecs: []ftifmapi.ScaleUpResourceSpecItem{
								{
									Type: instance.Spec.Type,
									Spec: ftifmapi.Condition{
										Condition: []ftifmapi.ConditionItem{
											{
												Column:   "model",
												Operator: "eq",
												Value:    instance.Spec.Model,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	jsonData, _ := json.Marshal(body)

	pathPrefix := fmt.Sprintf("fabric_manager/api/v1/machines/%s/update", machineID)
	req, err := http.NewRequest("PATCH", "https://"+f.compositionServiceEndpoint+pathPrefix, bytes.NewBuffer(jsonData))
	if err != nil {
		clientLog.Error(err, "failed to create HTTP request for FM scaleup", "ComposableResource", instance.Name)
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	params := url.Values{}
	params.Add("tenant_uuid", f.tenantID)
	req.URL.RawQuery = params.Encode()

	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	response, err := client.Do(req)
	if err != nil {
		clientLog.Error(err, "failed to send scaleup request to FM", "ComposableResource", instance.Name)
		return "", "", err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		clientLog.Error(err, "failed to read scaleup response body from FM", "ComposableResource", instance.Name)
		return "", "", err
	}

	if response.StatusCode != http.StatusOK {
		errBody := &ftifmapi.ErrorBody{}
		if err := json.Unmarshal(data, errBody); err != nil {
			clientLog.Error(err, "failed to unmarshal FM scaleup error response body into errBody", "ComposableResource", instance.Name)
			return "", "", fmt.Errorf("failed to unmarshal FM scaleup error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process FM scaleup request. FM returned code: '%s', error message: '%s'", errBody.Code, errBody.Message)
		clientLog.Error(err, "failed to process FM scaleup request", "ComposableResource", instance.Name)
		return "", "", err
	}

	scaleUpResponse := &ftifmapi.ScaleUpResponse{}
	if err := json.Unmarshal(data, scaleUpResponse); err != nil {
		clientLog.Error(err, "failed to unmarshal FM scaleup response body into scaleUpResponse", "ComposableResource", instance.Name)
		return "", "", fmt.Errorf("failed to unmarshal FM scaleup response body into scaleUpResponse. Original error: %w", err)
	}

	if len(scaleUpResponse.Data.Machines) > 0 && len(scaleUpResponse.Data.Machines[0].Resources) > 0 && scaleUpResponse.Data.Machines[0].Resources[0].Type == instance.Spec.Type {
		resource := scaleUpResponse.Data.Machines[0].Resources[0]

		for _, spec := range resource.Spec.Condition {
			if spec.Column == "model" && spec.Operator == "eq" && spec.Value == instance.Spec.Model {
				if resource.OptionStatus == "0" {
					return resource.ResourceUUID, resource.ResourceUUID, nil
				} else if resource.OptionStatus == "1" {
					clientLog.Info("the FM attached device called is in Warning state in FM", "ComposableResource", instance.Name)
					return resource.ResourceUUID, resource.ResourceUUID, nil
				} else if resource.OptionStatus == "2" {
					err := fmt.Errorf("the FM attached device called by %s is in Critical state in FM", instance.Name)
					clientLog.Error(err, "failed to attach device", "ComposableResource", instance.Name)
					return "", "", err
				} else {
					err := fmt.Errorf("the FM attached device called by %s is in unknown state '%s' in FM", instance.Name, resource.OptionStatus)
					clientLog.Error(err, "failed to attach device", "ComposableResource", instance.Name)
					return "", "", err
				}
			}
		}
	}

	return "", "", fmt.Errorf("can not find the added gpu when using FM to add gpu")
}

func (f *FTIClient) RemoveResource(instance *v1alpha1.ComposableResource) error {
	clientLog.Info("start removing resource", "ComposableResource", instance.Name)

	machineID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node MachineID from cluster", "ComposableResource", instance.Name)
		return err
	}

	token, err := f.token.GetToken()
	if err != nil {
		clientLog.Error(err, "failed to get authentication token for FM scaledown", "ComposableResource", instance.Name)
		return err
	}

	body := ftifmapi.ScaleDownBody{
		Tenants: ftifmapi.ScaleDownTenants{
			TenantUUID: f.tenantID,
			Machines: []ftifmapi.ScaleDownMachineItem{
				{
					MachineUUID: machineID,
					Resources: []ftifmapi.ScaleDownResourceItem{
						{
							ResourceSpecs: []ftifmapi.ScaleDownResourceSpecItem{
								{
									Type:         instance.Spec.Type,
									ResourceUUID: instance.Status.DeviceID,
								},
							},
						},
					},
				},
			},
		},
	}
	jsonData, _ := json.Marshal(body)

	pathPrefix := fmt.Sprintf("fabric_manager/api/v1/machines/%s/update", machineID)
	req, err := http.NewRequest("DELETE", "https://"+f.compositionServiceEndpoint+pathPrefix, bytes.NewBuffer(jsonData))
	if err != nil {
		clientLog.Error(err, "failed to create new HTTP request for FM scaledown", "ComposableResource", instance.Name)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	params := url.Values{}
	params.Add("tenant_uuid", f.tenantID)
	req.URL.RawQuery = params.Encode()

	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	response, err := client.Do(req)
	if err != nil {
		clientLog.Error(err, "failed to send scaledown request to FM", "ComposableResource", instance.Name)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		data, err := io.ReadAll(response.Body)
		if err != nil {
			clientLog.Error(err, "failed to read scaledown response body from FM", "ComposableResource", instance.Name)
			return err
		}

		errBody := &ftifmapi.ErrorBody{}
		if err := json.Unmarshal(data, errBody); err != nil {
			clientLog.Error(err, "failed to unmarshal scaledown error response body into errBody", "ComposableResource", instance.Name)
			return fmt.Errorf("failed to unmarshal scaledown error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process scaledown request. FM returned code: %s, error message: %s", errBody.Code, errBody.Message)
		clientLog.Error(err, "failed to process scaledown request", "ComposableResource", instance.Name)
		return err
	}

	return nil
}

func (f *FTIClient) CheckResource(instance *v1alpha1.ComposableResource) error {
	clientLog.Info("start checking resource", "ComposableResource", instance.Name)

	machineID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node MachineID", "ComposableResource", instance.Name)
		return err
	}

	machineData := &ftifmapi.GetMachineData{}
	machineData, err = f.getMachineInfo(machineID)
	if err != nil {
		clientLog.Error(err, "failed to get MachineInfo from fm", "ComposableResource", instance.Name)
		return err
	}

	// Check whether the device associated with this ComposableResource exists in FM.
	for _, resource := range machineData.Machines[0].Resources {
		if resource.Type != instance.Spec.Type {
			continue
		}

		for _, condition := range resource.Spec.Condition {
			if condition.Column != "model" || condition.Operator != "eq" || condition.Value != instance.Spec.Model {
				continue
			}

			if resource.ResourceUUID == instance.Status.DeviceID {
				if resource.OptionStatus == "0" {
					// The target device exists and has no error, return OK.
					return nil
				} else if resource.OptionStatus == "1" {
					return fmt.Errorf("the target gpu '%s' is showing a Warning status in FM", instance.Status.DeviceID)
				} else if resource.OptionStatus == "2" {
					return fmt.Errorf("the target gpu '%s' is showing a Critical status in FM", instance.Status.DeviceID)
				} else {
					return fmt.Errorf("the target gpu '%s' has unknown status '%s' in FM", instance.Status.DeviceID, resource.OptionStatus)
				}
			}
		}
	}

	err = fmt.Errorf("the target device '%s' cannot be found in CDI system", instance.Status.DeviceID)
	clientLog.Error(err, "failed to search device", "ComposableResource", instance.Name)
	return err
}

func (f *FTIClient) GetResources() (deviceInfoList []cdi.DeviceInfo, err error) {
	clientLog.Info("start getting resources")

	nodeList := &corev1.NodeList{}
	if err := f.client.List(f.ctx, nodeList); err != nil {
		clientLog.Error(err, "failed to list nodes")
		return nil, err
	}

	deviceInfoList = []cdi.DeviceInfo{}

	for _, node := range nodeList.Items {
		machineID, err := f.getNodeMachineID(node.Name)
		if err != nil {
			clientLog.Error(err, "failed to get machineID for cluster", "node", node.Name)
			continue
		}

		machineData, err := f.getMachineInfo(machineID)
		if err != nil {
			clientLog.Error(err, "failed to get machineInfo from FM", "machineID", machineID)
			continue
		}

		if len(machineData.Machines) == 0 {
			continue
		}
		for _, resource := range machineData.Machines[0].Resources {
			if resource.Type != "gpu" {
				continue
			}

			deviceInfoList = append(deviceInfoList, cdi.DeviceInfo{
				NodeName:    node.Name,
				MachineUUID: machineID,
				DeviceType:  resource.Type,
				DeviceID:    resource.ResourceUUID,
				CDIDeviceID: resource.ResourceUUID,
			})
		}
	}

	return deviceInfoList, nil
}

func (f *FTIClient) getNodeMachineID(nodeName string) (string, error) {
	if f.clusterID != "" {
		node := &corev1.Node{}
		if err := f.client.Get(f.ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return "", err
		}

		machineInfo := node.GetAnnotations()["machine.openshift.io/machine"]
		machineInfoParts := strings.Split(machineInfo, "/")
		if len(machineInfoParts) != 2 {
			return "", fmt.Errorf("failed to get annotation 'machine.openshift.io/machine' from Node %s, now is '%s'", nodeName, machineInfo)
		}

		machine := &machinev1beta1.Metal3Machine{}
		if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: machineInfoParts[0], Name: machineInfoParts[1]}, machine); err != nil {
			return "", err
		}

		bmhInfo := machine.GetAnnotations()["metal3.io/BareMetalHost"]
		bmhInfoParts := strings.Split(bmhInfo, "/")
		if len(bmhInfoParts) != 2 {
			return "", fmt.Errorf("failed to get annotation 'metal3.io/BareMetalHost' from Machine %s, now is '%s'", machine.Name, bmhInfo)
		}

		bmh := &metal3v1alpha1.BareMetalHost{}
		if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: bmhInfoParts[0], Name: bmhInfoParts[1]}, bmh); err != nil {
			return "", err
		}

		if bmh.GetAnnotations() == nil || bmh.GetAnnotations()["cluster-manager.cdi.io/machine"] == "" {
			return "", fmt.Errorf("failed to get annotation 'cluster-manager.cdi.io/machine' from BareMetalHost %s, now is '%s'", bmh.Name, bmh.GetAnnotations()["cluster-manager.cdi.io/machine"])
		}

		return bmh.GetAnnotations()["cluster-manager.cdi.io/machine"], nil
	} else {
		node := &corev1.Node{}
		if err := f.client.Get(f.ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return "", err
		}

		providerID := node.Spec.ProviderID
		machineUUID, found := strings.CutPrefix(providerID, "fti_cdi://")
		if !found {
			return "", fmt.Errorf("invalid format: expected 'fti_cdi://machineUUID', now is '%s'", providerID)
		}

		return machineUUID, nil
	}
}

func (f *FTIClient) getMachineInfo(machineID string) (*ftifmapi.GetMachineData, error) {
	pathPrefix := fmt.Sprintf("fabric_manager/api/v1/machines/%s", machineID)
	req, err := http.NewRequest("GET", "https://"+f.compositionServiceEndpoint+pathPrefix, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	params := url.Values{}
	params.Add("tenant_uuid", f.tenantID)
	req.URL.RawQuery = params.Encode()

	token, err := f.token.GetToken()
	if err != nil {
		return nil, err
	}

	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		errBody := &ftifmapi.ErrorBody{}
		if err := json.Unmarshal(body, errBody); err != nil {
			return nil, fmt.Errorf("failed to unmarshal FM get error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process FM get request. FM return code: '%s', error message: '%s'", errBody.Code, errBody.Message)
		return nil, err
	}

	machineData := &ftifmapi.GetMachineResponse{}
	if err := json.Unmarshal(body, machineData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal FM get machine response body into machineData: %w", err)
	}

	return &machineData.Data, nil
}
