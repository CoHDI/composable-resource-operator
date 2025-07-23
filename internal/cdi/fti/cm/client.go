package cm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

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
	fticmapi "github.com/IBM/composable-resource-operator/internal/cdi/fti/cm/api"
)

var (
	clientLog        = ctrl.Log.WithName("fti_cm_client")
	addComplete      = "ADD_COMPLETE"
	addFailed        = "ADD_FAILED"
	removeFailed     = "REMOVE_FAILED"
	cmRequestTimeout = 60 * time.Second
)

type FTIClient struct {
	compositionServiceEndpoint string
	tenantID                   string
	clusterID                  string
	ctx                        context.Context
	client                     client.Client
	clientSet                  *kubernetes.Clientset
	token                      *fti.CachedToken
}

type scaleUpRequestBody struct {
	Target scaleUpTarget `json:"increase_resource_count"`
}

type scaleUpTarget struct {
	SpecUUID    string `json:"spec_uuid"`
	DeviceCount int    `json:"device_count"`
}

type scaleDownRequestBody struct {
	Target scaleDownTarget `json:"remove_resources"`
}

type scaleDownTarget struct {
	SpecUUID    string   `json:"spec_uuid"`
	DeviceCount int      `json:"device_count"`
	Devices     []string `json:"devices"`
}

func newHttpClient(ctx context.Context, token *oauth2.Token) *http.Client {
	client := oauth2.NewClient(ctx, oauth2.StaticTokenSource(token))
	client.Timeout = cmRequestTimeout
	return client
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

	machineUUID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node machineUUID from cluster", "ComposableResource", instance.Name)
		return "", "", err
	}

	machineData, err := f.getMachineInfo(machineUUID)
	if err != nil {
		clientLog.Error(err, "failed to get node MachineInfo from CM", "ComposableResource", instance.Name)
		return "", "", err
	}

	composableResourceList := &v1alpha1.ComposableResourceList{}
	if err := f.client.List(f.ctx, composableResourceList); err != nil {
		clientLog.Error(err, "failed to list ComposableResource", "ComposableResource", instance.Name)
		return "", "", err
	}

	matchingSpecUUID, machingSpecDeviceCount, unusedDeviceUUID, unusedDeviceErrorMessage := checkAddingResources(machineData, composableResourceList, instance)
	if unusedDeviceUUID != "" {
		return unusedDeviceUUID, unusedDeviceUUID, unusedDeviceErrorMessage
	}

	scaleUpRequest := scaleUpRequestBody{
		Target: scaleUpTarget{
			SpecUUID:    matchingSpecUUID,
			DeviceCount: machingSpecDeviceCount + 1,
		},
	}
	scaleUpRequestBody, _ := json.Marshal(scaleUpRequest)

	pathPrefix := fmt.Sprintf("cluster_manager/cluster_autoscaler/v3/tenants/%s/clusters/%s/machines/%s/actions/resize", f.tenantID, f.clusterID, machineUUID)
	req, err := http.NewRequest("POST", "https://"+f.compositionServiceEndpoint+pathPrefix, bytes.NewBuffer(scaleUpRequestBody))
	if err != nil {
		clientLog.Error(err, "failed to create HTTP request for CM scaleup", "ComposableResource", instance.Name)
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	token, err := f.token.GetToken()
	if err != nil {
		clientLog.Error(err, "failed to get authentication token for CM scaleup", "ComposableResource", instance.Name)
		return "", "", err
	}

	client := newHttpClient(f.ctx, token)
	response, err := client.Do(req)
	if err != nil {
		clientLog.Error(err, "failed to send scaleup request to CM", "ComposableResource", instance.Name)
		return "", "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		clientLog.Error(err, "failed to read scaleup response body from CM", "ComposableResource", instance.Name)
		return "", "", err
	}

	if response.StatusCode != http.StatusOK {
		errBody := &fticmapi.ErrorBody{}
		if err := json.Unmarshal(body, errBody); err != nil {
			clientLog.Error(err, "failed to unmarshal CM scaleup error response body into errBody", "ComposableResource", instance.Name)
			return "", "", fmt.Errorf("failed to unmarshal CM scaleup error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process CM scaleup request. http returned status: '%d', cm return code: '%s', error message: '%s'", errBody.Status, errBody.Detail.Code, errBody.Detail.Message)
		clientLog.Error(err, "failed to process CM scaleup request", "ComposableResource", instance.Name)
		return "", "", err
	}

	return "", "", cdi.ErrWaitingDeviceAttaching
}

func (f *FTIClient) RemoveResource(instance *v1alpha1.ComposableResource) error {
	clientLog.Info("start removing resource", "ComposableResource", instance.Name)

	machineID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node MachineID from cluster", "ComposableResource", instance.Name)
		return err
	}

	machineData, err := f.getMachineInfo(machineID)
	if err != nil {
		clientLog.Error(err, "failed to get MachineInfo from CM", "ComposableResource", instance.Name)
		return err
	}

	specUUID, deviceCount, err := checkRemovingResources(machineData, instance)
	if err != nil {
		instance.Status.Error = err.Error()
		if err := f.client.Status().Update(f.ctx, instance); err != nil {
			clientLog.Error(err, "failed to update composableResource", "composableResource", instance.Name)
			return err
		}
	}
	if specUUID == "" {
		return nil
	}

	scaleDownRequest := scaleDownRequestBody{
		Target: scaleDownTarget{
			SpecUUID:    specUUID,
			DeviceCount: deviceCount - 1,
			Devices:     []string{instance.Status.DeviceID},
		},
	}
	scaleDownRequestBody, _ := json.Marshal(scaleDownRequest)

	pathPrefix := fmt.Sprintf("cluster_manager/cluster_autoscaler/v3/tenants/%s/clusters/%s/machines/%s/actions/resize", f.tenantID, f.clusterID, machineID)
	req, err := http.NewRequest("POST", "https://"+f.compositionServiceEndpoint+pathPrefix, bytes.NewBuffer(scaleDownRequestBody))
	if err != nil {
		clientLog.Error(err, "failed to create new HTTP request for CM scaledown", "ComposableResource", instance.Name)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	token, err := f.token.GetToken()
	if err != nil {
		clientLog.Error(err, "failed to get authentication token for CM scaledown", "ComposableResource", instance.Name)
		return err
	}

	client := newHttpClient(context.Background(), token)
	response, err := client.Do(req)
	if err != nil {
		clientLog.Error(err, "failed to send scaledown request to CM", "ComposableResource", instance.Name)
		return err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		clientLog.Error(err, "failed to read scaledown response body from CM", "ComposableResource", instance.Name)
		return err
	}

	if response.StatusCode != http.StatusOK {
		errBody := &fticmapi.ErrorBody{}
		if err := json.Unmarshal(body, errBody); err != nil {
			clientLog.Error(err, "failed to unmarshal CM scaledown error response body into errBody", "ComposableResource", instance.Name)
			return fmt.Errorf("failed to unmarshal CM scaledown error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process CM scaledown request. http returned status: %d, cm return code: %s, error message: %s", errBody.Status, errBody.Detail.Code, errBody.Detail.Message)
		clientLog.Error(err, "failed to process CM scaledown request", "ComposableResource", instance.Name)
		return err
	}

	return cdi.ErrWaitingDeviceDetaching
}

func (f *FTIClient) CheckResource(instance *v1alpha1.ComposableResource) error {
	clientLog.Info("start checking resource", "ComposableResource", instance.Name)

	machineID, err := f.getNodeMachineID(instance.Spec.TargetNode)
	if err != nil {
		clientLog.Error(err, "failed to get node machine ID", "ComposableResource", instance.Name)
		return err
	}

	machineData, err := f.getMachineInfo(machineID)
	if err != nil {
		clientLog.Error(err, "failed to get MachineInfo from cm", "ComposableResource", instance.Name)
		return err
	}

	// Check whether the passed ComposableResource exists in CDI system.
	for _, resourceSpec := range machineData.Cluster.Machine.ResourceSpecs {
		if resourceSpec.Type != instance.Spec.Type {
			continue
		}

		for _, condition := range resourceSpec.Selector.Expression.Conditions {
			if condition.Column != "model" || condition.Operator != "eq" || condition.Value != instance.Spec.Model {
				continue
			}

			for _, device := range resourceSpec.Devices {
				if device.DeviceUUID == instance.Status.DeviceID {
					if device.Detail.ResourceOPStatus == "0" {
						// The target device exists and has no error, return OK.
						return nil
					} else if device.Detail.ResourceOPStatus == "1" {
						return fmt.Errorf("the target gpu '%s' is showing a Warning status in CM", instance.Status.DeviceID)
					} else if device.Detail.ResourceOPStatus == "2" {
						return fmt.Errorf("the target gpu '%s' is showing a Critical status in CM", instance.Status.DeviceID)
					} else {
						return fmt.Errorf("the target gpu '%s' has unknown status '%s' in CM", instance.Status.DeviceID, device.Detail.ResourceOPStatus)
					}
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
			return nil, err
		}

		machineData, err := f.getMachineInfo(machineID)
		if err != nil {
			clientLog.Error(err, "failed to get machineInfo from CM", "machineID", machineID)
			return nil, err
		}

		for _, resourceSpec := range machineData.Cluster.Machine.ResourceSpecs {
			if resourceSpec.Type != "gpu" {
				continue
			}

			for _, device := range resourceSpec.Devices {
				deviceInfoList = append(deviceInfoList, cdi.DeviceInfo{
					NodeName:    node.Name,
					MachineUUID: machineID,
					DeviceType:  resourceSpec.Type,
					DeviceID:    device.DeviceUUID,
					CDIDeviceID: device.DeviceUUID,
				})
			}
		}
	}

	return deviceInfoList, nil
}

func (f *FTIClient) getNodeMachineID(nodeName string) (string, error) {
	node := &corev1.Node{}
	if err := f.client.Get(f.ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return "", err
	}

	machineInfo := node.GetAnnotations()["machine.openshift.io/machine"]
	machineInfoParts := strings.Split(machineInfo, "/")
	if len(machineInfoParts) != 2 {
		return "", fmt.Errorf("failed to get annotation 'machine.openshift.io/machine' from Node %s, now is '%s'", node.Name, machineInfo)
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
}

func (f *FTIClient) getMachineInfo(machineID string) (*fticmapi.Data, error) {
	token, err := f.token.GetToken()
	if err != nil {
		return nil, err
	}

	pathPrefix := fmt.Sprintf("cluster_manager/cluster_autoscaler/v3/tenants/%s/clusters/%s/machines/%s", f.tenantID, f.clusterID, machineID)
	req, err := http.NewRequest("GET", "https://"+f.compositionServiceEndpoint+pathPrefix, nil)
	if err != nil {
		return nil, err
	}

	client := newHttpClient(f.ctx, token)
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
		errBody := &fticmapi.ErrorBody{}
		if err := json.Unmarshal(body, errBody); err != nil {
			return nil, fmt.Errorf("failed to unmarshal CM get error response body into errBody. Original error: %w", err)
		}

		err = fmt.Errorf("failed to process CM get request. http returned status: '%d', cm return code: '%s', error message: '%s'", errBody.Status, errBody.Detail.Code, errBody.Detail.Message)
		return nil, err
	}

	machineData := &fticmapi.MachineData{}
	if err := json.Unmarshal(body, machineData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CM get machine response body into machineData: %w", err)
	}

	return &machineData.Data, nil
}

func checkAddingResources(machineData *fticmapi.Data, composableResourceList *v1alpha1.ComposableResourceList, instance *v1alpha1.ComposableResource) (string, int, string, error) {
	existingDevices := make(map[string]bool)
	for _, resource := range composableResourceList.Items {
		existingDevices[resource.Status.DeviceID] = true
	}

	var specUUID string
	var deviceCount int
	for _, resourceSpec := range machineData.Cluster.Machine.ResourceSpecs {
		if !isSpecMatch(resourceSpec, instance) {
			continue
		}

		if unusedDevice := findAvailableDevice(resourceSpec, existingDevices); unusedDevice != nil {
			if unusedDevice.Status == addComplete {
				return "", 0, unusedDevice.DeviceUUID, nil
			} else if unusedDevice.Status == addFailed {
				return "", 0, unusedDevice.DeviceUUID, fmt.Errorf("an error occurred with the resource in CM: '%s'", unusedDevice.StatusReason)
			}
		}

		specUUID = resourceSpec.SpecUUID
		deviceCount = resourceSpec.DeviceCount
		break
	}

	return specUUID, deviceCount, "", nil
}

func checkRemovingResources(machineData *fticmapi.Data, instance *v1alpha1.ComposableResource) (string, int, error) {
	var specUUID string
	var deviceCount int
	for _, resourceSpec := range machineData.Cluster.Machine.ResourceSpecs {
		if !isSpecMatch(resourceSpec, instance) {
			continue
		}

		specUUID = resourceSpec.SpecUUID
		deviceCount = resourceSpec.DeviceCount

		for _, device := range resourceSpec.Devices {
			if device.DeviceUUID == instance.Status.DeviceID {
				if device.Status == removeFailed {
					return specUUID, deviceCount, fmt.Errorf("%s", device.StatusReason)
				}
				return specUUID, deviceCount, nil
			}
		}

		break
	}

	return "", 0, nil
}

func isSpecMatch(resourceSpec fticmapi.ResourceSpec, instance *v1alpha1.ComposableResource) bool {
	if resourceSpec.Type != instance.Spec.Type {
		return false
	}

	for _, condition := range resourceSpec.Selector.Expression.Conditions {
		if condition.Column == "model" && condition.Operator == "eq" && condition.Value == instance.Spec.Model {
			return true
		}
	}

	return false
}

func findAvailableDevice(resourceSpec fticmapi.ResourceSpec, existingDevices map[string]bool) *fticmapi.Device {
	for _, device := range resourceSpec.Devices {
		if !existingDevices[device.DeviceUUID] {
			return &device
		}
	}

	return nil
}
