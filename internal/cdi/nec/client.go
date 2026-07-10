package nec

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
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/CoHDI/composable-resource-operator/api/v1alpha1"
	"github.com/CoHDI/composable-resource-operator/internal/cdi"
)

var (
	clientLog               = ctrl.Log.WithName("nec_client")
	requestTimeout          = 30 * time.Second
	layoutApplyPollInterval = 10 * time.Second
	layoutApplyPollAttempts = 6
)

type NECClient struct {
	layoutApplyEndpoint          string
	configurationManagerEndpoint string
	httpClient                   *http.Client
	client                       client.Client
	ctx                          context.Context
}

type resourceListResponse struct {
	Count     int             `json:"count"`
	Resources []resourceEntry `json:"resources"`
}

type nodeListResponse struct {
	Count int         `json:"count"`
	Nodes []nodeEntry `json:"nodes"`
}

type nodeEntry struct {
	ID        string          `json:"id"`
	Name      string          `json:"name,omitempty"`
	Resources []nodeResources `json:"resources"`
}

type nodeResources struct {
	Device   managerDevice `json:"device"`
	Detected bool          `json:"detected"`
}

type resourceEntry struct {
	Device   managerDevice `json:"device"`
	Detected bool          `json:"detected"`
	NodeIDs  []string      `json:"nodeIDs"`
}

type managerDevice struct {
	DeviceID   string         `json:"deviceID"`
	Type       string         `json:"type"`
	Model      string         `json:"model"`
	Attribute  map[string]any `json:"attribute"`
	Status     deviceStatus   `json:"status"`
	PowerState string         `json:"powerState"`
	Links      []deviceLink   `json:"links"`
}

type deviceStatus struct {
	State  string `json:"state"`
	Health string `json:"health"`
}

type deviceLink struct {
	Type     string `json:"type"`
	DeviceID string `json:"deviceID"`
}

type layoutApplyRequest struct {
	Procedures []layoutApplyProcedure `json:"procedures"`
}

type layoutApplyResponse struct {
	ApplyID string `json:"applyID"`
}

type layoutApplyStatusResponse struct {
	ApplyID        string `json:"applyID"`
	Status         string `json:"status"`
	RollbackStatus string `json:"rollbackStatus,omitempty"`
}

type layoutApplyProcedure struct {
	OperationID         int    `json:"operationID"`
	Operation           string `json:"operation"`
	SourceDeviceID      string `json:"sourceDeviceID"`
	DestinationDeviceID string `json:"destinationDeviceID"`
	Dependencies        []int  `json:"dependencies"`
}

func NewNECClient(ctx context.Context, c client.Client) (*NECClient, error) {
	ip := os.Getenv("NEC_CDIM_IP")
	layoutApplyPort := os.Getenv("LAYOUT_APPLY_PORT")
	configurationManagerPort := os.Getenv("CONFIGURATION_MANAGER_PORT")

	layoutApplyEndpoint, err := buildEndpointFromIPAndPort(ip, layoutApplyPort)
	if err != nil {
		return nil, fmt.Errorf("failed to build layout-apply endpoint: %w", err)
	}

	configurationManagerEndpoint, err := buildEndpointFromIPAndPort(ip, configurationManagerPort)
	if err != nil {
		return nil, fmt.Errorf("failed to build configuration-manager endpoint: %w", err)
	}

	return &NECClient{
		layoutApplyEndpoint:          layoutApplyEndpoint,
		configurationManagerEndpoint: configurationManagerEndpoint,
		httpClient:                   &http.Client{Timeout: requestTimeout},
		client:                       c,
		ctx:                          ctx,
	}, nil
}

func (n *NECClient) AddResource(instance *v1alpha1.ComposableResource) (deviceID string, CDIDeviceID string, err error) {
	if instance == nil {
		return "", "", fmt.Errorf("instance is nil")
	}
	clientLog.Info("start adding resource", "ComposableResource", instance.Name)
	targetNodeName := instance.Spec.TargetNode
	if targetNodeName == "" {
		return "", "", fmt.Errorf("instance.Spec.TargetNode (kubernetes node name) is required")
	}

	resources, err := n.getAllResources()
	if err != nil {
		clientLog.Error(err, "failed to get all resources", "ComposableResource", instance.Name)
		return "", "", err
	}

	nodeID, err := n.getNodeIDFromNodeName(targetNodeName)
	if err != nil {
		clientLog.Error(err, "failed to get node ID from node name", "ComposableResource", instance.Name)
		return "", "", err
	}

	fabricIODeviceID, err := n.resolveAttachFabricIODeviceID(nodeID)
	if err != nil {
		clientLog.Error(err, "failed to resolve FabricIODevice ID", "ComposableResource", instance.Name)
		return "", "", err
	}

	target, err := n.pickAttachTarget(resources, nodeID, instance.Spec.Model, instance.Spec.Type)
	if err != nil {
		clientLog.Error(err, "failed to pick attach target GPU", "ComposableResource", instance.Name)
		return "", "", err
	}

	gpuDeviceID := target.Device.DeviceID
	if gpuDeviceID == "" {
		return "", "", fmt.Errorf("gpu deviceID is empty for selected resource")
	}

	request := newLayoutApplyRequest("connect", fabricIODeviceID, gpuDeviceID)

	applyID, err := n.postLayoutApply(request)
	if err != nil {
		if isLayoutApplyAlreadyRunning(err) {
			return "", "", cdi.ErrWaitingDeviceAttaching
		}
		clientLog.Error(err, "failed to post layout apply for connect", "ComposableResource", instance.Name)
		return "", "", err
	}

	if err := n.waitLayoutApply(applyID, cdi.ErrWaitingDeviceAttaching); err != nil {
		clientLog.Error(err, "failed to wait for layout apply", "ComposableResource", instance.Name)
		return "", "", err
	}

	// NOTE: NEC CDIM cannot provide GPU UUID. For prototype use, read a fixed UUID from env.
	provisionalGPUUUID, err := getProvisionalGPUUUIDFromEnv()
	if err != nil {
		clientLog.Error(err, "failed to get provisional GPU UUID from env", "ComposableResource", instance.Name)
		return "", "", err
	}

	// Keep CDIDeviceID as NEC manager deviceID so disconnect path still works.
	return provisionalGPUUUID, target.Device.DeviceID, nil
}

func (n *NECClient) RemoveResource(instance *v1alpha1.ComposableResource) error {
	if instance == nil {
		return fmt.Errorf("instance is nil")
	}
	clientLog.Info("start removing resource", "ComposableResource", instance.Name)

	resourceID := instance.Status.CDIDeviceID
	if resourceID == "" {
		return fmt.Errorf("instance.Status.CDIDeviceID is required")
	}

	resource, err := n.getResourceByID(resourceID)
	if err != nil {
		clientLog.Error(err, "failed to get resource by ID", "ComposableResource", instance.Name)
		return err
	}

	fabricIODeviceID := ""
	for _, link := range resource.Device.Links {
		if strings.EqualFold(link.Type, "destinationFabricAdapter") {
			fabricIODeviceID = link.DeviceID
			break
		}
	}
	if fabricIODeviceID == "" {
		clientLog.Info("GPU is already detached; destinationFabricAdapter link not found", "resourceID", resourceID)
		return nil
	}

	request := newLayoutApplyRequest("disconnect", fabricIODeviceID, resourceID)

	applyID, err := n.postLayoutApply(request)
	if err != nil {
		if isLayoutApplyAlreadyRunning(err) {
			return cdi.ErrWaitingDeviceDetaching
		}
		clientLog.Error(err, "failed to post layout apply for disconnect", "ComposableResource", instance.Name)
		return err
	}
	if err := n.waitLayoutApply(applyID, cdi.ErrWaitingDeviceDetaching); err != nil {
		clientLog.Error(err, "failed to wait for layout apply", "ComposableResource", instance.Name)
		return err
	}

	return nil
}

func (n *NECClient) CheckResource(instance *v1alpha1.ComposableResource) error {
	if instance == nil {
		return fmt.Errorf("instance is nil")
	}
	clientLog.Info("start checking resource", "ComposableResource", instance.Name)

	resourceID := instance.Status.CDIDeviceID
	if resourceID == "" {
		return fmt.Errorf("instance.Status.CDIDeviceID is required")
	}

	resource, err := n.getResourceByID(resourceID)
	if err != nil {
		clientLog.Error(err, "failed to get resource by ID", "ComposableResource", instance.Name)
		return err
	}

	status := strings.ToLower(resource.Device.Status.State)
	health := strings.ToLower(resource.Device.Status.Health)

	if isHealthyStatus(status, health) {
		return nil
	}

	return fmt.Errorf("resource is not healthy: id=%s status=%s health=%s", resourceID, resource.Device.Status.State, resource.Device.Status.Health)
}

func (n *NECClient) GetResources() (deviceInfoList []cdi.DeviceInfo, err error) {
	// NOTE: NEC CDIM cannot provide GPU UUID. For prototype use, read a fixed UUID from env.
	provisionalGPUUUID, err := getProvisionalGPUUUIDFromEnv()
	if err != nil {
		clientLog.Error(err, "failed to get provisional GPU UUID from env")
		return nil, err
	}
	clientLog.Info("start getting resources")

	nodes, err := n.getAllNodes()
	if err != nil {
		clientLog.Error(err, "failed to get all nodes")
		return nil, err
	}

	deviceInfoList = []cdi.DeviceInfo{}
	for _, node := range nodes {
		nodeID := node.ID
		if nodeID == "" {
			continue
		}

		k8sNodeName, err := n.resolveKubernetesNodeName(nodeID)
		if err != nil {
			clientLog.Error(err, "failed to resolve kubernetes node name", "nodeID", nodeID)
			continue
		}

		for _, resource := range node.Resources {
			if !resource.Detected {
				continue
			}
			if !isTargetTypeGPU(resource.Device.Type, "gpu") {
				continue
			}

			deviceInfoList = append(deviceInfoList, cdi.DeviceInfo{
				NodeName:    k8sNodeName,
				MachineUUID: nodeID,
				DeviceType:  strings.ToLower(resource.Device.Type),
				Model:       resource.Device.Model,
				DeviceID:    provisionalGPUUUID,
				CDIDeviceID: resource.Device.DeviceID,
			})
		}
	}

	return deviceInfoList, nil
}

func (n *NECClient) postLayoutApply(payload layoutApplyRequest) (string, error) {
	body, err := n.doRequest(n.layoutApplyEndpoint, http.MethodPost, "/layout-apply", payload)
	if err != nil {
		return "", err
	}

	response := layoutApplyResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("failed to unmarshal /layout-apply response: %w", err)
	}
	if response.ApplyID == "" {
		return "", fmt.Errorf("/layout-apply response does not contain applyID")
	}

	return response.ApplyID, nil
}

func (n *NECClient) getLayoutApplyStatus(applyID string) (layoutApplyStatusResponse, error) {
	body, err := n.doRequest(n.layoutApplyEndpoint, http.MethodGet, "/layout-apply/"+applyID, nil)
	if err != nil {
		return layoutApplyStatusResponse{}, err
	}

	response := layoutApplyStatusResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return layoutApplyStatusResponse{}, fmt.Errorf("failed to unmarshal /layout-apply/%s response: %w", applyID, err)
	}

	return response, nil
}

func (n *NECClient) waitLayoutApply(applyID string, waitingErr error) error {
	for i := 0; i < layoutApplyPollAttempts; i++ {
		statusResp, err := n.getLayoutApplyStatus(applyID)
		if err != nil {
			return fmt.Errorf("failed to get layout-apply status for applyID=%s: %w", applyID, err)
		}

		status := strings.ToUpper(statusResp.Status)
		switch status {
		case "COMPLETED":
			return nil
		case "IN_PROGRESS", "CANCELING", "":
			if i < layoutApplyPollAttempts-1 {
				time.Sleep(layoutApplyPollInterval)
				continue
			}
			return waitingErr
		case "FAILED", "SUSPENDED", "CANCELED":
			return fmt.Errorf("layout-apply failed: applyID=%s status=%s rollbackStatus=%s", applyID, statusResp.Status, statusResp.RollbackStatus)
		default:
			return fmt.Errorf("layout-apply returned unknown status: applyID=%s status=%s rollbackStatus=%s", applyID, statusResp.Status, statusResp.RollbackStatus)
		}
	}

	return waitingErr
}

func isLayoutApplyAlreadyRunning(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	// E40010: Already running. Cannot start multiple instances.
	return strings.Contains(message, "status=409") && strings.Contains(message, "E40010")
}

func (n *NECClient) getAllResources() ([]resourceEntry, error) {
	body, err := n.doRequest(n.configurationManagerEndpoint, http.MethodGet, "/resources?detail=true", nil)
	if err != nil {
		return nil, err
	}

	response := resourceListResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal /resources response: %w", err)
	}

	return response.Resources, nil
}

func (n *NECClient) getResourceByID(id string) (resourceEntry, error) {
	body, err := n.doRequest(n.configurationManagerEndpoint, http.MethodGet, "/resources/"+id, nil)
	if err != nil {
		return resourceEntry{}, err
	}

	resource := resourceEntry{}
	if err := json.Unmarshal(body, &resource); err != nil {
		var wrapped struct {
			Resource resourceEntry `json:"resource"`
		}
		if wrappedErr := json.Unmarshal(body, &wrapped); wrappedErr != nil {
			return resourceEntry{}, fmt.Errorf("failed to unmarshal /resources/%s response: %w", id, err)
		}
		resource = wrapped.Resource
	}

	return resource, nil
}

func (n *NECClient) getAllNodes() ([]nodeEntry, error) {
	body, err := n.doRequest(n.configurationManagerEndpoint, http.MethodGet, "/nodes?detail=true", nil)
	if err != nil {
		return nil, err
	}

	response := nodeListResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal /nodes response: %w", err)
	}

	return response.Nodes, nil
}

func (n *NECClient) getNodeIDFromNodeName(k8sNodeName string) (string, error) {
	nodeName := k8sNodeName
	if nodeName == "" {
		return "", fmt.Errorf("kubernetes node name is required")
	}

	node := &corev1.Node{}
	if err := n.client.Get(n.ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return "", fmt.Errorf("failed to get kubernetes node %s: %w", nodeName, err)
	}

	providerID := node.Spec.ProviderID
	return n.resolveNodeID(providerID)
}

func (n *NECClient) resolveKubernetesNodeName(necNodeID string) (string, error) {
	nodeList := &corev1.NodeList{}
	if err := n.client.List(n.ctx, nodeList); err != nil {
		return "", fmt.Errorf("failed to list kubernetes nodes: %w", err)
	}
	for _, node := range nodeList.Items {
		if strings.EqualFold(node.Spec.ProviderID, necNodeID) {
			return node.Name, nil
		}
	}
	return "", fmt.Errorf("kubernetes node not found for NEC node ID: %s", necNodeID)
}

func (n *NECClient) resolveNodeID(nodeID string) (string, error) {
	if nodeID == "" {
		return "", fmt.Errorf("node id is required")
	}

	nodes, err := n.getAllNodes()
	if err != nil {
		return "", err
	}

	for _, node := range nodes {
		if strings.EqualFold(node.ID, nodeID) {
			return node.ID, nil
		}
	}

	return "", fmt.Errorf("node id not found: %s", nodeID)
}

func (n *NECClient) resolveAttachFabricIODeviceID(nodeID string) (string, error) {
	nodes, err := n.getAllNodes()
	if err != nil {
		return "", err
	}

	var targetNode *nodeEntry
	for i := range nodes {
		if strings.EqualFold(nodes[i].ID, nodeID) {
			targetNode = &nodes[i]
			break
		}
	}
	if targetNode == nil {
		return "", fmt.Errorf("node not found while resolving attach destination: %s", nodeID)
	}

	fabricHostDeviceID, err := findFabricHostDeviceIDFromNodeLinks(*targetNode)
	if err != nil {
		return "", err
	}

	fabricHostDeviceResource, err := n.getResourceByID(fabricHostDeviceID)
	if err != nil {
		return "", fmt.Errorf("failed to get FabricHostDevice resource by id=%s: %w", fabricHostDeviceID, err)
	}

	fabricIODeviceID, err := findFabricIODeviceIDFromResourceLinks(fabricHostDeviceResource)
	if err != nil {
		return "", err
	}

	fabricIODeviceResource, err := n.getResourceByID(fabricIODeviceID)
	if err != nil {
		return "", fmt.Errorf("failed to get FabricIODevice resource by id=%s: %w", fabricIODeviceID, err)
	}
	if !isFabricIODevice(fabricIODeviceResource.Device) {
		return "", fmt.Errorf("linked resource is not a FabricIODevice: resourceID=%s type=%s", fabricIODeviceResource.Device.DeviceID, fabricIODeviceResource.Device.Type)
	}

	return fabricIODeviceID, nil
}

func findFabricHostDeviceIDFromNodeLinks(node nodeEntry) (string, error) {
	for _, resource := range node.Resources {
		if !resource.Detected {
			continue
		}

		if isFabricHostDevice(resource.Device) {
			id := resource.Device.DeviceID
			if id != "" {
				return id, nil
			}
		}
	}

	return "", fmt.Errorf("failed to resolve FabricHostDevice id from node resources: node=%s", node.ID)
}

func findFabricIODeviceIDFromResourceLinks(resource resourceEntry) (string, error) {
	for _, link := range resource.Device.Links {
		if !strings.EqualFold(link.Type, "destinationFabricAdapter") {
			continue
		}

		id := link.DeviceID
		if id != "" {
			return id, nil
		}
	}

	return "", fmt.Errorf("failed to resolve FabricIODevice id from FabricHostDevice resource links: resourceID=%s", resource.Device.DeviceID)
}

func newLayoutApplyRequest(operation string, sourceDeviceID string, destinationDeviceID string) layoutApplyRequest {
	return layoutApplyRequest{
		Procedures: []layoutApplyProcedure{
			{
				OperationID:         1,
				Operation:           operation,
				SourceDeviceID:      sourceDeviceID,
				DestinationDeviceID: destinationDeviceID,
				Dependencies:        []int{},
			},
		},
	}
}

func (n *NECClient) pickAttachTarget(resources []resourceEntry, targetNodeID string, model string, resourceType string) (resourceEntry, error) {
	for _, resource := range resources {
		if !resource.Detected {
			continue
		}

		if !isTargetTypeGPU(resource.Device.Type, resourceType) {
			continue
		}
		if isConnectedToFabricIODevice(resource.Device.Links) {
			continue
		}
		if !isHealthyStatus(strings.ToLower(resource.Device.Status.State), strings.ToLower(resource.Device.Status.Health)) {
			continue
		}
		if model != "" && !strings.EqualFold(resource.Device.Model, model) {
			continue
		}

		return resource, nil
	}

	return resourceEntry{}, fmt.Errorf("no available GPU found for node=%s model=%s type=%s", targetNodeID, model, resourceType)
}

func isConnectedToFabricIODevice(links []deviceLink) bool {
	for _, link := range links {
		if strings.EqualFold(link.Type, "eeio") {
			return true
		}
	}

	return false
}

func (n *NECClient) doRequest(endpoint string, method string, path string, payload any) ([]byte, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("NEC manager endpoint is empty")
	}

	var bodyReader io.Reader
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request payload: %w", err)
		}
		bodyReader = bytes.NewBuffer(body)
	}

	req, err := http.NewRequest(method, endpoint+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := n.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("request failed: method=%s path=%s status=%d body=%s", method, path, res.StatusCode, string(body))
	}

	return body, nil
}

func buildEndpointFromIPAndPort(ip string, port string) (string, error) {
	if ip == "" || port == "" {
		return "", fmt.Errorf("env vars are required: NEC_CDIM_IP='%s', port='%s'", ip, port)
	}

	endpoint := fmt.Sprintf("http://%s:%s/cdim/api/v1", ip, port)
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return "", fmt.Errorf("invalid NEC_CDIM_IP or port: %w", err)
	}

	return endpoint, nil
}

func isHealthyStatus(status string, health string) bool {
	okStatus := status == "enabled"
	okHealth := health == "ok"

	return okStatus && okHealth
}

func isFabricHostDevice(device managerDevice) bool {
	if !strings.EqualFold(device.Type, "sourceFabricAdapter") {
		return false
	}

	deviceSpecificInformation, ok := device.Attribute["deviceSpecificInformation"].(map[string]any)
	if !ok {
		return false
	}

	status, ok := deviceSpecificInformation["status"]
	if !ok {
		return false
	}

	return strings.EqualFold(fmt.Sprintf("%v", status), "eesv")
}

func isFabricIODevice(device managerDevice) bool {
	if !strings.EqualFold(device.Type, "destinationFabricAdapter") {
		return false
	}

	deviceSpecificInformation, ok := device.Attribute["deviceSpecificInformation"].(map[string]any)
	if !ok {
		return false
	}

	status, ok := deviceSpecificInformation["status"]
	if !ok {
		return false
	}

	return strings.EqualFold(fmt.Sprintf("%v", status), "eeio")
}

func isTargetTypeGPU(deviceType string, requestedType string) bool {
	if requestedType != "" && !strings.EqualFold(requestedType, "gpu") {
		return false
	}

	return strings.EqualFold(deviceType, "gpu")
}

func getProvisionalGPUUUIDFromEnv() (string, error) {
	value := os.Getenv("NEC_PROVISIONAL_GPU_UUID")
	if value == "" {
		return "", fmt.Errorf("NEC_PROVISIONAL_GPU_UUID is required for NEC prototype mode (example: GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
	}

	if !strings.HasPrefix(strings.ToUpper(value), "GPU-") {
		value = "GPU-" + value
	}

	return value, nil
}
