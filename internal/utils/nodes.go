package utils

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
)

func RestartDaemonset(clientSet *kubernetes.Clientset, ctx context.Context, namespace string, name string) error {
	daemonset, err := clientSet.AppsV1().DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if daemonset.Spec.Template.Annotations == nil {
		daemonset.Spec.Template.Annotations = make(map[string]string)
	}

	daemonset.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)

	_, err = clientSet.AppsV1().DaemonSets(namespace).Update(ctx, daemonset, metav1.UpdateOptions{})
	return err
}

func GetNode(ctx context.Context, clientSet *kubernetes.Clientset, nodeName string) (*v1.Node, error) {
	node, err := clientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return node, nil
}

func GetAllNodes(ctx context.Context, clientSet *kubernetes.Clientset) (*v1.NodeList, error) {
	nodes, err := clientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func IsNodeExisted(ctx context.Context, clientSet *kubernetes.Clientset, nodeName string) error {
	_, err := GetNode(ctx, clientSet, nodeName)
	if err != nil {
		return err
	}

	return nil
}

func IsNodeCapacitySufficient(ctx context.Context, clientSet *kubernetes.Clientset, nodeName string, nodeSpec *v1alpha1.NodeSpec) (bool, error) {
	node, err := GetNode(ctx, clientSet, nodeName)
	if err != nil {
		return false, err
	}

	nodeCPU := node.Status.Capacity[v1.ResourceCPU]
	nodeEphemeralStorage := node.Status.Capacity[v1.ResourceEphemeralStorage]
	nodePods := node.Status.Capacity[v1.ResourcePods]
	nodeMemory := node.Status.Capacity[v1.ResourceMemory]

	nodeCPUQuantity, ok := nodeCPU.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's CPU resource to int64")
	}

	nodeMemoryQuantity, ok := nodeMemory.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's memory resource to int64")
	}

	nodePodsQuantity, ok := nodePods.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's pod resource to int64")
	}

	nodeEphemeralStorageQuantity, ok := nodeEphemeralStorage.AsInt64()
	if !ok {

		return false, fmt.Errorf("failed to convert node's ephemeral storage resource to int64")
	}

	if nodeCPUQuantity < nodeSpec.MilliCPU ||
		nodeMemoryQuantity < nodeSpec.Memory ||
		nodePodsQuantity < nodeSpec.AllowedPodNumber ||
		nodeEphemeralStorageQuantity < nodeSpec.EphemeralStorage {
		return false, nil
	}

	return true, nil
}

func IsGpusVisible(ctx context.Context, clientSet *kubernetes.Clientset, resourceType string, resource *v1alpha1.ComposableResource) (bool, error) {
	if resourceType == "DRA" {
		resourceSliceList, err := clientSet.ResourceV1alpha3().ResourceSlices().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}

		for _, rs := range resourceSliceList.Items {
			for _, device := range rs.Spec.Devices {
				for attrName, attrValue := range device.Basic.Attributes {
					if attrName == "uuid" && *attrValue.StringValue == resource.Status.DeviceID {
						return true, nil
					}
				}
			}
		}

		return false, nil
	} else {
		// TODO: When using device plugin, IsGpusVisible() needs to be modified.
		node, err := GetNode(ctx, clientSet, resource.Spec.TargetNode)
		if err != nil {
			return false, err
		}

		return isGpusVisible(node), nil
	}
}

func isGpusVisible(node *v1.Node) bool {
	if val, exists := node.Status.Allocatable["nvidia.com/gpu"]; exists {
		if val.IsZero() {
			return false
		} else {
			return true
		}
	}

	return false
}

func SetNodeSchedulable(ctx context.Context, clientSet *kubernetes.Clientset, request *v1alpha1.ComposableResource) (bool, error) {
	nodeNeedsUpdate := false

	node, err := GetNode(ctx, clientSet, request.Spec.TargetNode)
	if err != nil {
		return false, err
	}

	if node.Spec.Unschedulable {
		nodeNeedsUpdate = true

		if err := updateUnschedulableValue(ctx, clientSet, node, false); err != nil {
			return nodeNeedsUpdate, err
		}

		return nodeNeedsUpdate, nil
	}

	return nodeNeedsUpdate, nil
}

func updateUnschedulableValue(ctx context.Context, clientSet *kubernetes.Clientset, node *v1.Node, desiredState bool) error {
	node.Spec.Unschedulable = desiredState

	return updateNode(ctx, clientSet, node)
}

func updateNode(ctx context.Context, clientSet *kubernetes.Clientset, node *v1.Node) error {
	_, err := clientSet.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})

	return err
}
