package utils

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
)

var (
	nodesLog = ctrl.Log.WithName("utils_nodes")
)

func RestartDaemonset(ctx context.Context, client client.Client, namespace string, name string) error {
	daemonset := &appsv1.DaemonSet{}
	if err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, daemonset); err != nil {
		return err
	}

	if daemonset.Status.NumberReady < daemonset.Status.DesiredNumberScheduled ||
		daemonset.Status.CurrentNumberScheduled < daemonset.Status.DesiredNumberScheduled ||
		daemonset.Status.NumberUnavailable > 0 ||
		daemonset.Status.NumberMisscheduled > 0 {
		nodesLog.Info("skip restart because daemonSet is not in a fully stable and ready state", "namespace", namespace, "name", name)
		return nil
	}

	if daemonset.Spec.Template.Annotations == nil {
		daemonset.Spec.Template.Annotations = make(map[string]string)
	}

	restartedAt, ok := daemonset.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"]
	if ok {
		lastRestartTime, err := time.Parse(time.RFC3339, restartedAt)
		if err == nil {
			if time.Since(lastRestartTime) <= 5*time.Minute {
				nodesLog.Info("skip restart because daemonSet already restarted recently", "namespace", namespace, "name", name, "restartedAt", restartedAt)
				return nil
			}
		} else {
			return fmt.Errorf("failed to parse restartedAt annotation for DaemonSet %s/%s: '%v'", namespace, name, err)
		}
	}

	daemonset.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
	if err := client.Update(ctx, daemonset); err != nil {
		return err
	}

	nodesLog.Info("daemonSet restarted successfully", "namespace", namespace, "name", name)
	return nil
}

func CheckNodeCapacitySufficient(ctx context.Context, client client.Client, nodeName string, nodeSpec *v1alpha1.NodeSpec) (bool, error) {
	node, err := getNode(ctx, client, nodeName)
	if err != nil {
		return false, err
	}

	nodeCPU := node.Status.Capacity[corev1.ResourceCPU]
	nodeEphemeralStorage := node.Status.Capacity[corev1.ResourceEphemeralStorage]
	nodePods := node.Status.Capacity[corev1.ResourcePods]
	nodeMemory := node.Status.Capacity[corev1.ResourceMemory]

	nodeCPUQuantity, ok := nodeCPU.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's CPU resource '%v' to int64", nodeCPU)
	}

	nodeMemoryQuantity, ok := nodeMemory.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's memory resource '%v' to int64", nodeEphemeralStorage)
	}

	nodePodsQuantity, ok := nodePods.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's pod resource '%v' to int64", nodePods)
	}

	nodeEphemeralStorageQuantity, ok := nodeEphemeralStorage.AsInt64()
	if !ok {
		return false, fmt.Errorf("failed to convert node's ephemeral storage resource '%v' to int64", nodeMemory)
	}

	if nodeCPUQuantity < nodeSpec.MilliCPU ||
		nodeMemoryQuantity < nodeSpec.Memory ||
		nodePodsQuantity < nodeSpec.AllowedPodNumber ||
		nodeEphemeralStorageQuantity < nodeSpec.EphemeralStorage {
		return false, nil
	}

	return true, nil
}

func SetNodeSchedulable(ctx context.Context, client client.Client, request *v1alpha1.ComposableResource) (bool, error) {
	node := &corev1.Node{}
	if err := client.Get(ctx, types.NamespacedName{Name: request.Spec.TargetNode}, node); err != nil {
		return false, err
	}

	if node.Spec.Unschedulable {
		node.Spec.Unschedulable = false
		if err := client.Update(ctx, node); err != nil {
			return true, err
		}

		return true, nil
	}

	return false, nil
}

func GetAllNodes(ctx context.Context, client client.Client) (*corev1.NodeList, error) {
	nodeList := &corev1.NodeList{}
	if err := client.List(ctx, nodeList); err != nil {
		return nil, err
	}

	return nodeList, nil
}

func CheckNodeExisted(ctx context.Context, client client.Client, nodeName string) error {
	_, err := getNode(ctx, client, nodeName)
	if err != nil {
		return err
	}

	return nil
}

func getNode(ctx context.Context, client client.Client, nodeName string) (*corev1.Node, error) {
	node := &corev1.Node{}
	if err := client.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return nil, err
	}

	return node, nil
}
