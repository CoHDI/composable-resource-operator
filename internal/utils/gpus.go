package utils

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
)

type GPUStatus struct {
	UUID       string
	PciBusID   string
	GPUUsed    int
	MemoryUsed int
}

var (
	gpusLog = ctrl.Log.WithName("utils_gpus")
)

func DrainGPU(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID string, resourceType string) error {
	gpusLog.Info("start removing gpu", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)

	// Get the info about gpu.
	nvidiaPod, err := getNvidiaDriverDaemonsetPod(ctx, clientset, targetNodeName)
	if err != nil {
		return err
	}
	getGPUInfocommand := []string{"nvidia-smi", "--query-gpu=index,gpu_uuid,pci.bus_id", "--format=csv,noheader,nounits"}
	getGPUInfoStdout, getGPUInfoStderr, err := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		nvidiaPod.Namespace,
		nvidiaPod.Name,
		nvidiaPod.Spec.Containers[0].Name,
		getGPUInfocommand,
	)
	if getGPUInfoStderr != "" || err != nil {
		return fmt.Errorf("get gpu info command failed: '%v', stderr: '%s'", err, getGPUInfoStderr)
	}

	// Parse the info about gpu.
	targetIndex := ""
	targetGPUBusID := ""
	for _, line := range strings.Split(strings.TrimSpace(string(getGPUInfoStdout)), "\n") {
		parts := strings.Split(line, ",")

		index := strings.TrimSpace(parts[0])
		gpuUUID := strings.TrimSpace(parts[1])
		pciBusID := strings.TrimSpace(parts[2])

		if gpuUUID == targetGPUUUID {
			targetGPUBusID = strings.TrimPrefix(pciBusID, "0000")
			targetIndex = index
			break
		}
	}
	if targetGPUBusID == "" {
		// It can be considered to have been removed, so no error is required.
		gpusLog.Info("cannot find the gpu bus id, it should have been removed", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
		return nil
	}

	gpusLog.Info("find the gpu bus id", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID, "targetGPUBusID", targetGPUBusID, "targetIndex", targetIndex)

	// Detach with nvidia-smi command. Execute in nvidia-driver-daemonset Pod.
	disableCommand := []struct {
		cmd  []string
		desc string
	}{
		{[]string{"nvidia-smi", "-i", targetGPUUUID, "-pm", "0"}, "disable persistence mode"},
	}
	for _, step := range disableCommand {
		_, stdErr, execErr := execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			step.cmd,
		)
		if execErr != nil || stdErr != "" {
			return fmt.Errorf("deatch command '%s' failed: '%v', stderr: '%s'", step.desc, execErr, stdErr)
		}
	}

	// Check that /dev/nvidiaX is not open.
	var checkShell = `
        TARGET_FILE="/dev/nvidia` + targetIndex + `";
        for PID_DIR in /proc/[0-9]*; do
            PID=$(basename "$PID_DIR");
            CMD_NAME=$(cat "$PID_DIR/comm" 2>/dev/null || echo "[unknown]")

            for FD_SYMLINK in "$PID_DIR"/fd/*; do
                if [ -L "$FD_SYMLINK" ]; then
                    TARGET_PATH=$(readlink -f "$FD_SYMLINK" 2>/dev/null);
                    if [ "$TARGET_PATH" = "$TARGET_FILE" ]; then
                        echo "$CMD_NAME";
                        exit 0;
                    fi;
                fi;
            done;
        done;
    `
	checkCommand := []string{"sh", "-c", checkShell}
	checkStdout, checkStderr, err := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		nvidiaPod.Namespace,
		nvidiaPod.Name,
		nvidiaPod.Spec.Containers[0].Name,
		checkCommand,
	)
	if checkStderr != "" || err != nil {
		return fmt.Errorf("check /dev/nvidiaX command failed: '%v', stderr: '%s'", err, checkStderr)
	}
	if checkStdout != "" {
		return fmt.Errorf("check /dev/nvidiaX command failed: there is a process %s occupied the nvidiaX file", checkStdout)
	}

	// Delete the device file (Current NVDIA drivers do not automatically delete device files and must be manually deleted).
	if resourceType == "DRA" {
		commandInNvidia := []struct {
			cmd  []string
			desc string
		}{
			{[]string{"rm", "-f", "/run/nvidia/driver/dev/nvidia" + targetIndex}, "remve file /run/nvidia/driver/dev/nvidiaX"},
		}
		for _, step := range commandInNvidia {
			_, stderr, execErr := execCommandInPod(
				ctx,
				clientset,
				restConfig,
				nvidiaPod.Namespace,
				nvidiaPod.Name,
				nvidiaPod.Spec.Containers[0].Name,
				step.cmd,
			)
			if execErr != nil || stderr != "" {
				return fmt.Errorf("delete device file command '%s' failed: '%v', stderr: '%s'", step.desc, execErr, stderr)
			}
		}

		draPod, err := getDRAKubeletPluginPod(ctx, clientset, targetNodeName)
		if err != nil {
			return err
		}
		commandInDRA := []struct {
			cmd  []string
			desc string
		}{
			{[]string{"rm", "-f", "/dev/nvidia" + targetIndex}, "remove file /dev/nvidiaX"},
		}
		for _, step := range commandInDRA {
			_, stderr, execErr := execCommandInPod(
				ctx,
				clientset,
				restConfig,
				draPod.Namespace,
				draPod.Name,
				draPod.Spec.Containers[0].Name,
				step.cmd,
			)
			if execErr != nil || stderr != "" {
				return fmt.Errorf("delete device file command '%s' failed: '%v', stderr: '%s'", step.desc, execErr, stderr)
			}
		}
	}

	detachCommands := []struct {
		cmd  []string
		desc string
	}{
		{[]string{"nvidia-smi", "drain", "-p", targetGPUBusID, "-m", "1"}, "set maintenance mode"},
		{[]string{"nvidia-smi", "drain", "-p", targetGPUBusID, "-r"}, "reset GPU"},
	}
	for _, step := range detachCommands {
		_, stderr, execErr := execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			step.cmd,
		)
		if execErr != nil || stderr != "" {
			if step.desc == "reset GPU" {
				continue
			}
			return fmt.Errorf("deatch command '%s' failed: '%v', stderr: '%s'", step.desc, execErr, stderr)
		}
	}

	return nil
}

func IfGPUHasLoads(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID *string) (bool, error) {
	pod, err := getNvidiaDriverDaemonsetPod(ctx, clientset, targetNodeName)
	if err != nil {
		return true, err
	}

	command := []string{"nvidia-smi", "--query-accounted-apps=gpu_uuid", "--format=csv,noheader,nounits"}
	stdout, stderr, err := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		command,
	)
	if stderr != "" || err != nil {
		return true, fmt.Errorf("nvidia-smi failed: %v, stderr: %s", err, stderr)
	}

	if targetGPUUUID == nil {
		// When targetGPUUUID is nil, it means that there should be no load on the target node.
		if stdout != "" {
			return true, nil
		}
	} else {
		for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
			gpuUUID := strings.TrimSpace(line)
			if gpuUUID == *targetGPUUUID {
				return true, nil
			}
		}
	}

	return false, nil
}

func execCommandInPod(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, namespace, podName, containerName string, command []string) (string, string, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		return "", "", err
	}

	gpusLog.Info("start running the command", "podName", podName, "containerName", containerName, "command", command)

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(
		ctx,
		remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		},
	)
	return stdout.String(), stderr.String(), err
}

func getNvidiaDriverDaemonsetPod(ctx context.Context, clientset *kubernetes.Clientset, targetNodeName string) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/component=nvidia-driver"),
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", targetNodeName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no Pod named 'nvidia-driver-daemonset' found on node %s", targetNodeName)
	}

	pod := pods.Items[0]

	return &pod, nil
}

func getDRAKubeletPluginPod(ctx context.Context, clientset *kubernetes.Clientset, targetNodeName string) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/name=nvidia-dra-driver-gpu"),
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", targetNodeName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}

	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, "nvidia-dra-driver-gpu-kubelet-plugin") {
			return &pod, nil
		}
	}

	return nil, fmt.Errorf("no Pod named 'nvidia-dra-driver-gpu-kubelet-plugin' found on node %s", targetNodeName)
}

func CreateGPUTaintRule(ctx context.Context, client client.Client, resource *crov1alpha1.ComposableResource) error {
	gpuTaintRuleName := fmt.Sprintf("taint-rule-%s", resource.Status.DeviceID)
	gpuTaintRuleName = strings.ToLower(gpuTaintRuleName)
	gpuTaintRule := &resourcev1alpha3.DeviceTaintRule{}

	if err := client.Get(ctx, types.NamespacedName{Name: gpuTaintRuleName}, gpuTaintRule); err == nil {
		// This means that the DeviceTaintRule already exists and does not need to be created again.
		return nil
	}

	celExpression := fmt.Sprintf(`device.attributes["gpu.nvidia.com"].Uuid == '%s'`, resource.Status.DeviceID)
	gpuTaintRule = &resourcev1alpha3.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: gpuTaintRuleName,
		},
		Spec: resourcev1alpha3.DeviceTaintRuleSpec{
			Taint: resourcev1alpha3.DeviceTaint{
				Key:    "nvidia/gpu-model",
				Effect: "NoSchedule",
				Value:  resource.Spec.Model,
			},
			DeviceSelector: &resourcev1alpha3.DeviceTaintSelector{
				Driver: ptr.To("gpu.nvidia.com"),
				Selectors: []resourcev1alpha3.DeviceSelector{
					{
						CEL: &resourcev1alpha3.CELDeviceSelector{
							Expression: celExpression,
						},
					},
				},
			},
		},
	}

	if err := client.Create(ctx, gpuTaintRule); err != nil {
		return err
	}

	return nil
}

func DeleteGPUTaintRule(ctx context.Context, client client.Client, resource *crov1alpha1.ComposableResource) error {
	gpuTaintRuleName := fmt.Sprintf("gpu-taint-rule-%s", resource.Status.DeviceID)
	gpuTaintRule := &resourcev1alpha3.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: gpuTaintRuleName,
		},
	}

	err := client.Delete(ctx, gpuTaintRule)
	if k8serrors.IsNotFound(err) {
		return nil
	}

	return err
}
