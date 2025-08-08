package utils

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
)

var gpusLog = ctrl.Log.WithName("utils_gpus")

type AccountedAppInfo struct {
	GPUUUID     string
	ProcessName string
}

func (a AccountedAppInfo) String() string {
	return fmt.Sprintf("GPUUUID: '%s', ProcessName: '%s'", a.GPUUUID, a.ProcessName)
}

func CheckGPUVisible(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, deviceResourceType string, resource *crov1alpha1.ComposableResource) (bool, error) {
	if deviceResourceType == "DRA" {
		resourceSliceList := &resourcev1.ResourceSliceList{}
		if err := client.List(ctx, resourceSliceList); err != nil {
			return false, err
		}

		for _, rs := range resourceSliceList.Items {
			for _, device := range rs.Spec.Devices {
				for attrName, attrValue := range device.Attributes {
					if attrName == "uuid" && *attrValue.StringValue == resource.Status.DeviceID {
						return true, nil
					}
				}
			}
		}

		return false, nil
	} else {
		gpuInfos, err := getGPUInfoFromNvidiaPod(ctx, client, clientset, restConfig, resource.Spec.TargetNode, "gpu_uuid")
		if err != nil {
			return false, err
		}

		for _, gpuInfo := range gpuInfos {
			if gpuInfo["gpu_uuid"] == resource.Status.DeviceID {
				return true, nil
			}
		}

		return false, nil
	}
}

func CheckNoGPULoads(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID *string) error {
	pod, err := getNvidiaDriverDaemonsetPod(ctx, client, targetNodeName)
	if err != nil {
		return err
	}

	command := []string{"/usr/bin/nvidia-smi", "--query-compute-apps=gpu_uuid,process_name", "--format=csv,noheader,nounits"}
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
		return fmt.Errorf("run nvidia-smi in pod '%s' to check gpu loads failed: '%v', stderr: '%s'", pod.Name, err, stderr)
	}

	var accountedApps []AccountedAppInfo

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")

		appInfo := AccountedAppInfo{
			GPUUUID:     strings.TrimSpace(parts[0]),
			ProcessName: strings.TrimSpace(parts[1]),
		}
		accountedApps = append(accountedApps, appInfo)
	}

	if targetGPUUUID == nil {
		// When targetGPUUUID is nil, it means that there should be no load on the target node.
		if len(accountedApps) > 0 {
			return fmt.Errorf("found gpu loads on node '%s': '%v'", targetNodeName, accountedApps)
		}
	} else {
		// When targetGPUUUID is not nil, it means that there should be no load on the target gpu.
		for _, appInfo := range accountedApps {
			if appInfo.GPUUUID == *targetGPUUUID {
				return fmt.Errorf("found gpu load on gpu '%s': %v", *targetGPUUUID, accountedApps)
			}
		}
	}

	return nil
}

func DrainGPU(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID string, deviceResourceType string) error {
	gpusLog.Info("start draining gpu", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)

	nvidiaPod, err := getNvidiaDriverDaemonsetPod(ctx, client, targetNodeName)
	if err != nil {
		return err
	}

	// Get information about the GPU to be drained.
	gpuInfos, err := getGPUInfoFromNvidiaPod(ctx, client, clientset, restConfig, targetNodeName, "index,gpu_uuid,pci.bus_id")
	if err != nil {
		return err
	}

	targetGPUIndex := ""
	targetGPUBusID := ""
	for _, gpuInfo := range gpuInfos {
		if gpuInfo["gpu_uuid"] == targetGPUUUID {
			targetGPUIndex = gpuInfo["index"]
			targetGPUBusID = strings.TrimPrefix(gpuInfo["pci.bus_id"], "0000")
			break
		}
	}
	if targetGPUBusID == "" {
		// It can be considered to have been drained, so no error is required.
		gpusLog.Info("cannot find the gpu bus id, it should have been drained", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
		return nil
	}

	gpusLog.Info("find the gpu bus id", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID, "targetGPUBusID", targetGPUBusID, "targetIndex", targetGPUIndex)

	// Disable gpu with nvidia-smi command. It should be executed in nvidia-driver-daemonset Pod.
	disableCommand := []struct {
		cmd  []string
		desc string
	}{
		{[]string{"/usr/bin/nvidia-smi", "-i", targetGPUUUID, "-pm", "0"}, "disable persistence mode"},
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
	checkShell := `
        TARGET_FILE="/dev/nvidia` + targetGPUIndex + `";
        for PID_DIR in /proc/[0-9]*; do
            PID=$(/usr/bin/basename "$PID_DIR");
            CMD_NAME=$(/usr/bin/cat "$PID_DIR/comm" 2>/dev/null || /usr/bin/echo "[unknown]")

            for FD_SYMLINK in "$PID_DIR"/fd/*; do
                if [ -L "$FD_SYMLINK" ]; then
                    TARGET_PATH=$(/usr/bin/readlink -f "$FD_SYMLINK" 2>/dev/null);
                    if [ "$TARGET_PATH" = "$TARGET_FILE" ]; then
                        /usr/bin/echo "$CMD_NAME";
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
	if deviceResourceType == "DRA" {
		rmInNvidia := []struct {
			cmd  []string
			desc string
		}{
			{[]string{"/usr/bin/rm", "-f", "/run/nvidia/driver/dev/nvidia" + targetGPUIndex}, "remove file /run/nvidia/driver/dev/nvidiaX"},
		}
		for _, step := range rmInNvidia {
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

		rmInDRA := []struct {
			cmd  []string
			desc string
		}{
			{[]string{"/usr/bin/rm", "-f", "/dev/nvidia" + targetGPUIndex}, "remove file /dev/nvidiaX"},
		}
		for _, step := range rmInDRA {
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

	// Detach gpu with nvidia-smi command. It should be executed in nvidia-driver-daemonset Pod.
	detachCommands := []struct {
		cmd  []string
		desc string
	}{
		{[]string{"/usr/bin/nvidia-smi", "drain", "-p", targetGPUBusID, "-m", "1"}, "set maintenance mode"},
		{[]string{"/usr/bin/nvidia-smi", "drain", "-p", targetGPUBusID, "-r"}, "reset GPU"},
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

func RunNvidiaSmi(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string) error {
	_, err := getGPUInfoFromNvidiaPod(ctx, client, clientset, restConfig, targetNodeName, "gpu_uuid")
	if err != nil {
		return err
	}

	return nil
}

func CreateDeviceTaint(ctx context.Context, client client.Client, resource *crov1alpha1.ComposableResource) error {
	resourceSliceList := &resourcev1.ResourceSliceList{}
	if err := client.List(ctx, resourceSliceList); err != nil {
		return err
	}

	for i := range resourceSliceList.Items {
		rs := &resourceSliceList.Items[i]
		for j := range rs.Spec.Devices {
			device := &rs.Spec.Devices[j]
			for attrName, attrValue := range device.Attributes {
				if attrName != "uuid" || *attrValue.StringValue != resource.Status.DeviceID {
					continue
				}

				device.Taints = append(device.Taints, resourcev1.DeviceTaint{
					Key:    "k8s.io/device-uuid",
					Value:  resource.Status.DeviceID,
					Effect: resourcev1.DeviceTaintEffectNoSchedule,
				})

				if err := client.Update(ctx, rs); err != nil {
					return err
				}
				return nil
			}
		}
	}

	return fmt.Errorf("failed to create device taint for resource %s: can not found device '%s' in ResourceSlices", resource.Name, resource.Status.DeviceID)
}

func execCommandInPod(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, namespace string, podName string, containerName string, command []string) (string, string, error) {
	request := clientset.CoreV1().RESTClient().Post().Resource("pods").Name(podName).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", request.URL())
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

func getNvidiaDriverDaemonsetPod(ctx context.Context, c client.Client, targetNodeName string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(""),
		client.MatchingLabels{"app.kubernetes.io/component": "nvidia-driver"},
	}
	if err := c.List(ctx, podList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var foundPod *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName == targetNodeName {
			foundPod = pod
		}
	}
	if foundPod == nil {
		return nil, fmt.Errorf("no Pod with label 'app.kubernetes.io/component=nvidia-driver' found on node %s", targetNodeName)
	}

	pod := podList.Items[0]
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

func getGPUInfoFromNvidiaPod(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, queryArgs string) ([]map[string]string, error) {
	fieldNames := strings.Split(queryArgs, ",")

	nvidiaPod, err := getNvidiaDriverDaemonsetPod(ctx, client, targetNodeName)
	if err != nil {
		return nil, err
	}

	command := []string{"/usr/bin/nvidia-smi", "--query-gpu=" + queryArgs, "--format=csv,noheader,nounits"}
	stdout, stderr, err := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		nvidiaPod.Namespace,
		nvidiaPod.Name,
		nvidiaPod.Spec.Containers[0].Name,
		command,
	)
	if stderr != "" || err != nil {
		return nil, fmt.Errorf("get gpu info command failed: err='%v', stderr='%s'", err, stderr)
	}

	var gpuInfos []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")

		gpuInfo := make(map[string]string)
		for i, fieldName := range fieldNames {
			gpuInfo[fieldName] = strings.TrimSpace(parts[i])
		}
		gpuInfos = append(gpuInfos, gpuInfo)
	}

	return gpuInfos, nil
}
