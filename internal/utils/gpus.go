/**
 * (C) Copyright 2025 The CoHDI Authors.
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

package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	v1alpha3 "k8s.io/api/resource/v1alpha3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	crov1alpha1 "github.com/CoHDI/composable-resource-operator/api/v1alpha1"
	gpuv1 "github.com/NVIDIA/gpu-operator/api/v1"
)

var gpusLog = ctrl.Log.WithName("utils_gpus")

const (
	defaultNvidiaGpuOperatorNamespace = "gpu-operator"

	nvidiaDriverRoot = "/run/nvidia/driver"

	nvidiaDRADriverName        = "dra-driver-nvidia-gpu"
	nvidiaDRAKubeletPluginName = nvidiaDRADriverName + "-kubelet-plugin"
)

var (
	// Candidate paths for chroot binary in order of preference
	chrootCandidatePaths = []string{
		"/usr/sbin/chroot",
		"/usr/bin/chroot",
		"/sbin/chroot",
		"/bin/chroot",
		"/usr/local/sbin/chroot",
		"/usr/local/bin/chroot",
	}

	// Cache for resolved chroot paths per node/pod
	chrootPathCache = make(map[string]string)
	chrootPathMutex sync.Mutex
)

type AccountedAppInfo struct {
	GPUUUID     string
	ProcessName string
}

func (a AccountedAppInfo) String() string {
	return fmt.Sprintf("GPUUUID: '%s', ProcessName: '%s'", a.GPUUUID, a.ProcessName)
}

func NvidiaGpuOperatorNamespace() string {
	if ns := os.Getenv("NVIDIA_GPU_OPERATOR_NAMESPACE"); ns != "" {
		return ns
	}
	return defaultNvidiaGpuOperatorNamespace
}

func EnsureGPUDriverEnabled(ctx context.Context, c client.Client) error {
	clusterPolicy, err := getClusterPolicy(ctx, c)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("ClusterPolicy CR was not found. Please install GPU Operator: %w", err)
		}
		return fmt.Errorf("failed to get ClusterPolicy: %w", err)
	}

	if clusterPolicy.Spec.Driver.IsDriverEnabled() {
		return nil
	}

	return fmt.Errorf("NVIDIA driver is not enabled. Please set ClusterPolicy's driver.enabled to true")
}

func getClusterPolicy(ctx context.Context, c client.Client) (*gpuv1.ClusterPolicy, error) {
	clusterPolicyList := &gpuv1.ClusterPolicyList{}

	if err := c.List(ctx, clusterPolicyList); err != nil {
		return nil, err
	}
	if len(clusterPolicyList.Items) == 0 {
		return nil, apierrors.NewNotFound(gpuv1.GroupVersion.WithResource("clusterpolicies").GroupResource(), "")
	}

	return &clusterPolicyList.Items[0], nil
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

func CheckNoGPULoads(ctx context.Context, c client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID *string) error {

	if err := EnsureGPUDriverEnabled(ctx, c); err != nil {
		return err
	}

	pod, err := requireReadyNvidiaDriverPod(ctx, c, targetNodeName)
	if err != nil {
		if strings.Contains(err.Error(), "nvidia-driver-daemonset pod is not found") {
			gpusLog.Info("nvidia-driver-daemonset Pod is not found, it means that there is no GPU on the node, so there is no load, skip it", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
			return nil
		}
		return err
	}

	command := []string{"/usr/bin/nvidia-smi", "--query-compute-apps=gpu_uuid,process_name", "--format=csv,noheader,nounits"}

	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		command,
	)
	if strings.TrimSpace(string(stdOut)) == "No devices were found" {
		// If the GPU is not found for nvidia-smi command, it means that there is no GPU on the node, so there is no load.
		gpusLog.Info("GPU is not found for nvidia-smi command, it means that there is no GPU on the node, so there is no load, skip it", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
		return nil
	} else if stdErr != "" || execErr != nil {
		return fmt.Errorf("run nvidia-smi in pod '%s' to check gpu loads failed: '%v', stderr: '%s', stdout: '%s'", pod.Name, execErr, stdErr, stdOut)
	}

	var accountedApps []AccountedAppInfo

	lines := strings.Split(strings.TrimSpace(stdOut), "\n")
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

	for _, appInfo := range accountedApps {
		if targetGPUUUID == nil || appInfo.GPUUUID == *targetGPUUUID {
			return fmt.Errorf("found gpu load on gpu '%s': %v", appInfo.GPUUUID, accountedApps)
		}
	}
	gpusLog.Info("no gpu loads found", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)

	return nil
}

func DrainGPU(ctx context.Context, c client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, targetGPUUUID string, deviceResourceType string) error {
	gpusLog.Info("start draining gpu", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)

	if err := EnsureGPUDriverEnabled(ctx, c); err != nil {
		return err
	}

	if deviceResourceType == "DRA" {
		// OCP (K8S) + DRA
		nvidiaPod, err := requireReadyNvidiaDriverPod(ctx, c, targetNodeName)
		if err != nil {
			if !strings.Contains(err.Error(), "nvidia-driver-daemonset pod is not found") {
				return err
			}

			// If the nvidia-driver-daemonset Pod is not found and GPU labels are gone from the target node, the GPU has already been drained.
			checkNodeLabels, checkNodeLabelsErr := clientset.CoreV1().Nodes().Get(ctx, targetNodeName, metav1.GetOptions{})
			if checkNodeLabelsErr != nil {
				return checkNodeLabelsErr
			}
			hasPciLabel := checkNodeLabels.Labels["feature.node.kubernetes.io/pci-10de.present"] == "true"
			hasGpuLabel := checkNodeLabels.Labels["nvidia.com/gpu.present"] == "true"
			if !hasPciLabel && !hasGpuLabel {
				gpusLog.Info("GPU labels not present or 'false' on the target node (feature.node.kubernetes.io/pci-10de.present=true and nvidia.com/gpu.present=true not found). Drain is not required; skipping.", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
				return nil
			}

			return err
		}
		if err := killNvidiaPersistencedInDriverPod(ctx, clientset, restConfig, nvidiaPod); err != nil {
			return err
		}

		// Resolve the chroot command path once and reuse it
		chrootCmd, err := resolveChrootCommand(ctx, clientset, restConfig, nvidiaPod, targetNodeName)
		if err != nil {
			return err
		}

		// Get information about the GPU to be drained.
		gpuInfos, err := getGPUInfoFromProc(
			ctx,
			clientset,
			restConfig,
			nvidiaPod,
			targetNodeName,
			"device_minor,gpu_uuid,pci.bus_id",
			chrootCmd,
			nvidiaDriverRoot,
			"no GPUs detected via nvidia-driver-daemonset driver root proc scan",
			"Successfully got GPU info from nvidia-driver-daemonset driver root",
		)
		if err != nil {
			return err
		}

		targetGPUDeviceMinor := ""
		targetGPUBusID := ""
		for _, gpuInfo := range gpuInfos {
			if gpuInfo["gpu_uuid"] == targetGPUUUID {
				targetGPUDeviceMinor = gpuInfo["device_minor"]
				targetGPUBusID = strings.ToUpper(strings.TrimSpace(gpuInfo["pci.bus_id"]))
				break
			}
		}
		if targetGPUBusID == "" {
			// It can be considered to have been drained, so no error is required.
			gpusLog.Info("cannot find the gpu bus id, it should have been drained", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
			return nil
		}

		gpusLog.Info("find the gpu bus id", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID, "targetGPUBusID", targetGPUBusID, "targetGPUDeviceMinor", targetGPUDeviceMinor)

		// Get the GPU drain status.
		isDraining, err := checkGPUDrainStatus(ctx, clientset, restConfig, nvidiaPod, targetNodeName, targetGPUBusID)
		if err != nil {
			return err
		}

		// Disable gpu with nvidia-smi command. It should be executed in nvidia-driver-daemonset Pod.
		if !isDraining {
			stdOut, stdErr, execErr := execCommandInPod(
				ctx,
				clientset,
				restConfig,
				nvidiaPod.Namespace,
				nvidiaPod.Name,
				nvidiaPod.Spec.Containers[0].Name,
				[]string{chrootCmd, nvidiaDriverRoot, "/usr/bin/nvidia-smi", "-i", targetGPUUUID, "-pm", "0"},
			)
			if execErr != nil || stdErr != "" {
				if strings.Contains(stdOut, "No devices were found") {
					gpusLog.Info("GPU is not found for nvidia-smi -i command, skip it", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
				} else {
					return fmt.Errorf("deatch command 'disable persistence mode' failed: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
				}
			}
		} else {
			gpusLog.Info("GPU is already in draining status, skip the step", "step", "disable persistence mode", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
		}

		checkNvidiaShell := `
TARGET_FILE="/dev/nvidia` + targetGPUDeviceMinor + `";
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
		checkNvidiaCommand := []string{chrootCmd, nvidiaDriverRoot, "sh", "-c", checkNvidiaShell}
		stdOut, stdErr, execErr := execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			checkNvidiaCommand,
		)
		if stdErr != "" || execErr != nil {
			return fmt.Errorf("check /dev/nvidiaX command in nvidia-driver-daemonset driver root failed: '%v', stderr: '%s'", execErr, stdErr)
		}
		if stdOut != "" {
			return fmt.Errorf("check /dev/nvidiaX command in nvidia-driver-daemonset driver root failed: there is a process %s occupied the nvidiaX file", stdOut)
		}

		stdOut, stdErr, execErr = execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			[]string{chrootCmd, nvidiaDriverRoot, "/usr/bin/rm", "-f", "/dev/nvidia" + targetGPUDeviceMinor},
		)
		if execErr != nil || stdErr != "" {
			return fmt.Errorf("delete device file command 'remove file /dev/nvidiaX' failed: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
		}

		if !isDraining {
			stdOut, stdErr, execErr = execCommandInPod(
				ctx,
				clientset,
				restConfig,
				nvidiaPod.Namespace,
				nvidiaPod.Name,
				nvidiaPod.Spec.Containers[0].Name,
				[]string{chrootCmd, nvidiaDriverRoot, "/usr/bin/nvidia-smi", "drain", "-p", targetGPUBusID, "-m", "1"},
			)
			if execErr != nil || stdErr != "" {
				return fmt.Errorf("detach command 'set maintenance mode' failed: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
			}
		} else {
			gpusLog.Info("GPU is already in draining status, skip the step", "step", "set maintenance mode", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
		}

		isVM := isVMByCPUInfoInNvidiaDriverPod(ctx, clientset, restConfig, nvidiaPod, targetNodeName)
		resetMethod := "nvidia-smi drain -r"
		resetCommand := []string{chrootCmd, nvidiaDriverRoot, "/usr/bin/nvidia-smi", "drain", "-p", targetGPUBusID, "-r"}
		if isVM {
			resetMethod = "sysfs remove"
			busIDForSysfs := strings.ToLower(targetGPUBusID)
			resetCommand = []string{chrootCmd, nvidiaDriverRoot, "/bin/sh", "-c", fmt.Sprintf("/usr/bin/echo 1 | /usr/bin/tee /sys/bus/pci/devices/%s/remove > /dev/null", busIDForSysfs)}
		}
		gpusLog.Info("selected GPU reset method for nvidia-driver-daemonset pod", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID, "targetGPUBusID", targetGPUBusID, "isVM", isVM, "resetMethod", resetMethod)

		stdOut, stdErr, execErr = execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			resetCommand,
		)
		if execErr != nil || stdErr != "" {
			return fmt.Errorf("detach command 'reset GPU' failed: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
		}
	} else {
		// OCP + DEVICE_PLUGIN
		nvidiaPod, err := requireReadyNvidiaDriverPod(ctx, c, targetNodeName)
		if err != nil {
			return err
		}

		// Get information about the GPU to be drained.
		gpuInfos, err := getGPUInfoFromNvidiaPod(ctx, c, clientset, restConfig, targetNodeName, "device_minor,gpu_uuid,pci.bus_id")
		if err != nil {
			return err
		}

		targetGPUDeviceMinor := ""
		targetGPUBusID := ""
		for _, gpuInfo := range gpuInfos {
			if gpuInfo["gpu_uuid"] == targetGPUUUID {
				targetGPUDeviceMinor = gpuInfo["device_minor"]
				targetGPUBusID = strings.TrimPrefix(gpuInfo["pci.bus_id"], "0000")
				break
			}
		}
		if targetGPUBusID == "" {
			// It can be considered to have been drained, so no error is required.
			gpusLog.Info("cannot find the gpu bus id, it should have been drained", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID)
			return nil
		}

		gpusLog.Info("find the gpu bus id", "targetNodeName", targetNodeName, "targetGPUUUID", targetGPUUUID, "targetGPUBusID", targetGPUBusID, "targetGPUDeviceMinor", targetGPUDeviceMinor)

		// Disable gpu with nvidia-smi command. It should be executed in nvidia-driver-daemonset Pod.
		disableCommand := []struct {
			cmd  []string
			desc string
		}{
			{[]string{"/usr/bin/nvidia-smi", "-i", targetGPUUUID, "-pm", "0"}, "disable persistence mode"},
		}
		for _, step := range disableCommand {
			stdOut, stdErr, execErr := execCommandInPod(
				ctx,
				clientset,
				restConfig,
				nvidiaPod.Namespace,
				nvidiaPod.Name,
				nvidiaPod.Spec.Containers[0].Name,
				step.cmd,
			)
			if execErr != nil || stdErr != "" {
				return fmt.Errorf("deatch command '%s' failed: '%v', stderr: '%s', stdout: '%s'", step.desc, execErr, stdErr, stdOut)
			}
		}

		// Check that /dev/nvidiaX is not open.
		checkShell := `
TARGET_FILE="/dev/nvidia` + targetGPUDeviceMinor + `";
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
		stdOut, stdErr, execErr := execCommandInPod(
			ctx,
			clientset,
			restConfig,
			nvidiaPod.Namespace,
			nvidiaPod.Name,
			nvidiaPod.Spec.Containers[0].Name,
			checkCommand,
		)
		if stdErr != "" || execErr != nil {
			return fmt.Errorf("check /dev/nvidiaX command failed: '%v', stderr: '%s'", execErr, stdErr)
		}
		if stdOut != "" {
			return fmt.Errorf("check /dev/nvidiaX command failed: there is a process %s occupied the nvidiaX file", stdOut)
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
			stdOut, stdErr, execErr := execCommandInPod(
				ctx,
				clientset,
				restConfig,
				nvidiaPod.Namespace,
				nvidiaPod.Name,
				nvidiaPod.Spec.Containers[0].Name,
				step.cmd,
			)
			if execErr != nil || stdErr != "" {
				if step.desc == "reset GPU" {
					continue
				}
				return fmt.Errorf("detach command '%s' failed: '%v', stderr: '%s', stdout: '%s'", step.desc, execErr, stdErr, stdOut)
			}
		}
	}

	return nil
}

func RunNvidiaSmi(ctx context.Context, c client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string) error {
	_, err := getGPUInfoFromNvidiaPod(ctx, c, clientset, restConfig, targetNodeName, "gpu_uuid")
	if err != nil {
		return err
	}
	gpusLog.Info("Run nvidia-smi successfully in nvidia-driver-daemonset pod", "target_node_name", targetNodeName)
	return nil
}

func CreateDeviceTaint(ctx context.Context, c client.Client, resource *crov1alpha1.ComposableResource) error {
	taintName := fmt.Sprintf("%s-taint", resource.Name)

	existing := &v1alpha3.DeviceTaintRule{}
	err := c.Get(ctx, client.ObjectKey{Name: taintName}, existing)
	if err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	resourceSliceList := &resourcev1.ResourceSliceList{}
	if err := c.List(ctx, resourceSliceList); err != nil {
		return err
	}

	var (
		driverName string
		poolName   string
		deviceName string
	)
Loop:
	for _, rs := range resourceSliceList.Items {
		for _, device := range rs.Spec.Devices {
			for attrName, attrValue := range device.Attributes {
				if attrName == "uuid" && attrValue.StringValue != nil && *attrValue.StringValue == resource.Status.DeviceID {
					driverName = rs.Spec.Driver
					poolName = rs.Spec.Pool.Name
					deviceName = device.Name
					break Loop
				}
			}
		}
	}

	if deviceName == "" {
		gpusLog.Info("skip creating DeviceTaintRule: device not found in ResourceSlices", "uuid", resource.Status.DeviceID)
		return nil
	}

	taintRule := &v1alpha3.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: taintName,
		},
		Spec: v1alpha3.DeviceTaintRuleSpec{
			DeviceSelector: &v1alpha3.DeviceTaintSelector{
				Driver: &driverName,
				Pool:   &poolName,
				Device: &deviceName,
			},
			Taint: v1alpha3.DeviceTaint{
				Key:    "k8s.io/device-uuid",
				Value:  resource.Status.DeviceID,
				Effect: v1alpha3.DeviceTaintEffectNoSchedule,
			},
		},
	}

	if err := c.Create(ctx, taintRule); err != nil {
		return fmt.Errorf("failed to create DeviceTaintRule %s: %w", taintName, err)
	}

	return nil
}

func DeleteDeviceTaint(ctx context.Context, c client.Client, resource *crov1alpha1.ComposableResource) error {
	taintName := fmt.Sprintf("%s-taint", resource.Name)

	taintRule := &v1alpha3.DeviceTaintRule{}
	err := c.Get(ctx, client.ObjectKey{Name: taintName}, taintRule)
	if apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get DeviceTaintRule %s: %w", taintName, err)
	}

	if err := c.Delete(ctx, taintRule); err != nil {
		return fmt.Errorf("failed to delete DeviceTaintRule %s: %w", taintName, err)
	}

	return nil
}

func HasDeviceTaint(ctx context.Context, c client.Client, resource *crov1alpha1.ComposableResource) (bool, error) {
	taintName := fmt.Sprintf("%s-taint", resource.Name)

	taintRule := &v1alpha3.DeviceTaintRule{}
	err := c.Get(ctx, client.ObjectKey{Name: taintName}, taintRule)
	if apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get DeviceTaintRule %s: %w", taintName, err)
	}

	return true, nil
}

// getChrootCacheKey creates a unique cache key for a node/pod combination.
func getChrootCacheKey(targetNodeName string, pod *corev1.Pod) string {
	return fmt.Sprintf("%s|%s|%s", targetNodeName, pod.Namespace, pod.Name)
}

// resolveChrootCommand finds the correct path to the chroot binary on the node/pod.
// It tries the candidate paths and caches the result per node/pod to avoid repeated resolution.
func resolveChrootCommand(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, pod *corev1.Pod, targetNodeName string) (string, error) {
	cacheKey := getChrootCacheKey(targetNodeName, pod)

	// Check cache first
	chrootPathMutex.Lock()
	if cachedPath, exists := chrootPathCache[cacheKey]; exists {
		chrootPathMutex.Unlock()
		return cachedPath, nil
	}
	chrootPathMutex.Unlock()

	// Try each candidate path
	for _, candidate := range chrootCandidatePaths {
		_, _, err := execCommandInPod(ctx, clientset, restConfig, pod.Namespace, pod.Name, pod.Spec.Containers[0].Name, []string{"/bin/sh", "-c", fmt.Sprintf("[ -x %s ]", candidate)})
		if err == nil {
			// Path exists and is executable
			chrootPathMutex.Lock()
			chrootPathCache[cacheKey] = candidate
			chrootPathMutex.Unlock()
			gpusLog.Info("resolved chroot command path", "targetNodeName", targetNodeName, "podNamespace", pod.Namespace, "podName", pod.Name, "chrootPath", candidate)
			return candidate, nil
		}
	}

	// Verify if chroot is available in PATH
	out, _, err := execCommandInPod(ctx, clientset, restConfig,
		pod.Namespace, pod.Name, pod.Spec.Containers[0].Name,
		[]string{"/bin/sh", "-c", "command -v chroot"},
	)
	resolved := strings.TrimSpace(out)
	if err == nil && resolved != "" {
		gpusLog.Info("rejecting PATH-resolved chroot which does not match trusted candidate paths",
			"targetNodeName", targetNodeName, "podNamespace", pod.Namespace, "podName", pod.Name, "resolvedPath", resolved, "trustedPaths", chrootCandidatePaths)
		return "", fmt.Errorf("failed to resolve chroot command: trusted candidate paths %v not found, but command %s found in PATH on node %s, pod %s/%s",
			chrootCandidatePaths, resolved, targetNodeName, pod.Namespace, pod.Name)
	}

	// Failed to resolve chroot command
	return "", fmt.Errorf("failed to resolve chroot command: trusted candidate paths %v not found/chroot not found in PATH/exec in pod failed on node %s, pod %s/%s",
		chrootCandidatePaths, targetNodeName, pod.Namespace, pod.Name)
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

	var stdOut, stdErr bytes.Buffer
	err = executor.StreamWithContext(
		ctx,
		remotecommand.StreamOptions{
			Stdout: &stdOut,
			Stderr: &stdErr,
		},
	)
	return stdOut.String(), stdErr.String(), err
}

func requireReadyNvidiaDriverPod(ctx context.Context, c client.Client, targetNodeName string) (*corev1.Pod, error) {
	pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, targetNodeName)
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, errors.New(reason)
	}
	return pod, nil
}

func FindReadyNvidiaDriverPod(ctx context.Context, c client.Client, targetNodeName string) (*corev1.Pod, string, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(NvidiaGpuOperatorNamespace()),
		client.MatchingLabels{"app.kubernetes.io/component": "nvidia-driver"},
	}
	if err := c.List(ctx, podList, listOpts...); err != nil {
		return nil, "", fmt.Errorf("failed to list pods: %w", err)
	}

	foundOnNode := false
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != targetNodeName {
			continue
		}
		foundOnNode = true
		if pod.Status.Phase == corev1.PodRunning {
			podReady := false
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					podReady = true
					break
				}
			}
			if !podReady {
				gpusLog.Info("nvidia-driver-daemonset pod is still installing on node", "targetNodeName", targetNodeName, "podName", pod.Name)
				return nil, fmt.Sprintf("nvidia-driver-daemonset pod is not ready on node %s", targetNodeName), nil
			}
			return pod, "", nil
		}
	}

	if !foundOnNode {
		return nil, fmt.Sprintf("nvidia-driver-daemonset pod is not found on node %s", targetNodeName), nil
	}

	return nil, fmt.Sprintf("nvidia-driver-daemonset pod is not running on node %s", targetNodeName), nil
}

func getDRAKubeletPluginPod(ctx context.Context, clientset *kubernetes.Clientset, targetNodeName string) (*corev1.Pod, error) {
	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/name=%s", nvidiaDRADriverName),
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", targetNodeName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}

	for _, pod := range pods.Items {
		if strings.HasPrefix(pod.Name, nvidiaDRAKubeletPluginName) {
			return &pod, nil
		}
	}

	return nil, nil
}

func TerminateKubeletPluginPodOnNode(ctx context.Context, clientset *kubernetes.Clientset, targetNodeName string) error {
	gpusLog.Info("start terminate kubelet pod ", "target_node_name", targetNodeName)
	draPod, err := getDRAKubeletPluginPod(ctx, clientset, targetNodeName)

	if err != nil {
		return err
	}

	if draPod == nil {
		gpusLog.Info("skip terminate because DRA kubelet plugin pod not found")
		return nil
	}

	if time.Since(draPod.CreationTimestamp.Time) <= 10*time.Second {
		gpusLog.Info("skip terminate because DRA kubelet plugin pod already terminated recently")
		return nil
	}

	return clientset.CoreV1().Pods(draPod.Namespace).Delete(ctx, draPod.Name, metav1.DeleteOptions{})
}

func killNvidiaPersistencedInDriverPod(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, pod *corev1.Pod) error {
	killScript := `
pid_file="/var/run/nvidia-persistenced/nvidia-persistenced.pid"

[ -s "$pid_file" ] || exit 0

pid=$(cat "$pid_file" 2>/dev/null || true)
[ -n "$pid" ] || exit 0

[ -r "/proc/$pid/comm" ] || exit 0
comm=$(cat "/proc/$pid/comm" 2>/dev/null || true)
[ "$comm" = "nvidia-persiste" ] || exit 0

kill -9 "$pid" >/dev/null 2>&1 || true
exit 0
`
	command := []string{"/bin/sh", "-c", killScript}
	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		command,
	)
	if stdErr != "" || execErr != nil {
		return fmt.Errorf("failed to kill nvidia-persistenced in pod '%s': '%v', stderr: '%s', stdout: '%s'", pod.Name, execErr, stdErr, stdOut)
	}

	gpusLog.Info("ran kill command for nvidia-persistenced in nvidia-driver-daemonset pod", "podName", pod.Name, "namespace", pod.Namespace)
	return nil
}

func getGPUInfoFromNvidiaPod(ctx context.Context, client client.Client, clientset *kubernetes.Clientset, restConfig *rest.Config, targetNodeName string, queryArgs string) ([]map[string]string, error) {
	fieldNames := strings.Split(queryArgs, ",")

	nvidiaPod, err := requireReadyNvidiaDriverPod(ctx, client, targetNodeName)
	if err != nil {
		return nil, err
	}

	command := []string{"/usr/bin/nvidia-smi", "--query-gpu=" + queryArgs, "--format=csv,noheader,nounits"}
	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		nvidiaPod.Namespace,
		nvidiaPod.Name,
		nvidiaPod.Spec.Containers[0].Name,
		command,
	)
	if strings.TrimSpace(string(stdOut)) == "No devices were found" {
		gpusLog.Info("GPU is not found for nvidia-smi command in nvidia-driver-daemonset pod, it means that there is no GPU on the node, so it returns an empty value.", "targetNodeName", targetNodeName)
		return []map[string]string{}, nil
	} else if stdErr != "" || execErr != nil {
		return nil, fmt.Errorf("get gpu info command failed: err: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
	}

	var gpuInfos []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(string(stdOut)), "\n") {
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

func isVMByCPUInfoInNvidiaDriverPod(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, pod *corev1.Pod, targetNodeName string) bool {
	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		[]string{"/bin/sh", "-c", "if /usr/bin/grep -qi hypervisor /proc/cpuinfo 2>/dev/null; then /usr/bin/echo true; fi"},
	)
	if execErr != nil || stdErr != "" {
		gpusLog.Info("failed to detect virtualization from /proc/cpuinfo in nvidia-driver-daemonset pod, treat as bare metal", "targetNodeName", targetNodeName, "podName", pod.Name, "execErr", execErr, "stderr", stdErr, "stdout", stdOut)
		return false
	}

	isVM := strings.TrimSpace(stdOut) == "true"
	gpusLog.Info("detected virtualization from /proc/cpuinfo in nvidia-driver-daemonset pod", "targetNodeName", targetNodeName, "podName", pod.Name, "isVM", isVM)
	return isVM
}

func checkGPUDrainStatus(ctx context.Context, clientset *kubernetes.Clientset, restConfig *rest.Config, pod *corev1.Pod, targetNodeName string, targetGPUBusID string) (bool, error) {
	busID := strings.TrimSpace(targetGPUBusID)
	if busID == "" {
		return false, fmt.Errorf("target GPU bus ID is empty")
	}

	command := []string{"/usr/bin/nvidia-smi", "drain", "-p", busID, "-q"}

	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		command,
	)
	if execErr != nil || stdErr != "" {
		return false, fmt.Errorf("check gpu drain status command failed: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
	}

	trimmed := strings.TrimSpace(stdOut)
	if trimmed == "" {
		return false, fmt.Errorf("nvidia-smi drain query returned empty output (node=%s, busID=%s)", targetNodeName, busID)
	}

	for _, line := range strings.Split(trimmed, "\n") {
		value := strings.TrimSpace(line)
		lower := strings.ToLower(value)
		if !strings.Contains(lower, "drain") {
			continue
		}

		if idx := strings.Index(lower, ":"); idx >= 0 {
			status := strings.TrimSpace(lower[idx+1:])
			status = strings.Trim(status, ".")
			switch {
			case strings.Contains(status, "not draining"):
				gpusLog.Info("GPU is not draining", "node", targetNodeName, "targetGPUBusID", busID)
				return false, nil
			case strings.Contains(status, "draining"):
				gpusLog.Info("GPU is draining", "node", targetNodeName, "targetGPUBusID", busID)
				return true, nil
			}
		}
	}

	// Fallback: if no explicit drain state detected, treat as not drained.
	return false, fmt.Errorf("nvidia-smi drain query did not contain recognizable drain state (node=%s, busID=%s, raw=%s)", targetNodeName, busID, trimmed)
}

func getGPUInfoFromProc(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	restConfig *rest.Config,
	pod *corev1.Pod,
	targetNodeName string,
	queryArgs string,
	chrootBin string,
	chrootRoot string,
	noGPUMessage string,
	successMessage string,
) ([]map[string]string, error) {
	fieldNames := strings.Split(queryArgs, ",")

	script := `
if [ ! -d /proc/driver/nvidia/gpus ]; then
	exit 0
fi

for dir in /proc/driver/nvidia/gpus/*; do
	if [ ! -d "$dir" ]; then
		continue
	fi
	info="$dir/information"
	if [ ! -f "$info" ]; then
		continue
	fi
	minor=$(awk '/^Device Minor:/ {print $3; exit}' "$info")
	uuid=$(awk '/^GPU UUID:/ {print $3; exit}' "$info")
	bus=$(awk '/^Bus Location:/ {print $3; exit}' "$info")
	if [ -n "$minor" ] && [ -n "$uuid" ] && [ -n "$bus" ]; then
		printf '%s,%s,%s\n' "$minor" "$uuid" "$bus"
	fi
done
`
	command := []string{chrootBin, chrootRoot, "/bin/sh", "-c", script}
	stdOut, stdErr, execErr := execCommandInPod(
		ctx,
		clientset,
		restConfig,
		pod.Namespace,
		pod.Name,
		pod.Spec.Containers[0].Name,
		command,
	)
	if stdErr != "" || execErr != nil {
		return nil, fmt.Errorf("get gpu info command failed: err: '%v', stderr: '%s', stdout: '%s'", execErr, stdErr, stdOut)
	}

	trimmedStdout := strings.TrimSpace(stdOut)
	if trimmedStdout == "" {
		gpusLog.Info(noGPUMessage, "targetNodeName", targetNodeName)
		return []map[string]string{}, nil
	}

	var gpuInfos []map[string]string
	for _, line := range strings.Split(trimmedStdout, "\n") {
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 3 {
			return nil, fmt.Errorf("unexpected GPU information format: '%s'", line)
		}

		values := map[string]string{
			"device_minor": strings.TrimSpace(parts[0]),
			"gpu_uuid":     strings.TrimSpace(parts[1]),
			"pci.bus_id":   strings.TrimSpace(parts[2]),
		}

		gpuInfo := make(map[string]string)
		for _, fieldName := range fieldNames {
			name := strings.TrimSpace(fieldName)
			value, ok := values[name]
			if !ok {
				return nil, fmt.Errorf("unsupported field '%s' requested in queryArgs", name)
			}
			gpuInfo[name] = value
		}
		gpuInfos = append(gpuInfos, gpuInfo)
	}

	gpusLog.Info(successMessage, "targetNodeName", targetNodeName, "gpuInfos", gpuInfos)
	return gpuInfos, nil
}
