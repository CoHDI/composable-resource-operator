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
	"context"
	"errors"
	"os"

	"testing"

	crov1alpha1 "github.com/CoHDI/composable-resource-operator/api/v1alpha1"
	gpuv1 "github.com/NVIDIA/gpu-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	v1alpha3 "k8s.io/api/resource/v1alpha3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestUtilsGPUs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Utils GPUs Suite")
}

func testPod(nodeName string, phase corev1.PodPhase, ready corev1.ConditionStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nvidia-driver-pod",
			Namespace: "gpu-operator",
			Labels: map[string]string{
				"app.kubernetes.io/component": "nvidia-driver",
			},
		},
		Spec: corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{
			Phase: phase,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: ready,
			}},
		},
	}
}

func testResourceSlice(deviceUUID *string) *resourcev1.ResourceSlice {
	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "resourceslice-test"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: "nvidia",
			Pool: resourcev1.ResourcePool{
				Name: "test1",
			},
			Devices: []resourcev1.Device{{
				Name: "gpu-0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"uuid": {StringValue: deviceUUID},
				},
			}},
		},
	}
}

func testComposableResource(deviceUUID string) *crov1alpha1.ComposableResource {
	return &crov1alpha1.ComposableResource{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-0"},
		Status:     crov1alpha1.ComposableResourceStatus{DeviceID: deviceUUID},
		Spec:       crov1alpha1.ComposableResourceSpec{TargetNode: "worker-1"},
		
	}
}

var _ = Describe("GPU utility functions", func() {
	var (
		ctx    context.Context
		scheme *runtime.Scheme
	)
	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(resourcev1.AddToScheme(scheme)).To(Succeed())
		Expect(v1alpha3.AddToScheme(scheme)).To(Succeed())
		Expect(gpuv1.AddToScheme(scheme)).To(Succeed())
		Expect(crov1alpha1.AddToScheme(scheme)).To(Succeed())
	})
	Describe("AccountedAppInfo", func() {
		It("returns formated add info", func() {
			acc_app_info := AccountedAppInfo{
				GPUUUID: "123456789",
				ProcessName: "process1",
			}
			result := acc_app_info.String()
			Expect(result).To(Equal("GPUUUID: '123456789', ProcessName: 'process1'"))
		})
	})
	Describe("NvidiaGpuOperatorNamespace", func() {
		It("returns the default namespace when the environment variable is unset", func() {
			GinkgoT().Setenv("NVIDIA_GPU_OPERATOR_NAMESPACE", "")
			Expect(os.Unsetenv("NVIDIA_GPU_OPERATOR_NAMESPACE")).To(Succeed())

			Expect(NvidiaGpuOperatorNamespace()).To(Equal("gpu-operator"))
		})

		It("returns the default namespace when the environment variable is empty", func() {
			GinkgoT().Setenv("NVIDIA_GPU_OPERATOR_NAMESPACE", "")

			Expect(NvidiaGpuOperatorNamespace()).To(Equal("gpu-operator"))
		})

		It("returns the configured namespace when the environment variable is non-empty", func() {
			GinkgoT().Setenv("NVIDIA_GPU_OPERATOR_NAMESPACE", "custom-ns")

			Expect(NvidiaGpuOperatorNamespace()).To(Equal("custom-ns"))
		})
	})

	Describe("EnsureGPUDriverExists", func() {
		It("returns nil when an NVIDIA driver pod exists on the target node", func() {
			pod := testPod("worker-1", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

			err := EnsureGPUDriverExists(ctx, c, nil, nil, "worker-1")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns an error when no driver or ClusterPolicy exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			err := EnsureGPUDriverExists(ctx, c, nil, nil, "worker-1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ClusterPolicy CR was not found. Please install GPU Operator"))
		})
	})
	Describe("DetectGPUDriverType", func() {
		It("returns a GPU Operator installation error when no ClusterPolicy exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			driverType, err := DetectGPUDriverType(ctx, c, nil, nil, "worker-1")

			Expect(driverType).To(Equal(GPUDriverTypeNone))
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("ClusterPolicy CR was not found. Please install GPU Operator:"))
		})
	})
	Describe("getClusterPolicy", func() {
		It("returns the ClusterPolicy when exists", func() {

			policy := &gpuv1.ClusterPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-cluster-policy"},
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(policy).
				Build()

			got, err := getClusterPolicy(ctx, c)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).NotTo(BeNil())
			Expect(got.Name).To(Equal("gpu-cluster-policy"))
		})

		It("returns a NotFound error when no policy exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			got, err := getClusterPolicy(ctx, c)

			Expect(got).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
	Describe("hasNvidiaDriverDaemonsetPod", func() {
		It("returns true when an NVIDIA driver pod is on the target node", func() {
			pod := testPod("worker-1", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			found, err := hasNvidiaDriverDaemonsetPod(ctx, c, "worker-1")

			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
		})
		It("returns false when an NVIDIA driver pod is not on the target node", func() {
			pod := testPod("worker-2", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			found, err := hasNvidiaDriverDaemonsetPod(ctx, c, "worker-1")

			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		})
	})
	Describe("hasHostNvidiaProcDriver", func() {
		It("returns error when clientset or REST config is nil.", func() {
			result, err := hasHostNvidiaProcDriver(ctx, nil, nil, "worker-1")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeFalse())
			Expect(err.Error()).To(ContainSubstring("Kubernetes clientset or REST config is nil. Cannot check the host NVIDIA driver"))
		})
	})
	
	Describe("CheckGPUVisible", func() {
		It("return true when GPU is visible (device resource type DRA)", func() {
			deviceUUID := "device-uuid"
			resourceSlice := testResourceSlice(&deviceUUID)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(resourceSlice).
				Build()

			resource := testComposableResource(deviceUUID)
			result, err := CheckGPUVisible(ctx, c, nil, nil, "DRA", resource)
			Expect(result).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())
		})
		It("return false when no GPU is visible (device resource type DRA)", func() {
			deviceUUID := "device-uuid"
			resourceSlice := testResourceSlice(&deviceUUID)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(resourceSlice).
				Build()

			resource := testComposableResource("different-device-uuid")
			result, err := CheckGPUVisible(ctx, c, nil, nil, "DRA", resource)
			Expect(result).To(BeFalse())
			Expect(err).To(BeNil())
		})
		It("returns an error for a non-DRA resource type when the NVIDIA pod is missing", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			resource := testComposableResource("device-uuid")

			result, err := CheckGPUVisible(ctx, c, nil, nil, "NOtDRA", resource)

			Expect(result).To(BeFalse())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nvidia-driver-daemonset pod is not found on node worker-1"))
		})
	})

	Describe("CreateDeviceTaint", func() {
		It("creates a DeviceTaintRule ", func() {
			deviceUUID := "device-uuid"
			resourceSlice := testResourceSlice(&deviceUUID)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(resourceSlice).
				Build()

			resource := testComposableResource(deviceUUID)

			Expect(CreateDeviceTaint(ctx, c, resource)).To(Succeed())

			taintRule := &v1alpha3.DeviceTaintRule{}
			Expect(c.Get(ctx, client.ObjectKey{Name: "gpu-0-taint"}, taintRule)).To(Succeed())
			Expect(taintRule.Spec.DeviceSelector.Driver).To(HaveValue(Equal("nvidia")))
			Expect(taintRule.Spec.DeviceSelector.Pool).To(HaveValue(Equal("test1")))
			Expect(taintRule.Spec.DeviceSelector.Device).To(HaveValue(Equal("gpu-0")))
			Expect(taintRule.Spec.Taint).To(Equal(v1alpha3.DeviceTaint{
				Key:    "k8s.io/device-uuid",
				Value:  deviceUUID,
				Effect: v1alpha3.DeviceTaintEffectNoSchedule,
			}))
		})
		It("skip creating DeviceTaintRule:", func() {
			deviceUUID := "device-uuid"
			resourceSlice := &resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name: "resourceslice-test",
				},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: "nvidia",
					Pool: resourcev1.ResourcePool{
						Name: "test1",
					},
				},
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(resourceSlice).
				Build()

			resource := testComposableResource(deviceUUID)

			result := CreateDeviceTaint(ctx, c, resource)

			Expect(result).To(BeNil())
		})
		
	})

	Describe("DeleteDeviceTaint", func() {
		It("delete the DeviceTaintRule", func() {
			taintRule := &v1alpha3.DeviceTaintRule{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-0-taint"},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(taintRule).Build()
			resource := &crov1alpha1.ComposableResource{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-0"},
			}

			Expect(DeleteDeviceTaint(ctx, c, resource)).To(Succeed())

			err := c.Get(ctx, client.ObjectKey{Name: "gpu-0-taint"}, &v1alpha3.DeviceTaintRule{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
		It("DeviceTaintRule not exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			resource := &crov1alpha1.ComposableResource{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-0"},
			}

			Expect(DeleteDeviceTaint(ctx, c, resource)).To(Succeed())

			err := c.Get(ctx, client.ObjectKey{Name: "gpu-0-taint"}, &v1alpha3.DeviceTaintRule{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("HasDeviceTaint", func() {
		It("returns true when the DeviceTaintRule exists", func() {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(&v1alpha3.DeviceTaintRule{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0-taint"}}).
				Build()
			resource := &crov1alpha1.ComposableResource{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0"}}

			hasTaint, err := HasDeviceTaint(ctx, c, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(hasTaint).To(BeTrue())
		})

		It("returns false when the DeviceTaintRule does not exist", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			resource := &crov1alpha1.ComposableResource{ObjectMeta: metav1.ObjectMeta{Name: "gpu-0"}}

			hasTaint, err := HasDeviceTaint(ctx, c, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(hasTaint).To(BeFalse())
		})

	})
	Describe("getChrootCacheKey", func() {
		It("returns ChrootCacheKey", func() {
			pod := testPod("worker-1", corev1.PodRunning, corev1.ConditionTrue)

			result := getChrootCacheKey("worker-1", pod)

			Expect(result).To(Equal("worker-1|gpu-operator|nvidia-driver-pod"))
		})

	})
	Describe("FindReadyNvidiaDriverPod", func() {
		BeforeEach(func() {
			GinkgoT().Setenv("NVIDIA_GPU_OPERATOR_NAMESPACE", "gpu-operator")
		})

		It("returns an empty reason and the list error when the API request fails", func() {
			listErr := errors.New("failed to retrieve pods")
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						return listErr
					},
				}).Build()

			pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(pod).To(BeNil())
			Expect(reason).To(BeEmpty())
			Expect(err).To(MatchError("failed to list pods: failed to retrieve pods"))
			Expect(errors.Is(err, listErr)).To(BeTrue())
		})

		It("returns a not found reason when no driver pod exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(pod).To(BeNil())
			Expect(reason).To(Equal("nvidia-driver-daemonset pod is not found on node worker-1"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a not running reason when the driver pod is pending", func() {
			driverPod := testPod("worker-1", corev1.PodPending, corev1.ConditionTrue)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(driverPod).Build()

			pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(pod).To(BeNil())
			Expect(reason).To(Equal("nvidia-driver-daemonset pod is not running on node worker-1"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a not ready reason when the running driver pod is not ready", func() {
			driverPod := testPod("worker-1", corev1.PodRunning, corev1.ConditionFalse)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(driverPod).Build()

			pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(pod).To(BeNil())
			Expect(reason).To(Equal("nvidia-driver-daemonset pod is not ready on node worker-1"))
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns the running and ready driver pod with an empty reason", func() {
			driverPod := testPod("worker-1", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(driverPod).Build()

			pod, reason, err := FindReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(err).NotTo(HaveOccurred())
			Expect(reason).To(BeEmpty())
			Expect(pod).NotTo(BeNil())
			Expect(pod.Name).To(Equal(driverPod.Name))
			Expect(pod.Namespace).To(Equal(driverPod.Namespace))
			Expect(pod.Spec).To(Equal(driverPod.Spec))
			Expect(pod.Status).To(Equal(driverPod.Status))
		})
	})

	Describe("requireReadyNvidiaDriverPod", func() {
		BeforeEach(func() {
			GinkgoT().Setenv("NVIDIA_GPU_OPERATOR_NAMESPACE", "gpu-operator")
		})

		It("returns nil and the list error when the API request fails", func() {
			listErr := errors.New("failed to retrieve pods")
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						return listErr
					},
				}).Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(driverpod).To(BeNil())
			Expect(err).To(MatchError("failed to list pods: failed to retrieve pods"))
			Expect(errors.Is(err, listErr)).To(BeTrue())
		})

		It("returns nil and a not found error when no driver pod exists", func() {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")

			Expect(driverpod).To(BeNil())
			Expect(err).To(MatchError("nvidia-driver-daemonset pod is not found on node worker-1"))
		})

		It("returns the NVIDIA driver pod when it exists on the target node", func() {
			pod := testPod("worker-1", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(driverpod).NotTo(BeNil())
			Expect(driverpod.Name).To(Equal("nvidia-driver-pod"))
			Expect(driverpod.Namespace).To(Equal(pod.Namespace))
			Expect(driverpod.Spec).To(Equal(pod.Spec))
			Expect(driverpod.Status).To(Equal(pod.Status))
		})
		It("returns error when pod is not ready", func() {
			pod := testPod("worker-1", corev1.PodRunning, corev1.ConditionFalse)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")
			Expect(err).To(HaveOccurred())
			Expect(driverpod).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("nvidia-driver-daemonset pod is not ready on node worker-1"))
		})
		It("returns error when pod is in pending state", func() {
			pod := testPod("worker-1", corev1.PodPending, corev1.ConditionFalse)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")
			Expect(err).To(HaveOccurred())
			Expect(driverpod).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("nvidia-driver-daemonset pod is not running on node worker-1"))
		})
		It("returns error when pod is not found on node", func() {
			pod := testPod("worker-2", corev1.PodRunning, corev1.ConditionTrue)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			driverpod, err := requireReadyNvidiaDriverPod(ctx, c, "worker-1")
			Expect(driverpod).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nvidia-driver-daemonset pod is not found on node worker-1"))
		})
	})
})
