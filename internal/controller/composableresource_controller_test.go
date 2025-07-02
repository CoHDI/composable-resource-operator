/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"

	"github.com/agiledragon/gomonkey/v2"
	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	machinev1beta1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1alpha3 "k8s.io/api/resource/v1alpha3"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	fticmapi "github.com/IBM/composable-resource-operator/internal/cdi/fti/cm/api"
	ftifmapi "github.com/IBM/composable-resource-operator/internal/cdi/fti/fm/api"
)

func createComposableResource(
	composableResourceName string,
	baseComposableResourceSpec *crov1alpha1.ComposableResourceSpec,
	baseComposableResourceStatus *crov1alpha1.ComposableResourceStatus,
	targetState string,
) {
	composableResourceSpec := baseComposableResourceSpec.DeepCopy()
	composableResourceStatus := baseComposableResourceStatus.DeepCopy()

	if composableResourceName == "" {
		return
	}

	composableResource := &crov1alpha1.ComposableResource{
		ObjectMeta: metav1.ObjectMeta{Name: composableResourceName},
		Spec:       *composableResourceSpec,
	}

	Expect(k8sClient.Create(ctx, composableResource)).To(Succeed())

	if targetState == "" {
		return
	} else {
		composableResource.SetFinalizers([]string{composabilityFinalizer})
		Expect(k8sClient.Update(ctx, composableResource)).To(Succeed())

		composableResource.Status = *composableResourceStatus
		composableResource.Status.State = targetState

		Expect(k8sClient.Status().Update(ctx, composableResource)).To(Succeed())
	}
}

func triggerComposableResourceReconcile(controllerReconciler *ComposableResourceReconciler, requestName string, ignoreGet bool) (*crov1alpha1.ComposableResource, error) {
	namespacedName := types.NamespacedName{Name: requestName}

	_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: namespacedName})

	var composableResource *crov1alpha1.ComposableResource
	if !ignoreGet {
		composableResource = &crov1alpha1.ComposableResource{}
		Expect(k8sClient.Get(ctx, namespacedName, composableResource)).NotTo(HaveOccurred())
	}

	return composableResource, err
}

func deleteComposableResource(composableResourceName string) {
	resource := &crov1alpha1.ComposableResource{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResourceName}, resource)).NotTo(HaveOccurred())

	Expect(k8sClient.Delete(ctx, resource)).NotTo(HaveOccurred())
}

func generateCMMachineData(isAttachFailed bool, isDetachFailed bool, isSucceeded bool) []byte {
	machineData := fticmapi.MachineData{
		Data: fticmapi.Data{
			TenantUUID: "",
			Cluster: fticmapi.Cluster{
				ClusterUUID: "",
				Machine: fticmapi.Machine{
					UUID:         "",
					Name:         "",
					Status:       "",
					StatusReason: "",
					ResourceSpecs: []fticmapi.ResourceSpec{
						{
							SpecUUID: "GPU-device00-uuid-temp-fail-000000000000",
							Type:     "gpu",
							Selector: fticmapi.Selector{
								Version: "",
								Expression: fticmapi.Expression{
									Conditions: []fticmapi.Condition{
										{
											Column:   "model",
											Operator: "eq",
											Value:    "NVIDIA-A100-PCIE-80GB",
										},
									},
								},
							},
							MinCount:    0,
							MaxCount:    0,
							DeviceCount: 0,
							Devices:     nil,
						},
					},
				},
			},
		},
	}

	if isAttachFailed {
		machineData.Data.Cluster.Machine.ResourceSpecs[0].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-fail-000000000000",
				Status:       "ADD_FAILED",
				StatusReason: "add failed due to some reasons",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:     "",
					FabricID:       0,
					ResourceUUID:   "",
					FabricGID:      "",
					ResourceType:   "",
					ResourceName:   "",
					ResourceStatus: "",
					ResourceSpec: []fticmapi.DeviceResourceSpec{
						{
							ResourceSpecUUID: "",
							ProductName:      "",
							Model:            "",
							Vendor:           "",
							Removable:        true,
						},
					},
					TenantID:  "",
					MachineID: "",
				},
			},
		}
	}

	if isDetachFailed {
		machineData.Data.Cluster.Machine.ResourceSpecs[0].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-fail-000000000000",
				Status:       "REMOVE_FAILED",
				StatusReason: "remove failed due to some reasons",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:     "",
					FabricID:       0,
					ResourceUUID:   "",
					FabricGID:      "",
					ResourceType:   "",
					ResourceName:   "",
					ResourceStatus: "",
					ResourceSpec: []fticmapi.DeviceResourceSpec{
						{
							ResourceSpecUUID: "",
							ProductName:      "",
							Model:            "",
							Vendor:           "",
							Removable:        true,
						},
					},
					TenantID:  "",
					MachineID: "",
				},
			},
		}
	}

	if isSucceeded {
		machineData.Data.Cluster.Machine.ResourceSpecs[0].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-0000-000000000000",
				Status:       "ADD_COMPLETE",
				StatusReason: "",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:     "",
					FabricID:       0,
					ResourceUUID:   "",
					FabricGID:      "",
					ResourceType:   "",
					ResourceName:   "",
					ResourceStatus: "",
					ResourceSpec: []fticmapi.DeviceResourceSpec{
						{
							ResourceSpecUUID: "",
							ProductName:      "",
							Model:            "",
							Vendor:           "",
							Removable:        true,
						},
					},
					TenantID:  "",
					MachineID: "",
				},
			},
		}
	}

	jsonData, err := json.Marshal(machineData)
	Expect(err).NotTo(HaveOccurred())
	return jsonData
}

func generateFMMachineData(isAdded bool) []byte {
	data := ftifmapi.GetMachineResponse{
		Data: ftifmapi.GetMachineData{
			Machines: []ftifmapi.GetMachineItem{
				{
					FabricUUID:   "",
					FabricID:     0,
					MachineUUID:  "",
					MachineID:    0,
					MachineName:  "",
					TenantUUID:   "",
					Status:       0,
					StatusDetail: "",
					Resources:    []ftifmapi.GetMachineResource{},
				},
			},
		},
	}

	if isAdded {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.GetMachineResource{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "0",
			SerialNum:    "",
			Spec: ftifmapi.Condition{
				Condition: []ftifmapi.ConditionItem{
					{
						Column:   "model",
						Operator: "eq",
						Value:    "NVIDIA-A100-PCIE-80GB",
					},
				},
			},
		})
	}

	jsonData, err := json.Marshal(data)
	Expect(err).NotTo(HaveOccurred())
	return jsonData
}

func generateFMUpdateData(isAdded bool) []byte {
	data := ftifmapi.ScaleUpResponse{
		Data: ftifmapi.ScaleUpResponseData{
			Machines: []ftifmapi.ScaleUpResponseMachineItem{
				{
					FabricUUID:  "",
					FabricID:    0,
					MachineUUID: "",
					MachineID:   0,
					MachineName: "",
					TenantUUID:  "",
					Resources:   []ftifmapi.ScaleUpResponseResourceItem{},
				},
			},
		},
	}

	if isAdded {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.ScaleUpResponseResourceItem{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "0",
			SerialNum:    "",
			Spec: ftifmapi.Condition{
				Condition: []ftifmapi.ConditionItem{
					{
						Column:   "model",
						Operator: "eq",
						Value:    "NVIDIA-A100-PCIE-80GB",
					},
				},
			},
		})
	}

	jsonData, err := json.Marshal(data)
	Expect(err).NotTo(HaveOccurred())
	return jsonData
}

func generateFMError() []byte {
	data := ftifmapi.ErrorBody{
		Code:    "E02XXXX",
		Message: "fm internal error",
	}

	jsonData, err := json.Marshal(data)
	Expect(err).NotTo(HaveOccurred())

	return jsonData
}

var baseComposableResource = crov1alpha1.ComposableResource{
	Spec: crov1alpha1.ComposableResourceSpec{
		Type:        "gpu",
		Model:       "NVIDIA-A100-PCIE-80GB",
		TargetNode:  worker0Name,
		ForceDetach: false,
	},
	Status: crov1alpha1.ComposableResourceStatus{
		State:       "<change it>",
		Error:       "",
		DeviceID:    "",
		CDIDeviceID: "",
	},
}

var _ = Describe("ComposableResource Controller", Ordered, func() {
	var (
		clientSet            *kubernetes.Clientset
		controllerReconciler *ComposableResourceReconciler
	)

	BeforeAll(func() {
		var err error
		clientSet, err = kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		controllerReconciler = &ComposableResourceReconciler{
			Client:     k8sClient,
			ClientSet:  clientSet,
			Scheme:     k8sClient.Scheme(),
			RestConfig: cfg,
		}

		nodes := []*corev1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: worker0Name,
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
					},
				},
				Status: corev1.NodeStatus{
					Allocatable: corev1.ResourceList{
						"nvidia.com/gpu": k8sresource.MustParse("0"),
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: worker1Name,
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
					},
				},
				Spec: corev1.NodeSpec{
					Unschedulable: true,
				},
				Status: corev1.NodeStatus{
					Allocatable: corev1.ResourceList{
						"nvidia.com/gpu": k8sresource.MustParse("0"),
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: worker2Name,
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-worker-1",
					},
				},
				Status: corev1.NodeStatus{
					Allocatable: corev1.ResourceList{
						"nvidia.com/gpu": k8sresource.MustParse("0"),
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: worker3Name,
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-worker-2",
					},
				},
				Status: corev1.NodeStatus{
					Allocatable: corev1.ResourceList{
						"nvidia.com/gpu": k8sresource.MustParse("0"),
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: worker4Name,
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-worker-3",
					},
				},
				Status: corev1.NodeStatus{
					Allocatable: corev1.ResourceList{
						"nvidia.com/gpu": k8sresource.MustParse("0"),
					},
				},
			},
		}
		nodesToCreate := make([]*corev1.Node, len(nodes))
		for i, node := range nodes {
			nodesToCreate[i] = node.DeepCopy()
		}
		for i, node := range nodesToCreate {
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			node.Status = *nodes[i].Status.DeepCopy()
			Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		}

		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "openshift-machine-api"}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "gpu-operator"}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nvidia-dra-driver"}})).To(Succeed())

		draDaemonset := &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
				Namespace: "nvidia-dra-driver",
			},
			Spec: appsv1.DaemonSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin-app00"},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin-app00"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "test-container",
								Image: "nginx:alpine",
							},
						},
					},
				},
			},
		}
		k8sClient.Create(context.TODO(), draDaemonset)

		machine0 := &machinev1beta1.Metal3Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-worker-0",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"metal3.io/BareMetalHost": "openshift-machine-api/bmh-worker-0",
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine0)).To(Succeed())

		bmh0 := &metal3v1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bmh-worker-0",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"cluster-manager.cdi.io/machine": "machine0-uuid-0000-temp-000000000000",
				},
			},
		}
		Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

		machine1 := &machinev1beta1.Metal3Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-worker-1",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"metal3.io/BareMetalHost": "openshift-machine-api/bmh-worker-1",
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine1)).To(Succeed())

		bmh1 := &metal3v1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bmh-worker-1",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"cluster-manager.cdi.io/machine": "machine0-uuid-0000-temp-000000000001",
				},
			},
		}
		Expect(k8sClient.Create(ctx, bmh1)).To(Succeed())

		machine2 := &machinev1beta1.Metal3Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-worker-2",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"metal3.io/BareMetalHost": "openshift-machine-api/bmh-worker-2",
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine2)).To(Succeed())

		bmh2 := &metal3v1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bmh-worker-2",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"cluster-manager.cdi.io/machine": "machine0-uuid-0000-temp-000000000002",
				},
			},
		}
		Expect(k8sClient.Create(ctx, bmh2)).To(Succeed())

		machine3 := &machinev1beta1.Metal3Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-worker-3",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"metal3.io/BareMetalHost": "openshift-machine-api/bmh-worker-3",
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine3)).To(Succeed())

		bmh3 := &metal3v1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bmh-worker-3",
				Namespace: "openshift-machine-api",
				Annotations: map[string]string{
					"cluster-manager.cdi.io/machine": "machine0-uuid-0000-temp-000000000003",
				},
			},
		}
		Expect(k8sClient.Create(ctx, bmh3)).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "credentials-namespace"}})).To(Succeed())
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "credentials",
				Namespace: "credentials-namespace",
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"username":      []byte("test_user"),
				"password":      []byte("test_password"),
				"client_id":     []byte("test_client_id"),
				"client_secret": []byte("test_client_secret"),
				"realm":         []byte("test_realm"),
			},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		testTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/id_manager/realms/test_realm/protocol/openid-connect/token":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{
						"access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30",
						"token_type": "Bearer",
						"expires_in": 3600
					}`))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000000/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, false))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000001/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, true))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000000/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(true, false, false))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-0000000error/clusters/cluster0-uuid-temp-fail-000000000000/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(fticmapi.ErrorBody{
					Status: 404,
					Detail: fticmapi.ErrorDetail{
						Code:    "E04XXXX",
						Message: "the path cannot be found!",
					},
				})
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000001/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, true, false))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000000/machines/machine0-uuid-0000-temp-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000001/machines/machine0-uuid-0000-temp-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000000/machines/machine0-uuid-0000-temp-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(fticmapi.ErrorBody{
					Status: 404,
					Detail: fticmapi.ErrorDetail{
						Code:    "E04XXXX",
						Message: "the path is uncorrect",
					},
				})
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000001/machines/machine0-uuid-0000-temp-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData(false))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000000/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData(true))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000001":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData(false))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000001/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write(generateFMError())
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000002":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData(true))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000002/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData(false))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000003":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData(true))
			case "/fabric_manager/api/v1/machines/machine0-uuid-0000-temp-000000000003/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write(generateFMError())

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))

		http.DefaultTransport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}

		os.Setenv("FTI_CDI_ENDPOINT", strings.TrimPrefix(testTLSServer.URL, "https://"))
	})

	Describe("When using FTI_CDI and CM and DRA", func() {
		BeforeAll(func() {
			os.Setenv("CDI_PROVIDER_TYPE", "FTI_CDI")
			os.Setenv("FTI_CDI_API_TYPE", "CM")
			os.Setenv("DEVICE_RESOURCE_TYPE", "DRA")
		})

		Describe("In Reconcile function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus
				resourceState  string
				ignoreGet      bool

				setErrorMode  func()
				extraHandling func(composabilityRequestName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, tc.resourceState)

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, tc.ignoreGet)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					if composableResource != nil {
						Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
					}
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					cleanAllComposableResources()
				})
			},
				Entry("should fail when no ComposableResource CRs existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "unexisted-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					ignoreGet:    true,

					extraHandling: deleteComposableResource,

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "composableresources.cro.hpsys.ibm.ie.com \"unexisted-composable-resource\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "unexisted-composable-resource",
								Group:             "cro.hpsys.ibm.ie.com",
								Kind:              "composableresources",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
				Entry("should fail when k8s client status update fails", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),

					setErrorMode: func() {
						k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							return errors.New("status update fails")
						}
					},
					expectedReconcileError: errors.New("status update fails"),
				}),
			)
		})

		Describe("In handleNoneState function", func() {
			type testcase struct {
				resourceName string
				resourceSpec *crov1alpha1.ComposableResourceSpec

				setErrorMode func()

				expectedRequestFinalizer []string
				expectedRequestStatus    *crov1alpha1.ComposableResourceStatus
				expectedReconcileError   error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", "tenant00-uuid-temp-0000-000000000000")
				os.Setenv("FTI_CDI_CLUSTER_ID", "cluster0-uuid-temp-0000-000000000000")

				createComposableResource(tc.resourceName, tc.resourceSpec, nil, "")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.GetFinalizers()).To(Equal(tc.expectedRequestFinalizer))
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					cleanAllComposableResources()
				})
			},
				Entry("should succeed", testcase{
					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),

					expectedRequestFinalizer: []string{composabilityFinalizer},
					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when k8s client update fails", testcase{
					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),

					setErrorMode: func() {
						k8sClient.MockUpdate = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
							return errors.New("update fails")
						}
					},

					expectedReconcileError: errors.New("update fails"),
				}),
			)
		})

		Describe("In handleAttachingState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Attaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					resourceSliceToDelete := &resourcev1alpha3.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-resourceslice",
						},
					}
					Expect(k8sClient.Delete(ctx, resourceSliceToDelete)).To(Satisfy(func(getErr error) bool {
						return client.IgnoreNotFound(getErr) == nil
					}))

					cleanAllComposableResources()
				})
			},
				Entry("should succeed when the ComposableResource is justed created", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the added gpu has not been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the added gpu has been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						resourceSlice := &resourcev1alpha3.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "test-resourceslice",
							},

							Spec: resourcev1alpha3.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1alpha3.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: worker0Name,
								Devices: []resourcev1alpha3.Device{
									{
										Name: "device-0",
										Basic: &resourcev1alpha3.BasicDevice{
											Attributes: map[resourcev1alpha3.QualifiedName]resourcev1alpha3.DeviceAttribute{
												"uuid": resourcev1alpha3.DeviceAttribute{
													StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
												},
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"

						return composableResourceStatus
					}(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Cleaning"
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when CM failed to add gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: errors.New("add failed due to some reasons"),
				}),
				Entry("should fail when CM returns error code", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-0000000error",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: errors.New("http returned status: 404, cm return code: E04XXXX, error message: the path cannot be found!"),
				}),
			)
		})

		Describe("In handleOnlineState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Online")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					cleanAllComposableResources()
				})
			},
				Entry("should succeed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Cleaning"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when k8s client update fails", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					setErrorMode: func() {
						k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							switch res := obj.(type) {
							case *crov1alpha1.ComposableResource:
								if res.Status.State == "Cleaning" {
									return errors.New("status update fails")
								}
							}

							return original(obj, opts...)
						}
					},
					extraHandling: deleteComposableResource,

					expectedReconcileError: errors.New("status update fails"),
				}),
				Entry("should return error messages when the gpu cannot be found when doing CDIProvider.CheckResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResource := baseComposableResource.Status.DeepCopy()
						composableResource.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						return composableResource
					}(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						composableResourceStatus.Error = "the target device 'GPU-device00-uuid-temp-fail-000000000000' cannot be found in CDI system"
						return composableResourceStatus
					}(),
				}),
			)
		})

		Describe("In handleCleaningState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Cleaning")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					cleanAllComposableResources()
				})
			},
				Entry("should succeed when ComposableResource CR's uuid is empty", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when ComposableResource CR's uuid is not empty", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when k8s client update fails", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					setErrorMode: func() {
						k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							switch res := obj.(type) {
							case *crov1alpha1.ComposableResource:
								if res.Status.State == "Deleting" {
									return errors.New("status update fails")
								}
							}

							return original(obj, opts...)
						}
					},
					extraHandling: deleteComposableResource,

					expectedReconcileError: errors.New("status update fails"),
				}),
			)
		})

		Describe("In handleDetachingState function", func() {
			var (
				patches *gomonkey.Patches
			)

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError error
			}

			BeforeAll(func() {
				nvidiaPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver-daemonset-test",
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app.kubernetes.io/component": "nvidia-driver",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "worker-0",
						Containers: []corev1.Container{
							{Name: "nvidia-driver-ctr", Image: "nvcr.io/nvidia/driver"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nvidiaPod)).NotTo(HaveOccurred())

				draPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-dra-driver-gpu-kubelet-plugin-test",
						Namespace: "nvidia-dra-driver",
						Labels: map[string]string{
							"app.kubernetes.io/name": "nvidia-dra-driver-gpu",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: "worker-0",
						Containers: []corev1.Container{
							{Name: "compute-domains", Image: "nvcr.io/nvidia/k8s-dra-driver-gpu"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, draPod)).NotTo(HaveOccurred())

				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Detaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					draDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
							Namespace: "nvidia-dra-driver",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin-app00"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin-app00"},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "test-container",
											Image: "nginx:alpine",
										},
									},
								},
							},
						},
					}
					k8sClient.Create(context.TODO(), draDaemonset)

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should succeed when the ComposableResource is justed created", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the gpu has been deleted in CDI system", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						composableResourceStatus.Error = ""
						return composableResourceStatus
					}(),
				}),
				Entry("should delete again when CM failed to remove gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("GPU-device00-uuid-temp-0000-000000000000"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						composableResourceStatus.Error = "remove failed due to some reasons"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when the gpu load is existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("GPU-device00-uuid-temp-0000-000000000000"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: fmt.Errorf("there are gpu loads on the target, please check"),
				}),
				Entry("should fail when the controller can not check gpu loads", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte("can not find nvidia-smi command when checking"))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: fmt.Errorf("nvidia-smi failed: <nil>, stderr: can not find nvidia-smi command when checking"),
				}),
				Entry("should fail when the controller can not drain gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte("can not find nvidia-smi when getting"))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: fmt.Errorf("get gpu info command failed: '<nil>', stderr: 'can not find nvidia-smi when getting'"),
				}),
				Entry("should fail when nvidia-dra-driver-gpu-kubelet-plugin is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver",
							},
						}
						Expect(k8sClient.Delete(context.TODO(), draDaemonset)).NotTo(HaveOccurred())
						Expect(k8sClient.Get(context.TODO(), client.ObjectKey{Namespace: "nvidia-dra-driver", Name: "nvidia-dra-driver-gpu-kubelet-plugin"}, &appsv1.DaemonSet{})).To(HaveOccurred())

						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "daemonsets.apps \"nvidia-dra-driver-gpu-kubelet-plugin\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "nvidia-dra-driver-gpu-kubelet-plugin",
								Group:             "apps",
								Kind:              "daemonsets",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		Describe("In handleDeletingState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				ignoreGet             bool
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Deleting")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, tc.ignoreGet)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					if !tc.ignoreGet {
						Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
					} else {
						composableResourceList := &crov1alpha1.ComposableResourceList{}
						Expect(k8sClient.List(ctx, composableResourceList)).NotTo(HaveOccurred())
						Expect(composableResourceList.Items).To(HaveLen(0))
					}
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					cleanAllComposableResources()
				})
			},
				Entry("should succeed when the ComposableResource can be directly deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),
					ignoreGet:      true,

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the ComposableResource can be directly deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker1Name
						return composableResourceSpec
					}(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when k8s client update fails", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),
					ignoreGet:      true,

					extraHandling: deleteComposableResource,

					setErrorMode: func() {
						k8sClient.MockUpdate = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
							switch res := obj.(type) {
							case *crov1alpha1.ComposableResource:
								if res.Status.State == "Cleaning" {
									return errors.New("update fails")
								}
							}

							return errors.New("update fails")
						}
					},
					expectedReconcileError: errors.New("update fails"),
				}),
			)
		})

		AfterAll(func() {
			os.Unsetenv("CDI_PROVIDER_TYPE")
			os.Unsetenv("FTI_CDI_API_TYPE")
			os.Unsetenv("DEVICE_RESOURCE_TYPE")
		})
	})

	Describe("When using FTI_CDI and FM and DRA", func() {
		BeforeAll(func() {
			os.Setenv("CDI_PROVIDER_TYPE", "FTI_CDI")
			os.Setenv("FTI_CDI_API_TYPE", "FM")
			os.Setenv("DEVICE_RESOURCE_TYPE", "DRA")
		})

		Describe("In handleAttachingState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Attaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					resourceSliceToDelete := &resourcev1alpha3.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "test-resourceslice",
						},
					}
					Expect(k8sClient.Delete(ctx, resourceSliceToDelete)).To(Satisfy(func(getErr error) bool {
						return client.IgnoreNotFound(getErr) == nil
					}))

					cleanAllComposableResources()
				})
			},
				Entry("should succeed when the added gpu has not been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the added gpu has been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						resourceSlice := &resourcev1alpha3.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "test-resourceslice",
							},

							Spec: resourcev1alpha3.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1alpha3.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: worker0Name,
								Devices: []resourcev1alpha3.Device{
									{
										Name: "device-0",
										Basic: &resourcev1alpha3.BasicDevice{
											Attributes: map[resourcev1alpha3.QualifiedName]resourcev1alpha3.DeviceAttribute{
												"uuid": resourcev1alpha3.DeviceAttribute{
													StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
												},
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Cleaning"
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when FM failed to add gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker2Name
						return composableResourceSpec
					}(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: errors.New("fm returned code: E02XXXX, error message: fm internal error"),
				}),
			)
		})

		Describe("In handleDetachingState function", func() {
			var (
				patches *gomonkey.Patches
			)

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError error
			}

			BeforeAll(func() {
				nvidiaPod1 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver-daemonset-test-in-worker3",
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app.kubernetes.io/component": "nvidia-driver",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker3Name,
						Containers: []corev1.Container{
							{Name: "nvidia-driver-ctr", Image: "nvcr.io/nvidia/driver"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nvidiaPod1)).NotTo(HaveOccurred())

				draPod1 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-dra-driver-gpu-kubelet-plugin-test-in-worker3",
						Namespace: "nvidia-dra-driver",
						Labels: map[string]string{
							"app.kubernetes.io/name": "nvidia-dra-driver-gpu",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker3Name,
						Containers: []corev1.Container{
							{Name: "compute-domains", Image: "nvcr.io/nvidia/k8s-dra-driver-gpu"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, draPod1)).NotTo(HaveOccurred())

				nvidiaPod2 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver-daemonset-test-in-worker4",
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app.kubernetes.io/component": "nvidia-driver",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker4Name,
						Containers: []corev1.Container{
							{Name: "nvidia-driver-ctr", Image: "nvcr.io/nvidia/driver"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nvidiaPod2)).NotTo(HaveOccurred())

				draPod2 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-dra-driver-gpu-kubelet-plugin-test-in-worker4",
						Namespace: "nvidia-dra-driver",
						Labels: map[string]string{
							"app.kubernetes.io/name": "nvidia-dra-driver-gpu",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker4Name,
						Containers: []corev1.Container{
							{Name: "compute-domains", Image: "nvcr.io/nvidia/k8s-dra-driver-gpu"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, draPod2)).NotTo(HaveOccurred())

				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Detaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should succeed when the gpu is ready to be deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker3Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when CM failed to remove gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000001",

					resourceName: "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker4Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("GPU-device00-uuid-temp-0000-000000000000"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: errors.New("fm returned code: E02XXXX, error message: fm internal error"),
				}),
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		AfterAll(func() {
			os.Unsetenv("CDI_PROVIDER_TYPE")
			os.Unsetenv("FTI_CDI_API_TYPE")
			os.Unsetenv("DEVICE_RESOURCE_TYPE")
		})
	})

	Describe("When using FTI_CDI and FM and DEVICE_PLUGIN", func() {
		BeforeAll(func() {
			os.Setenv("CDI_PROVIDER_TYPE", "FTI_CDI")
			os.Setenv("FTI_CDI_API_TYPE", "FM")
			os.Setenv("DEVICE_RESOURCE_TYPE", "DEVICE_PLUGIN")

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "nvidia-gpu-operator",
				},
			}
			Expect(k8sClient.Create(context.TODO(), ns)).NotTo(HaveOccurred())
		})

		Describe("In handleAttachingState function", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				createDevicePluginDaemonset bool
				createDCGMDaemonset         bool
				resourceName                string
				resourceSpec                *crov1alpha1.ComposableResourceSpec
				resourceStatus              *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				if tc.createDevicePluginDaemonset {
					devicePluginDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-device-plugin-daemonset",
							Namespace: "nvidia-gpu-operator",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset-app00"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-device-plugin-daemonset-app00"},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "test-container",
											Image: "nginx:alpine",
										},
									},
								},
							},
						},
					}
					k8sClient.Create(context.TODO(), devicePluginDaemonset)
				}
				if tc.createDCGMDaemonset {
					dcgmDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dcgm",
							Namespace: "nvidia-gpu-operator",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-dcgm-app00"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-dcgm-app00"},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "test-container",
											Image: "nginx:alpine",
										},
									},
								},
							},
						},
					}
					k8sClient.Create(context.TODO(), dcgmDaemonset)
				}

				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Attaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					cleanAllComposableResources()

					devicePluginDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-device-plugin-daemonset",
							Namespace: "nvidia-gpu-operator",
						},
					}
					k8sClient.Delete(context.TODO(), devicePluginDaemonset)

					dcgmDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dcgm",
							Namespace: "nvidia-gpu-operator",
						},
					}
					k8sClient.Delete(context.TODO(), dcgmDaemonset)
				})
			},
				Entry("should succeed when the added gpu has not been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec:                baseComposableResource.Spec.DeepCopy(),
					resourceStatus:              baseComposableResource.Status.DeepCopy(),

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when the added gpu has been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec:                baseComposableResource.Spec.DeepCopy(),
					resourceStatus:              baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						composableResource := &crov1alpha1.ComposableResource{}
						Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResourceName}, composableResource)).NotTo(HaveOccurred())

						node := &corev1.Node{}
						k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)

						node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("1")
						Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should succeed when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec:                baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Cleaning"
						composableResourceStatus.DeviceID = "GPU-device00-test-test-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when FM failed to add gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker2Name
						return composableResourceSpec
					}(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: errors.New("fm returned code: E02XXXX, error message: fm internal error"),
				}),
				Entry("should fail when DevicePluginDaemonset is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: false,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec:                baseComposableResource.Spec.DeepCopy(),
					resourceStatus:              baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "daemonsets.apps \"nvidia-device-plugin-daemonset\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "nvidia-device-plugin-daemonset",
								Group:             "apps",
								Kind:              "daemonsets",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
				Entry("should fail when DevicePluginDaemonset is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         false,
					resourceName:                "test-composable-resource",
					resourceSpec:                baseComposableResource.Spec.DeepCopy(),
					resourceStatus:              baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "daemonsets.apps \"nvidia-dcgm\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "nvidia-dcgm",
								Group:             "apps",
								Kind:              "daemonsets",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
			)
		})

		Describe("In handleDetachingState function", func() {
			var (
				patches *gomonkey.Patches
			)

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				createDevicePluginDaemonset bool
				createDCGMDaemonset         bool
				resourceName                string
				resourceSpec                *crov1alpha1.ComposableResourceSpec
				resourceStatus              *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			BeforeAll(func() {
				nvidiaPod1 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver-daemonset-test-in-worker3",
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app.kubernetes.io/component": "nvidia-driver",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker3Name,
						Containers: []corev1.Container{
							{Name: "nvidia-driver-ctr", Image: "nvcr.io/nvidia/driver"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nvidiaPod1)).NotTo(HaveOccurred())

				nvidiaPod2 := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nvidia-driver-daemonset-test-in-worker4",
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app.kubernetes.io/component": "nvidia-driver",
						},
					},
					Spec: corev1.PodSpec{
						NodeName: worker4Name,
						Containers: []corev1.Container{
							{Name: "nvidia-driver-ctr", Image: "nvcr.io/nvidia/driver"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nvidiaPod2)).NotTo(HaveOccurred())

				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				if tc.createDevicePluginDaemonset {
					devicePluginDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-device-plugin-daemonset",
							Namespace: "nvidia-gpu-operator",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset-app00"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-device-plugin-daemonset-app00"},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "test-container",
											Image: "nginx:alpine",
										},
									},
								},
							},
						},
					}
					k8sClient.Create(context.TODO(), devicePluginDaemonset)
				}
				if tc.createDCGMDaemonset {
					dcgmDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dcgm",
							Namespace: "nvidia-gpu-operator",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-dcgm-app00"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-dcgm-app00"},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "test-container",
											Image: "nginx:alpine",
										},
									},
								},
							},
						},
					}
					k8sClient.Create(context.TODO(), dcgmDaemonset)
				}

				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Detaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != nil {
					Expect(err).To(HaveOccurred())
					Expect(err).To(MatchError(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					node := &corev1.Node{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource.Spec.TargetNode}, node)).NotTo(HaveOccurred())
					node.Status.Allocatable["nvidia.com/gpu"] = k8sresource.MustParse("0")
					Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

					cleanAllComposableResources()

					patches.Reset()

					devicePluginDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-device-plugin-daemonset",
							Namespace: "nvidia-gpu-operator",
						},
					}
					k8sClient.Delete(context.TODO(), devicePluginDaemonset)

					dcgmDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dcgm",
							Namespace: "nvidia-gpu-operator",
						},
					}
					k8sClient.Delete(context.TODO(), dcgmDaemonset)
				})
			},
				Entry("should succeed when the gpu is ready to be deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker3Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when CM failed to remove gpu", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker4Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "cluster0-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "cluster0-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: errors.New("fm returned code: E02XXXX, error message: fm internal error"),
				}),
				Entry("should fail when DevicePluginDaemonset is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: false,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker3Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "daemonsets.apps \"nvidia-device-plugin-daemonset\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "nvidia-device-plugin-daemonset",
								Group:             "apps",
								Kind:              "daemonsets",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
				Entry("should fail when DevicePluginDaemonset is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         false,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker3Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckLoadsExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockCheckNvidiaXExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte(""))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckLoadsExecutor, nil
								} else if strings.Contains(url.RawQuery, "TARGET_FILE") {
									return mockCheckNvidiaXExecutor, nil
								} else {
									return mockGetExecutor, nil
								}
							},
						)
					},

					expectedReconcileError: &k8serrors.StatusError{
						ErrStatus: metav1.Status{
							Status:  metav1.StatusFailure,
							Message: "daemonsets.apps \"nvidia-dcgm\" not found",
							Reason:  metav1.StatusReasonNotFound,
							Details: &metav1.StatusDetails{
								Name:              "nvidia-dcgm",
								Group:             "apps",
								Kind:              "daemonsets",
								UID:               "",
								Causes:            nil,
								RetryAfterSeconds: 0,
							},
							Code: http.StatusNotFound,
						},
					},
				}),
				Entry("should fail when the target node has loads", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					createDevicePluginDaemonset: true,
					createDCGMDaemonset:         true,
					resourceName:                "test-composable-resource",
					resourceSpec: func() *crov1alpha1.ComposableResourceSpec {
						composableResourceSpec := baseComposableResource.Spec.DeepCopy()
						composableResourceSpec.TargetNode = worker3Name
						return composableResourceSpec
					}(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						mockCheckExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("GPU-device00-uuid-temp-0000-000000000000"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}
						mockGetExecutor := &MockExecutor{
							StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
								opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
								opts.Stderr.Write([]byte(""))
								return nil
							},
						}

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, "--query-accounted-apps") {
									return mockCheckExecutor, nil
								}
								return mockGetExecutor, nil
							},
						)
					},

					expectedReconcileError: fmt.Errorf("there are gpu loads on the target, please check"),
				}),
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		AfterAll(func() {
			os.Unsetenv("CDI_PROVIDER_TYPE")
			os.Unsetenv("FTI_CDI_API_TYPE")
			os.Unsetenv("DEVICE_RESOURCE_TYPE")
		})
	})

	Describe("When using wrong env variables", func() {
		var (
			patches *gomonkey.Patches
		)

		type testcase struct {
			cdiProviderType    string
			ftiCdiApiType      string
			deviceResourceType string
			tenant_uuid        string
			cluster_uuid       string

			resourceName   string
			resourceSpec   *crov1alpha1.ComposableResourceSpec
			resourceStatus *crov1alpha1.ComposableResourceStatus
			resourceState  string
			ignoreGet      bool

			setErrorMode  func()
			extraHandling func(composabilityRequestName string)

			// expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
			expectedReconcileError error
		}

		BeforeAll(func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nvidia-dcgm-test-in-worker0",
					Namespace: "gpu-operator",
					Labels: map[string]string{
						"app.kubernetes.io/name": "nvidia-dcgm",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "worker-0",
					Containers: []corev1.Container{
						{Name: "dcgm", Image: "nvcr.io/nvidia/cloud-native/dcgm:3.3.1-1-ubuntu22.04"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).NotTo(HaveOccurred())

			patches = gomonkey.NewPatches()
		})

		DescribeTable("", func(tc testcase) {
			os.Setenv("CDI_PROVIDER_TYPE", tc.cdiProviderType)
			os.Setenv("FTI_CDI_API_TYPE", tc.ftiCdiApiType)
			os.Setenv("DEVICE_RESOURCE_TYPE", tc.deviceResourceType)
			os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
			os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

			createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, tc.resourceState)

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

			_, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, tc.ignoreGet)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Fail("should not go into this part!")
			}

			DeferCleanup(func() {
				os.Unsetenv("CDI_PROVIDER_TYPE")
				os.Unsetenv("FTI_CDI_API_TYPE")
				os.Unsetenv("DEVICE_RESOURCE_TYPE")
				os.Unsetenv("FTI_CDI_TENANT_ID")
				os.Unsetenv("FTI_CDI_CLUSTER_ID")

				cleanAllComposableResources()
			})
		},
			Entry("should fail when CDI_PROVIDER_TYPE is wrong", testcase{
				cdiProviderType:    "ERROR",
				ftiCdiApiType:      "CM",
				deviceResourceType: "DRA",
				tenant_uuid:        "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid:       "cluster0-uuid-temp-0000-000000000000",

				resourceName: "test-composable-resource",
				resourceSpec: baseComposableResource.Spec.DeepCopy(),

				expectedReconcileError: fmt.Errorf("CDI_PROVIDER_TYPE variable not set properly"),
			}),
			Entry("should fail when FTI_CDI_API_TYPE is wrong", testcase{
				cdiProviderType:    "FTI_CDI",
				ftiCdiApiType:      "ERROR",
				deviceResourceType: "DRA",
				tenant_uuid:        "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid:       "cluster0-uuid-temp-0000-000000000000",

				resourceName: "test-composable-resource",
				resourceSpec: baseComposableResource.Spec.DeepCopy(),

				expectedReconcileError: fmt.Errorf("FTI_CDI_API_TYPE variable not set properly"),
			}),
			Entry("should fail in handleAttachingState function when DEVICE_RESOURCE_TYPE is wrong", testcase{
				cdiProviderType:    "FTI_CDI",
				ftiCdiApiType:      "CM",
				deviceResourceType: "ERROR",
				tenant_uuid:        "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid:       "cluster0-uuid-temp-0000-000000000001",

				resourceName:   "test-composable-resource",
				resourceSpec:   baseComposableResource.Spec.DeepCopy(),
				resourceStatus: baseComposableResource.Status.DeepCopy(),
				resourceState:  "Attaching",

				expectedReconcileError: fmt.Errorf("DEVICE_RESOURCE_TYPE variable not set properly"),
			}),
			Entry("should fail in handleDetachingState function when DEVICE_RESOURCE_TYPE is wrong", testcase{
				cdiProviderType:    "FTI_CDI",
				ftiCdiApiType:      "CM",
				deviceResourceType: "ERROR",
				tenant_uuid:        "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid:       "cluster0-uuid-temp-0000-000000000000",

				resourceName: "test-composable-resource",
				resourceSpec: baseComposableResource.Spec.DeepCopy(),
				resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
					composableResourceStatus := baseComposableResource.Status.DeepCopy()
					composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
					composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
					return composableResourceStatus
				}(),
				resourceState: "Detaching",

				extraHandling: func(composableResourceName string) {
					mockCheckExecutor := &MockExecutor{
						StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
							opts.Stdout.Write([]byte("GPU-device00-uuid-temp-0000-000000000000"))
							opts.Stderr.Write([]byte(""))
							return nil
						},
					}
					mockGetExecutor := &MockExecutor{
						StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
							opts.Stdout.Write([]byte("0, GPU-device00-uuid-temp-0000-000000000000, 0000:1F:00.0"))
							opts.Stderr.Write([]byte(""))
							return nil
						},
					}

					patches.ApplyFunc(
						remotecommand.NewSPDYExecutor,
						func(_ *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
							if strings.Contains(url.RawQuery, "--query-accounted-apps") {
								return mockCheckExecutor, nil
							}
							return mockGetExecutor, nil
						},
					)
				},

				expectedReconcileError: fmt.Errorf("DEVICE_RESOURCE_TYPE variable not set properly, now is 'ERROR'"),
			}),
		)

		AfterAll(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
				client.InNamespace("gpu-operator"),
				&client.DeleteAllOfOptions{
					DeleteOptions: client.DeleteOptions{
						GracePeriodSeconds: ptr.To(int64(0)),
					},
				},
			)).NotTo(HaveOccurred())
		})
	})

	AfterAll(func() {
		os.Unsetenv("FTI_CDI_ENDPOINT")

		Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
	})
})
