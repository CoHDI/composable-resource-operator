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

package controller

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"strings"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	machinev1beta1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
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
		composableResource.SetFinalizers([]string{composabilityRequestFinalizer})
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

func generateCMMachineData(isAttachFailed bool, isDetachFailed bool, isSucceeded bool, isCompleteButError *string) []byte {
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
							SpecUUID:    "spec0000-uuid-temp-0000-000000000000",
							Type:        "cpu",
							MinCount:    0,
							MaxCount:    0,
							DeviceCount: 0,
							Devices:     nil,
						},
						{
							SpecUUID: "spec0000-uuid-temp-0000-000000000001",
							Type:     "gpu",
							Selector: fticmapi.Selector{
								Version: "",
								Expression: fticmapi.Expression{
									Conditions: []fticmapi.Condition{
										{
											Column:   "model",
											Operator: "eq",
											Value:    "NVIDIA-ANOTHER-GPU",
										},
									},
								},
							},
							MinCount:    0,
							MaxCount:    0,
							DeviceCount: 0,
							Devices:     nil,
						},
						{
							SpecUUID: "spec0000-uuid-temp-0000-000000000002",
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
		machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-fail-000000000000",
				Status:       "ADD_FAILED",
				StatusReason: "add failed due to some reasons",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:       "",
					FabricID:         0,
					ResourceUUID:     "",
					FabricGID:        "",
					ResourceType:     "",
					ResourceName:     "",
					ResourceStatus:   "",
					ResourceOPStatus: "0",
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
		machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-fail-000000000000",
				Status:       "REMOVE_FAILED",
				StatusReason: "remove failed due to some reasons",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:       "",
					FabricID:         0,
					ResourceUUID:     "",
					FabricGID:        "",
					ResourceType:     "",
					ResourceName:     "",
					ResourceStatus:   "",
					ResourceOPStatus: "0",
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
		machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
			{
				DeviceUUID:   "GPU-device00-uuid-temp-0000-000000000000",
				Status:       "ADD_COMPLETE",
				StatusReason: "",
				Detail: fticmapi.DeviceDetail{
					FabricUUID:       "",
					FabricID:         0,
					ResourceUUID:     "",
					FabricGID:        "",
					ResourceType:     "",
					ResourceName:     "",
					ResourceStatus:   "",
					ResourceOPStatus: "0",
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

	if isCompleteButError != nil {
		switch *isCompleteButError {
		case "1":
			machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
				{
					DeviceUUID:   "GPU-device00-uuid-temp-0000-000000000000",
					Status:       "ADD_COMPLETE",
					StatusReason: "",
					Detail: fticmapi.DeviceDetail{
						FabricUUID:       "",
						FabricID:         0,
						ResourceUUID:     "",
						FabricGID:        "",
						ResourceType:     "",
						ResourceName:     "",
						ResourceStatus:   "",
						ResourceOPStatus: "1",
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
		case "2":
			machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
				{
					DeviceUUID:   "GPU-device00-uuid-temp-0000-000000000000",
					Status:       "ADD_COMPLETE",
					StatusReason: "",
					Detail: fticmapi.DeviceDetail{
						FabricUUID:       "",
						FabricID:         0,
						ResourceUUID:     "",
						FabricGID:        "",
						ResourceType:     "",
						ResourceName:     "",
						ResourceStatus:   "",
						ResourceOPStatus: "2",
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
		case "3":
			machineData.Data.Cluster.Machine.ResourceSpecs[2].Devices = []fticmapi.Device{
				{
					DeviceUUID:   "GPU-device00-uuid-temp-0000-000000000000",
					Status:       "ADD_COMPLETE",
					StatusReason: "",
					Detail: fticmapi.DeviceDetail{
						FabricUUID:       "",
						FabricID:         0,
						ResourceUUID:     "",
						FabricGID:        "",
						ResourceType:     "",
						ResourceName:     "",
						ResourceStatus:   "",
						ResourceOPStatus: "3",
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
	}

	jsonData, err := json.Marshal(machineData)
	Expect(err).NotTo(HaveOccurred())
	return jsonData
}

func generateFMMachineData(state string) []byte {
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
					Resources: []ftifmapi.GetMachineResource{
						{
							ResourceUUID: "device00-uuid-temp-0000-other0000000",
							ResourceName: "",
							Type:         "memory",
							Status:       0,
							OptionStatus: "0",
							SerialNum:    "",
						},
						{
							ResourceUUID: "GPU-device00-uuid-temp-0000-other0000000",
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
										Value:    "NVIDIA-OTHER",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if state == "isNormal" {
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
	} else if state == "isWarning" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.GetMachineResource{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "1",
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
	} else if state == "isCritical" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.GetMachineResource{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "2",
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
	} else if state == "isUnknown" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.GetMachineResource{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "3",
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

func generateFMUpdateData(state string) []byte {
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

	if state == "isAdded" {
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
	} else if state == "isAddedWarning" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.ScaleUpResponseResourceItem{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "1",
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
	} else if state == "isAddedCritical" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.ScaleUpResponseResourceItem{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "2",
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
	} else if state == "isAddedUnknown" {
		data.Data.Machines[0].Resources = append(data.Data.Machines[0].Resources, ftifmapi.ScaleUpResponseResourceItem{
			ResourceUUID: "GPU-device00-uuid-temp-0000-000000000000",
			ResourceName: "",
			Type:         "gpu",
			Status:       0,
			OptionStatus: "3",
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

func newMockExecutor(stdout, stderr string) (remotecommand.Executor, error) {
	return &MockExecutor{
		StreamWithContextFunc: func(ctx context.Context, opts remotecommand.StreamOptions) error {
			if stdout != "" {
				_, _ = opts.Stdout.Write([]byte(stdout))
			}
			if stderr != "" {
				_, _ = opts.Stderr.Write([]byte(stderr))
			}
			return nil
		},
	}, nil
}

func createTokenPayload(expiry int64) string {
	payload := map[string]interface{}{
		"sub":   "1234567890",
		"name":  "John Doe",
		"admin": true,
		"iat":   time.Now().Unix(),
		"exp":   expiry,
	}
	payloadBytes, _ := json.Marshal(payload)
	return base64.RawURLEncoding.EncodeToString(payloadBytes)
}

var _ = Describe("ComposableResource Controller", Ordered, func() {
	var (
		controllerReconciler *ComposableResourceReconciler
		endpoint             string
	)

	BeforeAll(func() {
		clientSet, err := kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		controllerReconciler = &ComposableResourceReconciler{
			Client:     k8sClient,
			Clientset:  clientSet,
			Scheme:     k8sClient.Scheme(),
			RestConfig: cfg,
		}

		namespacesToCreate := []string{
			"composable-resource-operator-system",
			"openshift-machine-api",
			"nvidia-gpu-operator",
			"nvidia-dra-driver-gpu",
		}
		for _, nsName := range namespacesToCreate {
			ns := &corev1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: nsName}, ns); err != nil {
				if client.IgnoreNotFound(err) != nil {
					Expect(err).NotTo(HaveOccurred())
				}
				// Namespace does not exist, create it.
				Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}})).To(Succeed())
			}
		}

		testTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/id_manager/realms/test_realm/protocol/openid-connect/token":
				r.ParseForm()
				username := r.Form.Get("username")

				switch username {
				case "good_user":
					expiry := time.Now().Add(1 * time.Hour).Unix()
					tokenPayload := createTokenPayload(expiry)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"access_token":  "header." + tokenPayload + ".signature",
						"token_type":    "Bearer",
						"refresh_token": "a-valid-refresh-token",
						"expires_in":    3600,
					})
				case "bad_credentials_user":
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					w.Write([]byte(`{"error":"invalid_grant","error_description":"Invalid user credentials"}`))
				case "not_json_response_user":
					w.Header().Set("Content-Type", "text/html")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte("<html><body>This is not JSON!</body></html>"))
				case "invalid_token_format_user":
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"access_token": "this-is-not-a-valid-jwt",
						"token_type":   "Bearer",
					})
				case "bad_base64_payload_user":
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"access_token": "header.%%%%.signature",
						"token_type":   "Bearer",
					})
				case "bad_json_payload_user":
					badPayload := base64.RawURLEncoding.EncodeToString([]byte("this is not json"))
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"access_token": "header." + badPayload + ".signature",
						"token_type":   "Bearer",
					})
				default:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(`{"error":"unsupported_test_user"}`))
				}

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000000/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"status":404,"detail":{"code":"E02XXXX","message":"machine not found"}}`))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000001/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000002/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000000/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, false, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000000/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000003/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, false, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000003/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"status":404,"detail":{"code":"E02XXXX","message":"scaleup method not found"}}`))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000004/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, false, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000004/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000005/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(true, false, false, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000005/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000001/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, true, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000001/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000006/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				val := "1"
				w.Write(generateCMMachineData(false, false, false, &val))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000007/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				val := "2"
				w.Write(generateCMMachineData(false, false, false, &val))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000008/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				val := "3"
				w.Write(generateCMMachineData(false, false, false, &val))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000009/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, true, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000009/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"status":404,"detail":{"code":"E02XXXX","message":"scaledown method not found"}}`))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000010/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, true, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000010/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000011/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, true, false, nil))
			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000011/machines/machine0-uuid-temp-0000-000000000000/actions/resize":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-0000-000000000000/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData("isAdded"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000000/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":"E02XXXX","message":"scaleup method not found"}`))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000001/update":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000002/update":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000003/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData("isAddedWarning"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000004/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData("isAddedCritical"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000005/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData("isAddedUnknown"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000006/update":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMUpdateData(""))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"code":"E02XXXX","message":"machine not found"}`))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000001":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000002":
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("<html><body>This is not JSON!</body></html>"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000003":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData(""))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000004":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData("isWarning"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000005":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData("isCritical"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-fail-000000000006":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData("isUnknown"))

			case "/fabric_manager/api/v1/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateFMMachineData("isNormal"))

			default:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":"not found"}`))
			}
		}))
		http.DefaultTransport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		endpoint = strings.TrimPrefix(testTLSServer.URL, "https://")
		os.Setenv("FTI_CDI_ENDPOINT", endpoint)
	})

	Describe("When using FTI_CDI and CM and DRA", func() {
		BeforeAll(func() {
			os.Setenv("CDI_PROVIDER_TYPE", "FTI_CDI")
			os.Setenv("FTI_CDI_API_TYPE", "CM")
			os.Setenv("DEVICE_RESOURCE_TYPE", "DRA")
		})

		Describe("When the reconcile is just beginning", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus
				resourceState  string
				ignoreGet      bool

				extraHandling func(composabilityRequestName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError error
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

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
				Entry("should stop when the ComposableResource CR is not existed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "unexisted-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					ignoreGet:    true,
				}),
			)
		})

		Describe("When the ComposableResource is in None state", func() {
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
				Entry("should successfully enter Attaching state", testcase{
					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),

					expectedRequestFinalizer: []string{composabilityRequestFinalizer},
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

		Describe("When the ComposableResource is in Attaching state", func() {
			var patches *gomonkey.Patches

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError string
			}

			BeforeAll(func() {
				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Attaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					os.Setenv("FTI_CDI_ENDPOINT", endpoint)

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-gpu-operator"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-dra-driver-gpu"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &resourcev1.ResourceSlice{})).NotTo(HaveOccurred())

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should fail when trying to add resource because the targetNode is not found", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: "nodes \"worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the annotation is not found in targetNode", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedReconcileError: "failed to get annotation 'machine.openshift.io/machine' from Node worker-0, now is ''",
				}),
				Entry("should fail when trying to add resource because the corresponding Machine CR is not found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedReconcileError: "metal3machines.infrastructure.cluster.x-k8s.io \"machine-worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the annotation is not found in Machine CR", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

						machine0 := &machinev1beta1.Metal3Machine{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "machine-worker-0",
								Namespace: "openshift-machine-api",
							},
						}
						Expect(k8sClient.Create(ctx, machine0)).To(Succeed())
					},

					expectedReconcileError: "failed to get annotation 'metal3.io/BareMetalHost' from Machine machine-worker-0, now is ''",
				}),
				Entry("should fail when trying to add resource because the corresponding BareMetalHost CR is not found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
					},

					expectedReconcileError: "baremetalhosts.metal3.io \"bmh-worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the corresponding machine_uuid is not found in BareMetalHost CR", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())
					},

					expectedReconcileError: "failed to get annotation 'cluster-manager.cdi.io/machine' from BareMetalHost bmh-worker-0, now is ''",
				}),
				Entry("should fail when trying to add resource because the Secret CR is not found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: secrets \"credentials\" not found",
				}),
				Entry("should fail when trying to add resource because the ID manager responds with an error", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("bad_credentials_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: http returned code: 401, response body: {\"error\":\"invalid_grant\",\"error_description\":\"Invalid user credentials\"}",
				}),
				Entry("should fail when trying to add resource because the ID manager returns a non-JSON formatted response body", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("not_json_response_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: failed to read id_manager response body into Token: invalid character '<' looking for beginning of value",
				}),
				Entry("should fail when trying to add resource because the ID manager returns a invalid format access token", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("invalid_token_format_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: invalid access token: this-is-not-a-valid-jwt",
				}),
				Entry("should fail when trying to add resource because the ID manager returns a invalid base64 access token", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("bad_base64_payload_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: failed to decode id_manager payload: illegal base64 data at input byte 0",
				}),
				Entry("should fail when trying to add resource because the ID manager returns a non-JSON formatted Payload", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("bad_json_payload_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "unable to rotate token: failed to unmarshal id_manager json: invalid character 'h' in literal true (expecting 'r')",
				}),
				Entry("should fail when trying to get MachineInfo because the CM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to process CM get request. http returned status: '404', cm return code: 'E02XXXX', error message: 'machine not found'",
				}),
				Entry("should fail when trying to get MachineInfo because the CM returns a non-JSON formatted error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to unmarshal CM get error response body into errBody. Original error: invalid character '<' looking for beginning of value",
				}),
				Entry("should fail when trying to get MachineInfo because the CM returns a non-JSON formatted response body", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000002",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to unmarshal CM get machine response body into machineData: invalid character '<' looking for beginning of value",
				}),
				Entry("should fail when trying to send scaleup request to CM because the CM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to process CM scaleup request. http returned status: '404', cm return code: 'E02XXXX', error message: 'scaleup method not found'",
				}),
				Entry("should fail when trying to send scaleup request to CM because the CM returns a non-JSON formatted error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000004",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to unmarshal CM scaleup error response body into errBody. Original error: invalid character '<' looking for beginning of value",
				}),
				Entry("should wait when the GPU has not yet been added in CM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when the GPU reports an attachment error in CM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000005",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "an error occurred with the resource in CM: 'add failed due to some reasons'",
				}),
				Entry("should return error message when nvidia-device-plugin-daemonset pod can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.Error = "no Pod with label 'app.kubernetes.io/component=nvidia-driver' found on node worker-0"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should return error message when nvidia-dra-driver-gpu-kubelet-plugin Daemonset can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.Error = "daemonsets.apps \"nvidia-dra-driver-gpu-kubelet-plugin\" not found"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should wait when the added gpu has not been recognized by cluster and DRA pod has been restarted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should wait when the added gpu has not been recognized by cluster and DRA pod is not restarted as it was recently restarted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
										Annotations: map[string]string{
											"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
										},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should wait when the added gpu has not been recognized by cluster and DRA pod is not restarted as it is not ready", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
										Annotations: map[string]string{
											"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
										},
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
							Status: appsv1.DaemonSetStatus{
								DesiredNumberScheduled: 1,
								CurrentNumberScheduled: 1,
								NumberReady:            0,
								NumberUnavailable:      1,
								NumberMisscheduled:     0,
								ObservedGeneration:     1,
							},
						}
						draDaemonsetToCreate := draDaemonset.DeepCopy()
						Expect(k8sClient.Create(ctx, draDaemonsetToCreate)).NotTo(HaveOccurred())
						draDaemonsetToCreate.Status = *draDaemonset.Status.DeepCopy()
						Expect(k8sClient.Status().Update(ctx, draDaemonsetToCreate)).To(Succeed())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should return error message when the added gpu has not been recognized by cluster because annoation restartedAt can not be parsed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
										Annotations: map[string]string{
											"kubectl.kubernetes.io/restartedAt": "error",
										},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.Error = "failed to parse restartedAt annotation for DaemonSet nvidia-dra-driver-gpu/nvidia-dra-driver-gpu-kubelet-plugin: 'parsing time \"error\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"error\" as \"2006\"'"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should successfully enter Online state when the added gpu has been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000001",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should successfully enter Deleting state when user deletes the ComposableResource because ComposableResource has not got the device_uuid", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
				Entry("should successfully enter Detaching state when user deletes the ComposableResource because there is an error occurred", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.Error = "this is an error message"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"

						return composableResourceStatus
					}(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						composableResourceStatus.Error = "this is an error message"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should still enter Online state though user deletes the ComposableResource because there is no error occurred", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)

						deleteComposableResource(composableResourceName)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-dra-driver-gpu"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		Describe("When the ComposableResource is in Online state", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError string
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Online")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())

					cleanAllComposableResources()
				})
			},
				Entry("should report an error message when Machine CR can not be found in cluster", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "metal3machines.infrastructure.cluster.x-k8s.io \"machine-worker-0\" not found"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when CM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "failed to process CM get request. http returned status: '404', cm return code: 'E02XXXX', error message: 'machine not found'"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource can not be found in CM", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target device 'GPU-device00-uuid-temp-0000-000000000000' cannot be found in CDI system"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource has a Warning status shown in CM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000006",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' is showing a Warning status in CM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource has a Critical status shown in CM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000007",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' is showing a Critical status in CM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource has an unknown status shown in CM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000008",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' has unknown status '3' in CM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should stay in Online state", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should successfully enter Detaching state when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						return composableResourceStatus
					}(),
				}),
			)
		})

		Describe("When the ComposableResource is in Detaching state", func() {
			var patches *gomonkey.Patches

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError string
			}

			BeforeAll(func() {
				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Detaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-gpu-operator"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-dra-driver-gpu"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &resourcev1.ResourceSlice{})).NotTo(HaveOccurred())

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should fail when checking gpu loads because nvidia-driver-daemonset pod can not be found", testcase{
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

					expectedReconcileError: "no Pod with label 'app.kubernetes.io/component=nvidia-driver' found on node worker-0",
				}),
				Entry("should fail when checking gpu loads because nvidia-smi command failed", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "nvidia-smi: command not found")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "run nvidia-smi in pod 'nvidia-driver-daemonset-test' to check gpu loads failed: '<nil>', stderr: 'nvidia-smi: command not found'",
				}),
				Entry("should fail when checking gpu loads because there are gpu loads existed", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000, gpu_load_progress", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "found gpu load on gpu 'GPU-device00-uuid-temp-0000-000000000000': [GPUUUID: 'GPU-device00-uuid-temp-0000-000000000000', ProcessName: 'gpu_load_progress']",
				}),
				Entry("should fail when draining gpu because the nvidiaX file is being occupied", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("nvidia-persist", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "check /dev/nvidiaX command failed: there is a process nvidia-persist occupied the nvidiaX file",
				}),
				Entry("should fail when draining gpu because nvidia-dra-driver-gpu-kubelet-plugin pod can not be found", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "no Pod named 'nvidia-dra-driver-gpu-kubelet-plugin' found on node worker-0",
				}),
				Entry("should fail when removing gpu because the targetNode can not be found in cluster", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "nodes \"worker-0\" not found",
				}),
				Entry("should fail when removing gpu because the CM returns an error when trying to get machine info", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "failed to process CM get request. http returned status: '404', cm return code: 'E02XXXX', error message: 'machine not found'",
				}),
				Entry("should fail when removing gpu because the CM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000009",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "failed to process CM scaledown request. http returned status: 404, cm return code: E02XXXX, error message: scaledown method not found",
				}),
				Entry("should fail when removing gpu because the CM returns a non-JSON formatted error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000010",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "failed to unmarshal CM scaledown error response body into errBody. Original error: invalid character '<' looking for beginning of value",
				}),
				Entry("should wait when the ComposableResource is being removed in upstream server", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
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
				Entry("should rerun when the CM failed to remove gpu in upstream server", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000011",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-fail-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-fail-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
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
				Entry("should fail when the nvidia-dra-driver-gpu-kubelet-plugin pod can not be found in cluster", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						composableResource := &crov1alpha1.ComposableResource{
							ObjectMeta: metav1.ObjectMeta{
								Name: composableResource1Name,
								Labels: map[string]string{
									"app.kubernetes.io/managed-by": "temp",
								},
							},
							Spec: crov1alpha1.ComposableResourceSpec{
								Type:        baseComposabilityRequestUsingDifferentNode.Spec.Resource.Type,
								Model:       baseComposabilityRequestUsingDifferentNode.Spec.Resource.Model,
								TargetNode:  baseComposabilityRequestUsingDifferentNode.Status.Resources[composableResource1Name].NodeName,
								ForceDetach: baseComposabilityRequestUsingDifferentNode.Spec.Resource.ForceDetach,
							},
						}
						Expect(k8sClient.Create(ctx, composableResource)).To(Succeed())
						composableResource.Status.DeviceID = "GPU-device00-uuid-temp-0000-111100000000"
						Expect(k8sClient.Status().Update(ctx, composableResource)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "daemonsets.apps \"nvidia-dra-driver-gpu-kubelet-plugin\" not found",
				}),
				Entry("should successfully enter Deleting state when the GPU has been removed from the upstream server", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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
								Namespace: "nvidia-dra-driver-gpu",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						resourceSlice := &resourcev1.ResourceSlice{
							ObjectMeta: metav1.ObjectMeta{
								Name: "resourceslice-test",
							},
							Spec: resourcev1.ResourceSliceSpec{
								Driver: "nvidia",
								Pool: resourcev1.ResourcePool{
									Name:               "test-pool",
									ResourceSliceCount: 1,
								},
								NodeName: &worker0Name,
								Devices: []resourcev1.Device{
									{
										Name: "device-0",
										Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
											"uuid": {
												StringValue: ptr.To("GPU-device00-uuid-temp-0000-000000000000"),
											},
										},
									},
								},
							},
						}
						Expect(k8sClient.Create(ctx, resourceSlice)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
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
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		Describe("When the ComposableResource is in Deleting state", func() {
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
				expectedReconcileError string
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Deleting")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, tc.ignoreGet)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					if tc.ignoreGet {
						composableResourceList := &crov1alpha1.ComposableResourceList{}
						Expect(k8sClient.List(ctx, composableResourceList)).NotTo(HaveOccurred())
						Expect(composableResourceList.Items).To(HaveLen(0))
					} else {
						Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
					}
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())

					cleanAllComposableResources()
				})
			},
				Entry("should fail because targetNode can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedReconcileError: "nodes \"worker-0\" not found",
				}),
				Entry("should succeed when the ComposableResource can be directly deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),
					ignoreGet:      true,

					extraHandling: func(composableResourceName string) {
						deleteComposableResource(composableResourceName)

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},
				}),
				Entry("should wait when the ComposableResource can not be directly deleted", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						deleteComposableResource(composableResourceName)

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
								},
								Spec: corev1.NodeSpec{
									Unschedulable: true,
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Deleting"
						return composableResourceStatus
					}(),
				}),
			)
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
		})

		Describe("When the ComposableResource is in Attaching state", func() {
			var patches *gomonkey.Patches

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError string
			}

			BeforeAll(func() {
				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Attaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					os.Setenv("FTI_CDI_ENDPOINT", endpoint)

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-gpu-operator"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-dra-driver-gpu"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-gpu-operator"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &resourcev1.ResourceSlice{})).NotTo(HaveOccurred())

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should fail when trying to add resource because the targetNode is not found", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					expectedReconcileError: "nodes \"worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the annotation is not found in targetNode", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedReconcileError: "failed to get annotation 'machine.openshift.io/machine' from Node worker-0, now is ''",
				}),
				Entry("should fail when trying to add resource because the corresponding Machine CR is not found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedReconcileError: "metal3machines.infrastructure.cluster.x-k8s.io \"machine-worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the annotation is not found in Machine CR", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

						machine0 := &machinev1beta1.Metal3Machine{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "machine-worker-0",
								Namespace: "openshift-machine-api",
							},
						}
						Expect(k8sClient.Create(ctx, machine0)).To(Succeed())
					},

					expectedReconcileError: "failed to get annotation 'metal3.io/BareMetalHost' from Machine machine-worker-0, now is ''",
				}),
				Entry("should fail when trying to add resource because the corresponding BareMetalHost CR is not found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
					},

					expectedReconcileError: "baremetalhosts.metal3.io \"bmh-worker-0\" not found",
				}),
				Entry("should fail when trying to add resource because the corresponding machine_uuid is not found in BareMetalHost CR", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())
					},

					expectedReconcileError: "failed to get annotation 'cluster-manager.cdi.io/machine' from BareMetalHost bmh-worker-0, now is ''",
				}),
				Entry("should fail when trying to send scaleup request to FM because the FM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to process FM scaleup request. FM returned code: 'E02XXXX', error message: 'scaleup method not found'",
				}),
				Entry("should fail when trying to send scaleup request to FM because the FM returns a non-JSON formatted error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000004",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000001",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to unmarshal FM scaleup error response body into errBody. Original error: invalid character '<' looking for beginning of value",
				}),
				Entry("should fail when trying to send scaleup request to FM because the FM returns an non-JSON formatted response", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000002",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "failed to unmarshal FM scaleup response body into scaleUpResponse. Original error: invalid character '<' looking for beginning of value",
				}),
				Entry("should fail when trying to send scaleup request to FM because the added gpu is in Critical state in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000004",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "the FM attached device called by test-composable-resource is in Critical state in FM",
				}),
				Entry("should fail when trying to send scaleup request to FM because the added gpu is in unknown state in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000005",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "the FM attached device called by test-composable-resource is in unknown state '3' in FM",
				}),
				Entry("should fail when trying to send scaleup request to FM because the added gpu can not be found in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000003",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000006",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "can not find the added gpu when using FM to add gpu",
				}),
				Entry("should fail when nvidia-device-plugin Pod can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedReconcileError: "no Pod with label 'app.kubernetes.io/component=nvidia-driver' found on node worker-0",
				}),
				Entry("should return error message when nvidia-dcgm Daemonset can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.Error = "daemonsets.apps \"nvidia-dcgm\" not found"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should wait when the added gpu has not been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						dcgmDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dcgm",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dcgm"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dcgm"},
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
						Expect(k8sClient.Create(ctx, dcgmDaemonset)).NotTo(HaveOccurred())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Attaching"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should fail when the added gpu has not been recognized by cluster because nvidia-smi command failed", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						dcgmDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dcgm",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dcgm"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dcgm"},
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
						Expect(k8sClient.Create(ctx, dcgmDaemonset)).NotTo(HaveOccurred())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("", "nvidia-smi: command not found")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "get gpu info command failed: err='<nil>', stderr='nvidia-smi: command not found'",
				}),
				Entry("should successfully enter Online state when the added gpu has been recognized by cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						dcgmDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dcgm",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dcgm"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dcgm"},
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
						Expect(k8sClient.Create(ctx, dcgmDaemonset)).NotTo(HaveOccurred())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should successfully enter Online state though the added gpu is in Warning state in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000003",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						dcgmDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dcgm",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dcgm"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dcgm"},
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
						Expect(k8sClient.Create(ctx, dcgmDaemonset)).NotTo(HaveOccurred())

						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						draDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
								Namespace: "nvidia-dra-driver-gpu",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
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
						Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-dra-driver-gpu"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
			})
		})

		Describe("When the ComposableResource is in Online state", func() {
			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName   string
				resourceSpec   *crov1alpha1.ComposableResourceSpec
				resourceStatus *crov1alpha1.ComposableResourceStatus

				setErrorMode  func()
				extraHandling func(composableResourceName string)

				expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
				expectedReconcileError string
			}

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Online")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())

					cleanAllComposableResources()
				})
			},
				Entry("should report an error message when Machine CR can not be found in cluster", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "metal3machines.infrastructure.cluster.x-k8s.io \"machine-worker-0\" not found"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when FM returns an error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "failed to process FM get request. FM return code: 'E02XXXX', error message: 'machine not found'"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when FM returns a non-JSON formatted error message", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000001",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "failed to unmarshal FM get error response body into errBody. Original error: invalid character '<' looking for beginning of value"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when FM returns an non-JSON formatted response", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000002",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "failed to unmarshal FM get machine response body into machineData: invalid character '<' looking for beginning of value"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should stay in Online state", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource can not be found in FM", testcase{
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
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000003",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target device 'GPU-device00-uuid-temp-0000-000000000000' cannot be found in CDI system"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),

				Entry("should report an error message when the resource has a Warning status shown in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000006",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000004",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' is showing a Warning status in FM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource has a Critical status shown in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000007",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000005",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' is showing a Critical status in FM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),
				Entry("should report an error message when the resource has an unknown status shown in FM", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000008",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000006",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())
					},

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Online"
						composableResourceStatus.Error = "the target gpu 'GPU-device00-uuid-temp-0000-000000000000' has unknown status '3' in FM"
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),
				}),

				Entry("should successfully enter Detaching state when user deletes the ComposableResource", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

					resourceName:   "test-composable-resource",
					resourceSpec:   baseComposableResource.Spec.DeepCopy(),
					resourceStatus: baseComposableResource.Status.DeepCopy(),

					extraHandling: deleteComposableResource,

					expectedRequestStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.State = "Detaching"
						return composableResourceStatus
					}(),
				}),
			)
		})

		Describe("When the ComposableResource is in Detaching state", func() {
			var patches *gomonkey.Patches

			type testcase struct {
				tenant_uuid  string
				cluster_uuid string

				resourceName          string
				resourceSpec          *crov1alpha1.ComposableResourceSpec
				resourceStatus        *crov1alpha1.ComposableResourceStatus
				expectedRequestStatus *crov1alpha1.ComposableResourceStatus

				extraHandling func(composableResourceName string)

				setErrorMode           func()
				expectedReconcileError string
			}

			BeforeAll(func() {
				patches = gomonkey.NewPatches()
			})

			DescribeTable("", func(tc testcase) {
				os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
				os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

				createComposableResource(tc.resourceName, tc.resourceSpec, tc.resourceStatus, "Detaching")

				Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
				Expect(callFunction(tc.extraHandling, tc.resourceName)).NotTo(HaveOccurred())

				composableResource, err := triggerComposableResourceReconcile(controllerReconciler, tc.resourceName, false)

				if tc.expectedReconcileError != "" {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(tc.expectedReconcileError))
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status).To(Equal(*tc.expectedRequestStatus))
				}

				DeferCleanup(func() {
					os.Unsetenv("FTI_CDI_TENANT_ID")
					os.Unsetenv("FTI_CDI_CLUSTER_ID")

					k8sClient.MockUpdate = nil
					k8sClient.MockStatusUpdate = nil

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
					Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-gpu-operator"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
						client.InNamespace("nvidia-dra-driver-gpu"),
						&client.DeleteAllOfOptions{
							DeleteOptions: client.DeleteOptions{
								GracePeriodSeconds: ptr.To(int64(0)),
							},
						},
					)).NotTo(HaveOccurred())

					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())
					Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-gpu-operator"))).NotTo(HaveOccurred())

					cleanAllComposableResources()

					patches.Reset()
				})
			},
				Entry("should fail when checking gpu loads because nvidia-driver-daemonset pod can not be found", testcase{
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

					expectedReconcileError: "no Pod with label 'app.kubernetes.io/component=nvidia-driver' found on node worker-0",
				}),
				Entry("should fail when checking gpu loads because nvidia-smi command failed", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "nvidia-smi: command not found")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "run nvidia-smi in pod 'nvidia-driver-daemonset-test' to check gpu loads failed: '<nil>', stderr: 'nvidia-smi: command not found'",
				}),
				Entry("should fail when checking gpu loads because there are gpu loads existed", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000, gpu_load_progress", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "found gpu loads on node 'worker-0': '[GPUUUID: 'GPU-device00-uuid-temp-0000-000000000000', ProcessName: 'gpu_load_progress']'",
				}),
				Entry("should fail when draining gpu because the nvidiaX file is being occupied", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("nvidia-persist", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "check /dev/nvidiaX command failed: there is a process nvidia-persist occupied the nvidiaX file",
				}),
				Entry("should fail when removing gpu because the targetNode can not be found in cluster", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "nodes \"worker-0\" not found",
				}),

				Entry("should fail when trying to send scaledown request to FM because the FM returns an error message", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "failed to process scaledown request. FM returned code: E02XXXX, error message: scaleup method not found",
				}),
				Entry("should fail when trying to send scaledown request to FM because the FM returns a non-JSON formatted error message", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-fail-000000000001",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "failed to unmarshal scaledown error response body into errBody. Original error: invalid character '<' looking for beginning of value",
				}),

				Entry("should fail when nvidia-device-plugin Daemonset can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						composableResource := &crov1alpha1.ComposableResource{
							ObjectMeta: metav1.ObjectMeta{
								Name: composableResource1Name,
								Labels: map[string]string{
									"app.kubernetes.io/managed-by": "temp",
								},
							},
							Spec: crov1alpha1.ComposableResourceSpec{
								Type:        baseComposabilityRequestUsingDifferentNode.Spec.Resource.Type,
								Model:       baseComposabilityRequestUsingDifferentNode.Spec.Resource.Model,
								TargetNode:  baseComposabilityRequestUsingDifferentNode.Status.Resources[composableResource1Name].NodeName,
								ForceDetach: baseComposabilityRequestUsingDifferentNode.Spec.Resource.ForceDetach,
							},
						}
						Expect(k8sClient.Create(ctx, composableResource)).To(Succeed())
						composableResource.Status.DeviceID = "GPU-device00-uuid-temp-0000-111100000000"
						Expect(k8sClient.Status().Update(ctx, composableResource)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "daemonsets.apps \"nvidia-device-plugin-daemonset\" not found",
				}),
				Entry("should fail when nvidia-dcgm Daemonset can not be found in cluster", testcase{
					tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
					cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

					resourceName: "test-composable-resource",
					resourceSpec: baseComposableResource.Spec.DeepCopy(),
					resourceStatus: func() *crov1alpha1.ComposableResourceStatus {
						composableResourceStatus := baseComposableResource.Status.DeepCopy()
						composableResourceStatus.DeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						composableResourceStatus.CDIDeviceID = "GPU-device00-uuid-temp-0000-000000000000"
						return composableResourceStatus
					}(),

					extraHandling: func(composableResourceName string) {
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						composableResource := &crov1alpha1.ComposableResource{
							ObjectMeta: metav1.ObjectMeta{
								Name: composableResource1Name,
								Labels: map[string]string{
									"app.kubernetes.io/managed-by": "temp",
								},
							},
							Spec: crov1alpha1.ComposableResourceSpec{
								Type:        baseComposabilityRequestUsingDifferentNode.Spec.Resource.Type,
								Model:       baseComposabilityRequestUsingDifferentNode.Spec.Resource.Model,
								TargetNode:  baseComposabilityRequestUsingDifferentNode.Status.Resources[composableResource1Name].NodeName,
								ForceDetach: baseComposabilityRequestUsingDifferentNode.Spec.Resource.ForceDetach,
							},
						}
						Expect(k8sClient.Create(ctx, composableResource)).To(Succeed())
						composableResource.Status.DeviceID = "GPU-device00-uuid-temp-0000-111100000000"
						Expect(k8sClient.Status().Update(ctx, composableResource)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
								}
							},
						)
					},

					expectedReconcileError: "daemonsets.apps \"nvidia-dcgm\" not found",
				}),

				Entry("should successfully enter Deleting state when the GPU has been removed from the upstream server", testcase{
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
						nvidiaPod := &corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-driver-daemonset-test",
								Namespace: "nvidia-gpu-operator",
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

						nodesToCreate := []*corev1.Node{
							{
								ObjectMeta: metav1.ObjectMeta{
									Name: baseComposableResource.Spec.TargetNode,
									Annotations: map[string]string{
										"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
									},
								},
							},
						}
						for _, node := range nodesToCreate {
							Expect(k8sClient.Create(ctx, node)).To(Succeed())
						}

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
									"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
								},
							},
						}
						Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

						secret := &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "credentials",
								Namespace: "composable-resource-operator-system",
							},
							Type: corev1.SecretTypeOpaque,
							Data: map[string][]byte{
								"username":      []byte("good_user"),
								"password":      []byte("test_password"),
								"client_id":     []byte("test_client_id"),
								"client_secret": []byte("test_client_secret"),
								"realm":         []byte("test_realm"),
							},
						}
						Expect(k8sClient.Create(ctx, secret)).To(Succeed())

						nvidiaDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-device-plugin-daemonset",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-device-plugin-daemonset"},
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
						Expect(k8sClient.Create(ctx, nvidiaDaemonset)).NotTo(HaveOccurred())

						dcgmDaemonset := &appsv1.DaemonSet{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "nvidia-dcgm",
								Namespace: "nvidia-gpu-operator",
							},
							Spec: appsv1.DaemonSetSpec{
								Selector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"app": "nvidia-dcgm"},
								},
								Template: corev1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"app": "nvidia-dcgm"},
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
						Expect(k8sClient.Create(ctx, dcgmDaemonset)).NotTo(HaveOccurred())

						patches.ApplyFunc(
							remotecommand.NewSPDYExecutor,
							func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
								if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
									return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
								} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
									return newMockExecutor("", "")
								} else if strings.Contains(url.RawQuery, "command=-r") {
									return newMockExecutor("", "")
								} else {
									return newMockExecutor("", "this error should be reported")
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
			)

			AfterAll(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-gpu-operator"),
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

	Describe("When user provides wrong env variables", func() {
		var patches *gomonkey.Patches

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
					Namespace: "nvidia-gpu-operator",
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

				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
				Expect(k8sClient.DeleteAllOf(ctx, &machinev1beta1.Metal3Machine{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
				Expect(k8sClient.DeleteAllOf(ctx, &metal3v1alpha1.BareMetalHost{}, client.InNamespace("openshift-machine-api"))).NotTo(HaveOccurred())
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("composable-resource-operator-system"))).NotTo(HaveOccurred())

				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-gpu-operator"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
					client.InNamespace("nvidia-dra-driver-gpu"),
					&client.DeleteAllOfOptions{
						DeleteOptions: client.DeleteOptions{
							GracePeriodSeconds: ptr.To(int64(0)),
						},
					},
				)).NotTo(HaveOccurred())

				Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{}, client.InNamespace("nvidia-dra-driver-gpu"))).NotTo(HaveOccurred())

				Expect(k8sClient.DeleteAllOf(ctx, &resourcev1.ResourceSlice{})).NotTo(HaveOccurred())

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

				expectedReconcileError: fmt.Errorf("the env variable CDI_PROVIDER_TYPE has an invalid value: 'ERROR'"),
			}),
			Entry("should fail when FTI_CDI_API_TYPE is wrong", testcase{
				cdiProviderType:    "FTI_CDI",
				ftiCdiApiType:      "ERROR",
				deviceResourceType: "DRA",
				tenant_uuid:        "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid:       "cluster0-uuid-temp-0000-000000000000",

				resourceName: "test-composable-resource",
				resourceSpec: baseComposableResource.Spec.DeepCopy(),

				expectedReconcileError: fmt.Errorf("the env variable FTI_CDI_API_TYPE has an invalid value: 'ERROR'"),
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

				extraHandling: func(composableResourceName string) {
					nodesToCreate := []*corev1.Node{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: baseComposableResource.Spec.TargetNode,
								Annotations: map[string]string{
									"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
								},
							},
						},
					}
					for _, node := range nodesToCreate {
						Expect(k8sClient.Create(ctx, node)).To(Succeed())
					}

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
								"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
							},
						},
					}
					Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

					secret := &corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "credentials",
							Namespace: "composable-resource-operator-system",
						},
						Type: corev1.SecretTypeOpaque,
						Data: map[string][]byte{
							"username":      []byte("good_user"),
							"password":      []byte("test_password"),
							"client_id":     []byte("test_client_id"),
							"client_secret": []byte("test_client_secret"),
							"realm":         []byte("test_realm"),
						},
					}
					Expect(k8sClient.Create(ctx, secret)).To(Succeed())

					nvidiaPod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-driver-daemonset-test",
							Namespace: "nvidia-gpu-operator",
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

					draDaemonset := &appsv1.DaemonSet{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-dra-driver-gpu-kubelet-plugin",
							Namespace: "nvidia-dra-driver-gpu",
						},
						Spec: appsv1.DaemonSetSpec{
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
							},
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Labels: map[string]string{"app": "nvidia-dra-driver-gpu-kubelet-plugin"},
									Annotations: map[string]string{
										"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
									},
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
					Expect(k8sClient.Create(ctx, draDaemonset)).NotTo(HaveOccurred())

					patches.ApplyFunc(
						remotecommand.NewSPDYExecutor,
						func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
							if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=gpu_uuid")) {
								return newMockExecutor("GPU-device00-uuid-temp-0000-000000000000", "")
							} else {
								return newMockExecutor("", "this error should be reported")
							}
						},
					)
				},

				expectedReconcileError: fmt.Errorf("the env variable DEVICE_RESOURCE_TYPE has an invalid value: 'ERROR'"),
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
					nvidiaPod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nvidia-driver-daemonset-test",
							Namespace: "nvidia-gpu-operator",
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
							Namespace: "nvidia-dra-driver-gpu",
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

					nodesToCreate := []*corev1.Node{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: baseComposableResource.Spec.TargetNode,
								Annotations: map[string]string{
									"machine.openshift.io/machine": "openshift-machine-api/machine-worker-0",
								},
							},
						},
					}
					for _, node := range nodesToCreate {
						Expect(k8sClient.Create(ctx, node)).To(Succeed())
					}

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
								"cluster-manager.cdi.io/machine": "machine0-uuid-temp-0000-000000000000",
							},
						},
					}
					Expect(k8sClient.Create(ctx, bmh0)).To(Succeed())

					secret := &corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "credentials",
							Namespace: "composable-resource-operator-system",
						},
						Type: corev1.SecretTypeOpaque,
						Data: map[string][]byte{
							"username":      []byte("good_user"),
							"password":      []byte("test_password"),
							"client_id":     []byte("test_client_id"),
							"client_secret": []byte("test_client_secret"),
							"realm":         []byte("test_realm"),
						},
					}
					Expect(k8sClient.Create(ctx, secret)).To(Succeed())

					patches.ApplyFunc(
						remotecommand.NewSPDYExecutor,
						func(_ *rest.Config, method string, url *neturl.URL) (remotecommand.Executor, error) {
							if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-compute-apps=gpu_uuid,process_name")) {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, neturl.QueryEscape("--query-gpu=index,gpu_uuid,pci.bus_id")) {
								return newMockExecutor("0, GPU-device00-uuid-temp-0000-000000000000, 00000000:1F:00.0", "")
							} else if strings.Contains(url.RawQuery, "command=-pm&command=0") {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, neturl.QueryEscape("TARGET_FILE")) {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/run/nvidia/driver/dev/nvidia")) {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, neturl.QueryEscape("/dev/nvidia")) {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, "command=-m&command=1") {
								return newMockExecutor("", "")
							} else if strings.Contains(url.RawQuery, "command=-r") {
								return newMockExecutor("", "")
							} else {
								return newMockExecutor("", "this error should be reported")
							}
						},
					)
				},

				expectedReconcileError: fmt.Errorf("the env variable DEVICE_RESOURCE_TYPE has an invalid value: 'ERROR'"),
			}),
		)

		AfterAll(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
				client.InNamespace("nvidia-gpu-operator"),
				&client.DeleteAllOfOptions{
					DeleteOptions: client.DeleteOptions{
						GracePeriodSeconds: ptr.To(int64(0)),
					},
				},
			)).NotTo(HaveOccurred())
		})
	})
})
