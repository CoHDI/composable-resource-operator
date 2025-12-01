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
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	crov1alpha1 "github.com/CoHDI/composable-resource-operator/api/v1alpha1"
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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Upstreamsyncer Controller", Ordered, func() {
	BeforeAll(func() {
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
				default:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(`{"error":"unsupported_test_user"}`))
				}

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-fail-000000000000/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"status":404,"detail":{"code":"E02XXXX","message":"machine not found"}}`))

			case "/cluster_manager/cluster_autoscaler/v3/tenants/tenant00-uuid-temp-0000-000000000000/clusters/cluster0-uuid-temp-0000-000000000000/machines/machine0-uuid-temp-0000-000000000000":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(generateCMMachineData(false, false, true, nil))

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
		endpoint := strings.TrimPrefix(testTLSServer.URL, "https://")
		os.Setenv("FTI_CDI_ENDPOINT", endpoint)
	})

	Describe("When using CM", func() {
		var (
			reconciler *UpstreamSyncerReconciler
			adapter    *ComposableResourceAdapter
			clientSet  *kubernetes.Clientset
			patches    *gomonkey.Patches
		)

		type testcase struct {
			tenant_uuid  string
			cluster_uuid string

			isCreated          bool
			missingDevicesTime map[string]time.Time

			setErrorMode           func()
			extraHandling          func()
			expectedRequestStatus  *crov1alpha1.ComposableResourceStatus
			expectedReconcileError string
		}

		BeforeAll(func() {
			os.Setenv("CDI_PROVIDER_TYPE", "FTI_CDI")
			os.Setenv("FTI_CDI_API_TYPE", "CM")
			os.Setenv("DEVICE_RESOURCE_TYPE", "DEVICE_PLUGIN")

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

			var err error
			clientSet, err = kubernetes.NewForConfig(cfg)
			Expect(err).NotTo(HaveOccurred())

			reconciler = &UpstreamSyncerReconciler{
				Client:         k8sClient,
				ClientSet:      clientSet,
				Scheme:         scheme.Scheme,
				missingDevices: make(map[string]time.Time),
			}

			patches = gomonkey.NewPatches()
		})

		DescribeTable("", func(tc testcase) {
			os.Setenv("FTI_CDI_TENANT_ID", tc.tenant_uuid)
			os.Setenv("FTI_CDI_CLUSTER_ID", tc.cluster_uuid)

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling)).NotTo(HaveOccurred())

			if tc.missingDevicesTime != nil {
				reconciler.missingDevices = tc.missingDevicesTime
			}

			var err error
			adapter, err = NewComposableResourceAdapter(ctx, reconciler.Client, reconciler.ClientSet)
			Expect(err).NotTo(HaveOccurred())

			err = reconciler.syncUpstreamData(ctx, adapter)

			if tc.expectedReconcileError != "" {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())

				// Verify that the expected ComposableResources are created
				composableResourceList := &crov1alpha1.ComposableResourceList{}
				Expect(k8sClient.List(ctx, composableResourceList)).To(Succeed())

				find := false
				resourceName := ""
				for _, composableResource := range composableResourceList.Items {
					if composableResource.Labels["cohdi.io/ready-to-detach-device-id"] == "GPU-device00-uuid-temp-0000-000000000000" {
						find = true
						resourceName = composableResource.Name
					}
				}
				Expect(tc.isCreated).To(Equal(find))

				if tc.isCreated {
					controllerReconciler := &ComposableResourceReconciler{
						Client:     k8sClient,
						Clientset:  clientSet,
						Scheme:     k8sClient.Scheme(),
						RestConfig: cfg,
					}

					composableResource, err := triggerComposableResourceReconcile(controllerReconciler, resourceName, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(composableResource.Status.DeviceID).To(Equal("GPU-device00-uuid-temp-0000-000000000000"))
				}
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
			Entry("should fail because the corresponding Machine CR is not found in cluster", testcase{
				tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

				extraHandling: func() {
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

				expectedReconcileError: "failed to fetch data from upstream server: metal3machines.infrastructure.cluster.x-k8s.io \"machine-worker-0\" not found",
			}),
			Entry("should fail because the CM returns an error message", testcase{
				tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid: "cluster0-uuid-temp-fail-000000000000",

				extraHandling: func() {
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

				expectedReconcileError: "failed to fetch data from upstream server: failed to process CM get request. http returned status: '404', cm return code: 'E02XXXX', error message: 'machine not found'",
			}),
			Entry("should wait when there is an extra device in upstram server because it needs to track with grace period", testcase{
				tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

				extraHandling: func() {
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
			}),
			Entry("should wait when there is an extra device in upstram server because it is not exceeded grace period", testcase{
				tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

				missingDevicesTime: map[string]time.Time{
					"GPU-device00-uuid-temp-0000-000000000000": time.Now().Add(-1 * time.Minute),
				},
				isCreated: false,

				extraHandling: func() {
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
			}),
			Entry("should successfully create a ComposableResource for detaching because it has exceeded grace period", testcase{
				tenant_uuid:  "tenant00-uuid-temp-0000-000000000000",
				cluster_uuid: "cluster0-uuid-temp-0000-000000000000",

				missingDevicesTime: map[string]time.Time{
					"GPU-device00-uuid-temp-0000-000000000000": time.Now().Add(-20 * time.Minute),
				},
				isCreated: true,

				extraHandling: func() {
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
			}),
		)
	})
})
