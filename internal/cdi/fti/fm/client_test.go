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

package fm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CoHDI/composable-resource-operator/internal/cdi/fti"
	ftifmapi "github.com/CoHDI/composable-resource-operator/internal/cdi/fti/fm/api"
	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	machinev1beta1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestClients(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FTI Client Suite")
}

func testNode(name string, annotation string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				"machine.openshift.io/machine": annotation,
			},
		},
	}
}

func testMachine(annotation string) *machinev1beta1.Metal3Machine {
	return &machinev1beta1.Metal3Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "machine-1",
			Namespace: "openshift-machine-api",
			Annotations: map[string]string{
				"metal3.io/BareMetalHost": annotation,
			},
		},
	}
}

func testBaremetalhost(annotation string) *metal3v1alpha1.BareMetalHost {
	return &metal3v1alpha1.BareMetalHost{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bmh-1",
			Namespace: "openshift-machine-api",
			Annotations: map[string]string{
				"cluster-manager.cdi.io/machine": annotation,
			},
		},
	}
}

func newFakeClient(objects ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	Expect(corev1.AddToScheme(scheme)).To(Succeed())
	Expect(machinev1beta1.AddToScheme(scheme)).To(Succeed())
	Expect(metal3v1alpha1.AddToScheme(scheme)).To(Succeed())

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

var _ = Describe("cdi/fti/api functions", func() {
	Describe("formatFMErrorDetail", func() {
		It("returns formatted error message", func() {
			detail := ftifmapi.ErrorDetail{
				Code:    "FM001",
				Message: json.RawMessage(`"device not found"`),
			}
			result := formatFMErrorDetail(detail)
			Expect(result).To(Equal("code: 'FM001', error message: 'device not found'"))
		})
		It("falls back to raw message when payload is not a JSON string", func() {
			detail := ftifmapi.ErrorDetail{
				Code:    "FM002",
				Message: json.RawMessage(`{"error":"bad payload"}`),
			}
			result := formatFMErrorDetail(detail)
			Expect(result).To(Equal("code: 'FM002', error message: '{\"error\":\"bad payload\"}'"))
		})
	})
	Describe("newHttpClient", func() {
		It("returns httpClient ", func() {
			ctx := context.Background()
			token := &oauth2.Token{
				AccessToken: "test-access-token",
				TokenType:   "Bearer",
			}
			httpClient := newHttpClient(ctx, token)
			Expect(httpClient).NotTo(BeNil())
			Expect(httpClient.Timeout).To(Equal(fmRequestTimeout))
		})
	})
	Describe("NewFTIClient", func() {
		BeforeEach(func() {
			GinkgoT().Setenv("FTI_CDI_ENDPOINT", "fm.example.com")
			GinkgoT().Setenv("FTI_CDI_TENANT_ID", "tenant-123")
			GinkgoT().Setenv("FTI_CDI_CLUSTER_ID", "cluster-456")
		})

		It("returns NewFTIClient", func() {
			ctx := context.Background()
			kubeClient := newFakeClient()

			result := NewFTIClient(ctx, kubeClient, nil)

			Expect(result).NotTo(BeNil())
			Expect(result.compositionServiceEndpoint).To(Equal(
				"fm.example.com/",
			))
			Expect(result.tenantID).To(Equal("tenant-123"))
			Expect(result.clusterID).To(Equal("cluster-456"))
			Expect(result.ctx).To(Equal(ctx))
			Expect(result.client).To(Equal(kubeClient))
			Expect(result.token).NotTo(BeNil())
		})

	})
	Describe("GetResources", func() {
		It("returns an empty list when no nodes exist", func() {
			ctx := context.Background()
			kubeClient := newFakeClient()
			f := &FTIClient{
				ctx:    ctx,
				client: kubeClient,
			}

			resources, err := f.GetResources()

			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(BeEmpty())
		})

		It("skips a node when its machine ID cannot be resolved", func() {
			ctx := context.Background()
			node := testNode("worker-1", "openshift-machine-api/machine-1")
			kubeClient := newFakeClient(node)
			f := &FTIClient{
				ctx:    ctx,
				client: kubeClient,
			}

			resources, err := f.GetResources()

			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(BeEmpty())
		})
	})
	Describe("getMachineInfo", func() {
		It("returns an error when the token cannot be obtained", func() {
			server := httptest.NewServer(http.NotFoundHandler())
			DeferCleanup(server.Close)

			clientSet, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			Expect(err).NotTo(HaveOccurred())
			f := &FTIClient{
				compositionServiceEndpoint: "fm.example.com/",
				tenantID:                   "tenant-123",
				token:                      fti.NewCachedToken(clientSet, ""),
			}

			machineData, err := f.getMachineInfo("machine-uuid-123")

			Expect(machineData).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("unable to rotate token")))
		})
	})
	Describe("getNodeMachineID", func() {
		var ctx context.Context
		BeforeEach(func() {
			ctx = context.Background()
		})

		It("returns node machine ID ", func() {
			node := testNode("worker-1", "openshift-machine-api/machine-1")
			machine := testMachine("openshift-machine-api/bmh-1")

			bmh := testBaremetalhost("machine-uuid-123")

			kubeClient := newFakeClient(node, machine, bmh)
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}
			machineUUID, err := f.getNodeMachineID("worker-1")

			Expect(err).NotTo(HaveOccurred())
			Expect(machineUUID).To(Equal("machine-uuid-123"))

		})
		It("returns error when Node is not found", func() {
			kubeClient := newFakeClient()
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}

			machUUID, err := f.getNodeMachineID("missing-node")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
			Expect(machUUID).To(Equal(""))
		})
		It("returns error when Machine is not found for the Node annotation", func() {
			node := testNode("worker-1", "openshift-machine-api/machine-1")

			kubeClient := newFakeClient(node)
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}

			machUUID, err := f.getNodeMachineID("worker-1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
			Expect(machUUID).To(Equal(""))
		})
		It("returns error when BareMetalHost is not found for the Machine annotation", func() {
			node := testNode("worker-1", "openshift-machine-api/machine-1")
			machine := testMachine("openshift-machine-api/bmh-1")

			kubeClient := newFakeClient(node, machine)
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}

			machUUID, err := f.getNodeMachineID("worker-1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
			Expect(machUUID).To(Equal(""))
		})
		It("returns error: failed to get annotation 'machine.openshift.io/machine'", func() {
			node := testNode("worker-1", "")

			kubeClient := newFakeClient(node)
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}
			machineUUID, err := f.getNodeMachineID("worker-1")

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'machine.openshift.io/machine' from Node"))
			Expect(machineUUID).To(Equal(""))

		})
		It("returns error: failed to get annotation 'metal3.io/BareMetalHost' from Machine ", func() {
			node := testNode("worker-1", "openshift-machine-api/machine-1")
			machine := testMachine("")

			kubeClient := newFakeClient(node, machine)
			f := &FTIClient{
				clusterID: "cluster-uuid-456",
				ctx:       ctx,
				client:    kubeClient,
			}
			machineUUID, err := f.getNodeMachineID("worker-1")

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'metal3.io/BareMetalHost' from Machine"))
			Expect(machineUUID).To(Equal(""))

		})
		It("returns error when BareMetalHost machine annotation is empty", func() {
			node := testNode("worker-1", "openshift-machine-api/machine-1")
			machine := testMachine("openshift-machine-api/bmh-1")
			bmh := testBaremetalhost("")

			kubeClient := newFakeClient(node, machine, bmh)
			f := &FTIClient{
				clusterID: "clusterid-456",
				ctx:       ctx,
				client:    kubeClient,
			}
			machUUID, err := f.getNodeMachineID("worker-1")

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'cluster-manager.cdi.io/machine' from BareMetalHost"))
			Expect(machUUID).To(Equal(""))
		})
		It("returns no error when cluster ID is empty and ProviderID matches the fsas-cdi prefix", func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "worker-1",
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-1",
					},
				},
				Spec: corev1.NodeSpec{
					ProviderID: "fsas-cdi://machineUUID123",
				},
			}

			kubeClient := newFakeClient(node)
			f := &FTIClient{
				clusterID: "",
				ctx:       ctx,
				client:    kubeClient,
			}
			machUUID, err := f.getNodeMachineID("worker-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(machUUID).To(Equal("machineUUID123"))
		})

		It("returns error when clusterID is empty and the node does not exist", func() {
			kubeClient := newFakeClient()
			f := &FTIClient{
				clusterID: "",
				ctx:       ctx,
				client:    kubeClient,
			}

			machUUID, err := f.getNodeMachineID("missing-node")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
			Expect(machUUID).To(Equal(""))
		})
	})
})
