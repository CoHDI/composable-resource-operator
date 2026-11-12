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

package cm

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	crov1alpha1 "github.com/CoHDI/composable-resource-operator/api/v1alpha1"
	"github.com/CoHDI/composable-resource-operator/internal/cdi"
	"github.com/CoHDI/composable-resource-operator/internal/cdi/fti"
	fticmapi "github.com/CoHDI/composable-resource-operator/internal/cdi/fti/cm/api"
	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestFTICMClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FTI CM Client Suite")
}

func newTestMachine(name string, annotations map[string]string) *unstructured.Unstructured {
	machine := &unstructured.Unstructured{}
	machine.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "machine.openshift.io",
		Version: "v1beta1",
		Kind:    "Machine",
	})
	machine.SetName(name)
	machine.SetNamespace("openshift-machine-api")
	machine.SetAnnotations(annotations)
	return machine
}

func newTestNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				"machine.openshift.io/machine": "openshift-machine-api/machine-1",
			},
		},
	}
}

func newTestBareMetalHost(machineUUID string) *metal3v1alpha1.BareMetalHost {
	return &metal3v1alpha1.BareMetalHost{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bmh-1",
			Namespace: "openshift-machine-api",
			Annotations: map[string]string{
				"cluster-manager.cdi.io/machine": machineUUID,
			},
		},
	}
}

func clusterObjects(nodeName, machineUUID string) []client.Object {
	return []client.Object{
		newTestNode(nodeName),
		newTestMachine("machine-1", map[string]string{
			"metal3.io/BareMetalHost": "openshift-machine-api/bmh-1",
		}),
		newTestBareMetalHost(machineUUID),
	}
}

func newTestComposableResource(name, node, model, deviceID string) *crov1alpha1.ComposableResource {
	return &crov1alpha1.ComposableResource{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: crov1alpha1.ComposableResourceSpec{
			Type:       "gpu",
			Model:      model,
			TargetNode: node,
		},
		Status: crov1alpha1.ComposableResourceStatus{
			DeviceID: deviceID,
		},
	}
}

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	Expect(corev1.AddToScheme(s)).To(Succeed())
	Expect(metal3v1alpha1.AddToScheme(s)).To(Succeed())
	Expect(crov1alpha1.AddToScheme(s)).To(Succeed())
	return s
}

func newKubeClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithStatusSubresource(&crov1alpha1.ComposableResource{}).
		WithObjects(objects...).
		Build()
}

func credentialsSecret() *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "credentials",
			Namespace: "composable-resource-operator-system",
		},
		Data: map[string][]byte{
			"username":      []byte("good_user"),
			"password":      []byte("password"),
			"client_id":     []byte("client"),
			"client_secret": []byte("secret"),
			"realm":         []byte("test_realm"),
		},
	}
}

func newTokenClientset() *kubernetes.Clientset {
	secret := credentialsSecret()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/composable-resource-operator-system/secrets/credentials" {
			w.Header().Set("Content-Type", "application/json")
			Expect(json.NewEncoder(w).Encode(secret)).To(Succeed())
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	DeferCleanup(server.Close)

	clientSet, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	Expect(err).NotTo(HaveOccurred())
	return clientSet
}

func newFailingTokenClientset() *kubernetes.Clientset {
	server := httptest.NewServer(http.NotFoundHandler())
	DeferCleanup(server.Close)

	clientSet, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	Expect(err).NotTo(HaveOccurred())
	return clientSet
}

func writeTokenResponse(w http.ResponseWriter) {
	payload, err := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix()})
	Expect(err).NotTo(HaveOccurred())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	Expect(json.NewEncoder(w).Encode(map[string]any{
		"access_token":  "header." + base64.RawURLEncoding.EncodeToString(payload) + ".sig",
		"token_type":    "Bearer",
		"refresh_token": "refresh-token",
		"expires_in":    3600,
	})).To(Succeed())
}

func ignoreTLSVerify() {
	original := http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	DeferCleanup(func() {
		http.DefaultTransport = original
	})
}

type cmServerConfig struct {
	machineStatus int
	machineBody   []byte
	machineBodies map[string][]byte
	resizeStatus  int
	onResize      func(r *http.Request, body []byte)
}

func machineIDFromPath(path string) string {
	const marker = "/machines/"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	id := path[idx+len(marker):]
	if slash := strings.Index(id, "/"); slash >= 0 {
		id = id[:slash]
	}
	return id
}

func startCMServer(cfg cmServerConfig) string {
	ignoreTLSVerify()
	if cfg.machineStatus == 0 {
		cfg.machineStatus = http.StatusOK
	}
	if cfg.resizeStatus == 0 {
		cfg.resizeStatus = http.StatusOK
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/protocol/openid-connect/token"):
			writeTokenResponse(w)
		case strings.HasSuffix(r.URL.Path, "/actions/resize"):
			body, _ := io.ReadAll(r.Body)
			if cfg.onResize != nil {
				cfg.onResize(r, body)
			}
			w.WriteHeader(cfg.resizeStatus)
		case strings.Contains(r.URL.Path, "/machines/"):
			w.Header().Set("Content-Type", "application/json")
			if len(cfg.machineBodies) > 0 {
				body, ok := cfg.machineBodies[machineIDFromPath(r.URL.Path)]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(cfg.machineStatus)
				_, _ = w.Write(body)
				return
			}
			w.WriteHeader(cfg.machineStatus)
			if cfg.machineBody != nil {
				_, _ = w.Write(cfg.machineBody)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	DeferCleanup(server.Close)
	return strings.TrimPrefix(server.URL, "https://") + "/"
}

func newFTIClient(endpoint string, objects ...client.Object) *FTIClient {
	return &FTIClient{
		compositionServiceEndpoint: endpoint,
		tenantID:                   "tenant-123",
		clusterID:                  "cluster-456",
		ctx:                        context.Background(),
		client:                     newKubeClient(objects...),
		token:                      fti.NewCachedToken(newTokenClientset(), endpoint),
	}
}

func gpuSpec(specUUID, model string, devices ...fticmapi.Device) fticmapi.ResourceSpec {
	return fticmapi.ResourceSpec{
		SpecUUID: specUUID,
		Type:     "gpu",
		Selector: fticmapi.Selector{
			Expression: fticmapi.Expression{
				Conditions: []fticmapi.Condition{
					{Column: "model", Operator: "eq", Value: model},
				},
			},
		},
		DeviceCount: len(devices),
		Devices:     devices,
	}
}

func gpuDevice(deviceUUID, status, statusReason, resourceUUID, opStatus string) fticmapi.Device {
	return fticmapi.Device{
		DeviceUUID:   deviceUUID,
		Status:       status,
		StatusReason: statusReason,
		Detail: fticmapi.DeviceDetail{
			ResourceUUID:     resourceUUID,
			ResourceOPStatus: opStatus,
		},
	}
}

func machineDataJSON(specs ...fticmapi.ResourceSpec) []byte {
	body, err := json.Marshal(fticmapi.MachineData{
		Data: fticmapi.Data{
			Cluster: fticmapi.Cluster{
				Machine: fticmapi.Machine{ResourceSpecs: specs},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	return body
}

var _ = Describe("fti/cm client tests", func() {
	var (
		ctx    context.Context
		scheme *runtime.Scheme
	)
	BeforeEach(func() {
		os.Setenv("FTI_CDI_ENDPOINT", "fm.example.com")
		os.Setenv("FTI_CDI_TENANT_ID", "tenant-123")
		os.Setenv("FTI_CDI_CLUSTER_ID", "cluster-456")
		ctx = context.Background()
		scheme = runtime.NewScheme()

	})
	AfterEach(func() {
		os.Unsetenv("FTI_CDI_ENDPOINT")
		os.Unsetenv("FTI_CDI_TENANT_ID")
		os.Unsetenv("FTI_CDI_CLUSTER_ID")
	})
	Describe("NewFTIClient", func() {
		It("creates a new FTIClient with the configured endpoint (no suffix)", func() {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				Build()
			FTIclient := NewFTIClient(ctx, c, nil)

			Expect(FTIclient).NotTo(BeNil())
			Expect(FTIclient.compositionServiceEndpoint).To(Equal("fm.example.com/"))
			Expect(FTIclient.tenantID).To(Equal("tenant-123"))
			Expect(FTIclient.clusterID).To(Equal("cluster-456"))
		})
	})
	Describe("AddResource", func() {
		const (
			nodeName    = "node-1"
			machineUUID = "machine-uuid-123"
			model       = "NVIDIA-A100-80GB-PCIe"
			specUUID    = "spec-gpu-a100"
		)

		It("returns an error when the node machine ID cannot be resolved", func() {
			instance := newTestComposableResource("gpu-1", nodeName, model, "")
			ftiClient := newFTIClient("cm.example.com/", instance)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(instance)

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when the CM token cannot be obtained", func() {
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := &FTIClient{
				compositionServiceEndpoint: "cm.example.com/",
				tenantID:                   "tenant-123",
				clusterID:                  "cluster-456",
				ctx:                        ctx,
				client:                     newKubeClient(objects...),
				token:                      fti.NewCachedToken(newFailingTokenClientset(), "cm.example.com/"),
			}

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(ContainSubstring("unable to rotate token")))
		})

		It("returns an error when CM get machine info fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineStatus: http.StatusNotFound,
				machineBody:   []byte(`{"status":404}`),
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(ContainSubstring("failed to process CM get request")))
		})

		It("reuses an unused ADD_COMPLETE device without scaling up", func() {
			resizeCalled := false
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice("gpu-unused", addComplete, "", "res-unused", "0"),
				)),
				onResize: func(_ *http.Request, _ []byte) {
					resizeCalled = true
				},
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(err).NotTo(HaveOccurred())
			Expect(deviceID).To(Equal("gpu-unused"))
			Expect(cdiDeviceID).To(Equal("res-unused"))
			Expect(resizeCalled).To(BeFalse())
		})

		It("returns the device IDs with an error when the unused device is ADD_FAILED", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice("gpu-failed", addFailed, "add failed due to some reasons", "res-failed", "0"),
				)),
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(Equal("gpu-failed"))
			Expect(cdiDeviceID).To(Equal("res-failed"))
			Expect(err).To(MatchError(ContainSubstring("an error occurred with the resource in CM")))
			Expect(err).To(MatchError(ContainSubstring("add failed due to some reasons")))
		})

		It("requests one more device and returns ErrWaitingDeviceAttaching", func() {
			var resizeBody []byte
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice("gpu-existing", addComplete, "", "res-existing", "0"),
				)),
				onResize: func(_ *http.Request, body []byte) {
					resizeBody = body
				},
			})
			existing := newTestComposableResource("gpu-existing", nodeName, model, "gpu-existing")
			request := newTestComposableResource("gpu-1", nodeName, model, "")
			objects := append(clusterObjects(nodeName, machineUUID), existing, request)
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(request)

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(cdi.ErrWaitingDeviceAttaching))

			var got scaleUpRequestBody
			Expect(json.Unmarshal(resizeBody, &got)).To(Succeed())
			Expect(got.Target.SpecUUID).To(Equal(specUUID))
			Expect(got.Target.DeviceCount).To(Equal(2))
		})

		It("returns an error when the CM scaleup request fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody:  machineDataJSON(gpuSpec(specUUID, model)),
				resizeStatus: http.StatusInternalServerError,
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(ContainSubstring("failed to process CM scaleup request")))
		})

		It("sends a scaleup with an empty spec_uuid when no CM spec matches the model", func() {
			var resizeBody []byte
			endpoint := startCMServer(cmServerConfig{
				machineBody:  machineDataJSON(gpuSpec(specUUID, "NVIDIA-OTHER")),
				resizeStatus: http.StatusBadRequest,
				onResize: func(_ *http.Request, body []byte) {
					resizeBody = body
				},
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(ContainSubstring("failed to process CM scaleup request")))
			Expect(err).To(MatchError(ContainSubstring("http returned status: 400")))

			var got scaleUpRequestBody
			Expect(json.Unmarshal(resizeBody, &got)).To(Succeed())
			Expect(got.Target.SpecUUID).To(BeEmpty())
			Expect(got.Target.DeviceCount).To(Equal(1))
		})

		It("scales up again even when an unused device is still ADD_IN_PROGRESS", func() {
			var resizeBody []byte
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice("gpu-adding", "ADD_IN_PROGRESS", "", "res-adding", "0"),
				)),
				onResize: func(_ *http.Request, body []byte) {
					resizeBody = body
				},
			})
			objects := append(clusterObjects(nodeName, machineUUID), newTestComposableResource("gpu-1", nodeName, model, ""))
			ftiClient := newFTIClient(endpoint, objects...)

			deviceID, cdiDeviceID, err := ftiClient.AddResource(newTestComposableResource("gpu-1", nodeName, model, ""))

			Expect(deviceID).To(BeEmpty())
			Expect(cdiDeviceID).To(BeEmpty())
			Expect(err).To(MatchError(cdi.ErrWaitingDeviceAttaching))

			var got scaleUpRequestBody
			Expect(json.Unmarshal(resizeBody, &got)).To(Succeed())
			Expect(got.Target.SpecUUID).To(Equal(specUUID))
			Expect(got.Target.DeviceCount).To(Equal(2))
		})
	})
	Describe("RemoveResource", func() {
		const (
			nodeName    = "node-1"
			machineUUID = "machine-uuid-123"
			model       = "NVIDIA-A100-80GB-PCIe"
			specUUID    = "spec-gpu-a100"
			deviceID    = "gpu-1"
		)

		It("returns an error when the target node cannot be mapped to a CM machine", func() {
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			ftiClient := newFTIClient("cm.example.com/", instance)

			err := ftiClient.RemoveResource(instance)

			Expect(err).To(HaveOccurred())
		})

		It("returns an error when CM get machine info fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineStatus: http.StatusNotFound,
				machineBody:   []byte(`{"status":404}`),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.RemoveResource(instance)

			Expect(err).To(MatchError(ContainSubstring("failed to process CM get request")))
		})

		It("returns nil without scaling down when the device is not in CM", func() {
			resizeCalled := false
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model)),
				onResize: func(_ *http.Request, _ []byte) {
					resizeCalled = true
				},
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.RemoveResource(instance)

			Expect(err).NotTo(HaveOccurred())
			Expect(resizeCalled).To(BeFalse())
		})

		It("records the CM error and still scales down when the device is REMOVE_FAILED", func() {
			var resizeBody []byte
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, removeFailed, "remove failed due to some reasons", "res-1", "0"),
				)),
				onResize: func(_ *http.Request, body []byte) {
					resizeBody = body
				},
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.RemoveResource(instance)

			Expect(err).To(MatchError(cdi.ErrWaitingDeviceDetaching))

			updated := &crov1alpha1.ComposableResource{}
			Expect(ftiClient.client.Get(ctx, client.ObjectKey{Name: instance.Name}, updated)).To(Succeed())
			Expect(updated.Status.Error).To(Equal("remove failed due to some reasons"))

			var got scaleDownRequestBody
			Expect(json.Unmarshal(resizeBody, &got)).To(Succeed())
			Expect(got.Target.SpecUUID).To(Equal(specUUID))
			Expect(got.Target.DeviceCount).To(Equal(0))
			Expect(got.Target.Devices).To(Equal([]string{deviceID}))
		})

		It("requests one less device and returns ErrWaitingDeviceDetaching", func() {
			var resizeBody []byte
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", "0"),
					gpuDevice("gpu-2", addComplete, "", "res-2", "0"),
				)),
				onResize: func(_ *http.Request, body []byte) {
					resizeBody = body
				},
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.RemoveResource(instance)

			Expect(err).To(MatchError(cdi.ErrWaitingDeviceDetaching))

			var got scaleDownRequestBody
			Expect(json.Unmarshal(resizeBody, &got)).To(Succeed())
			Expect(got.Target.SpecUUID).To(Equal(specUUID))
			Expect(got.Target.DeviceCount).To(Equal(1))
			Expect(got.Target.Devices).To(Equal([]string{deviceID}))
		})

		It("returns an error when the CM scaledown request fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", "0"),
				)),
				resizeStatus: http.StatusInternalServerError,
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.RemoveResource(instance)

			Expect(err).To(MatchError(ContainSubstring("failed to process CM scaledown request")))
		})
	})
	Describe("CheckResource", func() {
		const (
			nodeName    = "node-1"
			machineUUID = "machine-uuid-123"
			model       = "NVIDIA-A100-80GB-PCIe"
			specUUID    = "spec-gpu-a100"
			deviceID    = "gpu-1"
		)

		It("returns an error when the target node cannot be mapped to a CM machine", func() {
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			ftiClient := newFTIClient("cm.example.com/", instance)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(HaveOccurred())
		})

		It("returns an error when CM get machine info fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineStatus: http.StatusNotFound,
				machineBody:   []byte(`{"status":404}`),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("failed to process CM get request")))
		})

		It("returns nil for a healthy device and ignores specs with a different type or model", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(
					fticmapi.ResourceSpec{Type: "cpu", Devices: []fticmapi.Device{
						gpuDevice(deviceID, addComplete, "", "res-1", "2critical"),
					}},
					gpuSpec("other-spec", "NVIDIA-ANOTHER-GPU",
						gpuDevice(deviceID, addComplete, "", "res-1", "2critical")),
					gpuSpec(specUUID, model, gpuDevice(deviceID, addComplete, "", "res-1", "0")),
				),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).NotTo(HaveOccurred())
		})

		It("returns an error when the device ResourceOPStatus is empty", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", ""),
				)),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("has empty status in CM")))
		})

		It("returns an error when the device ResourceOPStatus is Warning", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", "1warning"),
				)),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("showing a Warning status in CM")))
		})

		It("returns an error when the device ResourceOPStatus is Critical", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", "2critical"),
				)),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("showing a Critical status in CM")))
		})

		It("returns an error when the device ResourceOPStatus is unknown", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice(deviceID, addComplete, "", "res-1", "9unknown"),
				)),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("has unknown status '9unknown' in CM")))
		})

		It("returns an error when the device is not in any matching CM spec", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(gpuSpec(specUUID, model,
					gpuDevice("other-gpu", addComplete, "", "res-other", "0"),
				)),
			})
			instance := newTestComposableResource("gpu-1", nodeName, model, deviceID)
			objects := append(clusterObjects(nodeName, machineUUID), instance)
			ftiClient := newFTIClient(endpoint, objects...)

			err := ftiClient.CheckResource(instance)

			Expect(err).To(MatchError(ContainSubstring("cannot be found in CDI system")))
		})
	})
	Describe("GetResources", func() {
		const (
			nodeName    = "node-1"
			machineUUID = "machine-uuid-123"
			model       = "NVIDIA-A100-80GB-PCIe"
		)

		It("returns an empty list when no nodes exist", func() {
			ftiClient := newFTIClient("cm.example.com/")

			resources, err := ftiClient.GetResources()

			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(BeEmpty())
		})

		It("returns an error when a node cannot be mapped to a CM machine", func() {
			ftiClient := newFTIClient("cm.example.com/", newTestNode(nodeName))

			resources, err := ftiClient.GetResources()

			Expect(resources).To(BeNil())
			Expect(err).To(HaveOccurred())
		})

		It("returns an error when CM get machine info fails", func() {
			endpoint := startCMServer(cmServerConfig{
				machineStatus: http.StatusNotFound,
				machineBody:   []byte(`{"status":404}`),
			})
			ftiClient := newFTIClient(endpoint, clusterObjects(nodeName, machineUUID)...)

			resources, err := ftiClient.GetResources()

			Expect(resources).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("failed to process CM get request")))
		})

		It("returns gpu devices from CM and skips non-gpu specs", func() {
			endpoint := startCMServer(cmServerConfig{
				machineBody: machineDataJSON(
					fticmapi.ResourceSpec{
						Type: "cpu",
						Devices: []fticmapi.Device{
							gpuDevice("cpu-1", addComplete, "", "res-cpu", "0"),
						},
					},
					gpuSpec("spec-gpu", model,
						gpuDevice("gpu-1", addComplete, "", "res-gpu-1", "0"),
						gpuDevice("gpu-2", addComplete, "", "res-gpu-2", "0"),
					),
				),
			})
			ftiClient := newFTIClient(endpoint, clusterObjects(nodeName, machineUUID)...)

			resources, err := ftiClient.GetResources()

			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(Equal([]cdi.DeviceInfo{
				{
					NodeName:    nodeName,
					MachineUUID: machineUUID,
					DeviceType:  "gpu",
					DeviceID:    "gpu-1",
					CDIDeviceID: "res-gpu-1",
				},
				{
					NodeName:    nodeName,
					MachineUUID: machineUUID,
					DeviceType:  "gpu",
					DeviceID:    "gpu-2",
					CDIDeviceID: "res-gpu-2",
				},
			}))
		})

		It("combines gpu devices from multiple nodes", func() {
			const (
				node2Name    = "node-2"
				machine2UUID = "machine-uuid-456"
			)
			endpoint := startCMServer(cmServerConfig{
				machineBodies: map[string][]byte{
					machineUUID: machineDataJSON(gpuSpec("spec-gpu", model,
						gpuDevice("gpu-node1", addComplete, "", "res-node1", "0"),
					)),
					machine2UUID: machineDataJSON(gpuSpec("spec-gpu", model,
						gpuDevice("gpu-node2", addComplete, "", "res-node2", "0"),
					)),
				},
			})
			objects := append(clusterObjects(nodeName, machineUUID),
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: node2Name,
						Annotations: map[string]string{
							"machine.openshift.io/machine": "openshift-machine-api/machine-2",
						},
					},
				},
				newTestMachine("machine-2", map[string]string{
					"metal3.io/BareMetalHost": "openshift-machine-api/bmh-2",
				}),
				&metal3v1alpha1.BareMetalHost{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "bmh-2",
						Namespace: "openshift-machine-api",
						Annotations: map[string]string{
							"cluster-manager.cdi.io/machine": machine2UUID,
						},
					},
				},
			)
			ftiClient := newFTIClient(endpoint, objects...)

			resources, err := ftiClient.GetResources()

			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(ConsistOf(
				cdi.DeviceInfo{
					NodeName:    nodeName,
					MachineUUID: machineUUID,
					DeviceType:  "gpu",
					DeviceID:    "gpu-node1",
					CDIDeviceID: "res-node1",
				},
				cdi.DeviceInfo{
					NodeName:    node2Name,
					MachineUUID: machine2UUID,
					DeviceType:  "gpu",
					DeviceID:    "gpu-node2",
					CDIDeviceID: "res-node2",
				},
			))
		})
	})
	Describe("getNodeMachineID", func() {
		It("gets the machine ID for a node", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			Expect(metal3v1alpha1.AddToScheme(scheme)).To(Succeed())

			fti_client := &FTIClient{
				ctx: ctx,
				client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(
						&corev1.Node{
							ObjectMeta: metav1.ObjectMeta{
								Name: "node-1",
								Annotations: map[string]string{
									"machine.openshift.io/machine": "openshift-machine-api/machine-1",
								},
							},
						},
						newTestMachine("machine-1", map[string]string{
							"metal3.io/BareMetalHost": "openshift-machine-api/bmh-1",
						}),
						&metal3v1alpha1.BareMetalHost{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "bmh-1",
								Namespace: "openshift-machine-api",
								Annotations: map[string]string{
									"cluster-manager.cdi.io/machine": "machine-uuid-123",
								},
							},
						},
					).
					Build(),
			}
			machineID, err := fti_client.getNodeMachineID("node-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(machineID).To(Equal("machine-uuid-123"))
		})
		It("returns an error when the node does not exist", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			fti_client := &FTIClient{
				ctx:    ctx,
				client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}

			machineID, err := fti_client.getNodeMachineID("missing-node")

			Expect(machineID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
		It("returns an error when the node machine annotation is malformed", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						"machine.openshift.io/machine": "machine-1",
					},
				},
			}
			fti_client := &FTIClient{
				ctx: ctx,
				client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(node).
					Build(),
			}

			machineID, err := fti_client.getNodeMachineID("node-1")

			Expect(machineID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'machine.openshift.io/machine'"))
		})
		It("returns an error when the Machine does not exist", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/missing-machine",
					},
				},
			}
			fti_client := &FTIClient{
				ctx: ctx,
				client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(node).
					Build(),
			}

			machineID, err := fti_client.getNodeMachineID("node-1")

			Expect(machineID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
		It("returns an error when the Machine BareMetalHost annotation is missing", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-1",
					},
				},
			}
			machine := newTestMachine("machine-1", nil)
			fti_client := &FTIClient{
				ctx: ctx,
				client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(node, machine).
					Build(),
			}

			machineID, err := fti_client.getNodeMachineID("node-1")

			Expect(machineID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'metal3.io/BareMetalHost'"))
		})
		It("returns an error when the BareMetalHost machine annotation is missing", func() {
			Expect(corev1.AddToScheme(scheme)).To(Succeed())
			Expect(metal3v1alpha1.AddToScheme(scheme)).To(Succeed())
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						"machine.openshift.io/machine": "openshift-machine-api/machine-1",
					},
				},
			}
			machine := newTestMachine("machine-1", map[string]string{
				"metal3.io/BareMetalHost": "openshift-machine-api/bmh-1",
			})
			bmh := &metal3v1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bmh-1",
					Namespace: "openshift-machine-api",
				},
			}
			fti_client := &FTIClient{
				ctx: ctx,
				client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(node, machine, bmh).
					Build(),
			}

			machineID, err := fti_client.getNodeMachineID("node-1")

			Expect(machineID).To(BeEmpty())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get annotation 'cluster-manager.cdi.io/machine'"))
		})
	})
})
