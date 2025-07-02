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
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	metal3v1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	machinev1beta1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	worker0Name = "worker-0"
	worker1Name = "worker-1"
	worker2Name = "worker-2"
	worker3Name = "worker-3"
	worker4Name = "worker-4"
	worker5Name = "worker-5"
	worker6Name = "worker-6"
	worker7Name = "worker-7"
)
var (
	composableResource0Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000000"
	composableResource1Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000001"
	composableResource2Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000002"
	composableResource3Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000003"
	composableResource4Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000004"
	composableResource5Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000005"
	composableResource6Name = "gpu-nvidia-a100-pcie-80gb-00000000-temp-uuid-0000-000000000006"
)

type MyClient struct {
	client.Client
	MockGet          func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	MockUpdate       func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error
	MockStatusUpdate func(originalUpdate func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error
}

func (m *MyClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if m.MockGet != nil {
		return m.MockGet(ctx, key, obj, opts...)
	}
	return m.Client.Get(ctx, key, obj, opts...)
}

func (m *MyClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if m.MockUpdate != nil {
		return m.MockUpdate(ctx, obj, opts...)
	}
	return m.Client.Update(ctx, obj, opts...)
}

type myStatusWriter struct {
	client.StatusWriter
	mockStatusUpdate func(originalUpdate func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error
}

func (m *MyClient) Status() client.StatusWriter {
	return &myStatusWriter{
		StatusWriter:     m.Client.Status(),
		mockStatusUpdate: m.MockStatusUpdate,
	}
}

func (m *myStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if m.mockStatusUpdate != nil {
		original := func(o client.Object, opt ...client.SubResourceUpdateOption) error {
			return m.StatusWriter.Update(ctx, o, opt...)
		}

		return m.mockStatusUpdate(original, ctx, obj, opts...)
	}
	return m.StatusWriter.Update(ctx, obj, opts...)
}

type MockExecutor struct {
	StreamWithContextFunc func(context.Context, remotecommand.StreamOptions) error
	StreamFunc            func(remotecommand.StreamOptions) error
}

func (m *MockExecutor) StreamWithContext(ctx context.Context, opts remotecommand.StreamOptions) error {
	return m.StreamWithContextFunc(ctx, opts)
}

func (m *MockExecutor) Stream(opts remotecommand.StreamOptions) error {
	return m.StreamFunc(opts)
}

func callFunction(fn interface{}, args ...interface{}) error {
	v := reflect.ValueOf(fn)

	if v.IsNil() {
		return nil
	}

	if v.Kind() != reflect.Func {
		return fmt.Errorf("non-function passed, got %T", fn)
	}

	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		in[i] = reflect.ValueOf(arg)
	}
	v.Call(in)

	return nil
}

var ctx context.Context
var cancel context.CancelFunc
var testEnv *envtest.Environment
var cfg *rest.Config
var k8sClient *MyClient

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	format.MaxLength = 1000
	format.TruncatedDiff = false

	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	goModCache := filepath.Join(homedir.HomeDir(), "go", "pkg", "mod")
	machineCRDPath := filepath.Join(goModCache, "github.com", "metal3-io", "cluster-api-provider-metal3@v1.9.3", "config", "crd", "bases")
	bmhCRDPath := filepath.Join(goModCache, "github.com", "metal3-io", "baremetal-operator@v0.9.1", "config", "base", "crds", "bases")

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
			machineCRDPath,
			bmhCRDPath,
		},
		ErrorIfCRDPathMissing: true,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.33.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
		ControlPlane: envtest.ControlPlane{
			APIServer: &envtest.APIServer{
				Args: []string{
					"--runtime-config=resource.k8s.io/v1alpha3=true",
					"--feature-gates=DynamicResourceAllocation=true",
				},
			},
		},
	}

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	s := scheme.Scheme
	Expect(crov1alpha1.AddToScheme(s)).NotTo(HaveOccurred())
	Expect(machinev1beta1.AddToScheme(s)).NotTo(HaveOccurred())
	Expect(metal3v1alpha1.AddToScheme(s)).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	tempClient, err := client.New(cfg, client.Options{Scheme: s})
	Expect(err).NotTo(HaveOccurred())
	Expect(tempClient).NotTo(BeNil())
	k8sClient = &MyClient{
		Client: tempClient,
	}
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
