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

package devicetaintrule

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestDeviceTaintRuleClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DeviceTaintRule Client Suite")
}

var testTaintSpec = TaintSpec{
	Driver: "nvidia",
	Pool:   "test1",
	Device: "gpu-0",
	Key:    "k8s.io/device-uuid",
	Value:  "device-uuid",
	Effect: "NoSchedule",
}

func testGVR(version string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

func newDynamicClient(version string) *fake.FakeDynamicClient {
	gvr := testGVR(version)
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvr: "DeviceTaintRuleList",
	})
}

func newTestClient() *Client {
	return &Client{dynamic: newDynamicClient(versionAlpha3), gvr: testGVR(versionAlpha3)}
}

func expectTaintObject(obj *unstructured.Unstructured, version, name string, spec TaintSpec) {
	Expect(obj.GetAPIVersion()).To(Equal(group + "/" + version))
	Expect(obj.GetKind()).To(Equal("DeviceTaintRule"))
	Expect(obj.GetName()).To(Equal(name))

	driver, found, err := unstructured.NestedString(obj.Object, "spec", "deviceSelector", "driver")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(driver).To(Equal(spec.Driver))

	pool, found, err := unstructured.NestedString(obj.Object, "spec", "deviceSelector", "pool")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(pool).To(Equal(spec.Pool))

	device, found, err := unstructured.NestedString(obj.Object, "spec", "deviceSelector", "device")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(device).To(Equal(spec.Device))

	key, found, err := unstructured.NestedString(obj.Object, "spec", "taint", "key")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(key).To(Equal(spec.Key))

	value, found, err := unstructured.NestedString(obj.Object, "spec", "taint", "value")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(value).To(Equal(spec.Value))

	effect, found, err := unstructured.NestedString(obj.Object, "spec", "taint", "effect")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	Expect(effect).To(Equal(spec.Effect))
}

type stubDiscovery struct {
	discovery.DiscoveryInterface
	lists map[string]*metav1.APIResourceList
	errs  map[string]error
}

func (s *stubDiscovery) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	if s.errs != nil {
		if err, ok := s.errs[gv]; ok {
			return nil, err
		}
	}
	if s.lists != nil {
		if list, ok := s.lists[gv]; ok {
			return list, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: group, Resource: "apis"}, gv)
}

func dtrAPIResourceList(version string) *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: group + "/" + version,
		APIResources: []metav1.APIResource{{
			Name: resource,
			Kind: "DeviceTaintRule",
		}},
	}
}

func otherAPIResourceList(version string) *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: group + "/" + version,
		APIResources: []metav1.APIResource{{
			Name: "resourceclaims",
			Kind: "ResourceClaim",
		}},
	}
}

var _ = Describe("buildObject", func() {
	It("builds an unstructured DeviceTaintRule for the requested API version", func() {
		obj := buildObject(testGVR(versionBeta2), "gpu-0-taint", testTaintSpec)
		expectTaintObject(obj, versionBeta2, "gpu-0-taint", testTaintSpec)
	})
})

var _ = Describe("New and preferredVersion", func() {
	DescribeTable("selects the first registered DeviceTaintRule version",
		func(lists map[string]*metav1.APIResourceList, errs map[string]error, expectedVersion string) {
			dyn := newDynamicClient(expectedVersion)
			c, err := New(dyn, &stubDiscovery{lists: lists, errs: errs})
			Expect(err).NotTo(HaveOccurred())
			Expect(c).NotTo(BeNil())
			Expect(c.Version()).To(Equal(expectedVersion))
			Expect(c.gvr).To(Equal(testGVR(expectedVersion)))
		},
		Entry("prefers v1 when it is registered",
			map[string]*metav1.APIResourceList{
				group + "/" + versionV1:     dtrAPIResourceList(versionV1),
				group + "/" + versionBeta2:  dtrAPIResourceList(versionBeta2),
				group + "/" + versionAlpha3: dtrAPIResourceList(versionAlpha3),
			},
			nil,
			versionV1,
		),
		Entry("selects v1beta2 when v1 is unavailable",
			map[string]*metav1.APIResourceList{
				group + "/" + versionBeta2:  dtrAPIResourceList(versionBeta2),
				group + "/" + versionAlpha3: dtrAPIResourceList(versionAlpha3),
			},
			nil,
			versionBeta2,
		),
		Entry("falls back to v1alpha3 when only it is registered",
			map[string]*metav1.APIResourceList{
				group + "/" + versionAlpha3: dtrAPIResourceList(versionAlpha3),
			},
			nil,
			versionAlpha3,
		),
		Entry("skips a group version that does not list DeviceTaintRule",
			map[string]*metav1.APIResourceList{
				group + "/" + versionV1:     otherAPIResourceList(versionV1),
				group + "/" + versionBeta2:  dtrAPIResourceList(versionBeta2),
				group + "/" + versionAlpha3: dtrAPIResourceList(versionAlpha3),
			},
			nil,
			versionBeta2,
		),
		Entry("skips a group version that returns a discovery error",
			map[string]*metav1.APIResourceList{
				group + "/" + versionAlpha3: dtrAPIResourceList(versionAlpha3),
			},
			map[string]error{
				group + "/" + versionV1:    fmt.Errorf("v1 unavailable"),
				group + "/" + versionBeta2: fmt.Errorf("v1beta2 unavailable"),
			},
			versionAlpha3,
		),
	)

	It("returns an error when no DeviceTaintRule API version is available", func() {
		c, err := New(newDynamicClient(versionAlpha3), &stubDiscovery{
			lists: map[string]*metav1.APIResourceList{
				group + "/" + versionV1: otherAPIResourceList(versionV1),
			},
			errs: map[string]error{
				group + "/" + versionBeta2:  fmt.Errorf("not found"),
				group + "/" + versionAlpha3: fmt.Errorf("not found"),
			},
		})
		Expect(c).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("devicetaintrule: version discovery failed"))
		Expect(err.Error()).To(ContainSubstring("none of resource.k8s.io/{v1,v1beta2,v1alpha3} available on this cluster"))
	})
})

var _ = Describe("Client.Version", func() {
	It("returns the GVR version selected for the client", func() {
		c := &Client{gvr: testGVR(versionV1)}
		Expect(c.Version()).To(Equal(versionV1))
	})
})

var _ = Describe("Client.Create", func() {
	var (
		ctx context.Context
		c   *Client
	)

	BeforeEach(func() {
		ctx = context.Background()
		c = newTestClient()
	})

	It("creates a DeviceTaintRule with the expected payload", func() {
		Expect(c.Create(ctx, "gpu-0-taint", testTaintSpec)).To(Succeed())

		created, err := c.dynamic.Resource(c.gvr).Get(ctx, "gpu-0-taint", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		expectTaintObject(created, versionAlpha3, "gpu-0-taint", testTaintSpec)
	})

	It("treats AlreadyExists as success and does not create or modify another object", func() {
		Expect(c.Create(ctx, "gpu-0-taint", testTaintSpec)).To(Succeed())

		created, err := c.dynamic.Resource(c.gvr).Get(ctx, "gpu-0-taint", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		originalSpec, found, err := unstructured.NestedMap(created.Object, "spec")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		originalUID := created.GetUID()
		originalResourceVersion := created.GetResourceVersion()

		Expect(c.Create(ctx, "gpu-0-taint", TaintSpec{
			Driver: "other-driver",
			Pool:   "other-pool",
			Device: "gpu-1",
			Key:    "k8s.io/device-uuid",
			Value:  "other-uuid",
			Effect: "NoExecute",
		})).To(Succeed())

		list, err := c.dynamic.Resource(c.gvr).List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(list.Items).To(HaveLen(1))
		Expect(list.Items[0].GetName()).To(Equal("gpu-0-taint"))

		got, err := c.dynamic.Resource(c.gvr).Get(ctx, "gpu-0-taint", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(got.GetUID()).To(Equal(originalUID))
		Expect(got.GetResourceVersion()).To(Equal(originalResourceVersion))
		gotSpec, found, err := unstructured.NestedMap(got.Object, "spec")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(gotSpec).To(Equal(originalSpec))
	})

	It("returns a wrapped error for unexpected create failures", func() {
		dyn := newDynamicClient(versionAlpha3)
		dyn.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("apiserver unavailable")
		})
		c = &Client{dynamic: dyn, gvr: testGVR(versionAlpha3)}

		err := c.Create(ctx, "gpu-0-taint", testTaintSpec)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`devicetaintrule: failed to create "gpu-0-taint"`))
		Expect(err.Error()).To(ContainSubstring("apiserver unavailable"))
	})
})

var _ = Describe("Client.Delete", func() {
	var (
		ctx context.Context
		c   *Client
	)

	BeforeEach(func() {
		ctx = context.Background()
		c = newTestClient()
	})

	It("deletes an existing DeviceTaintRule", func() {
		Expect(c.Create(ctx, "gpu-0-taint", testTaintSpec)).To(Succeed())

		Expect(c.Delete(ctx, "gpu-0-taint")).To(Succeed())

		_, err := c.dynamic.Resource(c.gvr).Get(ctx, "gpu-0-taint", metav1.GetOptions{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("treats NotFound as success", func() {
		Expect(c.Delete(ctx, "missing-taint")).To(Succeed())
	})

	It("returns a wrapped error for unexpected delete failures", func() {
		dyn := newDynamicClient(versionAlpha3)
		dyn.PrependReactor("delete", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("delete forbidden")
		})
		c = &Client{dynamic: dyn, gvr: testGVR(versionAlpha3)}

		err := c.Delete(ctx, "gpu-0-taint")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`devicetaintrule: failed to delete "gpu-0-taint"`))
		Expect(err.Error()).To(ContainSubstring("delete forbidden"))
	})
})

var _ = Describe("Client.Exists", func() {
	var (
		ctx context.Context
		c   *Client
	)

	BeforeEach(func() {
		ctx = context.Background()
		c = newTestClient()
	})

	It("returns true when the DeviceTaintRule exists", func() {
		Expect(c.Create(ctx, "gpu-0-taint", testTaintSpec)).To(Succeed())

		exists, err := c.Exists(ctx, "gpu-0-taint")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeTrue())
	})

	It("returns false when the DeviceTaintRule does not exist", func() {
		exists, err := c.Exists(ctx, "missing-taint")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())
	})

	It("returns the Get error when it is not NotFound", func() {
		dyn := newDynamicClient(versionAlpha3)
		dyn.PrependReactor("get", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("get failed")
		})
		c = &Client{dynamic: dyn, gvr: testGVR(versionAlpha3)}

		exists, err := c.Exists(ctx, "gpu-0-taint")
		Expect(exists).To(BeFalse())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("get failed"))
	})
})
