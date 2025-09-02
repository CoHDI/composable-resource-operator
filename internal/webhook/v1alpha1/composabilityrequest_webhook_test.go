/**
 * (C) Copyright IBM Corp. 2025.
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

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
)

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

var baseComposabilityRequest = crov1alpha1.ComposabilityRequest{
	Spec: crov1alpha1.ComposabilityRequestSpec{
		Resource: crov1alpha1.ScalarResourceDetails{
			Type:             "gpu",
			Model:            "NVIDIA-A100-PCIE-80GB",
			Size:             2,
			ForceDetach:      false,
			AllocationPolicy: "samenode",
			TargetNode:       "",
			OtherSpec:        nil,
		},
	},
	Status: crov1alpha1.ComposabilityRequestStatus{
		State: "<change it>",
		Error: "",
		Resources: map[string]crov1alpha1.ScalarResourceStatus{
			composableResource0Name: {
				State:       "",
				DeviceID:    "",
				CDIDeviceID: "",
				NodeName:    worker0Name,
				Error:       "",
			},
			composableResource1Name: {
				State:       "",
				DeviceID:    "",
				CDIDeviceID: "",
				NodeName:    worker0Name,
				Error:       "",
			},
		},
		ScalarResource: crov1alpha1.ScalarResourceDetails{
			Type:             "gpu",
			Model:            "NVIDIA-A100-PCIE-80GB",
			Size:             2,
			ForceDetach:      false,
			AllocationPolicy: "samenode",
			TargetNode:       "",
			OtherSpec:        nil,
		},
	},
}

func cleanAllComposabilityRequests() {
	requestList := &crov1alpha1.ComposabilityRequestList{}
	Expect(k8sClient.List(ctx, requestList)).To(Succeed())

	for i := range requestList.Items {
		obj := &requestList.Items[i]
		Expect(k8sClient.Delete(ctx, obj)).To(Or(BeNil(), WithTransform(k8serrors.IsNotFound, BeTrue())))
	}
}

var _ = Describe("ComposabilityRequest Webhook", func() {
	Context("When creating ComposabilityRequest under Validating Webhook", Ordered, func() {
		It("", func() {
			composabilityRequest1 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request1"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.TargetNode = worker1Name
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest1)).NotTo(HaveOccurred())

			composabilityRequest6 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request6"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest6)).NotTo(HaveOccurred())

			composabilityRequest0 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request0"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					return spec
				}(),
			}
			err := k8sClient.Create(ctx, composabilityRequest0)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: composabilityRequest resource test-composability-request6 with type gpu and model NVIDIA-A100-PCIE-80GB already exists"))

			composabilityRequest6.Status = *baseComposabilityRequest.Status.DeepCopy()
			Expect(k8sClient.Status().Update(ctx, composabilityRequest6)).NotTo(HaveOccurred())

			composabilityRequest4 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request4"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.AllocationPolicy = "differentnode"
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest4)).NotTo(HaveOccurred())

			composabilityRequest2 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request2"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.TargetNode = worker0Name
					return spec
				}(),
			}
			err = k8sClient.Create(ctx, composabilityRequest2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: composabilityRequest resource test-composability-request6 with type gpu and model NVIDIA-A100-PCIE-80GB already exists"))

			composabilityRequest3 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request3"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.AllocationPolicy = "differentnode"
					spec.Resource.TargetNode = worker0Name
					return spec
				}(),
			}
			err = k8sClient.Create(ctx, composabilityRequest3)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: TargetNode cannot be specified when AllocationPolicy is set to 'differentnode'"))

			composabilityRequest5 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request5"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.AllocationPolicy = "differentnode"
					return spec
				}(),
			}
			err = k8sClient.Create(ctx, composabilityRequest5)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: composabilityRequest resource test-composability-request4 with type gpu and model NVIDIA-A100-PCIE-80GB already exists"))
		})

		AfterAll(func() {
			cleanAllComposabilityRequests()
		})
	})

	Context("When updating ComposabilityRequest under Validating Webhook", Ordered, func() {
		It("", func() {
			composabilityRequest1 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request1"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.TargetNode = worker1Name
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest1)).NotTo(HaveOccurred())

			composabilityRequest0 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request0"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest0)).NotTo(HaveOccurred())
			composabilityRequest0.Status = *baseComposabilityRequest.Status.DeepCopy()
			Expect(k8sClient.Status().Update(ctx, composabilityRequest0)).NotTo(HaveOccurred())

			composabilityRequest4 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request4"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.AllocationPolicy = "differentnode"
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest4)).NotTo(HaveOccurred())

			composabilityRequest2 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request2"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.TargetNode = worker7Name
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest2)).NotTo(HaveOccurred())
			composabilityRequest2.Spec.Resource.TargetNode = worker0Name
			err := k8sClient.Update(ctx, composabilityRequest2)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: composabilityRequest resource test-composability-request0 with type gpu and model NVIDIA-A100-PCIE-80GB already exists"))

			composabilityRequest3 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request3"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.AllocationPolicy = "samenode"
					spec.Resource.TargetNode = worker5Name
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest3)).NotTo(HaveOccurred())
			composabilityRequest3.Spec.Resource.AllocationPolicy = "differentnode"
			err = k8sClient.Update(ctx, composabilityRequest3)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: TargetNode cannot be specified when AllocationPolicy is set to 'differentnode'"))

			composabilityRequest5 := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "test-composability-request5"},
				Spec: *func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.Model = "NVIDIA-H100-PCIE-80GB"
					spec.Resource.AllocationPolicy = "differentnode"
					return spec
				}(),
			}
			Expect(k8sClient.Create(ctx, composabilityRequest5)).NotTo(HaveOccurred())
			composabilityRequest5.Spec.Resource.Model = "NVIDIA-A100-PCIE-80GB"
			err = k8sClient.Create(ctx, composabilityRequest5)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("admission webhook \"vcomposabilityrequest.kb.io\" denied the request: composabilityRequest resource test-composability-request4 with type gpu and model NVIDIA-A100-PCIE-80GB already exists"))
		})

		AfterAll(func() {
			cleanAllComposabilityRequests()
		})
	})
})
