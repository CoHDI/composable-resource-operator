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

package controller

import (
	"context"
	"errors"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
)

func createComposabilityRequest(
	composabilityRequestName string,
	baseComposabilityRequestSpec *crov1alpha1.ComposabilityRequestSpec,
	baseComposabilityRequestStatus *crov1alpha1.ComposabilityRequestStatus,
	targetState string,
) {
	composabilityRequestSpec := baseComposabilityRequestSpec.DeepCopy()
	composabilityRequestStatus := baseComposabilityRequestStatus.DeepCopy()

	composabilityRequest := &crov1alpha1.ComposabilityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: composabilityRequestName},
		Spec:       *composabilityRequestSpec,
	}

	Expect(k8sClient.Create(ctx, composabilityRequest)).NotTo(HaveOccurred())

	if targetState == "" {
		return
	} else {
		composabilityRequest.SetFinalizers([]string{composabilityRequestFinalizer})
		Expect(k8sClient.Update(ctx, composabilityRequest)).NotTo(HaveOccurred())

		if composabilityRequestStatus == nil {
			composabilityRequest.Status.State = targetState
			composabilityRequest.Status.ScalarResource = composabilityRequest.Spec.Resource
		} else {
			composabilityRequest.Status = *composabilityRequestStatus
			composabilityRequest.Status.State = targetState
		}

		Expect(k8sClient.Status().Update(ctx, composabilityRequest)).NotTo(HaveOccurred())
	}
}

func triggerComposabilityRequestReconcile(controllerReconciler *ComposabilityRequestReconciler, requestName string) (*crov1alpha1.ComposabilityRequest, error) {
	namespacedName := types.NamespacedName{Name: requestName}

	_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: namespacedName})

	composabilityRequest := &crov1alpha1.ComposabilityRequest{}
	getErr := k8sClient.Get(ctx, namespacedName, composabilityRequest)
	if k8serrors.IsNotFound(getErr) {
		return nil, err
	}

	return composabilityRequest, err
}

func deleteComposabilityRequest(composabilityRequestName string) {
	request := &crov1alpha1.ComposabilityRequest{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composabilityRequestName}, request)).NotTo(HaveOccurred())

	Expect(k8sClient.Delete(ctx, request)).NotTo(HaveOccurred())
}

func cleanComposabilityRequest(composabilityRequestName string) {
	request := &crov1alpha1.ComposabilityRequest{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: composabilityRequestName}, request); err != nil {
		Expect(k8serrors.IsNotFound(err)).To(BeTrue())
		return
	}

	request.SetFinalizers(nil)
	Expect(k8sClient.Update(ctx, request)).To(Succeed())

	if err := k8sClient.Get(ctx, types.NamespacedName{Name: composabilityRequestName}, request); err == nil {
		Expect(k8sClient.Delete(ctx, request)).NotTo(HaveOccurred())
	}
}

func createTempComposableResource(composabilityRequestName string, composableResourceName string, typeName string, modelName string, targetNode string, forceDetach bool) {
	composableResource := &crov1alpha1.ComposableResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: composableResourceName,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": composabilityRequestName,
			},
		},
		Spec: crov1alpha1.ComposableResourceSpec{
			Type:        typeName,
			Model:       modelName,
			TargetNode:  targetNode,
			ForceDetach: forceDetach,
		},
	}
	Expect(k8sClient.Create(ctx, composableResource)).To(Succeed())
}

func listComposableResources() *crov1alpha1.ComposableResourceList {
	composableResourceList := &crov1alpha1.ComposableResourceList{}

	listOpts := []client.ListOption{client.InNamespace("")}
	Expect(k8sClient.List(ctx, composableResourceList, listOpts...)).To(Succeed())

	return composableResourceList
}

func cleanAllComposabilityRequests() {
	requestList := &crov1alpha1.ComposabilityRequestList{}
	Expect(k8sClient.List(ctx, requestList)).To(Succeed())

	for i := range requestList.Items {
		obj := &requestList.Items[i]
		obj.SetFinalizers(nil)
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		Expect(k8sClient.Delete(ctx, obj)).To(Or(BeNil(), WithTransform(k8serrors.IsNotFound, BeTrue())))
	}
}

func cleanAllComposableResources() {
	composableResourceList := &crov1alpha1.ComposableResourceList{}
	listOpts := []client.ListOption{client.InNamespace("")}
	Expect(k8sClient.List(ctx, composableResourceList, listOpts...)).To(Succeed())

	for i := range composableResourceList.Items {
		obj := &composableResourceList.Items[i]
		obj.SetFinalizers(nil)
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		Expect(k8sClient.Delete(ctx, obj)).To(Or(BeNil(), WithTransform(k8serrors.IsNotFound, BeTrue())))
	}
}

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
		State: "",
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

var baseComposabilityRequestUsingDifferentNode = crov1alpha1.ComposabilityRequest{
	Spec: crov1alpha1.ComposabilityRequestSpec{
		Resource: crov1alpha1.ScalarResourceDetails{
			Type:             "gpu",
			Model:            "NVIDIA-A100-PCIE-80GB",
			Size:             2,
			ForceDetach:      false,
			AllocationPolicy: "differentnode",
			TargetNode:       "",
			OtherSpec:        nil,
		},
	},
	Status: crov1alpha1.ComposabilityRequestStatus{
		State: "",
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
				NodeName:    worker1Name,
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

func createBaseComposableResources(composabilityRequestName string) {
	// These need to be consistent with content in baseComposabilityRequest.Status.
	createTempComposableResource(
		composabilityRequestName,
		composableResource0Name,
		baseComposabilityRequest.Spec.Resource.Type,
		baseComposabilityRequest.Spec.Resource.Model,
		baseComposabilityRequest.Status.Resources[composableResource0Name].NodeName,
		baseComposabilityRequest.Spec.Resource.ForceDetach,
	)
	createTempComposableResource(
		composabilityRequestName,
		composableResource1Name,
		baseComposabilityRequest.Spec.Resource.Type,
		baseComposabilityRequest.Spec.Resource.Model,
		baseComposabilityRequest.Status.Resources[composableResource1Name].NodeName,
		baseComposabilityRequest.Spec.Resource.ForceDetach,
	)
}

func createBaseComposableResourcesUsingDifferentNode(composabilityRequestName string) {
	// These need to be consistent with content in baseComposabilityRequest.Status.
	createTempComposableResource(
		composabilityRequestName,
		composableResource0Name,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.Type,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.Model,
		baseComposabilityRequestUsingDifferentNode.Status.Resources[composableResource0Name].NodeName,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.ForceDetach,
	)
	createTempComposableResource(
		composabilityRequestName,
		composableResource1Name,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.Type,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.Model,
		baseComposabilityRequestUsingDifferentNode.Status.Resources[composableResource1Name].NodeName,
		baseComposabilityRequestUsingDifferentNode.Spec.Resource.ForceDetach,
	)
}

var _ = Describe("ComposabilityRequest Controller", Ordered, func() {
	var (
		clientSet            *kubernetes.Clientset
		controllerReconciler *ComposabilityRequestReconciler
	)

	BeforeAll(func() {
		var err error
		clientSet, err = kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())

		controllerReconciler = &ComposabilityRequestReconciler{
			Client:    k8sClient,
			Clientset: clientSet,
			Scheme:    k8sClient.Scheme(),
		}
	})

	Describe("When user provides the ComposabilityRequest with invalid values", func() {
		type testcase struct {
			requestName string
			requestSpec *crov1alpha1.ComposabilityRequestSpec

			expectedReconcileErrorMessage string
		}

		DescribeTable("", func(tc testcase) {
			composabilityRequest := &crov1alpha1.ComposabilityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: tc.requestName},
				Spec:       *tc.requestSpec,
			}

			err := k8sClient.Create(ctx, composabilityRequest)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(tc.expectedReconcileErrorMessage))
		},
			Entry("should fail when user provides an invalid .spec.resource.type", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Type = "UnknownType"
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.type: Unsupported value: \"UnknownType\": supported values: \"gpu\", \"cxlmemory\"",
			}),
			Entry("should fail when user provides an invalid .spec.resource.size", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Size = -1
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.size: Invalid value: -1: spec.resource.size in body should be greater than or equal to 0",
			}),
			Entry("should fail when user provides an invalid .spec.resource.allocation_policy", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "UnknownPolicy"
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.allocation_policy: Unsupported value: \"UnknownPolicy\": supported values: \"samenode\", \"differentnode\"",
			}),
			Entry("should fail when user provides an invalid .spec.resource.other_spec.milli_cpu", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         -1,
						Memory:           1,
						EphemeralStorage: 1,
						AllowedPodNumber: 1,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.other_spec.milli_cpu: Invalid value: -1: spec.resource.other_spec.milli_cpu in body should be greater than or equal to 0",
			}),
			Entry("should fail when user provides an invalid .spec.resource.other_spec.memory", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         1,
						Memory:           -1,
						EphemeralStorage: 1,
						AllowedPodNumber: 1,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.other_spec.memory: Invalid value: -1: spec.resource.other_spec.memory in body should be greater than or equal to 0",
			}),
			Entry("should fail when user provides an invalid .spec.resource.other_spec.ephemeral_storage", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         1,
						Memory:           1,
						EphemeralStorage: -1,
						AllowedPodNumber: 1,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.other_spec.ephemeral_storage: Invalid value: -1: spec.resource.other_spec.ephemeral_storage in body should be greater than or equal to 0",
			}),
			Entry("should fail when user provides an invalid .spec.resource.other_spec.allowed_pod_number", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         1,
						Memory:           1,
						EphemeralStorage: 1,
						AllowedPodNumber: -1,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileErrorMessage: "ComposabilityRequest.cro.hpsys.ibm.ie.com \"test-composability-request\" is invalid: spec.resource.other_spec.allowed_pod_number: Invalid value: -1: spec.resource.other_spec.allowed_pod_number in body should be greater than or equal to 0",
			}),
		)
	})

	Describe("When the ComposabilityRequest fails to update status", func() {
		type testcase struct {
			requestName        string
			requestSpec        *crov1alpha1.ComposabilityRequestSpec
			requestStatus      *crov1alpha1.ComposabilityRequestStatus
			requestStatusState string

			setErrorMode func()

			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, tc.requestStatusState)
			// Use Delete() to trigger r.Status().Update(ctx, request) in controller to facilitate error handling.
			deleteComposabilityRequest(tc.requestName)

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())

			_, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
			}

			DeferCleanup(func() {
				k8sClient.MockUpdate = nil
				k8sClient.MockStatusUpdate = nil

				cleanComposabilityRequest(tc.requestName)
			})
		},
			Entry("should wait when the request is invalid", testcase{
				requestName:   "unknown-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: nil,
			}),
			Entry("should fail when k8s client status update fails in handleNodeAllocatingState function", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "NodeAllocating",

				setErrorMode: func() {
					k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						return errors.New("status update fails")
					}
				},

				expectedReconcileError: errors.New("status update fails"),
			}),
			Entry("should return error when k8s client status update fails in handleUpdatingState function", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "Updating",

				setErrorMode: func() {
					k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						return errors.New("status update fails")
					}
				},

				expectedReconcileError: errors.New("status update fails"),
			}),
			Entry("should return error when k8s client status update fails in handleRunningState function", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "Running",

				setErrorMode: func() {
					k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						return errors.New("status update fails")
					}
				},

				expectedReconcileError: errors.New("status update fails"),
			}),
			Entry("should return error when k8s client status update fails in handleCleaningState function", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "Cleaning",

				setErrorMode: func() {
					k8sClient.MockStatusUpdate = func(original func(client.Object, ...client.SubResourceUpdateOption) error, ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						return errors.New("status update fails")
					}
				},

				expectedReconcileError: errors.New("status update fails"),
			}),
			Entry("should return error when k8s client status update fails in handleDeletingState function", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "Deleting",

				setErrorMode: func() {
					k8sClient.MockUpdate = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return errors.New("update fails")
					}
				},

				expectedReconcileError: errors.New("update fails"),
			}),
			Entry("should return error when the passed state is wrong", testcase{
				requestName:        "test-composability-request",
				requestSpec:        baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus:      baseComposabilityRequest.Status.DeepCopy(),
				requestStatusState: "unknown",

				expectedReconcileError: errors.New("the composabilityRequest state 'unknown' is invalid"),
			}),
		)
	})

	Describe("When the reconcile was triggered by a ComposableResource", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus
			resourceName  string

			extraHandling func(composabilityRequestName string, composableResourceName string)

			expectedRequestStatus  *crov1alpha1.ComposabilityRequestStatus
			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "Running")

			Expect(callFunction(tc.extraHandling, tc.requestName, tc.resourceName)).NotTo(HaveOccurred())

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: tc.resourceName}})

			composabilityRequest := &crov1alpha1.ComposabilityRequest{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tc.requestName}, composabilityRequest)).NotTo(HaveOccurred())

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.Status).To(Equal(*tc.expectedRequestStatus))
			}

			DeferCleanup(func() {
				cleanAllComposabilityRequests()
				cleanAllComposableResources()
			})
		},
			Entry("should successfully update .status.scalarResource in ComposabilityRequest", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					resource := baseComposabilityRequest.Status.Resources[composableResource0Name]
					resource.State = "Updating"

					status := baseComposabilityRequest.Status.DeepCopy()
					status.Resources[composableResource0Name] = resource
					return status
				}(),
				resourceName: composableResource0Name,

				extraHandling: func(composabilityRequestName string, composableResourceName string) {
					createTempComposableResource(
						composabilityRequestName,
						composableResourceName,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						baseComposabilityRequest.Status.Resources[composableResourceName].NodeName,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
				},

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Running"
					return status
				}(),
			}),
			Entry("should fail when the corresponding ComposabilityRequest does not exist", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					resource := baseComposabilityRequest.Status.Resources[composableResource0Name]
					resource.State = "Updating"

					status := baseComposabilityRequest.Status.DeepCopy()
					status.Resources[composableResource0Name] = resource
					return status
				}(),
				resourceName: composableResource0Name,

				extraHandling: func(composabilityRequestName string, composableResourceName string) {
					createTempComposableResource(
						"unknown-composability-request",
						composableResourceName,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						baseComposabilityRequest.Status.Resources[composableResourceName].NodeName,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
				},

				expectedReconcileError: &k8serrors.StatusError{
					ErrStatus: metav1.Status{
						Status:  metav1.StatusFailure,
						Message: "composabilityrequests.cro.hpsys.ibm.ie.com \"unknown-composability-request\" not found",
						Reason:  metav1.StatusReasonNotFound,
						Details: &metav1.StatusDetails{
							Name:              "unknown-composability-request",
							Group:             "cro.hpsys.ibm.ie.com",
							Kind:              "composabilityrequests",
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

	Describe("When the ComposabilityRequest is in None state", func() {
		type testcase struct {
			requestName string
			requestSpec *crov1alpha1.ComposabilityRequestSpec

			setErrorMode func()

			expectedRequestFinalizer []string
			expectedRequestStatus    *crov1alpha1.ComposabilityRequestStatus
			expectedReconcileError   error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, nil, "")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())

			composabilityRequest, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.GetFinalizers()).To(Equal(tc.expectedRequestFinalizer))
				Expect(composabilityRequest.Status).To(Equal(*tc.expectedRequestStatus))
			}

			DeferCleanup(func() {
				k8sClient.MockUpdate = nil

				cleanAllComposabilityRequests()
			})
		},
			Entry("successfully update the composabilityRequest's status to NodeAllocating", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),

				expectedRequestFinalizer: []string{composabilityRequestFinalizer},
				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					composabilityRequestStatus := baseComposabilityRequest.Status.DeepCopy()
					composabilityRequestStatus.State = "NodeAllocating"
					composabilityRequestStatus.Resources = nil
					return composabilityRequestStatus
				}(),
			}),
			Entry("should fail when k8s client update fails", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),

				setErrorMode: func() {
					k8sClient.MockUpdate = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return errors.New("update fails")
					}
				},

				expectedReconcileError: errors.New("update fails"),
			}),
		)
	})

	Describe("When the ComposabilityRequest is in NodeAllocating state", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus

			setErrorMode  func()
			extraHandling func(composabilityRequestName string)

			// Direct comparison is not possible because a ComposableResource's name contains a random UUID.
			expectedDeletedComposableResources []string
			expectedUsedComposableResources    []string
			expectedUsedNodes                  []string
			expectedReconcileError             error
		}

		BeforeAll(func() {
			nodes := []*corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker0Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("8"),
							corev1.ResourceMemory:           resource.MustParse("16Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("512Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker1Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("2"),
							corev1.ResourceMemory:           resource.MustParse("16Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("512Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker2Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("8"),
							corev1.ResourceMemory:           resource.MustParse("4Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("512Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker3Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("8"),
							corev1.ResourceMemory:           resource.MustParse("16Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("128Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker4Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("8"),
							corev1.ResourceMemory:           resource.MustParse("16Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("512Gi"),
							corev1.ResourcePods:             resource.MustParse("20"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker5Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("8"),
							corev1.ResourceMemory:           resource.MustParse("16Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("512Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker6Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("32"),
							corev1.ResourceMemory:           resource.MustParse("64Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("2048Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: worker7Name},
					Status: corev1.NodeStatus{
						Capacity: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("32"),
							corev1.ResourceMemory:           resource.MustParse("64Gi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("2048Gi"),
							corev1.ResourcePods:             resource.MustParse("100"),
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
		})

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "NodeAllocating")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling, tc.requestName)).NotTo(HaveOccurred())

			composabilityRequest, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.Status.State).To(Equal("Updating"))

				var usedComposableResourcesName []string
				var usedNodesName []string
				for resourceName, resource := range composabilityRequest.Status.Resources {
					usedComposableResourcesName = append(usedComposableResourcesName, resourceName)
					usedNodesName = append(usedNodesName, resource.NodeName)
				}

				if tc.expectedDeletedComposableResources != nil {
					for _, usedComposableResource := range usedComposableResourcesName {
						Expect(usedComposableResource).NotTo(BeElementOf(tc.expectedDeletedComposableResources))
					}
				}

				if tc.expectedUsedComposableResources != nil {
					for _, usedComposableResource := range usedComposableResourcesName {
						Expect(usedComposableResource).To(BeElementOf(tc.expectedUsedComposableResources))
					}
				}

				Expect(composabilityRequest.Status.Resources).To(HaveLen(len(tc.expectedUsedNodes)))
				Expect(usedNodesName).To(ConsistOf(tc.expectedUsedNodes))
			}

			DeferCleanup(func() {
				cleanAllComposabilityRequests()
				cleanAllComposableResources()
			})
		},
			Entry("should succeed with samenode     , with no        TargerNode, with no            OtherSpec", testcase{
				requestName:       "test-composability-request",
				requestSpec:       baseComposabilityRequest.Spec.DeepCopy(),
				expectedUsedNodes: []string{worker0Name, worker0Name},
			}),
			Entry("should succeed with samenode     , with no        TargerNode, with satisfiable   OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         32,
						Memory:           64 * 1024 * 1024 * 1024,
						EphemeralStorage: 2048 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),

				expectedUsedNodes: []string{worker6Name, worker6Name},
			}),
			Entry("should fail    with samenode     , with no        TargerNode, with unsatisfiable OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         64,
						Memory:           8 * 1024 * 1024 * 1024,
						EphemeralStorage: 256 * 1024 * 1024 * 1024,
						AllowedPodNumber: 50,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileError: errors.New("insufficient number of available nodes"),
			}),
			Entry("should succeed with samenode     , with existed   TargerNode, with no            OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.TargetNode = worker0Name
					return composabilityRequestSpec
				}(),

				expectedUsedNodes: []string{worker0Name, worker0Name},
			}),
			Entry("should succeed with samenode     , with existed   TargerNode, with satisfiable   OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.TargetNode = worker0Name
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         8,
						Memory:           16 * 1024 * 1024 * 1024,
						EphemeralStorage: 512 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),

				expectedUsedNodes: []string{worker0Name, worker0Name},
			}),
			Entry("should fail    with samenode     , with existed   TargerNode, with unsatisfiable OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.TargetNode = worker0Name
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         64,
						Memory:           16 * 1024 * 1024 * 1024,
						EphemeralStorage: 512 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileError: errors.New("TargetNode does not meet spec's requirements"),
			}),
			Entry("should fail    with samenode     , with unexisted TargerNode, with no            OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.TargetNode = "worker-unknown"
					return composabilityRequestSpec
				}(),

				expectedReconcileError: errors.New("the target node does not existed"),
			}),
			Entry("should succeed with differentnode, with no        TargerNode, with no            OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "differentnode"
					return composabilityRequestSpec
				}(),

				expectedUsedNodes: []string{worker0Name, worker1Name},
			}),
			Entry("should succeed with differentnode, with no        TargerNode, with satisfiable   OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "differentnode"
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         8,
						Memory:           16 * 1024 * 1024 * 1024,
						EphemeralStorage: 512 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),

				expectedUsedNodes: []string{worker0Name, worker5Name},
			}),
			Entry("should fail    with differentnode, with no        TargerNode, with unsatisfiable OtherSpec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "differentnode"
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         64,
						Memory:           16 * 1024 * 1024 * 1024,
						EphemeralStorage: 512 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),

				expectedReconcileError: errors.New("insufficient number of available nodes"),
			}),

			Entry("should succeed when user changes the type              with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Type = "cxlmemory"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker0Name},
			}),
			Entry("should succeed when user changes the model             with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Model = "NVIDIA-H100-PCIE-80GB"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker0Name},
			}),
			Entry("should succeed when user changes the size              with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Size = 3
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedUsedNodes: []string{worker0Name, worker0Name, worker0Name},
			}),
			Entry("should succeed when user changes the ForceDetach       with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.ForceDetach = true
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker0Name},
			}),
			Entry("should succeed when user changes the AllocationPolicy  with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "differentnode"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedUsedNodes: []string{worker0Name, worker1Name},
			}),
			Entry("should succeed when user changes the TargetNode        with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.TargetNode = worker1Name
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker1Name, worker1Name},
			}),
			Entry("should succeed when user changes the OtherSpec         with samenode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         32,
						Memory:           64 * 1024 * 1024 * 1024,
						EphemeralStorage: 2048 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker6Name, worker6Name},
			}),

			Entry("should succeed when user changes the type              with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Type = "cxlmemory"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker1Name},
			}),
			Entry("should succeed when user changes the model             with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Model = "NVIDIA-H100-PCIE-80GB"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker1Name},
			}),
			Entry("should succeed when user changes the size              with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Size = 3
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedUsedNodes: []string{worker0Name, worker1Name, worker2Name},
			}),
			Entry("should succeed when user changes the ForceDetach       with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.ForceDetach = true
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker0Name, worker1Name},
			}),
			Entry("should succeed when user changes the AllocationPolicy  with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.AllocationPolicy = "samenode"
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedUsedNodes: []string{worker0Name, worker0Name},
			}),
			Entry("should succeed when user changes the OtherSpec         with differentnode", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequestUsingDifferentNode.Spec.DeepCopy()
					composabilityRequestSpec.Resource.OtherSpec = &crov1alpha1.NodeSpec{
						MilliCPU:         32,
						Memory:           64 * 1024 * 1024 * 1024,
						EphemeralStorage: 2048 * 1024 * 1024 * 1024,
						AllowedPodNumber: 100,
					}
					return composabilityRequestSpec
				}(),
				requestStatus: baseComposabilityRequestUsingDifferentNode.Status.DeepCopy(),

				extraHandling: createBaseComposableResourcesUsingDifferentNode,

				expectedDeletedComposableResources: []string{composableResource0Name, composableResource1Name},
				expectedUsedNodes:                  []string{worker6Name, worker7Name},
			}),

			Entry("should succeed when user changes the size when there are extra ComposableResource CRs", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					composabilityRequestSpec := baseComposabilityRequest.Spec.DeepCopy()
					composabilityRequestSpec.Resource.Size = 1
					return composabilityRequestSpec
				}(),
				requestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					composabilityRequestStatus := baseComposabilityRequest.Status.DeepCopy()
					composabilityRequestStatus.ScalarResource.Size = 1

					composabilityRequestStatus.Resources[composableResource2Name] = crov1alpha1.ScalarResourceStatus{
						State:       "Online",
						DeviceID:    "",
						CDIDeviceID: "",
						NodeName:    worker0Name,
						Error:       "unknown error",
					}
					composabilityRequestStatus.Resources[composableResource3Name] = crov1alpha1.ScalarResourceStatus{
						State:       "Attaching",
						DeviceID:    "",
						CDIDeviceID: "",
						NodeName:    worker0Name,
						Error:       "",
					}
					composabilityRequestStatus.Resources[composableResource4Name] = crov1alpha1.ScalarResourceStatus{
						State:       "Attaching",
						DeviceID:    "",
						CDIDeviceID: "",
						NodeName:    worker0Name,
						Error:       "unknown error",
					}
					composabilityRequestStatus.Resources[composableResource5Name] = crov1alpha1.ScalarResourceStatus{
						State:       "",
						DeviceID:    "",
						CDIDeviceID: "",
						NodeName:    worker0Name,
						Error:       "",
					}
					composabilityRequestStatus.Resources[composableResource6Name] = crov1alpha1.ScalarResourceStatus{
						State:       "",
						DeviceID:    "",
						CDIDeviceID: "",
						NodeName:    worker0Name,
						Error:       "unknown error",
					}

					return composabilityRequestStatus
				}(),

				extraHandling: func(composabilityRequestName string) {
					createTempComposableResource(
						composabilityRequestName,
						composableResource0Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						baseComposabilityRequest.Status.Resources[composableResource0Name].NodeName,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource0 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource0Name}, resource0)).NotTo(HaveOccurred())
					resource0.SetAnnotations(map[string]string{
						"cohdi.io/delete-device":  "true",
						"cohdi.io/last-used-time": "2023-10-27T12:30:00Z",
					})
					Expect(k8sClient.Update(ctx, resource0)).NotTo(HaveOccurred())

					createTempComposableResource(
						composabilityRequestName,
						composableResource1Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						baseComposabilityRequest.Status.Resources[composableResource1Name].NodeName,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource1 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource1Name}, resource1)).NotTo(HaveOccurred())
					resource1.SetAnnotations(map[string]string{
						"cohdi.io/delete-device":  "true",
						"cohdi.io/last-used-time": "2023-10-27T10:30:00Z",
					})
					Expect(k8sClient.Update(ctx, resource1)).NotTo(HaveOccurred())

					createTempComposableResource(
						composabilityRequestName,
						composableResource2Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						worker0Name,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource2 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource2Name}, resource2)).NotTo(HaveOccurred())
					resource2.SetAnnotations(map[string]string{
						"cohdi.io/delete-device":  "true",
						"cohdi.io/last-used-time": "error-RFC3339",
					})
					Expect(k8sClient.Update(ctx, resource2)).NotTo(HaveOccurred())
					resource2.Status.State = "Online"
					Expect(k8sClient.Status().Update(ctx, resource2)).NotTo(HaveOccurred())

					createTempComposableResource(
						composabilityRequestName,
						composableResource3Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						worker0Name,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource3 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource3Name}, resource3)).NotTo(HaveOccurred())
					resource3.Status.State = "Attaching"
					Expect(k8sClient.Status().Update(ctx, resource3)).NotTo(HaveOccurred())

					createTempComposableResource(
						composabilityRequestName,
						composableResource4Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						worker0Name,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource4 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource4Name}, resource4)).NotTo(HaveOccurred())
					resource4.Status.State = "Attaching"
					resource4.Status.DeviceID = "GPU-device00-test-test-0000-000000000000"
					resource4.Status.CDIDeviceID = "GPU-device00-test-test-0000-000000000000"
					Expect(k8sClient.Status().Update(ctx, resource4)).NotTo(HaveOccurred())

					deleteConfirmAnnoation := make(map[string]string)
					deleteConfirmAnnoation["cohdi.io/delete-device"] = "true"
					deleteConfirmAnnoation["cohdi.io/last-used-time"] = "2023-10-27T10:30:00Z"

					createTempComposableResource(
						composabilityRequestName,
						composableResource5Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						worker0Name,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource5 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource5Name}, resource5)).NotTo(HaveOccurred())
					resource5.Status.State = "Online"
					Expect(k8sClient.Status().Update(ctx, resource5)).NotTo(HaveOccurred())

					createTempComposableResource(
						composabilityRequestName,
						composableResource6Name,
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						worker0Name,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
					resource6 := &crov1alpha1.ComposableResource{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{Name: composableResource6Name}, resource6)).NotTo(HaveOccurred())
					resource6.Status.Error = "unknown error"
					Expect(k8sClient.Status().Update(ctx, resource6)).NotTo(HaveOccurred())
					resource6.SetAnnotations(deleteConfirmAnnoation)
					Expect(k8sClient.Update(ctx, resource6)).NotTo(HaveOccurred())
				},

				expectedUsedComposableResources: []string{composableResource0Name},
				expectedUsedNodes:               []string{worker0Name},
			}),
			Entry("should succeed when there are ComposabilityRequests existed", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),

				extraHandling: func(composabilityRequestName string) {
					request1 := baseComposabilityRequest.Spec.DeepCopy()
					request1.Resource.TargetNode = worker1Name
					createComposabilityRequest("composability-request-existed-1", request1, nil, "NodeAllocating")

					request2 := baseComposabilityRequest.Spec.DeepCopy()
					status2 := baseComposabilityRequest.Status.DeepCopy()
					createComposabilityRequest("composability-request-existed-2", request2, status2, "NodeAllocating")
				},

				expectedUsedNodes: []string{worker2Name, worker2Name},
			}),
		)

		AfterAll(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
		})
	})

	Describe("When the ComposabilityRequest is in Updating state", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus

			setErrorMode  func()
			extraHandling func(composabilityRequestName string)

			expectedRequestStatus  *crov1alpha1.ComposabilityRequestStatus
			expectedResourceItems  []crov1alpha1.ComposableResource
			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "Updating")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling, tc.requestName)).NotTo(HaveOccurred())

			composabilityRequest, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.Status).To(Equal(*tc.expectedRequestStatus))

				composableResourceList := listComposableResources()
				for _, composableResource := range composableResourceList.Items {
					Expect(tc.expectedResourceItems).To(ContainElement(
						gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
							// Since the name of ComposableResource generated by handleUpdatingState function contains a random UUID, it cannot be checked here.
							"Spec":   Equal(composableResource.Spec),
							"Status": Equal(composableResource.Status),
						}),
					))
				}
			}

			DeferCleanup(func() {
				cleanComposabilityRequest(tc.requestName)
				cleanAllComposableResources()
			})
		},
			Entry("should succeed when there are no ComposableResource CRs that need to be deleted", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Updating"
					resource0ToUpdate := status.Resources[composableResource0Name]
					resource1ToUpdate := status.Resources[composableResource1Name]
					resource0ToUpdate.State = "Online"
					resource1ToUpdate.State = "Online"
					status.Resources[composableResource0Name] = resource0ToUpdate
					status.Resources[composableResource1Name] = resource1ToUpdate

					return status
				}(),

				extraHandling: func(composabilityRequestName string) {
					createBaseComposableResources(composabilityRequestName)
				},

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Running"
					resource0ToUpdate := status.Resources[composableResource0Name]
					resource1ToUpdate := status.Resources[composableResource1Name]
					resource0ToUpdate.State = "Online"
					resource1ToUpdate.State = "Online"
					status.Resources[composableResource0Name] = resource0ToUpdate
					status.Resources[composableResource1Name] = resource1ToUpdate

					return status
				}(),
				expectedResourceItems: []crov1alpha1.ComposableResource{
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
				},
			}),
			Entry("should wait when there are no ComposableResource CRs that need to be deleted", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Updating"
					return status
				}(),
				expectedResourceItems: []crov1alpha1.ComposableResource{
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
				},
			}),
			Entry("should wait when there are ComposableResource CRs that need to be deleted", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: func(composabilityRequestName string) {
					createBaseComposableResources(composabilityRequestName)

					createTempComposableResource(
						composabilityRequestName,
						"gpu-nvidia-a100-pcie-80gb-00000000-uuid-wait-tobe-deleted00000",
						baseComposabilityRequest.Spec.Resource.Type,
						baseComposabilityRequest.Spec.Resource.Model,
						baseComposabilityRequest.Spec.Resource.TargetNode,
						baseComposabilityRequest.Spec.Resource.ForceDetach,
					)
				},

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Updating"
					return status
				}(),
				expectedResourceItems: []crov1alpha1.ComposableResource{
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
					{
						Spec: crov1alpha1.ComposableResourceSpec{
							Type:        baseComposabilityRequest.Spec.Resource.Type,
							Model:       baseComposabilityRequest.Spec.Resource.Model,
							TargetNode:  worker0Name,
							ForceDetach: baseComposabilityRequest.Spec.Resource.ForceDetach,
						},
					},
				},
			}),
			Entry("should succeed when user changes the ComposabilityRequest Spec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.Size = 3
					return spec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "NodeAllocating"
					status.ScalarResource.Size = 3
					return status
				}(),
			}),
			Entry("should succeed when user deletes the ComposabilityRequest", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: deleteComposabilityRequest,

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Cleaning"
					return status
				}(),
			}),
		)
	})

	Describe("When the ComposabilityRequest is in Running state", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus

			setErrorMode  func()
			extraHandling func(composabilityRequestName string)

			expectedRequestStatus  *crov1alpha1.ComposabilityRequestStatus
			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "Running")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling, tc.requestName)).NotTo(HaveOccurred())

			composabilityRequest, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.Status).To(Equal(*tc.expectedRequestStatus))
			}

			DeferCleanup(func() {
				cleanComposabilityRequest(tc.requestName)
			})
		},
			Entry("should succeed", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Running"
					return status
				}(),
			}),
			Entry("should succeed when user changes the ComposabilityRequest Spec", testcase{
				requestName: "test-composability-request",
				requestSpec: func() *crov1alpha1.ComposabilityRequestSpec {
					spec := baseComposabilityRequest.Spec.DeepCopy()
					spec.Resource.Size = 3
					return spec
				}(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "NodeAllocating"
					status.ScalarResource.Size = 3
					return status
				}(),
			}),
			Entry("should succeed when user deletes the ComposabilityRequest", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: deleteComposabilityRequest,

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Cleaning"
					return status
				}(),
			}),
		)
	})

	Describe("When the ComposabilityRequest is in Cleaning state", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus

			setErrorMode  func()
			extraHandling func(composabilityRequestName string)

			expectedRequestStatus  *crov1alpha1.ComposabilityRequestStatus
			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "Cleaning")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())
			Expect(callFunction(tc.extraHandling, tc.requestName)).NotTo(HaveOccurred())

			deleteComposabilityRequest(tc.requestName)

			composabilityRequest, err := triggerComposabilityRequestReconcile(controllerReconciler, tc.requestName)

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(composabilityRequest.Status).To(Equal(*tc.expectedRequestStatus))

				composableResourceList := listComposableResources()
				Expect(composableResourceList.Items).To(HaveLen(0))
			}

			DeferCleanup(func() {
				cleanComposabilityRequest(tc.requestName)
			})
		},
			Entry("should succeed when ComposableResource CRs have not been completely deleted", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				extraHandling: createBaseComposableResources,

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Cleaning"
					return status
				}(),
			}),
			Entry("should succeed when ComposableResource CRs have been completely deleted", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),

				expectedRequestStatus: func() *crov1alpha1.ComposabilityRequestStatus {
					status := baseComposabilityRequest.Status.DeepCopy()
					status.State = "Deleting"
					return status
				}(),
			}),
		)
	})

	Describe("When the ComposabilityRequest is in Deleting state", func() {
		type testcase struct {
			requestName   string
			requestSpec   *crov1alpha1.ComposabilityRequestSpec
			requestStatus *crov1alpha1.ComposabilityRequestStatus

			setErrorMode func()

			expectedReconcileError error
		}

		DescribeTable("", func(tc testcase) {
			createComposabilityRequest(tc.requestName, tc.requestSpec, tc.requestStatus, "Deleting")

			Expect(callFunction(tc.setErrorMode)).NotTo(HaveOccurred())

			deleteComposabilityRequest(tc.requestName)

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: tc.requestName}})

			if tc.expectedReconcileError != nil {
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(tc.expectedReconcileError))
			} else {
				Expect(err).NotTo(HaveOccurred())

				requestList := &crov1alpha1.ComposableResourceList{}
				listOpts := []client.ListOption{client.InNamespace("")}
				Expect(k8sClient.List(ctx, requestList, listOpts...)).To(Succeed())
				Expect(requestList.Items).To(HaveLen(0))
			}

			DeferCleanup(func() {
				k8sClient.MockUpdate = nil
			})
		},
			Entry("should succeed", testcase{
				requestName:   "test-composability-request",
				requestSpec:   baseComposabilityRequest.Spec.DeepCopy(),
				requestStatus: baseComposabilityRequest.Status.DeepCopy(),
			}),
			Entry("should fail when k8s client update fails", testcase{
				requestName: "test-composability-request",
				requestSpec: baseComposabilityRequest.Spec.DeepCopy(),

				setErrorMode: func() {
					k8sClient.MockUpdate = func(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
						return errors.New("update fails")
					}
				},

				expectedReconcileError: errors.New("update fails"),
			}),
		)
	})
})
