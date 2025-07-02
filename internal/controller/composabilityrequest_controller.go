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
	"reflect"
	"sort"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	util "github.com/IBM/composable-resource-operator/internal/utils"
)

const composabilityFinalizer = "com.ie.ibm.hpsys/finalizer"

var (
	composabilityRequestLog = ctrl.Log.WithName("composability_request_controller")
)

// ComposabilityRequestReconciler reconciles a ComposabilityRequest object
type ComposabilityRequestReconciler struct {
	client.Client
	ClientSet *kubernetes.Clientset
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composableresources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composableresources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ComposabilityRequest object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ComposabilityRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	composabilityRequestLog.Info("start reconciling", "request", req.NamespacedName)

	composabilityRequest := &crov1alpha1.ComposabilityRequest{}
	getRequestErr := r.Client.Get(ctx, req.NamespacedName, composabilityRequest)
	if getRequestErr != nil && !errors.IsNotFound(getRequestErr) {
		composabilityRequestLog.Error(getRequestErr, "failed to get composabilityRequest", "request", req.NamespacedName)
		return requeueOnErr(getRequestErr)
	}
	if getRequestErr == nil {
		return r.handleRequestChange(ctx, composabilityRequest)
	}

	composableResource := &crov1alpha1.ComposableResource{}
	getResourceErr := r.Client.Get(ctx, req.NamespacedName, composableResource)
	if getResourceErr != nil && !errors.IsNotFound(getResourceErr) {
		composabilityRequestLog.Error(getRequestErr, "failed to get composableResource", "request", req.NamespacedName)
		return requeueOnErr(getRequestErr)
	}
	if getResourceErr == nil {
		return r.handleComposableResourceChange(ctx, composableResource)
	}

	err := fmt.Errorf("could not find the resource: %s", req.NamespacedName)
	composabilityRequestLog.Error(err, "failed to get resource", "request", req.NamespacedName)
	return requeueOnErr(err)
}

func (r *ComposabilityRequestReconciler) handleRequestChange(ctx context.Context, composabilityRequest *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	var result ctrl.Result
	var err error
	switch composabilityRequest.Status.State {
	case "":
		result, err = r.handleNoneState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle None state")
			return requeueOnErr(err)
		}
	case "NodeAllocating":
		result, err = r.handleNodeAllocatingState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle NodeAllocating state")
			return requeueOnErr(err)
		}
	case "Updating":
		result, err = r.handleUpdatingState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle Updating state")
			return requeueOnErr(err)
		}
	case "Running":
		result, err = r.handleRunningState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle Running state")
			return requeueOnErr(err)
		}
	case "Cleaning":
		result, err = r.handleCleaningState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle Cleaning state")
			return requeueOnErr(err)
		}
	case "Deleting":
		result, err = r.handleDeletingState(ctx, composabilityRequest)
		if err != nil {
			composabilityRequestLog.Error(err, "failed to handle Deleting state")
			return requeueOnErr(err)
		}
	default:
		err = fmt.Errorf("the composabilityRequest state \"%s\" is invaild", composabilityRequest.Status.State)
		composabilityRequestLog.Error(err, "failed to handle composabilityRequest state")
		return requeueOnErr(err)
	}

	return result, nil
}

func (r *ComposabilityRequestReconciler) handleComposableResourceChange(ctx context.Context, composableResource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composabilityRequestName := composableResource.ObjectMeta.GetLabels()["app.kubernetes.io/managed-by"]
	namespacedName := types.NamespacedName{Namespace: "", Name: composabilityRequestName}
	composabilityRequest := &crov1alpha1.ComposabilityRequest{}
	if err := r.Get(ctx, namespacedName, composabilityRequest); err != nil {
		composabilityRequestLog.Error(err, "failed to get composabilityRequest", "composabilityRequest", namespacedName)
		return requeueOnErr(err)
	}

	// Synchronize changes in ComposableResource to ComposabilityRequest.Status.Resources.
	for name, resource := range composabilityRequest.Status.Resources {
		if name == composableResource.Name {
			resource.State = composableResource.Status.State
			resource.Error = composableResource.Status.Error
			resource.DeviceID = composableResource.Status.DeviceID
			resource.CDIDeviceID = composableResource.Status.CDIDeviceID
			composabilityRequest.Status.Resources[name] = resource
			break
		}
	}

	return ctrl.Result{}, r.Status().Update(ctx, composabilityRequest)
}

func (r *ComposabilityRequestReconciler) handleNoneState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling None state", "composabilityRequest", request.Name)

	if !controllerutil.ContainsFinalizer(request, composabilityFinalizer) {
		controllerutil.AddFinalizer(request, composabilityFinalizer)
		if err := r.Update(ctx, request); err != nil {
			composabilityRequestLog.Error(err, "failed to add finalizer", "composabilityRequest", request.Name)
			return requeueOnErr(err)
		}
	}

	request.Status.State = "NodeAllocating"
	request.Status.ScalarResource = request.Spec.Resource
	return ctrl.Result{}, r.Status().Update(ctx, request)
}

func (r *ComposabilityRequestReconciler) handleNodeAllocatingState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling NodeAllocating state", "composabilityRequest", request.Name)

	if request.DeletionTimestamp != nil {
		request.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, request)
	}

	// Get all ComposableResource CRs managed by this ComposabilityRequest.
	resourceList := &crov1alpha1.ComposableResourceList{}
	listOpts := []client.ListOption{
		client.MatchingLabels{
			"app.kubernetes.io/managed-by": request.Name,
		},
	}
	if err := r.List(ctx, resourceList, listOpts...); err != nil {
		composabilityRequestLog.Error(err, "failed to list relevant ComposableResource instances", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	// Get all ComposabilityRequest CRs.
	requestList := &crov1alpha1.ComposabilityRequestList{}
	if err := r.List(ctx, requestList); err != nil {
		composabilityRequestLog.Error(err, "failed to list ComposabilityRequest instances", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	resourcesToAllocate := request.Spec.Resource.Size
	resourcesToDelete := 0
	differentAllocatedNodes := make(map[string]bool)
	sameAllocatedNode := ""

	for _, resource := range resourceList.Items {
		// Check ComposableResource CRs and remove those that do not match this ComposabilityRequest from ComposabilityRequest.Status.Resources.
		if resourcesToAllocate > 0 {
			if resource.Spec.Type != request.Spec.Resource.Type || resource.Spec.Model != request.Spec.Resource.Model || resource.Spec.ForceDetach != request.Spec.Resource.ForceDetach {
				delete(request.Status.Resources, resource.Name)
				continue
			}

			if request.Spec.Resource.TargetNode != "" && resource.Spec.TargetNode != request.Spec.Resource.TargetNode {
				delete(request.Status.Resources, resource.Name)
				continue
			}

			if request.Spec.Resource.OtherSpec != nil {
				isSufficient, err := util.IsNodeCapacitySufficient(ctx, r.ClientSet, resource.Spec.TargetNode, request.Spec.Resource.OtherSpec)
				if err != nil {
					composabilityRequestLog.Error(err, "failed to check TargetNode capacity", "TargetNode", resource.Spec.TargetNode, "composabilityRequest", request.Name)
					return requeueOnErr(err)
				}

				if !isSufficient {
					delete(request.Status.Resources, resource.Name)
					continue
				}
			}

			switch request.Spec.Resource.AllocationPolicy {
			case "differentnode":
				// Make sure that all TargetNodes in request.Status.Resources are not the same.
				if differentAllocatedNodes[resource.Spec.TargetNode] {
					delete(request.Status.Resources, resource.Name)
					continue
				} else {
					differentAllocatedNodes[resource.Spec.TargetNode] = true
				}
			case "samenode":
				// Make sure that all TargetNodes in request.Status.Resources are the same.
				if sameAllocatedNode == "" {
					sameAllocatedNode = resource.Spec.TargetNode
				} else {
					if sameAllocatedNode != resource.Spec.TargetNode {
						delete(request.Status.Resources, resource.Name)
						continue
					}
				}
			}

			// If this ComposableResource CR passes all the above checks, it means that it meets the requirement of ComposabilityRequest CR and can be kept.
			resourcesToAllocate--
		} else {
			// ComposableResource CRs that need to be allocated have been met, so ready to delete extra ComposableResource CRs from request.Status.Resources.
			resourcesToDelete++
		}
	}

	type deleteResourceWithSortKey struct {
		name    string
		sortKey time.Time
	}

	// Remove extra devices according to priority.
	if resourcesToDelete > 0 {
		deleteResourcePriority := make([][]deleteResourceWithSortKey, 6)

		for _, resource := range resourceList.Items {
			var sortTime time.Time

			lastUsedTimeStr, annotationExists := resource.Annotations["infradds.io/last-used-time"]
			if annotationExists {
				parsedTime, parseErr := time.Parse(time.RFC3339, lastUsedTimeStr)
				if parseErr == nil {
					sortTime = parsedTime
				} else {
					composabilityRequestLog.Info("failed to parse Annotation 'infradds.io/last-used-time', use CreationTimestamp", "last-used-time", lastUsedTimeStr, "ComposableResource", resource.Name)
					sortTime = resource.CreationTimestamp.Time
				}
			} else {
				sortTime = resource.CreationTimestamp.Time
			}

			if resource.Status.State == "None" || (resource.Status.State == "Attaching" && resource.Status.DeviceID == "") {
				deleteResourcePriority[0] = append(deleteResourcePriority[0], deleteResourceWithSortKey{name: resource.Name, sortKey: sortTime})
			} else if resource.Status.State == "Online" && resource.ObjectMeta.GetAnnotations()["infradds.io/delete-device"] == "true" {
				deleteResourcePriority[1] = append(deleteResourcePriority[1], deleteResourceWithSortKey{name: resource.Name, sortKey: sortTime})
			} else if resource.Status.State == "Attaching" {
				deleteResourcePriority[2] = append(deleteResourcePriority[2], deleteResourceWithSortKey{name: resource.Name, sortKey: sortTime})
			} else if resource.Status.State == "Online" {
				deleteResourcePriority[3] = append(deleteResourcePriority[3], deleteResourceWithSortKey{name: resource.Name, sortKey: sortTime})
			} else {
				deleteResourcePriority[4] = append(deleteResourcePriority[4], deleteResourceWithSortKey{name: resource.Name, sortKey: sortTime})
			}
		}

		for level := range deleteResourcePriority {
			sort.Slice(deleteResourcePriority[level], func(i, j int) bool {
				return deleteResourcePriority[level][i].sortKey.Before(deleteResourcePriority[level][j].sortKey)
			})
		}

	deleteResourcePriorityLoop:
		for i := 0; ; i++ {
			for _, deleteResource := range deleteResourcePriority[i] {
				delete(request.Status.Resources, deleteResource.name)
				resourcesToDelete--

				if resourcesToDelete == 0 {
					break deleteResourcePriorityLoop
				}
			}
		}
	}

	allocatingNodes := []string{}
	nodes, err := util.GetAllNodes(ctx, r.ClientSet)
	if err != nil {
		composabilityRequestLog.Error(err, "failed to get all nodes", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	// Choose different allocating nodes according to different AllocationPolicy in ComposabilityRequest.Spec.Resource.AllocationPolicy.
	if request.Spec.Resource.AllocationPolicy == "samenode" && request.Spec.Resource.TargetNode != "" {
		if err = util.IsNodeExisted(ctx, r.ClientSet, request.Spec.Resource.TargetNode); err != nil {
			err := fmt.Errorf("the target node does not existed")
			composabilityRequestLog.Error(err, "failed to find the TargetNode", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
			return requeueOnErr(fmt.Errorf("the target node does not existed"))
		}

		if request.Spec.Resource.OtherSpec != nil {
			result, err := util.IsNodeCapacitySufficient(ctx, r.ClientSet, request.Spec.Resource.TargetNode, request.Spec.Resource.OtherSpec)
			if err != nil {
				composabilityRequestLog.Error(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
				return requeueOnErr(err)
			}
			if !result {
				err = fmt.Errorf("TargetNode does not meet spec's requirements")
				composabilityRequestLog.Error(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
				return requeueOnErr(err)
			}
		}

		for i := 0; i < int(resourcesToAllocate); i++ {
			allocatingNodes = append(allocatingNodes, request.Spec.Resource.TargetNode)
		}
	}
	if request.Spec.Resource.AllocationPolicy == "samenode" && request.Spec.Resource.TargetNode == "" {
		if len(request.Status.Resources) > 0 {
			// There are ComposableResources in the ComposabilityRequest, just use the TargetNode of them.
			for i := 0; i < int(resourcesToAllocate); i++ {
				allocatingNodes = append(allocatingNodes, sameAllocatedNode)
			}
		} else {
			// Select a node that meets ComposabilityRequest CR's requirement as TargetNode.
		checkNodeLoop:
			for _, node := range nodes.Items {
				if request.Spec.Resource.OtherSpec != nil {
					result, err := util.IsNodeCapacitySufficient(ctx, r.ClientSet, node.Name, request.Spec.Resource.OtherSpec)
					if err != nil {
						composabilityRequestLog.Error(err, "failed to check node capacity", "node", node.Name, "composabilityRequest", request.Name)
						return requeueOnErr(err)
					}

					if !result {
						continue
					}
				}

				// Check if the node is already occupied by other ComposabilityRequest.
				for _, req := range requestList.Items {
					if req.Name == request.Name {
						continue
					}

					targetNode := ""
					if req.Spec.Resource.AllocationPolicy == "samenode" {
						if req.Spec.Resource.TargetNode == "" {
							for _, v := range req.Status.Resources {
								targetNode = v.NodeName
								break
							}
						} else {
							targetNode = req.Spec.Resource.TargetNode
						}
					}

					if targetNode == node.Name {
						continue checkNodeLoop
					}
				}

				for i := 0; i < int(resourcesToAllocate); i++ {
					allocatingNodes = append(allocatingNodes, node.Name)
				}
				break
			}

			if len(allocatingNodes) != int(resourcesToAllocate) {
				err := fmt.Errorf("insufficient number of available nodes")
				composabilityRequestLog.Error(err, "failed to get available nodes", "composabilityRequest", request.Name)
				return requeueOnErr(fmt.Errorf("insufficient number of available nodes"))
			}
		}
	}
	if request.Spec.Resource.AllocationPolicy == "differentnode" {
		for _, node := range nodes.Items {
			if request.Spec.Resource.OtherSpec != nil {
				result, err := util.IsNodeCapacitySufficient(ctx, r.ClientSet, node.Name, request.Spec.Resource.OtherSpec)
				if err != nil {
					composabilityRequestLog.Error(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
					return requeueOnErr(err)
				}

				if !result {
					continue
				}
			}
			if !util.ContainsString(allocatingNodes, node.Name) && !differentAllocatedNodes[node.Name] {
				allocatingNodes = append(allocatingNodes, node.Name)
			}
			if len(allocatingNodes) == int(resourcesToAllocate) {
				break
			}
		}
		if len(allocatingNodes) != int(resourcesToAllocate) {
			return requeueOnErr(fmt.Errorf("insufficient number of available nodes"))
		}
	}
	// TODO: The current logic is that if the current environment cannot meet the GPU requirements, it will continue to wait. It may needs to be modified.
	// https://docs.google.com/document/d/17Yqm7_z1QE0y4WHM1lL4Ja_RdcgRBR2KA19zgou_MSY/edit?disco=AAABUT77og0

	if request.Status.Resources == nil {
		request.Status.Resources = make(map[string]crov1alpha1.ScalarResourceStatus)
	}
	for i := 0; i < len(allocatingNodes); i++ {
		resourceName := util.GenerateComposableResourceName(ctx, r.Client, request.Spec.Resource.Model, request.Spec.Resource.Type)
		request.Status.Resources[resourceName] = crov1alpha1.ScalarResourceStatus{
			NodeName: allocatingNodes[i],
		}
	}

	request.Status.State = "Updating"
	return ctrl.Result{}, r.Status().Update(ctx, request)
}

func (r *ComposabilityRequestReconciler) handleUpdatingState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling Updating state", "composabilityRequest", request.Name)

	if request.DeletionTimestamp != nil {
		request.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, request)
	}

	if !reflect.DeepEqual(request.Status.ScalarResource, crov1alpha1.ScalarResourceDetails{}) && !reflect.DeepEqual(request.Status.ScalarResource, request.Spec.Resource) {
		request.Status.State = "NodeAllocating"
		request.Status.ScalarResource = request.Spec.Resource
		return ctrl.Result{}, r.Status().Update(ctx, request)
	}

	// Get all ComposableResource CRs managed by this ComposabilityRequest CR
	resourceList := &crov1alpha1.ComposableResourceList{}
	listOpts := []client.ListOption{
		client.MatchingLabels{
			"app.kubernetes.io/managed-by": request.Name,
		},
	}
	if err := r.List(ctx, resourceList, listOpts...); err != nil {
		composabilityRequestLog.Error(err, "failed to list ComposableResource instances", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	excludedResources := map[string]bool{}

	// Delete redundant ComposableResource CRs.
	for _, resource := range resourceList.Items {
		_, ok := request.Status.Resources[resource.Name]
		if !ok {
			if err := r.Delete(ctx, &resource); err != nil {
				composabilityRequestLog.Error(err, "failed to delete ComposableResource", "composabilityRequest", request.Name)
				return requeueOnErr(err)
			}
		} else {
			excludedResources[resource.Name] = true
		}
	}

	// Create missing ComposableResource CRs.
	for resourceName, resource := range request.Status.Resources {
		if !excludedResources[resourceName] {
			composableResource := &crov1alpha1.ComposableResource{
				ObjectMeta: metav1.ObjectMeta{
					Name: resourceName,
					Labels: map[string]string{
						"app.kubernetes.io/managed-by": request.Name,
					},
				},
				Spec: crov1alpha1.ComposableResourceSpec{
					Type:        request.Spec.Resource.Type,
					Model:       request.Spec.Resource.Model,
					TargetNode:  resource.NodeName,
					ForceDetach: request.Spec.Resource.ForceDetach,
				},
			}
			if err := r.Create(ctx, composableResource); err != nil {
				composabilityRequestLog.Error(err, "failed to create ComposableResource", "composabilityRequest", request.Name)
				return requeueOnErr(err)
			}
		}
	}

	canRun := true
	for _, resource := range request.Status.Resources {
		if resource.State != "Online" {
			canRun = false
		}
	}

	if canRun {
		request.Status.State = "Running"
		return ctrl.Result{}, r.Status().Update(ctx, request)
	} else {
		composabilityRequestLog.Info("waiting for all ComposableResources to go online", "composabilityRequest", request.Name)
		return requeueAfter(20*time.Second, nil)
	}

}

func (r *ComposabilityRequestReconciler) handleRunningState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("Start handling Running state", "composabilityRequest", request.Name)

	if request.DeletionTimestamp != nil {
		request.Status.State = "Cleaning"
		return ctrl.Result{RequeueAfter: 10 * time.Second}, r.Status().Update(ctx, request)
	}

	if diff := cmp.Diff(request.Status.ScalarResource, request.Spec.Resource); diff != "" {
		composabilityRequestLog.Info("detected changes in spec, redoing NodeAllocating",
			"composabilityRequest", request.Name,
			"diff", diff,
		)
		request.Status.State = "NodeAllocating"
		request.Status.ScalarResource = request.Spec.Resource
		return ctrl.Result{}, r.Status().Update(ctx, request)
	}

	return requeueAfter(20*time.Second, nil)
}

func (r *ComposabilityRequestReconciler) handleCleaningState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling Cleaning state", "composabilityRequest", request.Name)

	resourceList := &crov1alpha1.ComposableResourceList{}
	listOpts := []client.ListOption{
		client.MatchingLabels{
			"app.kubernetes.io/managed-by": request.Name,
		},
	}
	if err := r.List(ctx, resourceList, listOpts...); err != nil {
		composabilityRequestLog.Error(err, "failed to list ComposableResource instances", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	if len(resourceList.Items) == 0 {
		if request.DeletionTimestamp != nil {
			request.Status.State = "Deleting"
			return ctrl.Result{}, r.Status().Update(ctx, request)
		}
	} else {
		for _, resource := range resourceList.Items {
			if err := r.Delete(ctx, &resource); err != nil {
				composabilityRequestLog.Error(err, "failed to delete ComposableResource", "composabilityRequest", request.Name)
				return requeueOnErr(err)
			}
		}
	}

	return requeueAfter(10*time.Second, nil)
}

func (r *ComposabilityRequestReconciler) handleDeletingState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling Deleting state", "composabilityRequest", request.Name)

	if controllerutil.ContainsFinalizer(request, composabilityFinalizer) {
		controllerutil.RemoveFinalizer(request, composabilityFinalizer)
	}
	if err := r.Update(ctx, request); err != nil {
		composabilityRequestLog.Error(err, "failed to remove finalizer", "composabilityRequest", request.Name)
		return requeueOnErr(err)
	}

	return ctrl.Result{}, nil
}

func requeueAfter(duration time.Duration, err error) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: duration}, err
}

func doNotRequeue() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func requeueOnErr(err error) (ctrl.Result, error) {
	return ctrl.Result{}, err
}

func resourceStatusUpdatePredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldResource, ok1 := e.ObjectOld.(*crov1alpha1.ComposableResource)
			newResource, ok2 := e.ObjectNew.(*crov1alpha1.ComposableResource)
			if ok1 && ok2 {
				return !reflect.DeepEqual(oldResource.Status, newResource.Status)
			}
			return false
		},
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ComposabilityRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&crov1alpha1.ComposabilityRequest{}).
		Watches(&crov1alpha1.ComposableResource{}, &handler.EnqueueRequestForObject{}, builder.WithPredicates(resourceStatusUpdatePredicate())).
		Complete(r)
}
