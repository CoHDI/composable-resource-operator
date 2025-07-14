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
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/google/go-cmp/cmp"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/utils"
)

const (
	composabilityRequestFinalizer            = "com.ie.ibm.hpsys/finalizer"
	composableResourceLastUsedTimeAnnotation = "infradds.io/last-used-time"
	composableResourceDeleteDeviceAnnotation = "infradds.io/delete-device"
)

var (
	composabilityRequestLog = ctrl.Log.WithName("composability_request_controller")
)

// ComposabilityRequestReconciler reconciles a ComposabilityRequest object
type ComposabilityRequestReconciler struct {
	client.Client
	Clientset *kubernetes.Clientset
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
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ComposabilityRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	composabilityRequestLog.Info("start reconciling", "request", req.NamespacedName)

	composabilityRequest := &crov1alpha1.ComposabilityRequest{}
	foundComposabilityRequest, err := r.tryGet(ctx, req.NamespacedName, composabilityRequest)
	if err != nil {
		return r.requeueOnErr(err, "failed to get composabilityRequest", "request", req.NamespacedName)
	}
	if foundComposabilityRequest {
		return r.handleComposabilityRequestChange(ctx, composabilityRequest)
	}

	composableResource := &crov1alpha1.ComposableResource{}
	foundComposableResource, err := r.tryGet(ctx, req.NamespacedName, composableResource)
	if err != nil {
		return r.requeueOnErr(err, "failed to get composableResource", "request", req.NamespacedName)
	}
	if foundComposableResource {
		return r.handleComposableResourceChange(ctx, composableResource)
	}

	notFoundErr := fmt.Errorf("could not find the resource: %s", req.NamespacedName)
	composabilityRequestLog.Error(notFoundErr, "failed to get resource", "request", req.NamespacedName)
	return r.doNotRequeue()
}

func (r *ComposabilityRequestReconciler) handleComposabilityRequestChange(ctx context.Context, composabilityRequest *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	var result ctrl.Result
	var err error

	switch composabilityRequest.Status.State {
	case "":
		result, err = r.handleNoneState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle None state", "composabilityRequest", composabilityRequest.Name)
		}
	case "NodeAllocating":
		result, err = r.handleNodeAllocatingState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle NodeAllocating state", "composabilityRequest", composabilityRequest.Name)
		}
	case "Updating":
		result, err = r.handleUpdatingState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Updating state", "composabilityRequest", composabilityRequest.Name)
		}
	case "Running":
		result, err = r.handleRunningState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Running state", "composabilityRequest", composabilityRequest.Name)
		}
	case "Cleaning":
		result, err = r.handleCleaningState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Cleaning state", "composabilityRequest", composabilityRequest.Name)
		}
	case "Deleting":
		result, err = r.handleDeletingState(ctx, composabilityRequest)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Deleting state", "composabilityRequest", composabilityRequest.Name)
		}
	default:
		err = fmt.Errorf("the composabilityRequest state '%s' is invalid", composabilityRequest.Status.State)
		return r.requeueOnErr(err, "failed to handle composabilityRequest state", "composabilityRequest", composabilityRequest.Name)
	}

	return result, nil
}

func (r *ComposabilityRequestReconciler) handleComposableResourceChange(ctx context.Context, composableResource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composabilityRequestName := composableResource.ObjectMeta.GetLabels()["app.kubernetes.io/managed-by"]
	composabilityRequest := &crov1alpha1.ComposabilityRequest{}
	if err := r.Get(ctx, types.NamespacedName{Name: composabilityRequestName}, composabilityRequest); err != nil {
		return r.requeueOnErr(err, "failed to get composabilityRequest", "composabilityRequest", composabilityRequestName)
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

	if !controllerutil.ContainsFinalizer(request, composabilityRequestFinalizer) {
		controllerutil.AddFinalizer(request, composabilityRequestFinalizer)
		if err := r.Update(ctx, request); err != nil {
			return r.requeueOnErr(err, "failed to add finalizer", "composabilityRequest", request.Name)
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

	// List ComposableResources managed by this ComposabilityRequest.
	composableResourceList := &crov1alpha1.ComposableResourceList{}
	if err := r.List(ctx, composableResourceList, client.MatchingLabels{"app.kubernetes.io/managed-by": request.Name}); err != nil {
		return r.requeueOnErr(err, "failed to list ComposableResources managed by this composabilityRequest", "composabilityRequest", request.Name)
	}

	// List ComposabilityRequests.
	composabilityRequestList := &crov1alpha1.ComposabilityRequestList{}
	if err := r.List(ctx, composabilityRequestList); err != nil {
		return r.requeueOnErr(err, "failed to list ComposabilityRequests", "composabilityRequest", request.Name)
	}

	// Get all nodes.
	nodes, err := utils.GetAllNodes(ctx, r.Client)
	if err != nil {
		return r.requeueOnErr(err, "failed to get all nodes", "composabilityRequest", request.Name)
	}

	resourcesToAllocate := request.Spec.Resource.Size
	resourcesToDelete := 0
	allocatedNodesForDifferentPolicy := make(map[string]bool)
	targetNodeForSamePolicy := ""

	for _, resource := range composableResourceList.Items {
		// Check ComposableResources and remove those that do not match this ComposabilityRequest from ComposabilityRequest.Status.Resources.
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
				isSufficient, err := utils.CheckNodeCapacitySufficient(ctx, r.Client, resource.Spec.TargetNode, request.Spec.Resource.OtherSpec)
				if err != nil {
					return r.requeueOnErr(err, "failed to check TargetNode capacity", "TargetNode", resource.Spec.TargetNode, "composabilityRequest", request.Name)
				}
				if !isSufficient {
					delete(request.Status.Resources, resource.Name)
					continue
				}
			}

			switch request.Spec.Resource.AllocationPolicy {
			case "differentnode":
				// Make sure that all TargetNodes in request.Status.Resources are not the same.
				if allocatedNodesForDifferentPolicy[resource.Spec.TargetNode] {
					delete(request.Status.Resources, resource.Name)
					continue
				} else {
					allocatedNodesForDifferentPolicy[resource.Spec.TargetNode] = true
				}
			case "samenode":
				// Make sure that all TargetNodes in request.Status.Resources are the same.
				if targetNodeForSamePolicy == "" {
					targetNodeForSamePolicy = resource.Spec.TargetNode
				} else {
					if targetNodeForSamePolicy != resource.Spec.TargetNode {
						delete(request.Status.Resources, resource.Name)
						continue
					}
				}
			}

			// If this ComposableResource passes all the above checks, it means that it meets this ComposabilityRequest's requirement and can be kept.
			resourcesToAllocate--
		} else {
			// The target size of ComposableResources has been met. Therefore, the size of remaining ComposableResources is being recorded for deletion.
			resourcesToDelete++
		}
	}

	composabilityRequestLog.Info("start removing ComposableResources according to the deletion priority", "composabilityRequest", request.Name)

	// Remove ComposableResources according to the deletion priority.
	if resourcesToDelete > 0 {
		type PrioritizedResources struct {
			name    string
			sortKey time.Time
		}
		resourcesByDeletionPriority := make([][]PrioritizedResources, 5)

		for _, composableResource := range composableResourceList.Items {
			var sortTime time.Time

			lastUsedTime := composableResource.Annotations[composableResourceLastUsedTimeAnnotation]
			parsedLastUsedTime, err := time.Parse(time.RFC3339, lastUsedTime)
			if err == nil {
				sortTime = parsedLastUsedTime
			} else {
				composabilityRequestLog.Info("failed to parse LastUsedTime Annotation, so use CreationTimestamp", "LastUsedTime", lastUsedTime, "ComposableResource", composableResource.Name)
				sortTime = composableResource.CreationTimestamp.Time
			}

			if composableResource.Status.State == "None" || (composableResource.Status.State == "Attaching" && composableResource.Status.DeviceID == "") {
				resourcesByDeletionPriority[0] = append(resourcesByDeletionPriority[0], PrioritizedResources{name: composableResource.Name, sortKey: sortTime})
			} else if composableResource.Status.State == "Online" && composableResource.ObjectMeta.GetAnnotations()[composableResourceDeleteDeviceAnnotation] == "true" {
				resourcesByDeletionPriority[1] = append(resourcesByDeletionPriority[1], PrioritizedResources{name: composableResource.Name, sortKey: sortTime})
			} else if composableResource.Status.State == "Attaching" {
				resourcesByDeletionPriority[2] = append(resourcesByDeletionPriority[2], PrioritizedResources{name: composableResource.Name, sortKey: sortTime})
			} else if composableResource.Status.State == "Online" {
				resourcesByDeletionPriority[3] = append(resourcesByDeletionPriority[3], PrioritizedResources{name: composableResource.Name, sortKey: sortTime})
			} else {
				resourcesByDeletionPriority[4] = append(resourcesByDeletionPriority[4], PrioritizedResources{name: composableResource.Name, sortKey: sortTime})
			}
		}

		for priorityLevel := range resourcesByDeletionPriority {
			sort.Slice(resourcesByDeletionPriority[priorityLevel], func(i, j int) bool {
				return resourcesByDeletionPriority[priorityLevel][i].sortKey.Before(resourcesByDeletionPriority[priorityLevel][j].sortKey)
			})
		}

	deleteResourcesByPriorityLoop:
		for i := 0; ; i++ {
			for _, resource := range resourcesByDeletionPriority[i] {
				delete(request.Status.Resources, resource.name)
				resourcesToDelete--

				if resourcesToDelete == 0 {
					break deleteResourcesByPriorityLoop
				}
			}
		}
	}

	composabilityRequestLog.Info("start allocating nodes based on the AllocationPolicy.", "composabilityRequest", request.Name)

	// Start allocating nodes based on the AllocationPolicy.
	allocatingNodes := []string{}
	if request.Spec.Resource.AllocationPolicy == "samenode" && request.Spec.Resource.TargetNode != "" {
		if err = utils.CheckNodeExisted(ctx, r.Client, request.Spec.Resource.TargetNode); err != nil {
			err := fmt.Errorf("the target node does not existed")
			return r.requeueOnErr(err, "failed to find the TargetNode", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
		}

		if request.Spec.Resource.OtherSpec != nil {
			result, err := utils.CheckNodeCapacitySufficient(ctx, r.Client, request.Spec.Resource.TargetNode, request.Spec.Resource.OtherSpec)
			if err != nil {
				return r.requeueOnErr(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
			}
			if !result {
				err = fmt.Errorf("TargetNode does not meet spec's requirements")
				return r.requeueOnErr(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
			}
		}

		for i := 0; i < int(resourcesToAllocate); i++ {
			allocatingNodes = append(allocatingNodes, request.Spec.Resource.TargetNode)
		}
	}
	if request.Spec.Resource.AllocationPolicy == "samenode" && request.Spec.Resource.TargetNode == "" {
		if len(request.Status.Resources) > 0 {
			// There are ComposableResources in this ComposabilityRequest, just use the TargetNode of them.
			for i := 0; i < int(resourcesToAllocate); i++ {
				allocatingNodes = append(allocatingNodes, targetNodeForSamePolicy)
			}
		} else {
			// Select a node that meets ComposabilityRequest's requirement as TargetNode.
		checkNodeLoop:
			for _, node := range nodes.Items {
				if request.Spec.Resource.OtherSpec != nil {
					result, err := utils.CheckNodeCapacitySufficient(ctx, r.Client, node.Name, request.Spec.Resource.OtherSpec)
					if err != nil {
						return r.requeueOnErr(err, "failed to check node capacity", "node", node.Name, "composabilityRequest", request.Name)
					}
					if !result {
						continue
					}
				}

				// Check if the node has been already occupied by other ComposabilityRequest.
				for _, req := range composabilityRequestList.Items {
					// Exclude the current reconcile's composabilityRequest from this composabilityRequestList.
					if req.Name == request.Name {
						continue
					}

					// Get the target node for this composabilityRequest.
					targetNode := ""
					if req.Spec.Resource.AllocationPolicy == "samenode" {
						if req.Spec.Resource.TargetNode == "" {
							// Select targetNode of the first ComposableResource in req.Status.Resources.
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
				return r.requeueOnErr(err, "failed to get available nodes", "composabilityRequest", request.Name)
			}
		}
	}
	if request.Spec.Resource.AllocationPolicy == "differentnode" {
		for _, node := range nodes.Items {
			if request.Spec.Resource.OtherSpec != nil {
				result, err := utils.CheckNodeCapacitySufficient(ctx, r.Client, node.Name, request.Spec.Resource.OtherSpec)
				if err != nil {
					return r.requeueOnErr(err, "failed to check TargetNode capacity", "TargetNode", request.Spec.Resource.TargetNode, "composabilityRequest", request.Name)
				}
				if !result {
					continue
				}
			}
			if !utils.ContainsString(allocatingNodes, node.Name) && !allocatedNodesForDifferentPolicy[node.Name] {
				allocatingNodes = append(allocatingNodes, node.Name)
			}
			if len(allocatingNodes) == int(resourcesToAllocate) {
				break
			}
		}

		if len(allocatingNodes) != int(resourcesToAllocate) {
			err := fmt.Errorf("insufficient number of available nodes")
			return r.requeueOnErr(err, "failed to get available nodes", "composabilityRequest", request.Name)
		}
	}
	// TODO: The current logic is that if the current env cannot meet the GPU requirements, it will continue to wait. It may needs to be modified.
	// https://docs.google.com/document/d/17Yqm7_z1QE0y4WHM1lL4Ja_RdcgRBR2KA19zgou_MSY/edit?disco=AAABUT77og0

	if request.Status.Resources == nil {
		request.Status.Resources = make(map[string]crov1alpha1.ScalarResourceStatus)
	}
	for i := 0; i < len(allocatingNodes); i++ {
		resourceName := utils.GenerateComposableResourceName(request.Spec.Resource.Type)
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

	if !reflect.DeepEqual(request.Status.ScalarResource, request.Spec.Resource) {
		request.Status.State = "NodeAllocating"
		request.Status.ScalarResource = request.Spec.Resource
		return ctrl.Result{}, r.Status().Update(ctx, request)
	}

	// List all ComposableResources managed by this ComposabilityRequest.
	composableResourceList := &crov1alpha1.ComposableResourceList{}
	if err := r.List(ctx, composableResourceList, client.MatchingLabels{"app.kubernetes.io/managed-by": request.Name}); err != nil {
		return r.requeueOnErr(err, "failed to list ComposableResource instances", "composabilityRequest", request.Name)
	}

	existedResources := map[string]bool{}

	// Delete redundant ComposableResources.
	for _, resource := range composableResourceList.Items {
		_, existed := request.Status.Resources[resource.Name]
		if !existed {
			if err := r.Delete(ctx, &resource); err != nil {
				return r.requeueOnErr(err, "failed to delete ComposableResource", "composabilityRequest", request.Name)
			}
		} else {
			existedResources[resource.Name] = true
		}
	}

	// Create missing ComposableResources.
	for resourceName, resource := range request.Status.Resources {
		if !existedResources[resourceName] {
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
				return r.requeueOnErr(err, "failed to create ComposableResource", "composabilityRequest", request.Name)
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
		composabilityRequestLog.Info("waiting for all ComposableResources to become online", "composabilityRequest", request.Name)
		return r.requeueAfter(30*time.Second, nil)
	}

}

func (r *ComposabilityRequestReconciler) handleRunningState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("Start handling Running state", "composabilityRequest", request.Name)

	if request.DeletionTimestamp != nil {
		request.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, request)
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

	return r.requeueAfter(30*time.Second, nil)
}

func (r *ComposabilityRequestReconciler) handleCleaningState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling Cleaning state", "composabilityRequest", request.Name)

	composableResourceList := &crov1alpha1.ComposableResourceList{}
	if err := r.List(ctx, composableResourceList, client.MatchingLabels{"app.kubernetes.io/managed-by": request.Name}); err != nil {
		return r.requeueOnErr(err, "failed to list ComposableResource", "composabilityRequest", request.Name)
	}

	if len(composableResourceList.Items) == 0 {
		request.Status.State = "Deleting"
		return ctrl.Result{}, r.Status().Update(ctx, request)
	} else {
		for _, resource := range composableResourceList.Items {
			if err := r.Delete(ctx, &resource); err != nil {
				return r.requeueOnErr(err, "failed to delete ComposableResource", "composabilityRequest", request.Name)
			}
		}
	}

	return r.requeueAfter(30*time.Second, nil)
}

func (r *ComposabilityRequestReconciler) handleDeletingState(ctx context.Context, request *crov1alpha1.ComposabilityRequest) (ctrl.Result, error) {
	composabilityRequestLog.Info("start handling Deleting state", "composabilityRequest", request.Name)

	if controllerutil.ContainsFinalizer(request, composabilityRequestFinalizer) {
		controllerutil.RemoveFinalizer(request, composabilityRequestFinalizer)
	}
	if err := r.Update(ctx, request); err != nil {
		return r.requeueOnErr(err, "failed to remove finalizer", "composabilityRequest", request.Name)
	}

	return ctrl.Result{}, nil
}

func (r *ComposabilityRequestReconciler) requeueOnErr(err error, msg string, keysAndValues ...any) (ctrl.Result, error) {
	composabilityRequestLog.Error(err, msg, keysAndValues...)
	return ctrl.Result{}, err
}

func (r *ComposabilityRequestReconciler) requeueAfter(duration time.Duration, err error) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: duration}, err
}

func (r *ComposabilityRequestReconciler) doNotRequeue() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func (r *ComposabilityRequestReconciler) tryGet(ctx context.Context, name types.NamespacedName, resource client.Object) (bool, error) {
	err := r.Get(ctx, name, resource)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
		Watches(
			&crov1alpha1.ComposableResource{},
			&handler.EnqueueRequestForObject{},
			builder.WithPredicates(resourceStatusUpdatePredicate()),
		).
		Complete(r)
}
