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
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
	util "github.com/IBM/composable-resource-operator/internal/utils"
)

// ComposableResourceReconciler reconciles a ComposableResource object
type ComposableResourceReconciler struct {
	client.Client
	ClientSet  *kubernetes.Clientset
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
}

var (
	composableResourceLog = ctrl.Log.WithName("composable_resource_controller")
)

// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composableresources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composableresources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composableresources/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=apps,resources=daemonsets/status,verbs=get
// +kububuilder:rbac:groups=machine.openshift.io,resources=machines,verbs=get;list;watch
// +kububuilder:rbac:groups=machine.openshift.io,resources=machines/status,verbs=get
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts/status,verbs=get
// +kubebuilder:rbac:groups=resource.k8s.io,resources=devicetaintrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=resource.k8s.io,resources=devicetaintrules/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get,resourceNames=credentials
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ComposableResource object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ComposableResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	composableResourceLog.Info("start reconciling", "request", req.NamespacedName)

	composableResource := &crov1alpha1.ComposableResource{}
	if err := r.Get(ctx, req.NamespacedName, composableResource); err != nil {
		composableResourceLog.Error(err, "failed to get composableResource", "request", req.NamespacedName)
		return requeueOnErr(err)
	}

	adapter, err := NewComposableResourceAdapter(nil, composableResourceLog, ctx, r.Client, r.ClientSet)
	if err != nil {
		composableResourceLog.Error(err, "failed to create ComposableResource Adapter", "request", req.NamespacedName)
		return requeueOnErr(err)
	}

	var result ctrl.Result
	switch composableResource.Status.State {
	case "":
		result, err = r.handleNoneState(ctx, composableResource)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle None state")
			return requeueOnErr(err)
		}
	case "Attaching":
		result, err = r.handleAttachingState(ctx, composableResource, adapter)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle Attaching state")
			return requeueOnErr(err)
		}
	case "Online":
		result, err = r.handleOnlineState(ctx, composableResource, adapter)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle Online state")
			return requeueOnErr(err)
		}
	case "Cleaning":
		result, err = r.handleCleaningState(ctx, composableResource)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle Cleaning state")
			return requeueOnErr(err)
		}
	case "Detaching":
		result, err = r.handleDetachingState(ctx, composableResource, adapter)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle Detaching state")
			return requeueOnErr(err)
		}
	case "Deleting":
		result, err = r.handleDeletingState(ctx, composableResource)
		if err != nil {
			composableResourceLog.Error(err, "failed to handle Deleting state")
			return requeueOnErr(err)
		}
	}

	return result, nil
}

func (r *ComposableResourceReconciler) handleNoneState(ctx context.Context, resource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composableResourceLog.Info("start handling None state", "ComposableResource", resource.Name)

	if !controllerutil.ContainsFinalizer(resource, composabilityFinalizer) {
		controllerutil.AddFinalizer(resource, composabilityFinalizer)
		if err := r.Update(ctx, resource); err != nil {
			composableResourceLog.Error(err, "failed to update composableResource", "ComposableResource", resource.Name)
			return requeueOnErr(err)
		}
	}

	resource.Status.State = "Attaching"
	return ctrl.Result{}, r.Status().Update(ctx, resource)
}

func (r *ComposableResourceReconciler) handleAttachingState(ctx context.Context, resource *crov1alpha1.ComposableResource, adapter *ComposableResourceAdapter) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Attaching state", "ComposableResource", resource.Name)

	if resource.DeletionTimestamp != nil && (resource.Status.DeviceID != "" || resource.Status.Error != "") {
		resource.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	resourceType := os.Getenv("DEVICE_RESOURCE_TYPE")

	if resource.Status.DeviceID == "" {
		deviceID, CDIDeviceID, err := adapter.CDIProvider.AddResource(resource)
		if err != nil {
			if errors.Is(err, cdi.ErrWaitingDeviceAttaching) {
				// It takes time to add a device, so wait to requeue.
				composableResourceLog.Info("the device is being installed, please wait", "ComposableResource", resource.Name)
				return requeueAfter(30*time.Second, nil)
			}
			// Write the error message into .Status.Error to let user know.
			resource.Status.Error = err.Error()
			if err := r.Status().Update(ctx, resource); err != nil {
				composableResourceLog.Error(err, "failed to update composableResource", "composableResource", resource.Name)
				return requeueOnErr(err)
			}

			return requeueOnErr(err)
		}

		composableResourceLog.Info("found an available deviceID", "deviceID", deviceID, "ComposableResource", resource.Name)

		resource.Status.Error = ""
		resource.Status.DeviceID = deviceID
		resource.Status.CDIDeviceID = CDIDeviceID
		if err = r.Status().Update(ctx, resource); err != nil {
			composableResourceLog.Error(err, "failed to update composableResource", "composableResource", resource.Name)
			return requeueOnErr(err)
		}

		if resourceType == "DEVICE_PLUGIN" {
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-gpu-operator", "nvidia-device-plugin-daemonset"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-gpu-operator", "nvidia-dcgm"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-dcgm", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
		} else if resourceType == "DRA" {
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-dra-driver", "nvidia-dra-driver-gpu-kubelet-plugin"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
		} else {
			return requeueOnErr(fmt.Errorf("DEVICE_RESOURCE_TYPE variable not set properly"))
		}
	}

	visible, err := util.IsGpusVisible(ctx, r.ClientSet, resourceType, resource)
	if err != nil {
		composableResourceLog.Error(err, "failed to check if the gpu has been recognized by cluster", "ComposableResource", resource.Name)
		return requeueOnErr(err)
	}
	if visible {
		resource.Status.State = "Online"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	} else {
		composableResourceLog.Info("waiting for the cluster to recognize the newly added device", "ComposableResource", resource.Name)
		return requeueAfter(10*time.Second, nil)
	}
}

func (r *ComposableResourceReconciler) handleOnlineState(ctx context.Context, resource *crov1alpha1.ComposableResource, adapter *ComposableResourceAdapter) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Online state", "ComposableResource", resource.Name)

	if resource.DeletionTimestamp != nil {
		resource.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	// Check if there are any error messages in CDI system for this ComposableResource.
	if err := adapter.CDIProvider.CheckResource(resource); err != nil {
		resource.Status.Error = err.Error()

		if err := r.Status().Update(ctx, resource); err != nil {
			composableResourceLog.Error(err, "failed to update ComposableResource", "ComposableResource", resource.Name)
			return requeueOnErr(err)
		}
	}

	return requeueAfter(10*time.Second, nil)
}

func (r *ComposableResourceReconciler) handleCleaningState(ctx context.Context, resource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Cleaning state", "ComposableResource", resource.Name)

	if resource.Status.DeviceID != "" {
		resource.Status.State = "Detaching"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	resource.Status.State = "Deleting"
	return ctrl.Result{}, r.Status().Update(ctx, resource)
}

func (r *ComposableResourceReconciler) handleDetachingState(ctx context.Context, resource *crov1alpha1.ComposableResource, adapter *ComposableResourceAdapter) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Detaching state", "ComposableResource", resource.Name)

	resourceType := os.Getenv("DEVICE_RESOURCE_TYPE")
	if resourceType != "DEVICE_PLUGIN" && resourceType != "DRA" {
		err := fmt.Errorf("DEVICE_RESOURCE_TYPE variable not set properly, now is '%s'", resourceType)
		composableResourceLog.Error(err, "failed to read DEVICE_RESOURCE_TYPE", "composableResource", resource.Name)
		return requeueOnErr(err)
	}

	if resource.Status.DeviceID != "" {
		// Make sure there is no load on the target GPU.
		if !resource.Spec.ForceDetach {
			var hasLoads bool
			var err error
			if resourceType == "DEVICE_PLUGIN" {
				hasLoads, err = util.IfGPUHasLoads(ctx, r.ClientSet, r.RestConfig, resource.Spec.TargetNode, nil)
			} else if resourceType == "DRA" {
				hasLoads, err = util.IfGPUHasLoads(ctx, r.ClientSet, r.RestConfig, resource.Spec.TargetNode, &resource.Status.DeviceID)
			}

			if err != nil {
				composableResourceLog.Error(err, "failed to check gpu loads in TargetNode", "TargetNode", resource.Spec.TargetNode, "composableResource", resource.Name)
				return requeueOnErr(err)
			}

			if hasLoads {
				err := fmt.Errorf("there are gpu loads on the target, please check")
				composableResourceLog.Error(err, "failed to detach device", "TargetNode", resource.Spec.TargetNode, "ComposableResource", resource.Name)
				return requeueOnErr(err)
			}
		}

		if err := util.CreateGPUTaintRule(ctx, r.Client, resource); err != nil {
			composableResourceLog.Error(err, "failed to create DeviceTaintRule", "composableResource", resource.Name)
			return requeueOnErr(err)
		}

		// Use nvidia-smi to remove gpu from the target node.
		if err := util.DrainGPU(ctx, r.ClientSet, r.RestConfig, resource.Spec.TargetNode, resource.Status.DeviceID, resourceType); err != nil {
			composableResourceLog.Error(err, "failed to drain target gpu", "deviceID", resource.Status.DeviceID, "composableResource", resource.Name)
			return requeueOnErr(err)
		}

		if err := adapter.CDIProvider.RemoveResource(resource); err != nil {
			if errors.Is(err, cdi.ErrWaitingDeviceDetaching) {
				//It takes time to remove a device, so wait to requeue.
				composableResourceLog.Info("the device is being removed, please wait", "ComposableResource", resource.Name)
				return requeueAfter(30*time.Second, nil)
			}
			return requeueOnErr(err)
		}

		composableResourceLog.Info("the device has been removed", "ComposableResource", resource.Name)

		if resourceType == "DEVICE_PLUGIN" {
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-gpu-operator", "nvidia-device-plugin-daemonset"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-gpu-operator", "nvidia-dcgm"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-dcgm", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
		} else if resourceType == "DRA" {
			// TODO: need to confirm the DRA's namespace.
			if err := util.RestartDaemonset(r.ClientSet, ctx, "nvidia-dra-driver", "nvidia-dra-driver-gpu-kubelet-plugin"); err != nil {
				composableResourceLog.Error(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
				return requeueOnErr(err)
			}
		}

		if err := util.DeleteGPUTaintRule(ctx, r.Client, resource); err != nil {
			composableResourceLog.Error(err, "failed to delete DeviceTaintRule", "composableResource", resource.Name)
			return requeueOnErr(err)
		}

		resource.Status.DeviceID = ""
		resource.Status.CDIDeviceID = ""
		if err := r.Status().Update(ctx, resource); err != nil {
			composableResourceLog.Error(err, "failed to update composableResource", "composableResource", resource.Name)
			return requeueOnErr(err)
		}
	}

	resource.Status.State = "Deleting"
	return ctrl.Result{}, r.Status().Update(ctx, resource)
}

func (r *ComposableResourceReconciler) handleDeletingState(ctx context.Context, resource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Deleting state", "ComposableResource", resource.Name)

	needScheduleUpdate, err := util.SetNodeSchedulable(ctx, r.ClientSet, resource)
	if err != nil {
		composableResourceLog.Error(err, "failed to set node schedulable", "composableResource", resource.Name)
		return requeueOnErr(err)
	}
	if needScheduleUpdate {
		return requeueAfter(10*time.Second, nil)
	}

	if controllerutil.ContainsFinalizer(resource, composabilityFinalizer) {
		controllerutil.RemoveFinalizer(resource, composabilityFinalizer)
	}

	if err := r.Update(ctx, resource); err != nil {
		composableResourceLog.Error(err, "failed to update composableResource", "composableResource", resource.Name)
		return requeueOnErr(err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ComposableResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&crov1alpha1.ComposableResource{}).
		Complete(r)
}
