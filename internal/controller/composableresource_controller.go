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

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
	"github.com/IBM/composable-resource-operator/internal/utils"
)

// ComposableResourceReconciler reconciles a ComposableResource object
type ComposableResourceReconciler struct {
	client.Client
	Clientset  *kubernetes.Clientset
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
}

var composableResourceLog = ctrl.Log.WithName("composable_resource_controller")

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
// +kubebuilder:rbac:groups=machine.openshift.io,resources=machines,verbs=get;list;watch
// +kubebuilder:rbac:groups=machine.openshift.io,resources=machines/status,verbs=get
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts,verbs=get;list;watch
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get,resourceNames=credentials
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=resource.k8s.io,resources=resourceslices/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ComposableResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	composableResourceLog.Info("start reconciling", "request", req.NamespacedName)

	composableResource := &crov1alpha1.ComposableResource{}
	if err := r.Get(ctx, req.NamespacedName, composableResource); err != nil {
		if k8serrors.IsNotFound(err) {
			composableResourceLog.Error(err, "failed to get composableResource", "request", req.NamespacedName)
			return r.doNotRequeue()
		}
		return r.requeueOnErr(err, "failed to get composableResource", "request", req.NamespacedName)
	}

	adapter, err := NewComposableResourceAdapter(ctx, r.Client, r.Clientset)
	if err != nil {
		return r.requeueOnErr(err, "failed to create ComposableResource Adapter", "request", req.NamespacedName)
	}

	var result ctrl.Result
	switch composableResource.Status.State {
	case "":
		result, err = r.handleNoneState(ctx, composableResource)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle None state", "composableResource", composableResource.Name)
		}
	case "Attaching":
		result, err = r.handleAttachingState(ctx, composableResource, adapter)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Attaching state", "composableResource", composableResource.Name)
		}
	case "Online":
		result, err = r.handleOnlineState(ctx, composableResource, adapter)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Online state", "composableResource", composableResource.Name)
		}
	case "Cleaning":
		result, err = r.handleCleaningState(ctx, composableResource)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Cleaning state", "composableResource", composableResource.Name)
		}
	case "Detaching":
		result, err = r.handleDetachingState(ctx, composableResource, adapter)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Detaching state", "composableResource", composableResource.Name)
		}
	case "Deleting":
		result, err = r.handleDeletingState(ctx, composableResource)
		if err != nil {
			return r.requeueOnErr(err, "failed to handle Deleting state", "composableResource", composableResource.Name)
		}
	}

	return result, nil
}

func (r *ComposableResourceReconciler) handleNoneState(ctx context.Context, resource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composableResourceLog.Info("start handling None state", "ComposableResource", resource.Name)

	if deviceID, ok := resource.Labels["cohdi.io/ready-to-detach-device-uuid"]; ok && deviceID != "" {
		composableResourceLog.Info("detected ready-to-detach-device-uuid label, entering Detaching state", "ComposableResource", resource.Name, "deviceID", deviceID)
		resource.Status.DeviceID = deviceID
		resource.Status.CDIDeviceID = deviceID
		resource.Status.State = "Detaching"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	if !controllerutil.ContainsFinalizer(resource, composabilityRequestFinalizer) {
		controllerutil.AddFinalizer(resource, composabilityRequestFinalizer)
		if err := r.Update(ctx, resource); err != nil {
			return r.requeueOnErr(err, "failed to update composableResource", "ComposableResource", resource.Name)
		}
	}

	resource.Status.State = "Attaching"
	return ctrl.Result{}, r.Status().Update(ctx, resource)
}

func (r *ComposableResourceReconciler) handleAttachingState(ctx context.Context, resource *crov1alpha1.ComposableResource, adapter *ComposableResourceAdapter) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Attaching state", "ComposableResource", resource.Name)

	if resource.DeletionTimestamp != nil {
		resource.Status.State = "Cleaning"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	deviceResourceType := os.Getenv("DEVICE_RESOURCE_TYPE")

	if resource.Status.DeviceID == "" {
		deviceID, CDIDeviceID, err := adapter.CDIProvider.AddResource(resource)
		if err != nil {
			if errors.Is(err, cdi.ErrWaitingDeviceAttaching) {
				// It takes time to add a device, so wait to requeue.
				composableResourceLog.Info("the device is being installed, please wait", "ComposableResource", resource.Name)
				return r.requeueAfter(30*time.Second, nil)
			}

			// Write the error message into .Status.Error to let user know.
			resource.Status.Error = err.Error()
			if err := r.Status().Update(ctx, resource); err != nil {
				return r.requeueOnErr(err, "failed to update composableResource", "composableResource", resource.Name)
			}
			return r.requeueOnErr(err, "failed to add resource", "composableResource", resource.Name)
		}

		composableResourceLog.Info("found an available deviceID", "deviceID", deviceID, "ComposableResource", resource.Name)

		resource.Status.Error = ""
		resource.Status.DeviceID = deviceID
		resource.Status.CDIDeviceID = CDIDeviceID
		if err = r.Status().Update(ctx, resource); err != nil {
			return r.requeueOnErr(err, "failed to update composableResource", "composableResource", resource.Name)
		}
	}

	if deviceResourceType == "DEVICE_PLUGIN" {
		if err := utils.CheckNoGPULoads(ctx, r.Client, r.Clientset, r.RestConfig, resource.Spec.TargetNode, nil); err != nil {
			composableResourceLog.Error(err, "failed to check gpu loads in TargetNode", "TargetNode", resource.Spec.TargetNode, "composableResource", resource.Name)
		}

		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-gpu-operator", "nvidia-device-plugin-daemonset"); err != nil {
			composableResourceLog.Error(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
		}
		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-gpu-operator", "nvidia-dcgm"); err != nil {
			composableResourceLog.Error(err, "failed to restart nvidia-dcgm", "composableResource", resource.Name)
		}
	} else if deviceResourceType == "DRA" {
		if err := utils.RunNvidiaSmi(ctx, r.Client, r.Clientset, r.RestConfig, resource.Spec.TargetNode); err != nil {
			composableResourceLog.Error(err, "failed to run nvidia-smi in nvidia-driver-daemonset pod", "composableResource", resource.Name)
		}
		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-dra-driver", "nvidia-dra-driver-gpu-kubelet-plugin"); err != nil {
			composableResourceLog.Error(err, "failed to restart nvidia-dra-driver-gpu-kubelet-plugin", "composableResource", resource.Name)
		}
	} else {
		err := fmt.Errorf("the env variable DEVICE_RESOURCE_TYPE has an invalid value: '%s'", deviceResourceType)
		return r.requeueOnErr(err, "failed to check the env variable DEVICE_RESOURCE_TYPE value", "composableResource", resource.Name)
	}

	visible, err := utils.CheckGPUVisible(ctx, r.Client, r.Clientset, r.RestConfig, deviceResourceType, resource)
	if err != nil {
		return r.requeueOnErr(err, "failed to check if the gpu has been recognized by cluster", "ComposableResource", resource.Name)
	}
	if visible {
		resource.Status.State = "Online"
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	} else {
		composableResourceLog.Info("waiting for the cluster to recognize the newly added device", "ComposableResource", resource.Name)
		return r.requeueAfter(30*time.Second, nil)
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
			return r.requeueOnErr(err, "failed to update ComposableResource", "ComposableResource", resource.Name)
		}
	}

	return r.requeueAfter(30*time.Second, nil)
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

	deviceResourceType := os.Getenv("DEVICE_RESOURCE_TYPE")
	if deviceResourceType != "DEVICE_PLUGIN" && deviceResourceType != "DRA" {
		err := fmt.Errorf("the env variable DEVICE_RESOURCE_TYPE has an invalid value: '%s'", deviceResourceType)
		return r.requeueOnErr(err, "failed to check the env variable DEVICE_RESOURCE_TYPE value", "composableResource", resource.Name)
	}

	if resource.Status.DeviceID != "" {
		// Make sure there is no load on the target GPU.
		if !resource.Spec.ForceDetach {
			var err error
			if deviceResourceType == "DEVICE_PLUGIN" {
				// When using DEVICE_PLUGIN, ensure that all GPUs on the target GPU's Node are not under load.
				err = utils.CheckNoGPULoads(ctx, r.Client, r.Clientset, r.RestConfig, resource.Spec.TargetNode, nil)
			} else {
				// When using DRA, only the target GPU needs to be free from load.
				err = utils.CheckNoGPULoads(ctx, r.Client, r.Clientset, r.RestConfig, resource.Spec.TargetNode, &resource.Status.DeviceID)
			}

			if err != nil {
				return r.requeueOnErr(err, "failed to check gpu loads in TargetNode", "TargetNode", resource.Spec.TargetNode, "composableResource", resource.Name)
			}
		}

		// Create a DeviceTaintRule to block the GPU from being re-scheduled.
		if err := utils.CreateDeviceTaint(ctx, r.Client, resource); err != nil {
			return r.requeueOnErr(err, "failed to create DeviceTaintRule", "composableResource", resource.Name)
		}

		// Use nvidia-smi to remove gpu from the target node.
		if err := utils.DrainGPU(ctx, r.Client, r.Clientset, r.RestConfig, resource.Spec.TargetNode, resource.Status.DeviceID, deviceResourceType); err != nil {
			return r.requeueOnErr(err, "failed to drain target gpu", "deviceID", resource.Status.DeviceID, "composableResource", resource.Name)
		}

		if err := adapter.CDIProvider.RemoveResource(resource); err != nil {
			if errors.Is(err, cdi.ErrWaitingDeviceDetaching) {
				// It takes time to remove a device, so wait to requeue.
				composableResourceLog.Info("the device is being removed, please wait", "ComposableResource", resource.Name)
				return r.requeueAfter(30*time.Second, nil)
			}

			// Write the error message into .Status.Error to let user know.
			resource.Status.Error = err.Error()
			if err := r.Status().Update(ctx, resource); err != nil {
				return r.requeueOnErr(err, "failed to update composableResource", "composableResource", resource.Name)
			}
			return r.requeueOnErr(err, "failed to remove resource", "composableResource", resource.Name)
		}

		composableResourceLog.Info("the device has been removed", "ComposableResource", resource.Name)

		resource.Status.Error = ""
		resource.Status.DeviceID = ""
		resource.Status.CDIDeviceID = ""
		if err := r.Status().Update(ctx, resource); err != nil {
			return r.requeueOnErr(err, "failed to update composableResource", "composableResource", resource.Name)
		}
	}

	if deviceResourceType == "DEVICE_PLUGIN" {
		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-gpu-operator", "nvidia-device-plugin-daemonset"); err != nil {
			return r.requeueOnErr(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
		}
		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-gpu-operator", "nvidia-dcgm"); err != nil {
			return r.requeueOnErr(err, "failed to restart nvidia-dcgm", "composableResource", resource.Name)
		}
	} else {
		// TODO: need to confirm the DRA's namespace.
		if err := utils.RestartDaemonset(ctx, r.Client, "nvidia-dra-driver", "nvidia-dra-driver-gpu-kubelet-plugin"); err != nil {
			return r.requeueOnErr(err, "failed to restart nvidia-device-plugin-daemonset", "composableResource", resource.Name)
		}
	}

	resource.Status.State = "Deleting"
	return ctrl.Result{}, r.Status().Update(ctx, resource)
}

func (r *ComposableResourceReconciler) handleDeletingState(ctx context.Context, resource *crov1alpha1.ComposableResource) (ctrl.Result, error) {
	composableResourceLog.Info("start handling Deleting state", "ComposableResource", resource.Name)

	needScheduleUpdate, err := utils.SetNodeSchedulable(ctx, r.Client, resource)
	if err != nil {
		return r.requeueOnErr(err, "failed to set node schedulable", "composableResource", resource.Name)
	}
	if needScheduleUpdate {
		return r.requeueAfter(30*time.Second, nil)
	}

	if controllerutil.ContainsFinalizer(resource, composabilityRequestFinalizer) {
		controllerutil.RemoveFinalizer(resource, composabilityRequestFinalizer)
	}

	if err := r.Update(ctx, resource); err != nil {
		return r.requeueOnErr(err, "failed to update composableResource", "composableResource", resource.Name)
	}

	return ctrl.Result{}, nil
}

func (r *ComposableResourceReconciler) requeueOnErr(err error, msg string, keysAndValues ...any) (ctrl.Result, error) {
	composableResourceLog.Error(err, msg, keysAndValues...)
	return ctrl.Result{}, err
}

func (r *ComposableResourceReconciler) requeueAfter(duration time.Duration, err error) (ctrl.Result, error) {
	return ctrl.Result{RequeueAfter: duration}, err
}

func (r *ComposableResourceReconciler) doNotRequeue() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ComposableResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&crov1alpha1.ComposableResource{}).
		Complete(r)
}
