package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
	"github.com/IBM/composable-resource-operator/internal/utils"
)

var upstreamSyncerLog = ctrl.Log.WithName("upstream_syncer_controller")

const missingDeviceGracePeriod = 10 * time.Minute

type UpstreamSyncerReconciler struct {
	client.Client
	ClientSet *kubernetes.Clientset
	Scheme    *runtime.Scheme

	missingDevices map[string]time.Time
}

func (r *UpstreamSyncerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.missingDevices = make(map[string]time.Time)

	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		upstreamSyncerLog.Info("start upstream data synchronization goroutine")

		adapter, err := NewComposableResourceAdapter(ctx, r.Client, r.ClientSet)
		if err != nil {
			upstreamSyncerLog.Error(err, "failed to create ComposableResource Adapter")
			return err
		}

		syncInterval := 1 * time.Minute
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				upstreamSyncerLog.Info("start scheduled upstream data synchronization")
				if err := r.syncUpstreamData(ctx, adapter); err != nil {
					upstreamSyncerLog.Error(err, "failed to run scheduled upstream data synchronization")
				}
			case <-ctx.Done():
				upstreamSyncerLog.Info("stopping upstream data synchronization goroutine")
				return nil
			}
		}
	}))
}

func (r *UpstreamSyncerReconciler) syncUpstreamData(ctx context.Context, adapter *ComposableResourceAdapter) error {
	deviceInfoList, err := adapter.CDIProvider.GetResources()
	if err != nil {
		return fmt.Errorf("failed to fetch data from upstream server: %w", err)
	}

	composableResourceList := &crov1alpha1.ComposableResourceList{}
	if err := r.Client.List(ctx, composableResourceList); err != nil {
		return fmt.Errorf("failed to list ComposableResources: %w", err)
	}

	existingDeviceIDs := make(map[string]struct{})
	for _, cr := range composableResourceList.Items {
		if cr.Status.DeviceID != "" {
			existingDeviceIDs[cr.Status.DeviceID] = struct{}{}
		}
	}

	for _, deviceInfo := range deviceInfoList {
		deviceID := deviceInfo.DeviceID
		if _, exists := existingDeviceIDs[deviceID]; exists {
			if _, tracking := r.missingDevices[deviceID]; tracking {
				upstreamSyncerLog.Info("ComposableResource has been created for a previously missing device, removing it from tracking", "deviceID", deviceID)
				delete(r.missingDevices, deviceID)
			}
			continue
		}

		firstSeenTime, isTracking := r.missingDevices[deviceID]
		if !isTracking {
			upstreamSyncerLog.Info("Found a device on upstream server without a local ComposableResource, starting to track with grace period", "deviceID", deviceID)
			r.missingDevices[deviceID] = time.Now()
		} else {
			if time.Since(firstSeenTime) > missingDeviceGracePeriod {
				upstreamSyncerLog.Info("Grace period exceeded for missing device, creating ComposableResource to trigger detach", "deviceID", deviceID, "missingSince", firstSeenTime)
				if err := r.createDetachCR(ctx, deviceInfo); err != nil {
					upstreamSyncerLog.Error(err, "failed to create ComposableResource for detaching", "deviceID", deviceID)
				} else {
					delete(r.missingDevices, deviceID)
				}
			} else {
				upstreamSyncerLog.Info("Device is still missing but within grace period, skipping", "deviceID", deviceID)
			}
		}
	}

	upstreamDeviceIDs := make(map[string]struct{})
	for _, dev := range deviceInfoList {
		upstreamDeviceIDs[dev.DeviceID] = struct{}{}
	}

	for trackedDeviceID := range r.missingDevices {
		if _, existsOnUpstream := upstreamDeviceIDs[trackedDeviceID]; !existsOnUpstream {
			upstreamSyncerLog.Info("Tracked device is no longer present on the upstream server, removing from tracking map", "deviceID", trackedDeviceID)
			delete(r.missingDevices, trackedDeviceID)
		}
	}

	return nil
}

func (r *UpstreamSyncerReconciler) createDetachCR(ctx context.Context, deviceInfo cdi.DeviceInfo) error {
	newCR := &crov1alpha1.ComposableResource{
		ObjectMeta: ctrl.ObjectMeta{
			GenerateName: utils.GenerateComposableResourceName("gpu"),
			Labels: map[string]string{
				"cohdi.io/ready-to-detach-device-uuid": deviceInfo.DeviceID,
			},
		},
		Spec: crov1alpha1.ComposableResourceSpec{
			Type:        deviceInfo.DeviceType,
			TargetNode:  deviceInfo.NodeName,
			ForceDetach: false,
		},
	}
	if err := r.Client.Create(ctx, newCR); err != nil {
		return err
	}
	upstreamSyncerLog.Info("Successfully created a ComposableResource for detaching", "deviceID", deviceInfo.DeviceID)
	return nil
}
