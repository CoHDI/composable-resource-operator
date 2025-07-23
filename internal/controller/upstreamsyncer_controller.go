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
	"github.com/IBM/composable-resource-operator/internal/utils"
)

var upstreamSyncerLog = ctrl.Log.WithName("upstream_syncer_controller")

type UpstreamSyncerReconciler struct {
	client.Client
	ClientSet *kubernetes.Clientset
	Scheme    *runtime.Scheme
}

func (r *UpstreamSyncerReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
		if _, exists := existingDeviceIDs[deviceInfo.DeviceID]; !exists {
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
				upstreamSyncerLog.Error(err, "failed to create ComposableResource for detaching", "deviceID", deviceInfo.DeviceID)
			} else {
				upstreamSyncerLog.Info("created a ComposableResource for detaching", "deviceID", deviceInfo.DeviceID)
			}
		}
	}

	return nil
}
