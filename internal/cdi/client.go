package cdi

import (
	"errors"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
)

type DeviceInfo struct {
	NodeName    string
	MachineUUID string
	DeviceType  string
	DeviceID    string
	CDIDeviceID string
}

type CdiProvider interface {
	AddResource(instance *v1alpha1.ComposableResource) (deviceID string, CDIDeviceID string, err error)
	RemoveResource(instance *v1alpha1.ComposableResource) error
	CheckResource(instance *v1alpha1.ComposableResource) error
	GetResources() (deviceInfoList []DeviceInfo, err error)
}

var ErrWaitingDeviceAttaching = errors.New("device is attaching to the cluster")
var ErrWaitingDeviceDetaching = errors.New("device is detaching from the cluster")
