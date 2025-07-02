package cdi

import (
	"errors"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
)

type CdiProvider interface {
	AddResource(instance *v1alpha1.ComposableResource) (deviceID, CDIDeviceID string, err error)
	RemoveResource(instance *v1alpha1.ComposableResource) error
	CheckResource(instance *v1alpha1.ComposableResource) error
}

var ErrWaitingDeviceAttaching = errors.New("device is attaching to cluster")
var ErrWaitingDeviceDetaching = errors.New("device is detaching to cluster")
