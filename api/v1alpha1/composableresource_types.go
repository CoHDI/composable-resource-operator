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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ComposableResourceSpec defines the desired state of ComposableResource
type ComposableResourceSpec struct {
	// +kubebuilder:validation:Enum="gpu";"cxlmemory"
	Type        string `json:"type"`
	Model       string `json:"model"`
	TargetNode  string `json:"target_node"`
	ForceDetach bool   `json:"force_detach,omitempty"`
}

// ComposableResourceStatus defines the observed state of ComposableResource
type ComposableResourceStatus struct {
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
	DeviceID    string `json:"device_id,omitempty"`
	CDIDeviceID string `json:"cdi_device_id,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// ComposableResource is the Schema for the composableresources API
type ComposableResource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComposableResourceSpec   `json:"spec,omitempty"`
	Status ComposableResourceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ComposableResourceList contains a list of ComposableResource
type ComposableResourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComposableResource `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ComposableResource{}, &ComposableResourceList{})
}
