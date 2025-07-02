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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	StateEmpty           = ""
	StateOnline          = "Online"
	StatePending         = "Pending"
	StateFinalizingSetup = "FinalizingSetup"
	StateCleanup         = "Cleanup"
	StateFailed          = "Failed"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ComposabilityRequestSpec defines the desired state of ComposabilityRequest
type ComposabilityRequestSpec struct {
	Resource ScalarResourceDetails `json:"resource"`
}

type ScalarResourceDetails struct {
	// +kubebuilder:validation:Enum="gpu";"cxlmemory"
	Type  string `json:"type"`
	Model string `json:"model"`
	// +kubebuilder:validation:Minimum=0
	Size        int64 `json:"size"`
	ForceDetach bool  `json:"force_detach,omitempty"`
	// +kubebuilder:validation:Enum="samenode";"differentnode"
	// +kubebuilder:default=samenode
	AllocationPolicy string    `json:"allocation_policy,omitempty"`
	TargetNode       string    `json:"target_node,omitempty"`
	OtherSpec        *NodeSpec `json:"other_spec,omitempty"`
}

type NodeSpec struct {
	// +kubebuilder:validation:Minimum=0
	MilliCPU int64 `json:"milli_cpu,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Memory int64 `json:"memory,omitempty"`
	// +kubebuilder:validation:Minimum=0
	EphemeralStorage int64 `json:"ephemeral_storage,omitempty"`
	// +kubebuilder:validation:Minimum=0
	AllowedPodNumber int64 `json:"allowed_pod_number,omitempty"`
}

// ComposabilityRequestStatus defines the observed state of ComposabilityRequest
type ComposabilityRequestStatus struct {
	State          string                          `json:"state"`
	Error          string                          `json:"error,omitempty"`
	Resources      map[string]ScalarResourceStatus `json:"resources,omitempty"`
	ScalarResource ScalarResourceDetails           `json:"scalarResource,omitempty"`
}

type ScalarResourceStatus struct {
	State       string `json:"state"`
	DeviceID    string `json:"device_id,omitempty"`
	CDIDeviceID string `json:"cdi_device_id,omitempty"`
	NodeName    string `json:"node_name,omitempty"`
	Error       string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// ComposabilityRequest is the Schema for the composabilityrequests API
type ComposabilityRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComposabilityRequestSpec   `json:"spec,omitempty"`
	Status ComposabilityRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ComposabilityRequestList contains a list of ComposabilityRequest
type ComposabilityRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComposabilityRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ComposabilityRequest{}, &ComposabilityRequestList{})
}
