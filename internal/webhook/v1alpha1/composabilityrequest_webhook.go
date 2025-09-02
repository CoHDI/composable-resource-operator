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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	crov1alpha1 "github.com/IBM/composable-resource-operator/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var composabilityRequestWebhookLog = logf.Log.WithName("composabilityrequest-webhook")

// SetupComposabilityRequestWebhookWithManager registers the webhook for ComposabilityRequest in the manager.
func SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&crov1alpha1.ComposabilityRequest{}).
		WithValidator(&ComposabilityRequestCustomValidator{
			Client: mgr.GetClient(),
		}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-cro-hpsys-ibm-ie-com-v1alpha1-composabilityrequest,mutating=false,failurePolicy=fail,sideEffects=None,groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests,verbs=create;update,versions=v1alpha1,name=vcomposabilityrequest.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests,verbs=get;list;watch
// +kubebuilder:rbac:groups=cro.hpsys.ibm.ie.com,resources=composabilityrequests/status,verbs=get

// ComposabilityRequestCustomValidator struct is responsible for validating the ComposabilityRequest resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type ComposabilityRequestCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
	client.Client
}

var _ webhook.CustomValidator = &ComposabilityRequestCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type ComposabilityRequest.
func (v *ComposabilityRequestCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	composabilityrequest, ok := obj.(*crov1alpha1.ComposabilityRequest)
	if !ok {
		err := fmt.Errorf("expected a ComposabilityRequest object but got %T", obj)
		composabilityRequestWebhookLog.Error(err, "failed to get a ComposabilityRequest object", "obj", obj)
		return nil, err
	}

	composabilityRequestWebhookLog.Info("validation for ComposabilityRequest upon creation", "name", composabilityrequest.GetName())

	return validateRequest(v, composabilityrequest, ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type ComposabilityRequest.
func (v *ComposabilityRequestCustomValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	newComposabilityRequest, ok := newObj.(*crov1alpha1.ComposabilityRequest)
	if !ok {
		err := fmt.Errorf("expected a ComposabilityRequest object but got %T", newObj)
		composabilityRequestWebhookLog.Error(err, "failed to get a ComposabilityRequest object", "obj", newObj)
		return nil, err
	}

	composabilityRequestWebhookLog.Info("validation for ComposabilityRequest upon update", "name", newComposabilityRequest.GetName())

	return validateRequest(v, newComposabilityRequest, ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type ComposabilityRequest.
func (v *ComposabilityRequestCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	return nil, nil
}

func validateRequest(v *ComposabilityRequestCustomValidator, r *crov1alpha1.ComposabilityRequest, ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	composabilityRequestList := &crov1alpha1.ComposabilityRequestList{}
	if err := v.List(ctx, composabilityRequestList); err != nil {
		composabilityRequestWebhookLog.Error(err, "failed to list ComposabilityRequests")
		return nil, err
	}

	if r.Spec.Resource.AllocationPolicy == "differentnode" && r.Spec.Resource.TargetNode != "" {
		return nil, fmt.Errorf("TargetNode cannot be specified when AllocationPolicy is set to 'differentnode'")
	}

	// Check if any existing ComposabilityRequest has the same type and model.
	// A per-cluster specification (no target_node specification) and a per-target_node specification cannot be mixed.
	if r.Spec.Resource.AllocationPolicy == "differentnode" {
		for _, composabilityRequest := range composabilityRequestList.Items {
			if composabilityRequest.Name == r.Name {
				continue
			}

			if composabilityRequest.Spec.Resource.AllocationPolicy == "differentnode" && composabilityRequest.Spec.Resource.Type == r.Spec.Resource.Type && composabilityRequest.Spec.Resource.Model == r.Spec.Resource.Model {
				return nil, fmt.Errorf("composabilityRequest resource %s with type %s and model %s already exists", composabilityRequest.Name, r.Spec.Resource.Type, r.Spec.Resource.Model)
			}
		}
	} else if r.Spec.Resource.AllocationPolicy == "samenode" {
		for _, composabilityRequest := range composabilityRequestList.Items {
			var targetNode string

			if composabilityRequest.Name == r.Name {
				continue
			}

			if composabilityRequest.Spec.Resource.TargetNode == "" {
				for _, v := range composabilityRequest.Status.Resources {
					targetNode = v.NodeName
					break
				}
			} else {
				targetNode = composabilityRequest.Spec.Resource.TargetNode
			}

			if targetNode == r.Spec.Resource.TargetNode && composabilityRequest.Spec.Resource.Type == r.Spec.Resource.Type && composabilityRequest.Spec.Resource.Model == r.Spec.Resource.Model {
				return nil, fmt.Errorf("composabilityRequest resource %s with type %s and model %s already exists", composabilityRequest.Name, r.Spec.Resource.Type, r.Spec.Resource.Model)
			}
		}
	}

	return nil, nil
}
