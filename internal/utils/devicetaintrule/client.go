/**
 * (C) Copyright 2025 The CoHDI Authors.
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

// Package devicetaintrule provides a version-agnostic client for DeviceTaintRule
// objects. At construction time it probes the API server for the preferred API
// version (v1beta2, introduced in K8s 1.36) and falls back to v1alpha3
// (introduced in K8s 1.33, removed in K8s 1.39). All operations are performed
// through the dynamic client so that the binary is compatible with any K8s
// version in the range 1.33-1.39+.
package devicetaintrule

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
)

var log = ctrl.Log.WithName("devicetaintrule")

const (
	group    = "resource.k8s.io"
	resource = "devicetaintrules"

	versionV1     = "v1"
	versionBeta2  = "v1beta2"
	versionAlpha3 = "v1alpha3"
)

// TaintSpec holds the fields needed to construct a DeviceTaintRule.
type TaintSpec struct {
	Driver string
	Pool   string
	Device string
	Key    string
	Value  string
	Effect string
}

// Interface defines the operations exposed by a DeviceTaintRule client.
// The concrete implementation (*Client) uses the dynamic client to remain
// version-agnostic; tests may inject a mock that satisfies this interface.
type Interface interface {
	Create(ctx context.Context, name string, spec TaintSpec) error
	Delete(ctx context.Context, name string) error
	Exists(ctx context.Context, name string) (bool, error)
	Version() string
}

// Client is a version-agnostic client for DeviceTaintRule cluster-scoped objects.
type Client struct {
	dynamic dynamic.Interface
	gvr     schema.GroupVersionResource
}

// New creates a Client, probing the API server to select the best available
// API version. It prefers v1beta2 (K8s >= 1.36) and falls back to v1alpha3.
func New(dynamicClient dynamic.Interface, discoveryClient discovery.DiscoveryInterface) (*Client, error) {
	ver, err := preferredVersion(discoveryClient)
	if err != nil {
		return nil, fmt.Errorf("devicetaintrule: version discovery failed: %w", err)
	}
	gvr := schema.GroupVersionResource{Group: group, Version: ver, Resource: resource}
	log.Info("DeviceTaintRule API version selected", "version", ver)
	return &Client{dynamic: dynamicClient, gvr: gvr}, nil
}

// Version returns the API version the client is using (e.g. "v1alpha3").
func (c *Client) Version() string {
	return c.gvr.Version
}

// Create creates a DeviceTaintRule with the given name and spec. It is a no-op
// if the object already exists.
func (c *Client) Create(ctx context.Context, name string, spec TaintSpec) error {
	obj := buildObject(c.gvr, name, spec)
	_, err := c.dynamic.Resource(c.gvr).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("devicetaintrule: failed to create %q: %w", name, err)
	}
	return nil
}

// Delete deletes the named DeviceTaintRule. It is a no-op if the object does
// not exist.
func (c *Client) Delete(ctx context.Context, name string) error {
	err := c.dynamic.Resource(c.gvr).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("devicetaintrule: failed to delete %q: %w", name, err)
	}
	return nil
}

// Exists returns true if the named DeviceTaintRule is present on the cluster.
func (c *Client) Exists(ctx context.Context, name string) (bool, error) {
	_, err := c.dynamic.Resource(c.gvr).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// preferredVersion returns "v1beta2" when it is registered on the server,
// otherwise "v1alpha3".
func preferredVersion(dc discovery.DiscoveryInterface) (string, error) {
	for _, ver := range []string{versionV1, versionBeta2, versionAlpha3} {
		resources, err := dc.ServerResourcesForGroupVersion(group + "/" + ver)
		if err != nil {
			// Not available on this cluster -- try next version.
			continue
		}
		for _, r := range resources.APIResources {
			if r.Name == resource {
				return ver, nil
			}
		}
	}
	return "", fmt.Errorf("none of %s/{%s,%s,%s} available on this cluster",
		group, versionV1, versionBeta2, versionAlpha3)
}

// buildObject constructs an Unstructured DeviceTaintRule. The payload is
// identical for both v1alpha3 and v1beta2 since the fields are the same.
func buildObject(gvr schema.GroupVersionResource, name string, spec TaintSpec) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "DeviceTaintRule",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{
				"deviceSelector": map[string]interface{}{
					"driver": spec.Driver,
					"pool":   spec.Pool,
					"device": spec.Device,
				},
				"taint": map[string]interface{}{
					"key":    spec.Key,
					"value":  spec.Value,
					"effect": spec.Effect,
				},
			},
		},
	}
}
