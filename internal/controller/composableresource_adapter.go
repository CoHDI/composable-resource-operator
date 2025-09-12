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

package controller

import (
	"context"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/IBM/composable-resource-operator/internal/cdi"
	ftiCM "github.com/IBM/composable-resource-operator/internal/cdi/fti/cm"
	ftiFM "github.com/IBM/composable-resource-operator/internal/cdi/fti/fm"
	"github.com/IBM/composable-resource-operator/internal/cdi/sunfish"
)

type ComposableResourceAdapter struct {
	client      client.Client
	clientSet   *kubernetes.Clientset
	CDIProvider cdi.CdiProvider
}

func NewComposableResourceAdapter(ctx context.Context, client client.Client, clientSet *kubernetes.Clientset) (*ComposableResourceAdapter, error) {
	var cdiProvider cdi.CdiProvider

	deviceResourceType := os.Getenv("DEVICE_RESOURCE_TYPE")
	if deviceResourceType != "DEVICE_PLUGIN" && deviceResourceType != "DRA" {
		return nil, fmt.Errorf("the env variable DEVICE_RESOURCE_TYPE has an invalid value: '%s'", deviceResourceType)
	}

	switch cdiProviderType := os.Getenv("CDI_PROVIDER_TYPE"); cdiProviderType {
	case "SUNFISH":
		cdiProvider = sunfish.NewSunfishClient()
	case "FTI_CDI":
		switch ftiAPIType := os.Getenv("FTI_CDI_API_TYPE"); ftiAPIType {
		case "CM":
			cdiProvider = ftiCM.NewFTIClient(ctx, client, clientSet)
		case "FM":
			cdiProvider = ftiFM.NewFTIClient(ctx, client, clientSet)
		default:
			return nil, fmt.Errorf("the env variable FTI_CDI_API_TYPE has an invalid value: '%s'", ftiAPIType)
		}
	default:
		return nil, fmt.Errorf("the env variable CDI_PROVIDER_TYPE has an invalid value: '%s'", cdiProviderType)
	}

	return &ComposableResourceAdapter{client, clientSet, cdiProvider}, nil
}
