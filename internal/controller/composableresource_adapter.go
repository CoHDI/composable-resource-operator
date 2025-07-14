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
