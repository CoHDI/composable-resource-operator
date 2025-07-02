package controller

import (
	"context"
	"errors"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/IBM/composable-resource-operator/api/v1alpha1"
	"github.com/IBM/composable-resource-operator/internal/cdi"
	ftiCM "github.com/IBM/composable-resource-operator/internal/cdi/fti/cm"
	ftiFM "github.com/IBM/composable-resource-operator/internal/cdi/fti/fm"
	"github.com/IBM/composable-resource-operator/internal/cdi/sunfish"
)

type ComposableResourceAdapter struct {
	instance    *v1alpha1.ComposableResource
	logger      logr.Logger
	client      client.Client
	clientSet   *kubernetes.Clientset
	CDIProvider cdi.CdiProvider
}

func NewComposableResourceAdapter(instance *v1alpha1.ComposableResource, logger logr.Logger, ctx context.Context, client client.Client, clientSet *kubernetes.Clientset) (*ComposableResourceAdapter, error) {
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
			return nil, errors.New("FTI_CDI_API_TYPE variable not set properly")
		}
	default:
		return nil, errors.New("CDI_PROVIDER_TYPE variable not set properly")
	}

	return &ComposableResourceAdapter{instance, logger, client, clientSet, cdiProvider}, nil
}
