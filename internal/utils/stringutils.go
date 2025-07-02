package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GenerateComposableResourceName(ctx context.Context, cl client.Client, model string, typeName string) string {
	var candidateName string

	candidateName = fmt.Sprintf("%s-%s", typeName, uuid.New().String())
	candidateName = strings.ToLower(candidateName)

	return candidateName
}

func ContainsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
