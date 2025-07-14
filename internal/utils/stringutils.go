package utils

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func GenerateComposableResourceName(typeName string) string {
	var composableResourceName string

	composableResourceName = fmt.Sprintf("%s-%s", typeName, uuid.New().String())
	composableResourceName = strings.ToLower(composableResourceName)

	return composableResourceName
}

func ContainsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
