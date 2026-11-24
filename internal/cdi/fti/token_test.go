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

package fti

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/oauth2"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestFTIToken(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FTI Token Suite")
}

var _ = Describe("fti token tests", func() {
	Describe("NewCachedToken", func() {
		It("creates a new CachedToken with the configured endpoint", func() {
			cache := NewCachedToken(nil, "example.com")

			Expect(cache).NotTo(BeNil())
			Expect(cache.endpoint).To(Equal("example.com"))
			Expect(cache.leeway).To(Equal(30 * time.Second))
			Expect(cache.token).To(BeNil())
		})
	})

	Describe("GetToken", func() {
		It("returns the cached token when it is still valid", func() {
			cachedToken := &oauth2.Token{
				AccessToken: "cached-token",
				TokenType:   "Bearer",
				Expiry:      time.Now().Add(1 * time.Hour),
			}
			cache := &CachedToken{
				token:  cachedToken,
				leeway: 30 * time.Second,
			}

			result, err := cache.GetToken()

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeIdenticalTo(cachedToken))
		})

		It("returns an error when an expired token cannot be refreshed", func() {
			server := httptest.NewServer(http.NotFoundHandler())
			DeferCleanup(server.Close)

			clientSet, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			Expect(err).NotTo(HaveOccurred())
			expiredToken := &oauth2.Token{
				AccessToken: "expired-token",
				Expiry:      time.Now().Add(-1 * time.Hour),
			}
			cache := &CachedToken{
				clientSet: clientSet,
				token:     expiredToken,
				leeway:    30 * time.Second,
			}

			result, err := cache.GetToken()

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("unable to rotate token")))
		})
	})

	Describe("Token", func() {
		It("returns an error when the credentials Secret does not exist", func() {
			server := httptest.NewServer(http.NotFoundHandler())
			DeferCleanup(server.Close)

			clientSet, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			Expect(err).NotTo(HaveOccurred())
			cache := NewCachedToken(clientSet, "example.com/")

			result, err := cache.Token()

			Expect(result).To(BeNil())
			Expect(err).To(HaveOccurred())
		})
	})
})
