package fti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const tokenRequestTimeout = 30 * time.Second

type token struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
	RefreshToken     string `json:"refresh_token"`
	TokenType        string `json:"token_type"`
	IDToken          string `json:"id_token"`
	NotBeforePolicy  int64  `json:"not-before-policy"`
	SessionState     string `json:"session_state"`
	Scope            string `json:"scope"`
}

type accessToken struct {
	Expiry int64 `json:"exp"`
}

type CachedToken struct {
	sync.RWMutex
	clientSet *kubernetes.Clientset
	endpoint  string
	token     *oauth2.Token
	leeway    time.Duration
}

func NewCachedToken(clientSet *kubernetes.Clientset, endpoint string) *CachedToken {
	return &CachedToken{
		clientSet: clientSet,
		leeway:    30 * time.Second,
		endpoint:  endpoint,
	}
}

func (t *CachedToken) GetToken() (*oauth2.Token, error) {
	now := time.Now()
	t.RLock()
	token := t.token
	t.RUnlock()

	if token != nil && token.Expiry.Add(-1*t.leeway).After(now) {
		// cached token is existing and valid, so quit.
		return token, nil
	}

	t.Lock()
	defer t.Unlock()
	// Make a dubble check as acquiring lock may take time.
	if token := t.token; token != nil && token.Expiry.Add(-1*t.leeway).After(now) {
		return token, nil
	}

	token, err := t.Token()
	if err != nil {
		return nil, fmt.Errorf("unable to rotate token: %w", err)
	}

	// Update cache.
	t.token = token
	return token, nil
}

func (ts *CachedToken) Token() (*oauth2.Token, error) {
	// TODO: Need to confirm credential's name and namespace.
	namespace := "composable-resource-operator-system"
	secretName := "credentials"

	secret, err := ts.clientSet.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	client_id := string(secret.Data["client_id"])
	client_secret := string(secret.Data["client_secret"])
	realm := string(secret.Data["realm"])

	pathPrefix := fmt.Sprintf("id_manager/realms/%s/protocol/openid-connect/token", realm)
	data := url.Values{
		"client_id":     {client_id},
		"client_secret": {client_secret},
		"username":      {username},
		"password":      {password},
		"scope":         {"openid"},
		"response_type": {"id_token token"},
		"grant_type":    {"password"},
	}

	client := &http.Client{
		Timeout: tokenRequestTimeout,
	}
	response, err := client.PostForm("https://"+ts.endpoint+pathPrefix, data)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read id_manager response body: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http returned code: %d, response body: %s", response.StatusCode, string(bodyBytes))
	}

	responseToken := &token{}
	if err := json.Unmarshal(bodyBytes, responseToken); err != nil {
		return nil, fmt.Errorf("failed to read id_manager response body into Token: %v", err)
	}

	var token oauth2.Token

	token.AccessToken = responseToken.AccessToken
	token.TokenType = responseToken.TokenType
	token.RefreshToken = responseToken.RefreshToken

	accessTokenParts := strings.Split(responseToken.AccessToken, ".")
	if len(accessTokenParts) != 3 {
		return nil, fmt.Errorf("invalid access token: %s", responseToken.AccessToken)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(accessTokenParts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode id_manager payload: %s", err)
	}

	var result accessToken
	if err := json.Unmarshal(payloadBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal id_manager json: %s", err)
	}
	token.Expiry = time.Unix(result.Expiry, 0)

	return &token, nil
}
