// Package httpclient provides a shared HTTP client for REST API integrations.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// AuthConfig holds authentication configuration for HTTP requests.
type AuthConfig struct {
	// Type is the auth type: "bearer", "basic", or "none".
	Type     string
	Token    string
	Username string
	Password string
}

// Client is a reusable HTTP client with a fixed base URL and auth configuration.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Auth       AuthConfig
}

// New creates a Client with the given base URL, auth config, and timeout.
func New(baseURL string, auth AuthConfig, timeout time.Duration) *Client {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
		Auth:       auth,
	}
}

// Do builds and executes an HTTP request. When body is non-nil it is marshaled
// to JSON and Content-Type is set to application/json. The caller is responsible
// for reading and closing the response body.
func (c *Client) Do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("httpclient: marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("httpclient: build request %s %s: %w", method, path, err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	switch c.Auth.Type {
	case "bearer":
		if c.Auth.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Auth.Token)
		}
	case "basic":
		if c.Auth.Username != "" {
			req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
		}
	}

	return c.HTTPClient.Do(req)
}

// DoJSON calls Do and reads the response, unmarshaling a successful body into out.
// If out is nil the response body is discarded. Returns an error for HTTP status >= 400.
func (c *Client) DoJSON(ctx context.Context, method, path string, body any, out any) error {
	resp, err := c.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("httpclient: read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("httpclient: HTTP %d: %s", resp.StatusCode, data)
	}

	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("httpclient: decode response: %w", err)
		}
	}
	return nil
}
