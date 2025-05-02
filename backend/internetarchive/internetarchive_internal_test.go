package internetarchive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test the submitFixerTask function with a mock server
func TestSubmitFixerTask(t *testing.T) {
	// Set up the mock server for the tasks API
	var receivedPayload map[string]interface{}
	wasTaskCalled := false

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/services/tasks.php") {
			// Check that this is a POST request for task submission
			assert.Equal(t, "POST", r.Method)

			// Read the request body
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			// Parse the JSON payload
			err = json.Unmarshal(body, &receivedPayload)
			require.NoError(t, err)

			// Return a successful response
			response := TaskResponse{
				Success: true,
				Value: struct {
					TaskID int    `json:"task_id"`
					Log    string `json:"log"`
				}{
					TaskID: 12345,
					Log:    "https://catalogd.archive.org/log/12345",
				},
			}

			w.Header().Set("Content-Type", "application/json")
			responseBytes, err := json.Marshal(response)
			require.NoError(t, err)

			_, err = w.Write(responseBytes)
			require.NoError(t, err)

			wasTaskCalled = true
		} else if strings.HasSuffix(r.URL.Path, "/metadata/test_bucket") {
			// Return empty metadata
			response := MetadataResponse{
				Files:    []IAFile{},
				ItemSize: 0,
			}

			w.Header().Set("Content-Type", "application/json")
			responseBytes, err := json.Marshal(response)
			require.NoError(t, err)

			_, err = w.Write(responseBytes)
			require.NoError(t, err)
		}
	}))
	defer mockServer.Close()

	// Create a new Fs with our mock server URLs
	ctx := context.Background()
	m := configmap.Simple{
		"type":              "internetarchive",
		"access_key_id":     "test_key",
		"secret_access_key": "test_secret",
		"endpoint":          mockServer.URL,
		"front_endpoint":    mockServer.URL,
	}

	// Create a new Fs
	fsObj, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)

	// Test submitting a fixer task
	err = fsObj.(*Fs).submitFixerTask(ctx, "test_bucket", nil)
	require.NoError(t, err)

	// Verify the task API was called
	assert.True(t, wasTaskCalled, "The task API endpoint should have been called")

	// Check the submitted payload
	assert.Equal(t, "test_bucket", receivedPayload["identifier"])
	assert.Equal(t, "fixer.php", receivedPayload["cmd"])

	// Ensure args is an empty map (not nil)
	args, ok := receivedPayload["args"].(map[string]interface{})
	assert.True(t, ok, "Args should be a map")
	assert.Equal(t, 0, len(args), "Args should be an empty map")
}

// Test that Put calls submitFixerTask
func TestPutCallsFixerTask(t *testing.T) {
	// Set up the mock server
	wasTaskCalled := false

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/services/tasks.php") {
			// Mark that the fixer task was called
			wasTaskCalled = true

			// Return a successful response
			response := TaskResponse{
				Success: true,
				Value: struct {
					TaskID int    `json:"task_id"`
					Log    string `json:"log"`
				}{
					TaskID: 12345,
					Log:    "https://catalogd.archive.org/log/12345",
				},
			}

			w.Header().Set("Content-Type", "application/json")
			responseBytes, err := json.Marshal(response)
			require.NoError(t, err)

			_, err = w.Write(responseBytes)
			require.NoError(t, err)
		} else if strings.HasPrefix(r.URL.Path, "/metadata/") {
			// Return empty metadata
			response := MetadataResponse{
				Files:    []IAFile{},
				ItemSize: 0,
			}

			w.Header().Set("Content-Type", "application/json")
			responseBytes, err := json.Marshal(response)
			require.NoError(t, err)

			_, err = w.Write(responseBytes)
			require.NoError(t, err)
		} else if r.Method == "PUT" {
			// Simulate successful upload
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer mockServer.Close()

	// Create a new Fs with our mock server URLs
	ctx := context.Background()
	m := configmap.Simple{
		"type":              "internetarchive",
		"access_key_id":     "test_key",
		"secret_access_key": "test_secret",
		"endpoint":          mockServer.URL,
		"front_endpoint":    mockServer.URL,
	}

	// Create a new Fs
	fsObj, err := NewFs(ctx, "test", "", m)
	require.NoError(t, err)
	iaFs := fsObj.(*Fs)

	// Create a dummy object source
	src := &Object{
		fs:      iaFs,
		remote:  "test_bucket/test.txt",
		modTime: time.Time{}, // Use zero time
		size:    10,
	}

	// Call Put with a dummy reader
	_, err = fsObj.Put(ctx, strings.NewReader("test data"), src)

	// The error is expected since our mock doesn't fully simulate the entire upload process
	// We only care that the fixer task was called

	// Verify the task API was called
	assert.True(t, wasTaskCalled, "The fixer task should have been called during Put")
}
