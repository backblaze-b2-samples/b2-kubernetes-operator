//go:build live

/*
Copyright 2026 Backblaze, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package live

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // B2 requires SHA-1 checksums on upload.
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/backblaze-b2-samples/b2-kubernetes-operator/internal/b2"
)

// files is the small part of the B2 file API the tests need. The operator
// never touches files, so its client has none of this.
type files struct {
	auth *b2.Authorization
}

// fileVersion is one entry from b2_list_file_versions.
type fileVersion struct {
	FileID            string  `json:"fileId"`
	FileName          string  `json:"fileName"`
	Action            string  `json:"action"`
	ReplicationStatus *string `json:"replicationStatus"`
}

func newFiles(ctx context.Context, keyID, key string) (*files, error) {
	auth, err := b2.New(b2.Options{BaseURL: env("B2_LIVE_API_URL"), ApplicationKeyID: keyID, ApplicationKey: key}).Authorize(ctx)
	if err != nil {
		return nil, err
	}
	return &files{auth: auth}, nil
}

func (f *files) upload(ctx context.Context, bucketID, name string, data []byte) error {
	var target struct {
		UploadURL          string `json:"uploadUrl"`
		AuthorizationToken string `json:"authorizationToken"`
	}
	if err := f.call(ctx, "b2_get_upload_url", map[string]string{"bucketId": bucketID}, &target); err != nil {
		return err
	}
	sum := sha1.Sum(data) //nolint:gosec // required by the API
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.UploadURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", target.AuthorizationToken)
	req.Header.Set("X-Bz-File-Name", url.PathEscape(name))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Bz-Content-Sha1", hex.EncodeToString(sum[:]))
	return send(req, nil)
}

func (f *files) versions(ctx context.Context, bucketID string) ([]fileVersion, error) {
	var out struct {
		Files []fileVersion `json:"files"`
	}
	err := f.call(ctx, "b2_list_file_versions", map[string]any{"bucketId": bucketID, "maxFileCount": 1000}, &out)
	return out.Files, err
}

// empty deletes every file version so the bucket can be deleted.
func (f *files) empty(ctx context.Context, bucketID string) error {
	vs, err := f.versions(ctx, bucketID)
	if err != nil {
		return err
	}
	for _, v := range vs {
		if err := f.call(ctx, "b2_delete_file_version", map[string]string{"fileId": v.FileID, "fileName": v.FileName}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (f *files) call(ctx context.Context, op string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(f.auth.APIInfo.StorageAPI.APIURL, "/") + "/b2api/v4/" + op
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", f.auth.AuthorizationToken)
	return send(req, out)
}

func send(req *http.Request, out any) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %d %s", req.URL.Path, resp.StatusCode, body)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}
