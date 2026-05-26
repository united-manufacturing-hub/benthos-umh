// Copyright 2026 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// Portions of this file are derived from the warpstreamlabs/bento project:
//   https://github.com/warpstreamlabs/bento/blob/main/internal/impl/snowflake/output_snowflake_put_test.go
//
// Original work Copyright (c) 2024-present Bento contributors, licensed under
// the MIT License. A copy of the MIT License is reproduced in NOTICE.
// -----------------------------------------------------------------------------

package snowflake_put_plugin

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"
)

const (
	dummyUUID         = "12345678-90ab-cdef-1234-567890abcdef"
	testKeyPassphrase = "test123"
)

// keyKind selects which generated private key a test case uses.
type keyKind int

const (
	keyNone keyKind = iota
	keyPlaintext
	keyEncrypted
	keyMissing
)

// Generated once in TestMain so no private keys are committed to the repo.
var (
	testPrivateKey       *rsa.PrivateKey
	testPlaintextKeyPath string
	testEncryptedKeyPath string
	testMissingKeyPath   string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "snowflake_keys")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	testPrivateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	plainDER, err := x509.MarshalPKCS8PrivateKey(testPrivateKey)
	if err != nil {
		panic(err)
	}
	testPlaintextKeyPath = filepath.Join(dir, "rsa_key.pem")
	writePEM(testPlaintextKeyPath, "PRIVATE KEY", plainDER)

	encDER, err := pkcs8.MarshalPrivateKey(testPrivateKey, []byte(testKeyPassphrase), nil)
	if err != nil {
		panic(err)
	}
	testEncryptedKeyPath = filepath.Join(dir, "rsa_key.p8")
	writePEM(testEncryptedKeyPath, "ENCRYPTED PRIVATE KEY", encDER)

	testMissingKeyPath = filepath.Join(dir, "missing_key.pem")

	os.Exit(m.Run())
}

func writePEM(path string, blockType string, der []byte) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = pem.Encode(f, &pem.Block{Type: blockType, Bytes: der})
	if err != nil {
		panic(err)
	}
}

type MockDB struct {
	Queries      []string
	QueriesCount int
}

func (db *MockDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	db.Queries = append(db.Queries, query)
	db.QueriesCount++

	return nil, nil
}

func (db *MockDB) Close() error { return nil }

func (db *MockDB) hasQuery(query string) bool {
	return slices.Contains(db.Queries, query)
}

type MockUUIDGenerator struct{}

func (MockUUIDGenerator) NewV4() (uuid.UUID, error) {
	return uuid.Must(uuid.FromString(dummyUUID)), nil
}

type MockHTTPClient struct {
	SnowpipeHost string
	Queries      []string
	QueriesCount int
	Payloads     []string
	JWTs         []string
}

func (c *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.URL.Host = c.SnowpipeHost
	req.URL.Scheme = "http"

	query := req.URL.Path
	query += "?" + req.URL.RawQuery
	c.Queries = append(c.Queries, query)
	c.QueriesCount++

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body.Close()
	req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	c.Payloads = append(c.Payloads, strings.TrimSpace(string(bodyBytes)))

	c.JWTs = append(c.JWTs, req.Header.Get("Authorization"))

	return http.DefaultClient.Do(req)
}

func (c *MockHTTPClient) hasQuery(query string) bool {
	return slices.Contains(c.Queries, query)
}

func (c *MockHTTPClient) hasPayload(payload string) bool {
	return slices.Contains(c.Payloads, payload)
}

func TestSnowflakeOutput(t *testing.T) {
	type testCase struct {
		name                      string
		keyKind                   keyKind
		privateKeyPassphrase      string
		stage                     string
		fileName                  string
		fileExtension             string
		requestID                 string
		snowpipe                  string
		compression               string
		snowflakeHTTPResponseCode int
		snowflakeResponseCode     string
		wantPUTQuery              string
		wantPUTQueriesCount       int
		wantSnowpipeQuery         string
		wantSnowpipeQueriesCount  int
		wantSnowpipePayload       string
		errConfigContains         string
		errContains               string
	}

	keyPath := func(tc testCase) string {
		switch tc.keyKind {
		case keyPlaintext:
			return testPlaintextKeyPath
		case keyEncrypted:
			return testEncryptedKeyPath
		case keyMissing:
			return testMissingKeyPath
		default:
			return ""
		}
	}

	getSnowflakeWriter := func(t *testing.T, tc testCase) (*snowflakeWriter, error) {
		t.Helper()

		outputConfig := `
account: bento
region: east-us-2
cloud: azure
user: foobar
private_key_file: ` + keyPath(tc) + `
private_key_pass: ` + tc.privateKeyPassphrase + `
role: test_role
database: test_db
warehouse: test_warehouse
schema: test_schema
path: foo/bar/baz
stage: '` + tc.stage + `'
file_name: '` + tc.fileName + `'
file_extension: '` + tc.fileExtension + `'
upload_parallel_threads: 42
compression: ` + tc.compression + `
request_id: '` + tc.requestID + `'
snowpipe: '` + tc.snowpipe + `'
`

		spec := snowflakePutOutputConfig()
		env := service.NewEnvironment()
		conf, err := spec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		return newSnowflakeWriterFromConfig(conf, service.MockResources())
	}

	tests := []testCase{
		{
			name:         "executes snowflake query with plaintext SSH key",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "NONE",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                 "executes snowflake query with encrypted SSH key",
			keyKind:              keyEncrypted,
			privateKeyPassphrase: testKeyPassphrase,
			stage:                "@test_stage",
			compression:          "NONE",
			wantPUTQuery:         "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:              "fails to read missing SSH key",
			keyKind:           keyMissing,
			stage:             "@test_stage",
			compression:       "NONE",
			errConfigContains: "no such file or directory",
		},
		{
			name:              "fails to read encrypted SSH key without passphrase",
			keyKind:           keyEncrypted,
			stage:             "@test_stage",
			compression:       "NONE",
			errConfigContains: "failed to read private key: private key requires a passphrase, but private_key_passphrase was not supplied",
		},
		{
			name:         "executes snowflake query without compression",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "NONE",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:         "executes snowflake query with automatic compression",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "AUTO",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".gz @test_stage/foo/bar/baz AUTO_COMPRESS = TRUE SOURCE_COMPRESSION = AUTO_DETECT PARALLEL=42",
		},
		{
			name:         "executes snowflake query with gzip compression",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "GZIP",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".gz @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = GZIP PARALLEL=42",
		},
		{
			name:         "executes snowflake query with DEFLATE compression",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "DEFLATE",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".deflate @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = DEFLATE PARALLEL=42",
		},
		{
			name:         "executes snowflake query with RAW_DEFLATE compression",
			keyKind:      keyPlaintext,
			stage:        "@test_stage",
			compression:  "RAW_DEFLATE",
			wantPUTQuery: "PUT file://foo/bar/baz/" + dummyUUID + ".raw_deflate @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = RAW_DEFLATE PARALLEL=42",
		},
		{
			name:          "handles file name and file extension interpolation",
			keyKind:       keyPlaintext,
			stage:         "@test_stage",
			fileName:      `${! "deadbeef" }`,
			fileExtension: `${! "parquet" }`,
			compression:   "NONE",
			wantPUTQuery:  "PUT file://foo/bar/baz/deadbeef.parquet @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                      "executes snowflake query and calls Snowpipe",
			keyKind:                   keyPlaintext,
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       1,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=" + dummyUUID,
			wantSnowpipeQueriesCount:  1,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
		},
		{
			name:                      "gets error code from Snowpipe",
			keyKind:                   keyPlaintext,
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "FAILURE",
			errContains:               "received unexpected Snowpipe response code: FAILURE",
		},
		{
			name:                      "gets http error from Snowpipe",
			keyKind:                   keyPlaintext,
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusTeapot,
			errContains:               "received unexpected Snowpipe response status: 418",
		},
		{
			name:                "handles stage interpolation and runs a query for each sub-batch",
			keyKind:             keyPlaintext,
			stage:               `@test_stage_${! json("id") }`,
			compression:         "NONE",
			wantPUTQueriesCount: 2,
			wantPUTQuery:        "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage_bar/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                      "handles Snowpipe interpolation and runs a query for each sub-batch",
			keyKind:                   keyPlaintext,
			stage:                     "@test_stage",
			snowpipe:                  `test_pipe_${! json("id") }`,
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       2,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe_bar/insertFiles?requestId=" + dummyUUID,
			wantSnowpipeQueriesCount:  2,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
		},
		{
			name:                      "handles request_id interpolation and runs a query and makes a single Snowpipe call for the entire batch",
			keyKind:                   keyPlaintext,
			stage:                     `@test_stage`,
			snowpipe:                  `test_pipe`,
			requestID:                 `${! "deadbeef" }`,
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/deadbeef.json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       1,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=deadbeef",
			wantSnowpipeQueriesCount:  1,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/deadbeef.json"}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := getSnowflakeWriter(t, test)
			if test.errConfigContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errConfigContains)
				return
			}
			require.NoError(t, err)

			s.uuidGenerator = MockUUIDGenerator{}

			snowpipeTestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.snowflakeHTTPResponseCode)
				_, _ = w.Write([]byte(`{"ResponseCode": "` + test.snowflakeResponseCode + `"}`))
			}))
			t.Cleanup(snowpipeTestServer.Close)

			mockHTTPClient := MockHTTPClient{
				SnowpipeHost: snowpipeTestServer.Listener.Addr().String(),
			}
			s.httpClient = &mockHTTPClient

			mockDB := MockDB{}
			s.db = &mockDB

			err = s.WriteBatch(context.Background(), service.MessageBatch{
				service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
				service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
			})
			if test.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errContains)
				return
			}
			require.NoError(t, err)

			if test.wantPUTQueriesCount > 0 {
				assert.Equal(t, test.wantPUTQueriesCount, mockDB.QueriesCount)
			}
			if test.wantPUTQuery != "" {
				assert.True(t, mockDB.hasQuery(test.wantPUTQuery))
			}
			if test.wantSnowpipeQueriesCount > 0 {
				assert.Equal(t, test.wantSnowpipeQueriesCount, mockHTTPClient.QueriesCount)
				assert.Len(t, mockHTTPClient.JWTs, test.wantSnowpipeQueriesCount)
				// JWTs are signed with the ephemeral key generated in TestMain, so verify
				// them structurally rather than against a hardcoded token.
				for _, raw := range mockHTTPClient.JWTs {
					assertValidSnowpipeJWT(t, raw)
				}
			}
			if test.wantSnowpipeQuery != "" {
				assert.True(t, mockHTTPClient.hasQuery(test.wantSnowpipeQuery))
			}
			if test.wantSnowpipePayload != "" {
				assert.True(t, mockHTTPClient.hasPayload(test.wantSnowpipePayload))
			}
		})
	}
}

// assertValidSnowpipeJWT verifies the bearer token is signed by the generated
// key and carries the expected issuer/subject claims.
func assertValidSnowpipeJWT(t *testing.T, authHeader string) {
	t.Helper()

	tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(_ *jwt.Token) (any, error) {
		return &testPrivateKey.PublicKey, nil
	})
	require.NoError(t, err)
	assert.True(t, token.Valid)

	// account="bento", user="foobar" -> uppercased "BENTO.FOOBAR".
	sub, _ := claims["sub"].(string)
	iss, _ := claims["iss"].(string)
	assert.Equal(t, "BENTO.FOOBAR", sub)
	assert.True(t, strings.HasPrefix(iss, "BENTO.FOOBAR.SHA256:"))
}
