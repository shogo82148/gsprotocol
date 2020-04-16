package gsprotocol

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

func TestRoundTrip(t *testing.T) {
	// prepare the mock
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			if mock.generation != 0 {
				t.Errorf("want to get latest metadata, but generation %d is specified", mock.generation)
			}
			return &storage.ObjectAttrs{
				ContentType:        "text/plain",
				ContentLanguage:    "ja-JP",
				CacheControl:       "public, max-age=60",
				ContentEncoding:    "identity",
				ContentDisposition: "inline",
				Updated:            time.Date(2020, time.April, 15, 0, 56, 0, 0, time.UTC),
				Metadata: map[string]string{
					"foo": "bar",
				},
				Size:           int64(len(content)),
				Metageneration: 5,
				Generation:     1234567890,
				MD5:            []byte{0x0b, 0x46, 0xf3, 0x06, 0xe9, 0x2d, 0x88, 0x51, 0x5e, 0x06, 0xd4, 0x8a, 0x62, 0xdc, 0xc3, 0x19},
				CRC32C:         0x7f762fe2,
				StorageClass:   "MULTI_REGIONAL",
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			if mock.generation != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, mock.generation)
			}
			reader := ioutil.NopCloser(strings.NewReader(content))
			return storage.ReaderObjectAttrs{
				ContentType:     "text/plain",
				CacheControl:    "public, max-age=60",
				ContentEncoding: "identity",
				Size:            int64(len(content)),
			}, reader, nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			if gen != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, gen)
			}
			cp := *mock
			cp.generation = 1234567890
			return &cp
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
	}
	got, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("want %q, got %q", content, string(got))
	}

	tc := []struct{ key, value string }{
		{"etag", `"0b46f306e92d88515e06d48a62dcc319"`},
		{"Content-Type", "text/plain"},
		{"Content-Language", "ja-JP"},
		{"Cache-Control", "public, max-age=60"},
		{"Content-Encoding", "identity"},
		{"Content-Disposition", "inline"},
		{"Content-Length", strconv.Itoa(len(content))},
		{"Last-Modified", "Wed, 15 Apr 2020 00:56:00 GMT"},
		{"x-goog-meta-foo", "bar"},
		{"x-goog-metageneration", "5"},
		{"x-goog-generation", "1234567890"},
		{"x-goog-stored-content-length", strconv.Itoa(len(content))},
		{"x-goog-stored-content-encoding", "identity"},
		{"x-goog-storage-class", "MULTI_REGIONAL"},
	}
	for _, tt := range tc {
		got := resp.Header.Get(tt.key)
		if got != tt.value {
			t.Errorf("unexpected %s: want %v, got %v", tt.key, tt.value, got)
		}
	}
	hash := resp.Header["X-Goog-Hash"]
	if hash[0] != "md5=C0bzBuktiFFeBtSKYtzDGQ==" {
		t.Errorf("invalid md5: %s", hash[0])
	}
	if hash[1] != "crc32c=f3Yv4g==" {
		t.Errorf("invalid crc32c: %s", hash[0])
	}

	if resp.ContentLength != int64(len(content)) {
		t.Errorf("unexpected Content-Length: want %d, got %d", len(content), resp.ContentLength)
	}
	if resp.Header.Get("Content-Length") != strconv.Itoa(len(content)) {
		t.Errorf("unexpected Content-Length: want %q, got %q", strconv.Itoa(len(content)), resp.Header.Get("Content-Length"))
	}
}

func TestRoundTrip_withgeneration(t *testing.T) {
	// prepare the mock
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			if mock.generation != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, mock.generation)
			}
			return &storage.ObjectAttrs{
				ContentType:        "text/plain",
				ContentLanguage:    "ja-JP",
				CacheControl:       "public, max-age=60",
				ContentEncoding:    "identity",
				ContentDisposition: "inline",
				Metadata: map[string]string{
					"foo": "bar",
				},
				Size:           int64(len(content)),
				Metageneration: 5,
				Generation:     1234567890,
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader(content)), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			if gen != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, gen)
			}
			cp := *mock
			cp.generation = 1234567890
			return &cp
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key#1234567890", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
	}
	got, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("want %q, got %q", content, string(got))
	}
}

func TestRoundTrip_HEAD(t *testing.T) {
	// prepare the mock
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			if mock.generation != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, mock.generation)
			}
			return &storage.ObjectAttrs{
				ContentType:        "text/plain",
				ContentLanguage:    "ja-JP",
				CacheControl:       "public, max-age=60",
				ContentEncoding:    "identity",
				ContentDisposition: "inline",
				Metadata: map[string]string{
					"foo": "bar",
				},
				Size:           int64(len(content)),
				Metageneration: 5,
				Generation:     1234567890,
			}, nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			if gen != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, gen)
			}
			cp := *mock
			cp.generation = 1234567890
			return &cp
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodHead, "gs://bucket-name/object-key#1234567890", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
	}
	got, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "" {
		t.Errorf("want %q, got %q", "", string(got))
	}
}

func TestRoundTrip_IfMatch(t *testing.T) {
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return &storage.ObjectAttrs{
				ContentType: "text/plain",
				MD5:         []byte{0x0b, 0x46, 0xf3, 0x06, 0xe9, 0x2d, 0x88, 0x51, 0x5e, 0x06, 0xd4, 0x8a, 0x62, 0xdc, 0xc3, 0x19},
				CRC32C:      0x7f762fe2,
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader(content)), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			return mock
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	t.Run("precondition failed", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Match", `"etag-value"`)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Errorf("unexpected status: want %d, got %d", http.StatusPreconditionFailed, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "" {
			t.Errorf("want %q, got %q", "", string(got))
		}
	})

	t.Run("matched", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Match", `"etag-value", "0b46f306e92d88515e06d48a62dcc319"`)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, string(got))
		}
	})
}

func TestRoundTrip_IfNoneMatch(t *testing.T) {
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return &storage.ObjectAttrs{
				ContentType: "text/plain",
				MD5:         []byte{0x0b, 0x46, 0xf3, 0x06, 0xe9, 0x2d, 0x88, 0x51, 0x5e, 0x06, 0xd4, 0x8a, 0x62, 0xdc, 0xc3, 0x19},
				CRC32C:      0x7f762fe2,
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader(content)), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			return mock
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	t.Run("not matched", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-None-Match", `"etag-value"`)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, string(got))
		}
	})

	t.Run("not modified", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-None-Match", `"etag-value", "0b46f306e92d88515e06d48a62dcc319"`)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotModified {
			t.Errorf("unexpected status: want %d, got %d", http.StatusNotModified, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "" {
			t.Errorf("want %q, got %q", "", string(got))
		}
	})
}

func TestRoundTrip_IfModifiedSince(t *testing.T) {
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return &storage.ObjectAttrs{
				ContentType: "text/plain",
				Updated:     time.Date(2020, time.April, 15, 0, 56, 0, 0, time.UTC),
				MD5:         []byte{0x0b, 0x46, 0xf3, 0x06, 0xe9, 0x2d, 0x88, 0x51, 0x5e, 0x06, 0xd4, 0x8a, 0x62, 0xdc, 0xc3, 0x19},
				CRC32C:      0x7f762fe2,
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader(content)), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			return mock
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	t.Run("not modified", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Modified-Since", "Wed, 15 Apr 2020 00:56:00 GMT")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotModified {
			t.Errorf("unexpected status: want %d, got %d", http.StatusNotModified, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "" {
			t.Errorf("want %q, got %q", "", string(got))
		}
	})

	t.Run("modified", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Modified-Since", "Wed, 15 Apr 2020 00:55:59 GMT")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, string(got))
		}
	})

	t.Run("with If-None-Match", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-None-Match", `"etag-value", "0b46f306e92d88515e06d48a62dcc319"`)
		req.Header.Set("If-Modified-Since", "Wed, 15 Apr 2020 00:55:59 GMT") // will be ignored
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotModified {
			t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "" {
			t.Errorf("want %q, got %q", "", string(got))
		}
	})
}

func TestRoundTrip_IfUnmodifiedSince(t *testing.T) {
	const content = "Hello Google Cloud Storage!"
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return &storage.ObjectAttrs{
				ContentType: "text/plain",
				Updated:     time.Date(2020, time.April, 15, 0, 56, 0, 0, time.UTC),
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader(content)), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			return mock
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	t.Run("modified", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Unmodified-Since", "Wed, 15 Apr 2020 00:55:59 GMT")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Errorf("unexpected status: want %d, got %d", http.StatusPreconditionFailed, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "" {
			t.Errorf("want %q, got %q", "", string(got))
		}
	})

	t.Run("not modified", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("If-Unmodified-Since", "Wed, 15 Apr 2020 00:56:00 GMT")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("unexpected status: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		got, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, string(got))
		}
	})
}

func TestRoundTrip_NotFound(t *testing.T) {
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return nil, storage.ErrObjectNotExist
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unexpected status: want %d, got %d", http.StatusNotFound, resp.StatusCode)
	}
}

func TestRoundTrip_Error(t *testing.T) {
	object := &objectHandleMock{
		attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
			return nil, &googleapi.Error{
				Code: http.StatusBadRequest,
			}
		},
	}
	bucket := &bucketHandleMock{
		objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
			if name == "object-key" {
				return object
			}
			return objectMockNotFound
		},
	}
	mock := &storageClientMock{
		bucketFunc: func(mock *storageClientMock, name string) *bucketHandleMock {
			if name == "bucket-name" {
				return bucket
			}
			return bucketMockNotFount
		},
	}

	tr := &http.Transport{}
	tr.RegisterProtocol("gs", &Transport{client: mock})
	c := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodGet, "gs://bucket-name/object-key", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unexpected status: want %d, got %d", http.StatusNotFound, resp.StatusCode)
	}
}
