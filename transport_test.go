package gsprotocol

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
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
				Metadata: map[string]string{
					"foo": "bar",
				},
				Size:           int64(len(content)),
				Metageneration: 5,
				Generation:     1234567890,
			}, nil
		},
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			if mock.generation != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, mock.generation)
			}
			reader := ioutil.NopCloser(strings.NewReader(content))
			return storage.ReaderObjectAttrs{}, reader, nil
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

	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("unexpected Content-Type: want %q, got %q", "text/plain", resp.Header.Get("Content-Type"))
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
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader("Hello Google Cloud Storage!")), nil
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
