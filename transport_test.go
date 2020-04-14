package gsprotocol

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
)

func TestRoundTrip(t *testing.T) {
	// prepare the mock
	object := &objectHandleMock{
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader("Hello Google Cloud Storage!")), nil
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
	if string(got) != "Hello Google Cloud Storage!" {
		t.Errorf("want %q, got %q", "Hello Google Cloud Storage", string(got))
	}
}

func TestRoundTrip_withgeneration(t *testing.T) {
	// prepare the mock
	object := &objectHandleMock{
		newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
			return storage.ReaderObjectAttrs{}, ioutil.NopCloser(strings.NewReader("Hello Google Cloud Storage!")), nil
		},
		generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
			if gen != 1234567890 {
				t.Errorf("unexpected generation: want %d, got %d", 1234567890, gen)
			}
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
	if string(got) != "Hello Google Cloud Storage!" {
		t.Errorf("want %q, got %q", "Hello Google Cloud Storage", string(got))
	}
}
