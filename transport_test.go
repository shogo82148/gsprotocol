package gsprotocol

import (
	"io/ioutil"
	"net/http"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	mock := &storageClientMock{
		buckets: map[string]*bucketHandleMock{
			"bucket-name": {
				objects: map[string]*objectHandleMock{
					"object-key": {
						content: "Hello Google Cloud Storage!",
					},
				},
			},
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
	mock := &storageClientMock{
		buckets: map[string]*bucketHandleMock{
			"bucket-name": {
				objects: map[string]*objectHandleMock{
					"object-key": {
						content:    "Hello Google Cloud Storage!",
						generation: 1234567890,
					},
				},
			},
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
