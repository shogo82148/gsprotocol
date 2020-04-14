package gsprotocol

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// Transport serving the Google Cloud Storage objects.
type Transport struct {
	client storageClient
}

// NewTransport returns a new Transport.
func NewTransport(ctx context.Context, opts ...option.ClientOption) (*Transport, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Transport{
		client: newStorageClientImpl(client),
	}, nil
}

// NewTransportWithClient returns a new Transport.
func NewTransportWithClient(client *storage.Client) *Transport {
	return &Transport{
		client: newStorageClientImpl(client),
	}
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.Method {
	case http.MethodGet:
		return t.getObject(req)
	case http.MethodHead:
		return t.headObject(req)
	}
	return &http.Response{
		Status:     "405 Method Not Allowed",
		StatusCode: http.StatusMethodNotAllowed,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     make(http.Header),
		Body:       http.NoBody,
		Close:      true,
	}, nil
}

func (t *Transport) getObject(req *http.Request) (*http.Response, error) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	path := strings.TrimPrefix(req.URL.Path, "/")
	object := t.client.Bucket(host).Object(path)

	if fragment := req.URL.Fragment; fragment != "" {
		gen, err := strconv.ParseInt(fragment, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gsprotocol: invalid generation %s: %v", fragment, err)
		}
		object = object.Generation(gen)
	}

	body, err := object.NewReader(req.Context())
	if err != nil {
		return nil, err
	}
	attrs := body.Attrs()
	header := make(http.Header)
	if v := attrs.ContentType; v != "" {
		header.Set("Content-Type", v)
	}
	if v := attrs.ContentEncoding; v != "" {
		header.Set("Content-Encoding", v)
	}
	if v := attrs.CacheControl; v != "" {
		header.Set("Cache-Control", v)
	}
	if v := attrs.LastModified; !v.IsZero() {
		header.Set("Last-Modified", v.Format(http.TimeFormat))
	}
	if v := attrs.Generation; v != 0 {
		header.Set("x-goog-generation", strconv.FormatInt(v, 10))
	}
	if v := attrs.Metageneration; v != 0 {
		header.Set("x-goog-metageneration", strconv.FormatInt(v, 10))
	}

	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        header,
		Body:          body,
		ContentLength: attrs.Size,
		Close:         true,
	}, nil
}

func (t *Transport) headObject(req *http.Request) (*http.Response, error) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	path := strings.TrimPrefix(req.URL.Path, "/")

	_ = path
	return nil, nil
}
