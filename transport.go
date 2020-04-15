package gsprotocol

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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
	ctx := req.Context()

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	path := strings.TrimPrefix(req.URL.Path, "/")
	object := t.client.Bucket(host).Object(path)

	var attrs *storage.ObjectAttrs
	if fragment := req.URL.Fragment; fragment != "" {
		gen, err := strconv.ParseInt(fragment, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gsprotocol: invalid generation %s: %v", fragment, err)
		}
		object = object.Generation(gen)
		attrs, err = object.Attrs(ctx)
		if err != nil {
			return nil, fmt.Errorf("gsprotocol: failed to get attribute: %v", err)
		}
	} else {
		var err error
		attrs, err = object.Attrs(ctx)
		if err != nil {
			return nil, fmt.Errorf("gsprotocol: failed to get attribute: %v", err)
		}
		object = object.Generation(attrs.Generation)
	}

	body, err := object.NewReader(req.Context())
	if err != nil {
		return nil, err
	}
	header := makeHeader(attrs, body.Attrs())

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

func makeHeader(attrs *storage.ObjectAttrs, reader storage.ReaderObjectAttrs) http.Header {
	// common http headers
	header := make(http.Header)
	if v := reader.ContentType; v != "" {
		header.Set("Content-Type", v)
	}
	if v := attrs.ContentLanguage; v != "" {
		header.Set("Content-Language", v)
	}
	if v := reader.CacheControl; v != "" {
		header.Set("Cache-Control", v)
	}
	if v := reader.Size; v != 0 {
		header.Set("Content-Length", strconv.FormatInt(v, 10))
	}
	if v := reader.ContentEncoding; v != "" {
		header.Set("Content-Encoding", v)
	}
	if v := attrs.ContentDisposition; v != "" {
		header.Set("Content-Disposition", v)
	}
	if v := reader.LastModified; !v.IsZero() {
		header.Set("Last-Modified", v.Format(http.TimeFormat))
	}

	// hash
	if v := attrs.MD5; len(v) > 0 {
		header.Add("x-goog-hash", "md5="+base64.StdEncoding.EncodeToString(v))

		// attrs has Etag attribute, but it is invalid form e.g. `CPi68c7s4ugCEAM=`
		// ETag should be quoted like `"<etag_value>"`.
		// So we generate ETag from MD5.
		header.Set("ETag", `"`+hex.EncodeToString(v)+`"`)
	}
	var crc32 [4]byte
	binary.BigEndian.PutUint32(crc32[:], attrs.CRC32C)
	header.Add("x-goog-hash", "crc32c="+base64.StdEncoding.EncodeToString(crc32[:]))

	// custom headers by google
	if v := attrs.Generation; v != 0 {
		header.Set("x-goog-generation", strconv.FormatInt(v, 10))
	}
	if v := attrs.Metageneration; v != 0 {
		header.Set("x-goog-metageneration", strconv.FormatInt(v, 10))
	}
	for key, value := range attrs.Metadata {
		header.Set("x-goog-meta-"+key, value)
	}
	if v := attrs.Size; v != 0 {
		header.Set("x-goog-stored-content-length", strconv.FormatInt(v, 10))
	}
	if v := attrs.ContentEncoding; v != "" {
		header.Set("x-goog-stored-content-encoding", v)
	}
	if v := attrs.StorageClass; v != "" {
		header.Set("x-goog-storage-class", v)
	}
	return header
}
