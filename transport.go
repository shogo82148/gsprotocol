package gsprotocol

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
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
	object, attrs, err := t.objectAttrs(ctx, req)
	if err != nil {
		return handleError(err)
	}
	header := makeHeader(attrs)
	if resp := checkPreconditions(req, header, attrs); resp != nil {
		return resp, nil
	}

	body, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
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
	ctx := req.Context()
	_, attrs, err := t.objectAttrs(ctx, req)
	if err != nil {
		return handleError(err)
	}
	header := makeHeader(attrs)
	if resp := checkPreconditions(req, header, attrs); resp != nil {
		return resp, nil
	}

	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     header,
		Body:       http.NoBody,
		Close:      true,
	}, nil
}

func (t *Transport) objectAttrs(ctx context.Context, req *http.Request) (objectHandle, *storage.ObjectAttrs, error) {
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
			return nil, nil, fmt.Errorf("gsprotocol: invalid generation %s: %v", fragment, err)
		}
		object = object.Generation(gen)
		attrs, err = object.Attrs(ctx)
		if err != nil {
			return nil, nil, err
		}
	} else {
		var err error
		attrs, err = object.Attrs(ctx)
		if err != nil {
			return nil, nil, err
		}
		object = object.Generation(attrs.Generation)
	}
	return object, attrs, nil
}

func handleError(err error) (*http.Response, error) {
	if err == storage.ErrObjectNotExist || err == storage.ErrBucketNotExist {
		return &http.Response{
			Status:     "404 Not Found",
			StatusCode: http.StatusNotFound,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Close:      true,
		}, nil
	}
	if err, ok := err.(*googleapi.Error); ok {
		return &http.Response{
			Status:     fmt.Sprintf("%d %s", err.Code, http.StatusText(err.Code)),
			StatusCode: err.Code,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     err.Header,
			Body:       io.NopCloser(strings.NewReader(err.Body)),
			Close:      true,
		}, nil
	}
	return nil, err
}

// scanETag determines if a syntactically valid ETag is present at s. If so,
// the ETag and remaining text after consuming ETag is returned. Otherwise,
// it returns "", "".
func scanETag(s string) (etag string, remain string) {
	s = textproto.TrimString(s)
	start := 0
	if strings.HasPrefix(s, "W/") {
		start = 2
	}
	if len(s[start:]) < 2 || s[start] != '"' {
		return "", ""
	}
	// ETag is either W/"text" or "text".
	// See RFC 7232 2.3.
	for i := start + 1; i < len(s); i++ {
		c := s[i]
		switch {
		// Character values allowed in ETags.
		case c == 0x21 || c >= 0x23 && c <= 0x7E || c >= 0x80:
		case c == '"':
			return s[:i+1], s[i+1:]
		default:
			return "", ""
		}
	}
	return "", ""
}

// etagStrongMatch reports whether a and b match using strong ETag comparison.
// Assumes a and b are valid ETags.
func etagStrongMatch(a, b string) bool {
	return a == b && a != "" && a[0] == '"'
}

// etagWeakMatch reports whether a and b match using weak ETag comparison.
// Assumes a and b are valid ETags.
func etagWeakMatch(a, b string) bool {
	return strings.TrimPrefix(a, "W/") == strings.TrimPrefix(b, "W/")
}

// condResult is the result of an HTTP request precondition check.
// See https://tools.ietf.org/html/rfc7232 section 3.
type condResult int

const (
	condNone condResult = iota
	condTrue
	condFalse
)

func checkIfMatch(req *http.Request, header http.Header, attrs *storage.ObjectAttrs) condResult {
	im := req.Header.Get("If-Match")
	if im == "" {
		return condNone
	}
	for {
		im = textproto.TrimString(im)
		if len(im) == 0 {
			break
		}
		if im[0] == ',' {
			im = im[1:]
			continue
		}
		if im[0] == '*' {
			return condTrue
		}
		etag, remain := scanETag(im)
		if etag == "" {
			break
		}
		if etagStrongMatch(etag, header.Get("Etag")) {
			return condTrue
		}
		im = remain
	}
	return condFalse
}

func checkIfUnmodifiedSince(req *http.Request, header http.Header, attrs *storage.ObjectAttrs) condResult {
	ius := req.Header.Get("If-Unmodified-Since")
	if ius == "" || attrs.Updated.IsZero() {
		return condNone
	}
	t, err := http.ParseTime(ius)
	if err != nil {
		return condNone
	}

	// The Last-Modified header truncates sub-second precision so
	// the modtime needs to be truncated too.
	modtime := attrs.Updated.Truncate(time.Second)
	if modtime.Before(t) || modtime.Equal(t) {
		return condTrue
	}
	return condFalse
}

func checkIfNoneMatch(req *http.Request, header http.Header, attrs *storage.ObjectAttrs) condResult {
	inm := req.Header.Get("If-None-Match")
	if inm == "" {
		return condNone
	}
	for {
		inm = textproto.TrimString(inm)
		if len(inm) == 0 {
			break
		}
		if inm[0] == ',' {
			inm = inm[1:]
			continue
		}
		if inm[0] == '*' {
			return condFalse
		}
		etag, remain := scanETag(inm)
		if etag == "" {
			break
		}
		if etagWeakMatch(etag, header.Get("Etag")) {
			return condFalse
		}
		inm = remain
	}
	return condTrue
}

func checkIfModifiedSince(req *http.Request, header http.Header, attrs *storage.ObjectAttrs) condResult {
	ius := req.Header.Get("If-Modified-Since")
	if ius == "" || attrs.Updated.IsZero() {
		return condNone
	}
	t, err := http.ParseTime(ius)
	if err != nil {
		return condNone
	}

	// The Last-Modified header truncates sub-second precision so
	// the modtime needs to be truncated too.
	modtime := attrs.Updated.Truncate(time.Second)
	if modtime.Before(t) || modtime.Equal(t) {
		return condFalse
	}
	return condTrue
}

// checkPreconditions handles conditional requests, and return nil if the condition is satisfied.
// if it's not, return non nil response.
func checkPreconditions(req *http.Request, header http.Header, attrs *storage.ObjectAttrs) *http.Response {
	ch := checkIfMatch(req, header, attrs)
	if ch == condNone {
		ch = checkIfUnmodifiedSince(req, header, attrs)
	}
	if ch == condFalse {
		return &http.Response{
			Status:     "412 Precondition Failed",
			StatusCode: http.StatusPreconditionFailed,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     header,
			Body:       http.NoBody,
			Close:      true,
		}
	}
	ch = checkIfNoneMatch(req, header, attrs)
	if ch == condFalse || (ch == condNone && checkIfModifiedSince(req, header, attrs) == condFalse) {
		// RFC 7232 section 4.1:
		// a sender SHOULD NOT generate representation metadata other than the
		// above listed fields unless said metadata exists for the purpose of
		// guiding cache updates (e.g., Last-Modified might be useful if the
		// response does not have an ETag field).
		header.Del("Content-Type")
		header.Del("Content-Length")
		if header.Get("Etag") != "" {
			header.Del("Last-Modified")
		}
		return &http.Response{
			Status:     "304 Not Modified",
			StatusCode: http.StatusNotModified,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     header,
			Body:       http.NoBody,
			Close:      true,
		}
	}
	return nil
}

func makeHeader(attrs *storage.ObjectAttrs) http.Header {
	// common http headers
	header := make(http.Header)
	if v := attrs.ContentType; v != "" {
		header.Set("Content-Type", v)
	}
	if v := attrs.ContentLanguage; v != "" {
		header.Set("Content-Language", v)
	}
	if v := attrs.CacheControl; v != "" {
		header.Set("Cache-Control", v)
	}
	if v := attrs.Size; v != 0 {
		header.Set("Content-Length", strconv.FormatInt(v, 10))
	}
	if v := attrs.ContentEncoding; v != "" {
		header.Set("Content-Encoding", v)
	}
	if v := attrs.ContentDisposition; v != "" {
		header.Set("Content-Disposition", v)
	}
	if v := attrs.Updated; !v.IsZero() {
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
