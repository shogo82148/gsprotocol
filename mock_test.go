package gsprotocol

import (
	"context"
	"strings"

	"cloud.google.com/go/storage"
)

type storageClientMock struct {
	buckets map[string]*bucketHandleMock
}

func (c *storageClientMock) Bucket(name string) bucketHandle {
	return c.buckets[name]
}

type bucketHandleMock struct {
	objects map[string]*objectHandleMock
}

func (h *bucketHandleMock) Object(name string) objectHandle {
	return h.objects[name]
}

type objectHandleMock struct {
	attrs   storage.ReaderObjectAttrs
	content string
}

func (h *objectHandleMock) NewReader(ctx context.Context) (storageReader, error) {
	return &storageReaderMock{
		attrs:  h.attrs,
		reader: strings.NewReader(h.content),
	}, nil
}

type storageReaderMock struct {
	attrs  storage.ReaderObjectAttrs
	reader *strings.Reader
}

func (r *storageReaderMock) Attrs() storage.ReaderObjectAttrs {
	return r.attrs
}

func (r *storageReaderMock) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *storageReaderMock) Close() error {
	return nil
}
