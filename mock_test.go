package gsprotocol

import (
	"context"
	"fmt"
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

	// generation wanted
	// 0 means that the Generation will not be called.
	generation int64
}

func (h *objectHandleMock) NewReader(ctx context.Context) (storageReader, error) {
	return &storageReaderMock{
		attrs:  h.attrs,
		reader: strings.NewReader(h.content),
	}, nil
}

func (h *objectHandleMock) Generation(gen int64) objectHandle {
	if h.generation == 0 || h.generation != gen {
		panic(fmt.Sprintf("invalid generation: %d", gen))
	}
	return h
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
