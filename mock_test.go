package gsprotocol

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

var bucketMockNotFount = &bucketHandleMock{
	objectFunc: func(mock *bucketHandleMock, name string) *objectHandleMock {
		return objectMockNotFound
	},
}

var objectMockNotFound = &objectHandleMock{
	attrFunc: func(ctx context.Context, mock *objectHandleMock) (*storage.ObjectAttrs, error) {
		return nil, storage.ErrObjectNotExist
	},
	newReaderFunc: func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error) {
		return storage.ReaderObjectAttrs{}, nil, storage.ErrObjectNotExist
	},
	generationFunc: func(mock *objectHandleMock, gen int64) *objectHandleMock {
		return mock
	},
}

type storageClientMock struct {
	bucketFunc func(mock *storageClientMock, name string) *bucketHandleMock
}

func (c *storageClientMock) Bucket(name string) bucketHandle {
	if c.bucketFunc == nil {
		panic("unexpected call of Bucket")
	}
	return c.bucketFunc(c, name)
}

type bucketHandleMock struct {
	objectFunc func(mock *bucketHandleMock, name string) *objectHandleMock
}

func (h *bucketHandleMock) Object(name string) objectHandle {
	if h.objectFunc == nil {
		panic("unexpected call of Object")
	}
	return h.objectFunc(h, name)
}

type objectHandleMock struct {
	generation     int64
	attrFunc       func(ctx context.Context, mock *objectHandleMock) (attrs *storage.ObjectAttrs, err error)
	newReaderFunc  func(ctx context.Context, mock *objectHandleMock) (storage.ReaderObjectAttrs, io.ReadCloser, error)
	generationFunc func(mock *objectHandleMock, gen int64) *objectHandleMock
}

func (h *objectHandleMock) Attrs(ctx context.Context) (attrs *storage.ObjectAttrs, err error) {
	if h.attrFunc == nil {
		panic("unexpected call of Attrs")
	}
	return h.attrFunc(ctx, h)
}

func (h *objectHandleMock) NewReader(ctx context.Context) (storageReader, error) {
	attrs, reader, err := h.newReaderFunc(ctx, h)
	if err != nil {
		return nil, err
	}
	return &storageReaderMock{
		ReadCloser: reader,
		attrs:      attrs,
	}, nil
}

func (h *objectHandleMock) Generation(gen int64) objectHandle {
	if h.generationFunc == nil {
		panic("unexpected call of Generation")
	}
	return h.generationFunc(h, gen)
}

type storageReaderMock struct {
	io.ReadCloser
	attrs storage.ReaderObjectAttrs
}

func (r *storageReaderMock) Attrs() storage.ReaderObjectAttrs {
	return r.attrs
}
