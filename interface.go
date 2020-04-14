package gsprotocol

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// the interface for Dependency injection

// the interface for storage.Client
type storageClient interface {
	Bucket(name string) bucketHandle
}

// the interface for storage.BucketHandle
type bucketHandle interface {
	Object(name string) objectHandle
}

// the interface for storage.ObjectHandle
type objectHandle interface {
	Attrs(ctx context.Context) (attrs *storage.ObjectAttrs, err error)
	NewReader(ctx context.Context) (storageReader, error)
	Generation(gen int64) objectHandle
}

type storageReader interface {
	io.ReadCloser
	Attrs() storage.ReaderObjectAttrs
}
