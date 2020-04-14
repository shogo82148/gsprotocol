package gsprotocol

import (
	"context"

	"cloud.google.com/go/storage"
)

func newStorageClientImpl(client *storage.Client) storageClient {
	return storageClientImpl{client: client}
}

type storageClientImpl struct {
	client *storage.Client
}

func (c storageClientImpl) Bucket(name string) bucketHandle {
	return bucketHandleImpl{
		bucket: c.client.Bucket(name),
	}
}

type bucketHandleImpl struct {
	bucket *storage.BucketHandle
}

func (h bucketHandleImpl) Object(name string) objectHandle {
	return objectHandleImpl{
		object: h.bucket.Object(name),
	}
}

type objectHandleImpl struct {
	object *storage.ObjectHandle
}

func (h objectHandleImpl) NewReader(ctx context.Context) (storageReader, error) {
	reader, err := h.object.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return storageReaderImpl{
		reader: reader,
	}, nil
}

type storageReaderImpl struct {
	reader *storage.Reader
}

func (r storageReaderImpl) Attrs() storage.ReaderObjectAttrs {
	return r.reader.Attrs
}

func (r storageReaderImpl) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r storageReaderImpl) Close() error {
	return r.reader.Close()
}
