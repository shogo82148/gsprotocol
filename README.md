![Test](https://github.com/shogo82148/gsprotocol/workflows/Test/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/shogo82148/gsprotocol)](https://pkg.go.dev/github.com/shogo82148/gsprotocol)

# gsprotocol

Package gsprotocol provides the [http.RoundTripper](https://golang.org/pkg/net/http/#RoundTripper) interface for [Google Cloud Storage](https://cloud.google.com/storage/docs).

The typical use case is to register the "gs" protocol with a [http.Transport](https://golang.org/pkg/net/http/#Transport), as in:

```go
tr := &http.Transport{}
gs, err := gsprotocol.NewTransport(context.Background(), option.WithoutAuthentication())
if err != nil {
    // handle error
}
tr.RegisterProtocol("gs", gs)
c := &http.Client{Transport: tr}

resp, err := c.Get("gs://shogo82148-gsprotocol/example.txt")
if err != nil {
    // handle error
}
defer resp.Body.Close()
// read resp.Body
```

Google Cloud Storage supports object versioning.
To access the noncurrent version of an object, use a uri like `gs://[BUCKET_NAME]/[OBJECT_NAME]#[GENERATION_NUMBER]`.
For example,

```go
resp, err := c.Get("gs://shogo82148-gsprotocol/example.txt#1587160158394554")
```
