/*
Package gsprotocol provides the http.RoundTripper interface for Google Cloud Storage.

The typical use case is to register the "gs" protocol with a http.Transport, as in:

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

Google Cloud Storage supports object versioning.
To access the noncurrent version of an object, use a uri like gs://[BUCKET_NAME]/[OBJECT_NAME]#[GENERATION_NUMBER].
For example,

	resp, err := c.Get("gs://shogo82148-gsprotocol/example.txt#1587160158394554")
*/
package gsprotocol
