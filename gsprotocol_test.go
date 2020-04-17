package gsprotocol_test

import (
	"context"
	"io"
	"net/http"
	"os"

	"github.com/shogo82148/gsprotocol"
	"google.golang.org/api/option"
)

func ExampleNewTransport() {
	tr := &http.Transport{}
	gs, err := gsprotocol.NewTransport(context.Background(), option.WithoutAuthentication())
	if err != nil {
		panic(err)
	}
	tr.RegisterProtocol("gs", gs)
	c := &http.Client{Transport: tr}

	resp, err := c.Get("gs://shogo82148-gsprotocol/example.txt")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		panic(err)
	}

	// Output:
	// Hello Google Cloud Storage!
}
