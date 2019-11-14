package mocks

import (
	"bytes"
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type mockContextKey struct {
	name string
}

type TransportStatusOk struct {
	Transport http.RoundTripper
	Err       error
}

type TransportCardPayOk struct {
	Transport http.RoundTripper
}

type TransportStatusError struct {
	Transport http.RoundTripper
}

func NewClientStatusOk() *http.Client {
	return &http.Client{
		Transport: &TransportStatusOk{},
	}
}

func NewCardPayHttpClientStatusOk() *http.Client {
	return &http.Client{
		Transport: &TransportCardPayOk{},
	}
}

func (h *TransportStatusOk) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := context.WithValue(req.Context(), &mockContextKey{name: "mockRequestStart"}, time.Now())
	req = req.WithContext(ctx)

	var reqBody []byte

	if req.Body != nil {
		reqBody, _ = ioutil.ReadAll(req.Body)
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBody))

	zap.L().Info(
		req.URL.Path,
		zap.Any("request_headers", req.Header),
		zap.ByteString("request_body", reqBody),
	)

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("{}")),
		Header:     make(http.Header),
	}, nil
}

func (h *TransportCardPayOk) RoundTrip(req *http.Request) (*http.Response, error) {
	body := []byte("{}")

	if req.URL.Path == pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Path {
		body = []byte(`{"token_type": "bearer", "access_token": "123", "refresh_token": "123", "expires_in": 300, "refresh_expires_in": 900}`)
	}

	if req.URL.Path == pkg.CardPayPaths[pkg.PaymentSystemActionCreatePayment].Path {
		body = []byte(`{"redirect_url": "http://localhost"}`)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
		Header:     make(http.Header),
	}, nil
}

func (h *TransportStatusError) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       ioutil.NopCloser(strings.NewReader("{}")),
		Header:     make(http.Header),
	}, nil
}
