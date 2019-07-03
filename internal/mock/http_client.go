package mock

import (
	"io/ioutil"
	"net/http"
	"strings"
)

type TransportStatusOk struct {
	Transport http.RoundTripper
	Err       error
}

type TransportStatusError struct {
	Transport http.RoundTripper
}

func NewClientStatusOk() *http.Client {
	return &http.Client{
		Transport: &TransportStatusOk{},
	}
}

func NewClientStatusError() *http.Client {
	return &http.Client{
		Transport: &TransportStatusError{},
	}
}

func (h *TransportStatusOk) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader("{}")),
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
