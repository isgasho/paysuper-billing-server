package mock

import (
	"time"
)

type CacheMock struct{}

func NewCacheMock() *CacheMock {
	return &CacheMock{}
}

func (w *CacheMock) Set(string, interface{}, time.Duration) error {
	return nil
}

func (w *CacheMock) Get(string) (*[]byte, error) {
	return nil, nil
}
