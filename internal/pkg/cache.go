package pkg

import "time"

type CacheInterface interface {
	Set(string, interface{}, time.Duration) error
	Get(string, interface{}) error
	Delete(string) error
	Clean()
}
