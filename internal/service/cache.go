package service

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

const (
	CacheStorageKey = "cache:%s"
)

type CacheInterface interface {
	Set(string, interface{}, time.Duration) error
	Get(string) (*[]byte, error)
	GetSet(string, func() (interface{}, error), time.Duration) (*[]byte, error)
}

type Cache struct {
	redis *redis.ClusterClient
}

func NewCacheRedis(redis *redis.ClusterClient) *Cache {
	return &Cache{redis: redis}
}

func (c *Cache) Set(key string, value interface{}, duration time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	result := c.redis.Set(fmt.Sprintf(CacheStorageKey, key), b, duration)
	if result.Err() != nil {
		return result.Err()
	}

	return nil
}

func (c *Cache) Get(key string) (*[]byte, error) {
	result := c.redis.Get(fmt.Sprintf(CacheStorageKey, key))
	b, err := result.Bytes()
	if err != nil {
		return nil, err
	}

	return &b, nil
}

func (c *Cache) Delete(key string) error {
	result := c.redis.Del(fmt.Sprintf(CacheStorageKey, key))
	return result.Err()
}

func (c *Cache) Clean() error {
	result := c.redis.FlushAll()
	return result.Err()
}

func (c *Cache) GetSet(key string, fn func() (interface{}, error), duration time.Duration) (*[]byte, error) {
	val, err := c.Get(key)
	if err != nil {
		r, err := fn()
		if err == nil {
			c.Set(key, r, duration)
			val = r.(*[]byte)
		}
	}

	return val, nil
}
