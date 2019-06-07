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
	Delete(string) error
	Clean()
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
		return err
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

func (c *Cache) Clean() {
	c.redis.FlushAll()
}
