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

type Cache struct {
	redis redis.Cmdable
}

func NewCacheRedis(redis redis.Cmdable) *Cache {
	return &Cache{redis: redis}
}

func (c *Cache) Set(key string, value interface{}, duration time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := c.redis.Set(fmt.Sprintf(CacheStorageKey, key), b, duration).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(key string, obj interface{}) error {
	b, err := c.redis.Get(fmt.Sprintf(CacheStorageKey, key)).Bytes()

	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, obj); err != nil {
		return fmt.Errorf(errorInterfaceCast, err.Error())
	}

	return nil
}

func (c *Cache) Delete(key string) error {
	return c.redis.Del(fmt.Sprintf(CacheStorageKey, key)).Err()
}

func (c *Cache) Clean() {
	c.redis.FlushAll()
}
