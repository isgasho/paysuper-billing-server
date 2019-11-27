package service

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

const (
	CacheStorageKey = "cache:%s:%s"
	CacheVersionKey = "cache:versions"
)

type CacheInterface interface {
	Set(string, interface{}, time.Duration) error
	Get(string, interface{}) error
	Delete(string) error
	FlushAll()
	CleanOldestVersion(int) (int, error)
	HasVersionToClean(int) (bool, error)
}

type Cache struct {
	redis redis.Cmdable
	hash  string
}

func NewCacheRedis(r redis.Cmdable, hash string) (*Cache, error) {
	result := r.ZAdd(CacheVersionKey, redis.Z{Member: hash, Score: float64(time.Now().UnixNano())})

	if result.Err() != nil {
		return nil, result.Err()
	}

	return &Cache{redis: r, hash: hash}, nil
}

func (c *Cache) Set(key string, value interface{}, duration time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := c.redis.Set(c.getStorageKey(key), b, duration).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(key string, obj interface{}) error {
	b, err := c.redis.Get(c.getStorageKey(key)).Bytes()

	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, obj); err != nil {
		return fmt.Errorf(errorInterfaceCast, err.Error())
	}

	return nil
}

func (c *Cache) Delete(key string) error {
	return c.redis.Del(c.getStorageKey(key)).Err()
}

func (c *Cache) FlushAll() {
	c.redis.FlushAll()
}

func (c *Cache) CleanOldestVersion(versionLimit int) (int, error) {
	needToClean, err := c.HasVersionToClean(versionLimit)

	if err != nil {
		return 0, err
	}

	if !needToClean {
		return 0, nil
	}

	res := c.redis.ZRevRange(CacheVersionKey, 0, -1)

	if res.Err() != nil {
		return 0, res.Err()
	}

	for _, val := range res.Val()[versionLimit:] {
		if err = c.cleanVersion(val); err != nil {
			return 0, err
		}
	}

	return len(res.Val()) - versionLimit, nil
}

func (c *Cache) cleanVersion(version string) error {
	var cursor uint64
	var limit int64 = 1
	var err error

	for {
		var keys []string
		keys, cursor, err = c.redis.Scan(cursor, fmt.Sprintf(CacheStorageKey, version, "*"), limit).Result()

		if err != nil {
			return err
		}

		if len(keys) > 0 {
			res := c.redis.Unlink(keys...)

			if res.Err() != nil {
				return err
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

func (c *Cache) HasVersionToClean(versionLimit int) (bool, error) {
	res := c.redis.ZCard(CacheVersionKey)

	if res.Err() != nil {
		return false, res.Err()
	}

	return int(res.Val()) > versionLimit, nil
}

func (c Cache) getStorageKey(key string) string {
	return fmt.Sprintf(CacheStorageKey, c.hash, key)
}
