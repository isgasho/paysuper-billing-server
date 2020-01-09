package database

import (
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type CacheTestSuite struct {
	suite.Suite
	redis redis.Cmdable
	cache CacheInterface
	log   *zap.Logger
}

func Test_Cache(t *testing.T) {
	suite.Run(t, new(CacheTestSuite))
}

func (suite *CacheTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.redis = NewRedis(
		&redis.Options{
			Addr:     cfg.RedisHost,
			Password: cfg.RedisPassword,
		},
	)
	suite.cache, err = NewCacheRedis(suite.redis, "cache")
	assert.NoError(suite.T(), err)
}

func (suite *CacheTestSuite) TearDownTest() {
	suite.cache.FlushAll()
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_NoOldestVersions() {
	err := suite.cache.CleanOldestVersion()
	assert.NoError(suite.T(), err)
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_ReturnTrue() {
	for i := 0; i <= versionLimit; i++ {
		_, err := NewCacheRedis(suite.redis, fmt.Sprintf("cache%d", i))
		assert.NoError(suite.T(), err)
	}
	err := suite.cache.CleanOldestVersion()
	assert.NoError(suite.T(), err)
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_SuccessfullyDeletedKeys() {
	oldestCache, _ := NewCacheRedis(suite.redis, "cache_old")
	_, err := NewCacheRedis(suite.redis, "cache_new1")
	assert.NoError(suite.T(), err)
	_, err = NewCacheRedis(suite.redis, "cache_new2")
	assert.NoError(suite.T(), err)

	_ = oldestCache.Set("test", 1, 0)

	var val1 interface{}
	_ = oldestCache.Get("test", &val1)
	assert.NotEmpty(suite.T(), val1)

	err = suite.cache.CleanOldestVersion()
	assert.NoError(suite.T(), err)

	time.Sleep(1 * time.Second)

	var val2 interface{}
	_ = oldestCache.Get("test", &val2)
	assert.Empty(suite.T(), val2)
}
