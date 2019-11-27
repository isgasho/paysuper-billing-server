package service

import (
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type CacheTestSuite struct {
	suite.Suite
	service *Service
	redis   redis.Cmdable
	cache   CacheInterface
	log     *zap.Logger
}

func Test_Cache(t *testing.T) {
	suite.Run(t, new(CacheTestSuite))
}

func (suite *CacheTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.redis = database.NewRedis(
		&redis.Options{
			Addr:     cfg.RedisHost,
			Password: cfg.RedisPassword,
		},
	)
	suite.cache, err = NewCacheRedis(suite.redis, "cache")

	suite.service = NewBillingService(
		nil,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *CacheTestSuite) TearDownTest() {
	suite.cache.FlushAll()
}

func (suite *CacheTestSuite) TestCache_HasVersionToClean_ReturnFalse() {
	res, err := suite.cache.HasVersionToClean(2)
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), res)
}

func (suite *CacheTestSuite) TestCache_HasVersionToClean_ReturnFalse_ExistsVersion() {
	versionLimit := 2
	for i := 0; i <= versionLimit; i++ {
		_, _ = NewCacheRedis(suite.redis, "cache")
	}
	res, err := suite.cache.HasVersionToClean(versionLimit)
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), res)
}

func (suite *CacheTestSuite) TestCache_HasVersionToClean_ReturnTrue() {
	versionLimit := 2
	for i := 0; i <= versionLimit; i++ {
		_, _ = NewCacheRedis(suite.redis, fmt.Sprintf("cache%d", i))
	}
	res, err := suite.cache.HasVersionToClean(versionLimit)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), res)
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_NoOldestVersions() {
	cleaned, err := suite.cache.CleanOldestVersion(2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, cleaned)
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_ReturnTrue() {
	versionLimit := 2
	for i := 0; i <= versionLimit; i++ {
		_, _ = NewCacheRedis(suite.redis, fmt.Sprintf("cache%d", i))
	}
	cleaned, err := suite.cache.CleanOldestVersion(versionLimit)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 2, cleaned)
}

func (suite *CacheTestSuite) TestCache_CleanOldestVersion_SuccessfullyDeletedKeys() {
	versionLimit := 2
	oldestCache, _ := NewCacheRedis(suite.redis, "cache_old")
	_, _ = NewCacheRedis(suite.redis, "cache_new1")
	_, _ = NewCacheRedis(suite.redis, "cache_new2")

	_ = oldestCache.Set("test", 1, 0)

	var val1 interface{}
	_ = oldestCache.Get("test", &val1)
	assert.NotEmpty(suite.T(), val1)

	_, err := suite.cache.CleanOldestVersion(versionLimit)
	assert.NoError(suite.T(), err)

	var val2 interface{}
	_ = oldestCache.Get("test", &val2)
	assert.Empty(suite.T(), val2)
}
