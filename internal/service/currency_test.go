package service

import (
	"errors"
	"fmt"
	"github.com/elliotchance/redismock"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

func InitTestCurrency(db *database.Source, country []interface{}) error {
	if err := db.Collection(collectionCurrency).Insert(country...); err != nil {
		return err
	}

	return nil
}

type CurrencyTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cfg     *config.Config
	db      *database.Source
	cache   CacheInterface
	redis   *redismock.ClientMock
}

func Test_Currency(t *testing.T) {
	suite.Run(t, new(CurrencyTestSuite))
}

func (suite *CurrencyTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	settings := database.Connection{
		Host:     cfg.MongoHost,
		Database: cfg.MongoDatabase,
		User:     cfg.MongoUser,
		Password: cfg.MongoPassword,
	}

	db, err := database.NewDatabase(settings)

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.redis = mock.NewTestRedis()
	suite.cache = NewCacheRedis(suite.redis)
	suite.service = NewBillingService(db, cfg, make(chan bool, 1), nil, nil, nil, nil, nil, suite.cache)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.db = db
	suite.cfg = cfg
}

func (suite *CurrencyTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *CurrencyTestSuite) TestCurrency_GetCurrencyByCodeA3_Ok() {
	c, err := suite.service.currency.GetByCodeA3("RUB")

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), int32(643), c.CodeInt)
}

func (suite *CurrencyTestSuite) TestCurrency_GetCurrencyByCodeA3_Ok_ByCache() {
	ci := &mock.CacheInterface{}
	ci.On("Get", "currency:code_a3:RUB", mock2.Anything).
		Return(nil)
	suite.service.cacher = ci
	c, err := suite.service.currency.GetByCodeA3("RUB")

	assert.Nil(suite.T(), err)
	assert.IsType(suite.T(), &billing.Currency{}, c)
	assert.Equal(suite.T(), int32(0), c.CodeInt)
}

func (suite *CurrencyTestSuite) TestCurrency_GetCurrencyByCodeA3_NotFound() {
	_, err := suite.service.currency.GetByCodeA3("AAA")

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCurrency))
}

func (suite *CurrencyTestSuite) TestCurrency_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.currency.Insert(&billing.Currency{CodeA3: "RUB"}))
}

func (suite *CurrencyTestSuite) TestCurrency_Insert_ErrorCacheUpdate() {
	ci := &mock.CacheInterface{}
	ci.On("Set", "currency:code_a3:AAA", mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.currency.Insert(&billing.Currency{CodeA3: "AAA"})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}
