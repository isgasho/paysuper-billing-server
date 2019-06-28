package service

import (
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

type CurrencyRateTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
	rate    *billing.CurrencyRate
}

func Test_CurrencyRate(t *testing.T) {
	suite.Run(t, new(CurrencyRateTestSuite))
}

func (suite *CurrencyRateTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	suite.rate = &billing.CurrencyRate{
		Id:           bson.NewObjectId().Hex(),
		CurrencyFrom: 643,
		CurrencyTo:   641,
		Rate:         1,
		Date:         ptypes.TimestampNow(),
		IsActive:     true,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, nil, nil)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	if err = suite.service.currencyRate.Insert(suite.rate); err != nil {
		suite.FailNow("Insert rates test data failed", "%v", err)
	}
}

func (suite *CurrencyRateTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *CurrencyRateTestSuite) TestCurrencyRate_Get_Ok() {
	c, err := suite.service.currencyRate.GetFromTo(643, 641)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), suite.rate.Id, c.Id)
}

func (suite *CurrencyRateTestSuite) TestCurrencyRate_Get_NotFound() {
	c, err := suite.service.currencyRate.GetFromTo(641, 641)

	assert.Nil(suite.T(), c)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCurrencyRate))
}

func (suite *CurrencyRateTestSuite) TestCurrencyRate_Convert_Ok() {
	a, err := suite.service.currencyRate.Convert(643, 641, 10)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(10), a)
}

func (suite *CurrencyRateTestSuite) TestCurrencyRate_Convert_NotFound() {
	_, err := suite.service.currencyRate.Convert(641, 641, 10)

	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCurrencyRate))
}
