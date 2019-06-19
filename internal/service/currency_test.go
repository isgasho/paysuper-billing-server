package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/elliotchance/redismock"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

func InitTestCurrency(db *mongodb.Source, country []interface{}) error {
	if err := db.Collection(collectionCurrency).Insert(country...); err != nil {
		return err
	}

	return nil
}

type CurrencyTestSuite struct {
	suite.Suite
	service  *Service
	log      *zap.Logger
	cfg      *config.Config
	db       *mongodb.Source
	cache    CacheInterface
	redis    *redismock.ClientMock
	currency *billing.Currency
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

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.currency = &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{suite.currency}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.redis = mock.NewTestRedis()
	suite.cache = NewCacheRedis(suite.redis)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache)

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
	c, err := suite.service.currency.GetByCodeA3(suite.currency.CodeA3)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), suite.currency.CodeA3, c.CodeA3)
}

func (suite *CurrencyTestSuite) TestCurrency_GetCurrencyByCodeA3_Ok_ByCache() {
	ci := &mock.CacheInterface{}
	ci.On("Get", "currency:code_a3:"+suite.currency.CodeA3, mock2.Anything).
		Return(nil)
	suite.service.cacher = ci
	c, err := suite.service.currency.GetByCodeA3(suite.currency.CodeA3)

	assert.Nil(suite.T(), err)
	assert.IsType(suite.T(), &billing.Currency{}, c)
}

func (suite *CurrencyTestSuite) TestCurrency_GetCurrencyByCodeA3_NotFound() {
	_, err := suite.service.currency.GetByCodeA3("AAA")

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCurrency))
}

func (suite *CurrencyTestSuite) TestCurrency_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.currency.Insert(&billing.Currency{CodeA3: suite.currency.CodeA3}))
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

func (suite *CurrencyTestSuite) TestCurrency_GetByCodeInt_Ok() {
	c, err := suite.service.currency.GetByCodeInt(int(suite.currency.CodeInt))

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), int32(suite.currency.CodeInt), c.CodeInt)
}

func (suite *CurrencyTestSuite) TestCurrency_GetByCodeInt_Ok_ByCache() {
	ci := &mock.CacheInterface{}
	ci.On("Get", fmt.Sprintf(cacheCurrencyInt, suite.currency.CodeInt), mock2.Anything).
		Return(nil)
	suite.service.cacher = ci
	c, err := suite.service.currency.GetByCodeInt(int(suite.currency.CodeInt))

	assert.Nil(suite.T(), err)
	assert.IsType(suite.T(), &billing.Currency{}, c)
}

func (suite *CurrencyTestSuite) TestCurrency_GetByCodeInt_NotFound() {
	_, err := suite.service.currency.GetByCodeInt(3)

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCurrency))
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrencyList_Ok() {
	req := &grpc.GetCurrencyListRequest{
		Limit:  10,
		Offset: 0,
	}
	rsp := &billing.CurrencyList{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetAll", int(req.Offset), int(req.Limit)).
		Return([]*billing.Currency{suite.currency}, nil)
	suite.service.currency = currency

	err := suite.service.GetCurrencyList(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), rsp.Currency)
	assert.Len(suite.T(), rsp.Currency, 1)
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrencyList_ErrorService() {
	req := &grpc.GetCurrencyListRequest{
		Limit:  10,
		Offset: 0,
	}
	rsp := &billing.CurrencyList{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetAll", int(req.Offset), int(req.Limit)).
		Return(nil, errors.New("not found"))
	suite.service.currency = currency

	err := suite.service.GetCurrencyList(context.TODO(), req, rsp)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), rsp.Currency)
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrency_ByInt_Ok() {
	req := &billing.GetCurrencyRequest{Int: suite.currency.CodeInt}
	rsp := &billing.Currency{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetByCodeInt", int(suite.currency.CodeInt)).Return(suite.currency, nil)
	suite.service.currency = currency

	err := suite.service.GetCurrency(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), rsp.CodeInt)
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrency_ByInt_ErrorNotFound() {
	req := &billing.GetCurrencyRequest{Int: suite.currency.CodeInt}
	rsp := &billing.Currency{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetByCodeInt", int(suite.currency.CodeInt)).Return(nil, errors.New("not found"))
	suite.service.currency = currency

	err := suite.service.GetCurrency(context.TODO(), req, rsp)
	assert.Error(suite.T(), err)
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrency_ByA3_Ok() {
	req := &billing.GetCurrencyRequest{A3: suite.currency.CodeA3}
	rsp := &billing.Currency{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetByCodeA3", suite.currency.CodeA3).Return(suite.currency, nil)
	suite.service.currency = currency

	err := suite.service.GetCurrency(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), rsp.CodeA3)
}

func (suite *CurrencyTestSuite) TestPaymentMethod_GetCurrency_ByA3_ErrorNotFound() {
	req := &billing.GetCurrencyRequest{A3: suite.currency.CodeA3}
	rsp := &billing.Currency{}
	currency := &mock.CurrencyServiceInterface{}

	currency.On("GetByCodeA3", suite.currency.CodeA3).Return(nil, errors.New("not found"))
	suite.service.currency = currency

	err := suite.service.GetCurrency(context.TODO(), req, rsp)
	assert.Error(suite.T(), err)
}
