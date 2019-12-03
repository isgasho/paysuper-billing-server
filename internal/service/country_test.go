package service

import (
	"context"
	"errors"
	"fmt"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
)

type CountryTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
	country *billing.Country
}

func Test_Country(t *testing.T) {
	suite.Run(t, new(CountryTestSuite))
}

func (suite *CountryTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache, err = NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pg := &billing.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
		IsActive: true,
	}
	if err := suite.service.priceGroup.Insert(ctx, pg); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}

	suite.country = &billing.Country{
		Id:              primitive.NewObjectID().Hex(),
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "RUB",
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         3,
		VatDeadlineDays:        25,
		VatStoreYears:          5,
		VatCurrencyRatesPolicy: "last-day",
		VatCurrencyRatesSource: "cbrf",
	}
	if err := suite.service.country.Insert(ctx, suite.country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *CountryTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *CountryTestSuite) TestCountry_TestCountry() {

	req := &billing.GetCountryRequest{
		IsoCode: "RU",
	}
	res := &billing.Country{}
	err := suite.service.GetCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), res.PaymentsAllowed)

	req2 := &billing.Country{
		IsoCodeA2:       res.IsoCodeA2,
		Region:          res.Region,
		Currency:        res.Currency,
		PaymentsAllowed: false,
		ChangeAllowed:   res.ChangeAllowed,
		VatEnabled:      res.VatEnabled,
		VatCurrency:     res.VatCurrency,
		PriceGroupId:    res.PriceGroupId,
	}

	res2 := &billing.Country{}
	err = suite.service.UpdateCountry(context.TODO(), req2, res2)
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), res2.PaymentsAllowed)

	res3 := &billing.Country{}
	err = suite.service.GetCountry(context.TODO(), req, res3)
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), res3.PaymentsAllowed)
}

func (suite *CountryTestSuite) TestCountry_GetCountryByCodeA2_Ok() {
	c, err := suite.service.country.GetByIsoCodeA2(ctx, "RU")

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), suite.country.IsoCodeA2, c.IsoCodeA2)
}

func (suite *CountryTestSuite) TestCountry_GetCountryByCodeA2_NotFound() {
	_, err := suite.service.country.GetByIsoCodeA2(ctx, "AAA")

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCountry))
}

func (suite *CountryTestSuite) TestCountry_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.country.Insert(ctx, &billing.Country{IsoCodeA2: "RU"}))
}

func (suite *CountryTestSuite) TestCountry_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	ci.On("Set", "country:code_a2:AAA", mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.country.Insert(ctx, &billing.Country{IsoCodeA2: "AAA"})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *CountryTestSuite) TestCountry_GetAll_Ok() {
	// initially cache is empty
	c1 := &billing.CountriesList{}
	err := suite.service.cacher.Get(cacheCountryAll, c1)
	assert.EqualError(suite.T(), err, "redis: nil")

	// filling the cache
	c2 := &billing.CountriesList{}
	c2, err = suite.service.country.GetAll(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c2)
	assert.True(suite.T(), len(c2.Countries) > 0)

	// cache is already fulfilled
	c3 := &billing.CountriesList{}
	err = suite.service.cacher.Get(cacheCountryAll, c3)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), c3)
	assert.True(suite.T(), len(c3.Countries) > 0)

	// saving db connection and broke service db connection
	db := suite.service.db
	suite.service.db = nil

	// reading from cache, not from db
	c4 := &billing.CountriesList{}
	c4, err = suite.service.country.GetAll(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c4)
	assert.True(suite.T(), len(c4.Countries) > 0)

	// restoring db connection
	suite.service.db = db

	// inserting new country must clear cacheCountryAll cache
	assert.NoError(suite.T(), suite.service.country.Insert(ctx, &billing.Country{IsoCodeA2: "RU"}))
	c5 := &billing.CountriesList{}
	err = suite.service.cacher.Get(cacheCountryAll, c5)
	assert.EqualError(suite.T(), err, "redis: nil")
}

func (suite *CountryTestSuite) TestCountry_GetCountriesWithVatEnabled_Ok() {
	// initially cache is empty
	c1 := &billing.CountriesList{}
	err := suite.service.cacher.Get(cacheCountriesWithVatEnabled, c1)
	assert.EqualError(suite.T(), err, "redis: nil")

	// filling the cache
	c2 := &billing.CountriesList{}
	c2, err = suite.service.country.GetCountriesWithVatEnabled(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c2)
	assert.True(suite.T(), len(c2.Countries) > 0)

	// cache is already fulfilled
	c3 := &billing.CountriesList{}
	err = suite.service.cacher.Get(cacheCountriesWithVatEnabled, c3)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), c3)
	assert.True(suite.T(), len(c3.Countries) > 0)

	// saving db connection and broke service db connection
	db := suite.service.db
	suite.service.db = nil

	// reading from cache, not from db
	c4 := &billing.CountriesList{}
	c4, err = suite.service.country.GetCountriesWithVatEnabled(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c4)
	assert.True(suite.T(), len(c4.Countries) > 0)

	// restoring db connection
	suite.service.db = db

	// inserting new country must clear cacheCountryAll cache
	assert.NoError(suite.T(), suite.service.country.Insert(ctx, &billing.Country{IsoCodeA2: "US"}))
	c5 := &billing.CountriesList{}
	err = suite.service.cacher.Get(cacheCountriesWithVatEnabled, c5)
	assert.EqualError(suite.T(), err, "redis: nil")
}
