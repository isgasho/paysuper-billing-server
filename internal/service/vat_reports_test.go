package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type VatReportsTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
	country *billing.Country
}

func Test_VatReports(t *testing.T) {
	suite.Run(t, new(VatReportsTestSuite))
}

func (suite *VatReportsTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pg := &billing.PriceGroup{
		Id:       bson.NewObjectId().Hex(),
		Currency: "USD",
		IsSimple: true,
		Region:   "",
	}
	if err := suite.service.priceGroup.Insert(pg); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}

	countryRu := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "RUB",
		VatRate:         20,
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

	countryTr := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "TR",
		Region:          "West Asia",
		Currency:        "TRY",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "TRY",
		VatRate:         20,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         1,
		VatDeadlineDays:        0,
		VatStoreYears:          10,
		VatCurrencyRatesPolicy: "on-day",
		VatCurrencyRatesSource: "cbtr",
	}

	countryAd := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "AD",
		Region:          "South Europe",
		Currency:        "EUR",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      false,
		PriceGroupId:    pg.Id,
		VatCurrency:     "EUR",
		VatRate:         0,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         0,
		VatDeadlineDays:        0,
		VatStoreYears:          0,
		VatCurrencyRatesPolicy: "",
		VatCurrencyRatesSource: "",
	}

	countryCo := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "CO",
		Region:          "South America",
		Currency:        "COP",
		PaymentsAllowed: false,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "COP",
		VatRate:         0,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         2,
		VatDeadlineDays:        14,
		VatStoreYears:          10,
		VatCurrencyRatesPolicy: "on-day",
		VatCurrencyRatesSource: "cbco",
	}

	countries := []*billing.Country{countryRu, countryTr, countryAd, countryCo}
	if err := suite.service.country.MultipleInsert(countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *VatReportsTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *VatReportsTestSuite) TestVatReports_getLastVatReportTime() {
	_, _, err := suite.service.getLastVatReportTime(0)
	assert.Error(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfQuarter())
	assert.Equal(suite.T(), to, now.EndOfQuarter())

	from, to, err = suite.service.getLastVatReportTime(int32(1))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfMonth())
	assert.Equal(suite.T(), to, now.EndOfMonth())

	fromRef := now.BeginningOfMonth()
	toRef := now.EndOfMonth()

	if fromRef.Month()%2 == 0 {
		fromRef = fromRef.AddDate(0, -1, 0)
	} else {
		toRef = toRef.AddDate(0, 1, 0)
	}

	from, to, err = suite.service.getLastVatReportTime(int32(2))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, fromRef)
	assert.Equal(suite.T(), to, toRef)
	assert.Equal(suite.T(), fromRef.Month()%2, time.Month(1))
	assert.Equal(suite.T(), toRef.Month()%2, time.Month(0))
}

func (suite *VatReportsTestSuite) TestVatReports_getVatReportTimeForDate() {

	t, err := time.Parse(time.RFC3339, "2019-06-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	from, to, err = suite.service.getVatReportTimeForDate(int32(1), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-06-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-05-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-05-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-05-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-07-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-07-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-08-31T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-08-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-07-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-08-31T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-04-01T00:00:00Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-06-30T23:59:59Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")
}
