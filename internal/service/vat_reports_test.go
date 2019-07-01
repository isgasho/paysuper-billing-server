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
	_, _, err := suite.service.getLastVatReportTime("")
	assert.Error(suite.T(), err)

	_, _, err = suite.service.getLastVatReportTime("AD")
	assert.Error(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime("RU")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfQuarter())
	assert.Equal(suite.T(), to, now.EndOfQuarter())

	from, to, err = suite.service.getLastVatReportTime("TR")
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

	from, to, err = suite.service.getLastVatReportTime("CO")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, fromRef)
	assert.Equal(suite.T(), to, toRef)
	assert.Equal(suite.T(), fromRef.Month()%2, time.Month(1))
	assert.Equal(suite.T(), toRef.Month()%2, time.Month(0))
}

func (suite *VatReportsTestSuite) TestVatReports_getPreviousVatReportTime() {
	from, to, err := suite.service.getLastVatReportTime("RU")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfQuarter())
	assert.Equal(suite.T(), to, now.EndOfQuarter())

	fromPrev, toPrev, errPrev := suite.service.getPreviousVatReportTime("RU", 0)
	assert.NoError(suite.T(), errPrev)
	assert.Equal(suite.T(), fromPrev, now.BeginningOfQuarter())
	assert.Equal(suite.T(), toPrev, now.EndOfQuarter())

	fromRef := now.BeginningOfQuarter().AddDate(0, -3, 0)
	toRef := now.EndOfQuarter().AddDate(0, -3, 0)
	fromPrev, toPrev, errPrev = suite.service.getPreviousVatReportTime("RU", 1)
	assert.NoError(suite.T(), errPrev)
	assert.NotEqual(suite.T(), fromPrev, now.BeginningOfQuarter())
	assert.NotEqual(suite.T(), toPrev, now.EndOfQuarter())
	assert.Equal(suite.T(), fromPrev, fromRef)
	assert.Equal(suite.T(), toPrev, toRef)

	countryCode := "RU"
	country, err := suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)
	period := country.VatPeriodMonth
	assert.Equal(suite.T(), period, int32(3))
	count := 0
	for count <= 14 {
		shift := -1 * count * int(period)
		fromRef = now.BeginningOfQuarter().AddDate(0, shift, 0)
		toRef = now.EndOfQuarter().AddDate(0, shift, 0)
		fromPrev, toPrev, errPrev = suite.service.getPreviousVatReportTime(countryCode, count)
		assert.NoError(suite.T(), errPrev)
		assert.Equal(suite.T(), fromPrev, fromRef)
		assert.Equal(suite.T(), toPrev, toRef)
		count++
	}
	begin := now.BeginningOfQuarter().AddDate(0, -1*(3*14), 0)
	assert.Equal(suite.T(), fromPrev.Year(), begin.Year())
	assert.Equal(suite.T(), fromPrev.Month(), begin.Month())

	countryCode = "TR"
	country, err = suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)
	period = country.VatPeriodMonth
	assert.Equal(suite.T(), period, int32(1))
	count = 0
	for count <= 14 {
		shift := -1 * count * int(period)
		fromRef = now.BeginningOfMonth().AddDate(0, shift, 0)
		toRef = now.EndOfMonth().AddDate(0, shift, 0)
		fromPrev, toPrev, errPrev = suite.service.getPreviousVatReportTime(countryCode, count)
		assert.NoError(suite.T(), errPrev)
		assert.Equal(suite.T(), fromPrev, fromRef)
		assert.Equal(suite.T(), toPrev, toRef)
		count++
	}
	begin = now.BeginningOfMonth().AddDate(0, -1*14, 0)
	assert.Equal(suite.T(), fromPrev.Year(), begin.Year())
	assert.Equal(suite.T(), fromPrev.Month(), begin.Month())

	countryCode = "CO"
	country, err = suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)
	period = country.VatPeriodMonth
	assert.Equal(suite.T(), period, int32(2))
	count = 0
	for count <= 14 {
		shift := -1 * count * int(period)
		fromRef = now.BeginningOfMonth()
		toRef = now.EndOfMonth()
		if fromRef.Month()%2 == 0 {
			fromRef = fromRef.AddDate(0, -1, 0)
		} else {
			toRef = toRef.AddDate(0, 1, 0)
		}
		fromRef = fromRef.AddDate(0, shift, 0)
		toRef = toRef.AddDate(0, shift, 0)
		fromPrev, toPrev, errPrev = suite.service.getPreviousVatReportTime(countryCode, count)
		assert.NoError(suite.T(), errPrev)
		assert.Equal(suite.T(), fromPrev, fromRef)
		assert.Equal(suite.T(), toPrev, toRef)
		count++
	}
	begin = now.BeginningOfMonth().AddDate(0, -1*(2*14)+(int((now.BeginningOfMonth()).Month()%2)-1), 0)
	assert.Equal(suite.T(), fromPrev.Year(), begin.Year())
	assert.Equal(suite.T(), fromPrev.Month(), begin.Month())
}
