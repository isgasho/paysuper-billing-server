package service

import (
	"context"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type TurnoversTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
	country *billing.Country
}

func Test_Turnovers(t *testing.T) {
	suite.Run(t, new(TurnoversTestSuite))
}

func (suite *TurnoversTestSuite) SetupTest() {
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

	countries := []*billing.Country{countryRu, countryTr}
	if err := suite.service.country.MultipleInsert(countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *TurnoversTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *TurnoversTestSuite) TestTurnovers_getTurnover_Empty() {
	countryCode := "RU"

	country, err := suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, curPkg.RateTypeCentralbanks)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), amount, float64(0))
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover_World() {
	countryCode := ""
	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover_Empty() {
	countryCode := "RU"
	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) TestTurnovers_getTurnover() {
	countryCode := "RU"

	suite.fillAccountingEntries(countryCode, 10)

	country, err := suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, curPkg.RateTypeCentralbanks)
	assert.NoError(suite.T(), err)
	ref1 := suite.getTurnoverReference(from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy)
	assert.Equal(suite.T(), amount, ref1)

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "RUB", "on-day", curPkg.RateTypeOxr)
	assert.NoError(suite.T(), err)
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref2)
	assert.True(suite.T(), ref2 > ref1)

	suite.fillAccountingEntries("TR", 10)
	worldAmount, err = suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "RUB", "on-day", curPkg.RateTypeOxr)
	assert.NoError(suite.T(), err)
	ref3 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref3)
	assert.True(suite.T(), ref3 > ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover() {
	countryCode := "RU"

	suite.fillAccountingEntries(countryCode, 10)
	suite.fillAccountingEntries("TR", 10)

	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnover.Get(countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_CalcAnnualTurnovers() {
	countryCode := "RU"

	suite.fillAccountingEntries(countryCode, 10)
	suite.fillAccountingEntries("TR", 10)

	err := suite.service.CalcAnnualTurnovers(context.TODO(), &grpc.EmptyRequest{}, &grpc.EmptyResponse{})
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnover.Get(countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)

	at, err = suite.service.turnover.Get("", time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, "")
	assert.Equal(suite.T(), at.Currency, "EUR")

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "EUR", "on-day", curPkg.RateTypeOxr)
	assert.Equal(suite.T(), at.Amount, worldAmount)

	countries, err := suite.service.country.GetCountriesWithVatEnabled()
	assert.NoError(suite.T(), err)

	n, err := suite.service.db.Collection(collectionAnnualTurnovers).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, len(countries.Countries)+1)
}

func (suite *TurnoversTestSuite) fillAccountingEntries(countryCode string, daysMultiplier int) {
	country, err := suite.service.country.GetByIsoCodeA2(countryCode)
	assert.NoError(suite.T(), err)

	handler := &accountingEntry{
		Service:  suite.service,
		ctx:      context.TODO(),
		order:    nil,
		refund:   nil,
		merchant: nil,
		country:  country,
		req:      &grpc.CreateAccountingEntryRequest{},
	}

	currs := []string{"RUB", "USD"}
	types := []string{pkg.AccountingEntryTypeRealGrossRevenue, pkg.AccountingEntryTypeRealTaxFee}

	timeNow := time.Now()

	count := 0
	for count <= 14 {

		date := timeNow.AddDate(0, 0, -1*count*daysMultiplier)
		createdAt, err := ptypes.TimestampProto(date)
		assert.NoError(suite.T(), err)

		entry := &billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   types[count%2],
			Source: &billing.AccountingEntrySource{
				Id:   bson.NewObjectId().Hex(),
				Type: collectionOrder,
			},
			MerchantId: bson.NewObjectId().Hex(),
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  createdAt,
			Country:    countryCode,
			Amount:     float64(count),
			Currency:   currs[count%2],
		}
		err = handler.addEntry(entry)
		assert.NoError(suite.T(), err)
		count++
	}

	err = handler.saveAccountingEntries()
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) getTurnoverReference(from, to time.Time, countryCode, targetCurrency, currencyPolicy string) float64 {
	matchQuery := bson.M{
		"created_at": bson.M{"$gte": from, "$lte": to},
		"type":       bson.M{"$in": accountingEntriesForTurnover},
	}
	if countryCode != "" {
		matchQuery["country"] = countryCode
	} else {
		matchQuery["country"] = bson.M{"$ne": ""}
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
	}

	switch currencyPolicy {
	case pkg.VatCurrencyRatesPolicyOnDay:
		query = append(query, bson.M{"$group": bson.M{"_id": "$local_currency", "amount": bson.M{"$sum": "$local_amount"}}})
		break
	case pkg.VatCurrencyRatesPolicyLastDay:
		query = append(query, bson.M{"$group": bson.M{"_id": "$original_currency", "amount": bson.M{"$sum": "$original_amount"}}})
		break
	default:
		return -1
	}

	var res []*turnoverQueryResItem

	err := suite.service.db.Collection(collectionAccountingEntry).Pipe(query).All(&res)

	if err != nil {
		if err == mgo.ErrNotFound {
			return 0
		}
		assert.NoError(suite.T(), err)
	}

	amount := float64(0)

	for _, v := range res {
		if v.Id == targetCurrency {
			amount += v.Amount
			continue
		}

		req := &currencies.ExchangeCurrencyByDateCommonRequest{
			From:     v.Id,
			To:       targetCurrency,
			RateType: "",
			Amount:   v.Amount,
			Datetime: nil,
		}

		rsp, err := suite.service.curService.ExchangeCurrencyByDateCommon(context.TODO(), req)
		assert.NoError(suite.T(), err)
		amount += rsp.ExchangedAmount
	}

	return amount
}
