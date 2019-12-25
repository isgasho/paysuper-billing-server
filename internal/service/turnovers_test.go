package service

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/repository"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
	"time"
)

type TurnoversTestSuite struct {
	suite.Suite
	service          *Service
	log              *zap.Logger
	cache            database.CacheInterface
	country          *billing.Country
	operatingCompany *billing.OperatingCompany
}

func Test_Turnovers(t *testing.T) {
	suite.Run(t, new(TurnoversTestSuite))
}

func (suite *TurnoversTestSuite) SetupTest() {
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
	suite.cache, err = database.NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
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

	suite.operatingCompany = helperOperatingCompany(suite.Suite, suite.service)

	pg := &billing.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
		IsActive: true,
	}
	if err := suite.service.priceGroup.Insert(context.TODO(), pg); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}

	countryRu := &billing.Country{
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

	countryTr := &billing.Country{
		Id:              primitive.NewObjectID().Hex(),
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
	if err := suite.service.country.MultipleInsert(context.TODO(), countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *TurnoversTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *TurnoversTestSuite) TestTurnovers_getTurnover_Empty() {
	countryCode := "RU"

	country, err := suite.service.country.GetByIsoCodeA2(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, curPkg.RateTypeCentralbanks, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), amount, float64(0))
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover_World() {
	countryCode := ""
	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover_Empty() {
	countryCode := "RU"
	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) TestTurnovers_getTurnover() {
	countryCode := "RU"

	suite.fillAccountingEntries(suite.operatingCompany.Id, countryCode, 10)

	country, err := suite.service.country.GetByIsoCodeA2(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, curPkg.RateTypeCentralbanks, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref1 := suite.getTurnoverReference(from, to, suite.operatingCompany.Id, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy)
	assert.Equal(suite.T(), amount, ref1)

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "RUB", "on-day", curPkg.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), suite.operatingCompany.Id, "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref2)
	assert.True(suite.T(), ref2 > ref1)

	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)
	worldAmount, err = suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "RUB", "on-day", curPkg.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref3 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), suite.operatingCompany.Id, "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref3)
	assert.True(suite.T(), ref3 > ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover() {
	countryCode := "RU"

	suite.fillAccountingEntries(suite.operatingCompany.Id, countryCode, 10)
	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)

	err := suite.service.calcAnnualTurnover(context.TODO(), countryCode, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnover.Get(context.TODO(), suite.operatingCompany.Id, countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), suite.operatingCompany.Id, "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_CalcAnnualTurnovers() {
	countryCode := "RU"

	suite.fillAccountingEntries(suite.operatingCompany.Id, countryCode, 10)
	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)

	err := suite.service.CalcAnnualTurnovers(context.TODO(), &grpc.EmptyRequest{}, &grpc.EmptyResponse{})
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnover.Get(context.TODO(), suite.operatingCompany.Id, countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), suite.operatingCompany.Id, "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)

	at, err = suite.service.turnover.Get(context.TODO(), suite.operatingCompany.Id, "", time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, "")
	assert.Equal(suite.T(), at.Currency, "EUR")

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "EUR", "on-day", curPkg.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.Equal(suite.T(), at.Amount, worldAmount)

	countries, err := suite.service.country.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)

	n, err := suite.service.db.Collection(collectionAnnualTurnovers).CountDocuments(context.TODO(), bson.M{})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), len(countries.Countries)+1, n)
}

func (suite *TurnoversTestSuite) fillAccountingEntries(operatingCompanyId, countryCode string, daysMultiplier int) {
	country, err := suite.service.country.GetByIsoCodeA2(context.TODO(), countryCode)
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
			Id:     primitive.NewObjectID().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   types[count%2],
			Source: &billing.AccountingEntrySource{
				Id:   primitive.NewObjectID().Hex(),
				Type: repository.CollectionOrder,
			},
			MerchantId:         primitive.NewObjectID().Hex(),
			Status:             pkg.BalanceTransactionStatusAvailable,
			CreatedAt:          createdAt,
			Country:            countryCode,
			Amount:             float64(count),
			Currency:           currs[count%2],
			OperatingCompanyId: operatingCompanyId,
		}
		err = handler.addEntry(entry)
		assert.NoError(suite.T(), err)
		count++
	}

	err = handler.saveAccountingEntries()
	assert.NoError(suite.T(), err)
}

func (suite *TurnoversTestSuite) getTurnoverReference(from, to time.Time, operatingCompanyId, countryCode, targetCurrency, currencyPolicy string) float64 {
	matchQuery := bson.M{
		"created_at":           bson.M{"$gte": from, "$lte": to},
		"type":                 bson.M{"$in": accountingEntriesForTurnover},
		"operating_company_id": operatingCompanyId,
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

	cursor, err := suite.service.db.Collection(collectionAccountingEntry).Aggregate(context.TODO(), query)
	assert.NoError(suite.T(), err)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0
		}
		assert.NoError(suite.T(), err)
	}

	err = cursor.All(context.TODO(), &res)
	assert.NoError(suite.T(), err)
	amount := float64(0)

	for _, v := range res {
		if v.Id == targetCurrency {
			amount += v.Amount
			continue
		}

		req := &currencies.ExchangeCurrencyByDateCommonRequest{
			From:              v.Id,
			To:                targetCurrency,
			RateType:          curPkg.RateTypeOxr,
			ExchangeDirection: curPkg.ExchangeDirectionBuy,
			Amount:            v.Amount,
			Datetime:          nil,
		}

		rsp, err := suite.service.curService.ExchangeCurrencyByDateCommon(context.TODO(), req)
		assert.NoError(suite.T(), err)
		amount += rsp.ExchangedAmount
	}

	return amount
}
