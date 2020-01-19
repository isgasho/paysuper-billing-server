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
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/currenciespb"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	tools "github.com/paysuper/paysuper-tools/number"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

type TurnoversTestSuite struct {
	suite.Suite
	service          *Service
	log              *zap.Logger
	cache            database.CacheInterface
	country          *billingpb.Country
	operatingCompany *billingpb.OperatingCompany
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

	pg := &billingpb.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
		IsActive: true,
	}
	if err := suite.service.priceGroupRepository.Insert(context.TODO(), pg); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}

	countryRu := &billingpb.Country{
		Id:              primitive.NewObjectID().Hex(),
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "RUB",
		VatThreshold: &billingpb.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         3,
		VatDeadlineDays:        25,
		VatStoreYears:          5,
		VatCurrencyRatesPolicy: "last-day",
		VatCurrencyRatesSource: "cbrf",
	}

	countryTr := &billingpb.Country{
		Id:              primitive.NewObjectID().Hex(),
		IsoCodeA2:       "TR",
		Region:          "West Asia",
		Currency:        "TRY",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    pg.Id,
		VatCurrency:     "TRY",
		VatThreshold: &billingpb.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         1,
		VatDeadlineDays:        0,
		VatStoreYears:          10,
		VatCurrencyRatesPolicy: "on-day",
		VatCurrencyRatesSource: "cbtr",
	}

	countries := []*billingpb.Country{countryRu, countryTr}
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

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, currenciespb.RateTypeCentralbanks, "", suite.operatingCompany.Id)
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
	err := suite.service.updateOrderView(context.TODO(), []string{})
	assert.NoError(suite.T(), err)

	country, err := suite.service.country.GetByIsoCodeA2(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)

	amount, err := suite.service.getTurnover(context.TODO(), from, to, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy, currenciespb.RateTypeCentralbanks, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref1 := suite.getTurnoverReference(from, to, suite.operatingCompany.Id, countryCode, country.VatCurrency, country.VatCurrencyRatesPolicy)
	assert.Equal(suite.T(), amount, ref1)

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), now.EndOfYear(), "", "RUB", "on-day", currenciespb.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), now.EndOfYear(), suite.operatingCompany.Id, "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref2)
	assert.True(suite.T(), ref2 > ref1)

	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)
	err = suite.service.updateOrderView(context.TODO(), []string{})
	assert.NoError(suite.T(), err)

	worldAmount, err = suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), now.EndOfYear(), "", "RUB", "on-day", currenciespb.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	ref3 := suite.getTurnoverReference(now.BeginningOfYear(), now.EndOfYear(), suite.operatingCompany.Id, "", "RUB", "on-day")
	assert.Equal(suite.T(), worldAmount, ref3)
	assert.True(suite.T(), ref3 > ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_calcAnnualTurnover() {
	countryCode := "RU"

	suite.fillAccountingEntries(suite.operatingCompany.Id, countryCode, 10)
	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)
	err := suite.service.updateOrderView(context.TODO(), []string{})
	assert.NoError(suite.T(), err)

	err = suite.service.calcAnnualTurnover(context.TODO(), countryCode, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnoverRepository.Get(context.TODO(), suite.operatingCompany.Id, countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), now.EndOfDay(), suite.operatingCompany.Id, "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)
}

func (suite *TurnoversTestSuite) TestTurnovers_CalcAnnualTurnovers() {
	countryCode := "RU"

	suite.fillAccountingEntries(suite.operatingCompany.Id, countryCode, 10)
	suite.fillAccountingEntries(suite.operatingCompany.Id, "TR", 10)
	err := suite.service.updateOrderView(context.TODO(), []string{})
	assert.NoError(suite.T(), err)

	err = suite.service.CalcAnnualTurnovers(context.TODO(), &billingpb.EmptyRequest{}, &billingpb.EmptyResponse{})
	assert.NoError(suite.T(), err)

	at, err := suite.service.turnoverRepository.Get(context.TODO(), suite.operatingCompany.Id, countryCode, time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, countryCode)
	assert.Equal(suite.T(), at.Currency, "RUB")
	ref2 := suite.getTurnoverReference(now.BeginningOfYear(), time.Now(), suite.operatingCompany.Id, "RU", "RUB", "on-day")
	assert.Equal(suite.T(), at.Amount, ref2)

	at, err = suite.service.turnoverRepository.Get(context.TODO(), suite.operatingCompany.Id, "", time.Now().Year())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), at.Year, int32(time.Now().Year()))
	assert.Equal(suite.T(), at.Country, "")
	assert.Equal(suite.T(), at.Currency, "EUR")

	worldAmount, err := suite.service.getTurnover(context.TODO(), now.BeginningOfYear(), time.Now(), "", "EUR", "on-day", currenciespb.RateTypeOxr, "", suite.operatingCompany.Id)
	assert.Equal(suite.T(), tools.FormatAmount(at.Amount), tools.FormatAmount(worldAmount))

	countries, err := suite.service.country.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)

	n, err := suite.service.turnoverRepository.CountAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), len(countries.Countries)+1, n)
}

func (suite *TurnoversTestSuite) fillAccountingEntries(operatingCompanyId, countryCode string, daysMultiplier int) {
	maxEntries := 14
	maxDays := maxEntries * daysMultiplier

	country, err := suite.service.country.GetByIsoCodeA2(context.TODO(), countryCode)
	assert.NoError(suite.T(), err)

	handler := &accountingEntry{
		Service:  suite.service,
		ctx:      context.TODO(),
		order:    nil,
		refund:   nil,
		merchant: nil,
		country:  country,
		req:      &billingpb.CreateAccountingEntryRequest{},
	}

	currs := []string{"RUB", "USD"}
	types := []string{pkg.AccountingEntryTypeRealGrossRevenue, pkg.AccountingEntryTypeRealTaxFee}

	multiplier := 1
	startTime := time.Now()
	diff := now.New(startTime).EndOfYear().Sub(startTime)

	if int(diff.Hours()/24) < maxDays {
		multiplier = -1
	}

	count := 0
	for count <= maxEntries {

		date := startTime.AddDate(0, 0, multiplier*count*daysMultiplier)
		createdAt, err := ptypes.TimestampProto(date)
		assert.NoError(suite.T(), err)

		order := &billingpb.Order{
			Id:                 primitive.NewObjectID().Hex(),
			Status:             recurringpb.OrderPublicStatusProcessed,
			PrivateStatus:      recurringpb.OrderStatusProjectComplete,
			CreatedAt:          createdAt,
			TotalPaymentAmount: float64(count + 1),
			Currency:           currs[count%2],
			BillingAddress: &billingpb.OrderBillingAddress{
				Country: countryCode,
			},
			Tax: &billingpb.OrderTax{
				Type:     "",
				Rate:     0,
				Amount:   0,
				Currency: currs[count%2],
			},
			Project: &billingpb.ProjectOrder{
				Id:         primitive.NewObjectID().Hex(),
				MerchantId: primitive.NewObjectID().Hex(),
			},
			PaymentMethodOrderClosedAt: createdAt,
			OrderAmount:                float64(count),
			Type:                       pkg.OrderTypeOrder,
			OperatingCompanyId:         operatingCompanyId,
			ChargeCurrency:             currs[count%2],
			ChargeAmount:               float64(count + 1),
			IsProduction:               true,
		}

		_, err = suite.service.db.Collection(repository.CollectionOrder).InsertOne(context.TODO(), order)

		entry := &billingpb.AccountingEntry{
			Id:     primitive.NewObjectID().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   types[count%2],
			Source: &billingpb.AccountingEntrySource{
				Id:   order.Id,
				Type: repository.CollectionOrder,
			},
			MerchantId:         primitive.NewObjectID().Hex(),
			Status:             pkg.BalanceTransactionStatusAvailable,
			CreatedAt:          createdAt,
			Country:            countryCode,
			Amount:             float64(count + 1),
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
		"pm_order_close_date": bson.M{
			"$gte": from,
			"$lte": to,
		},
		"operating_company_id": operatingCompanyId,
		"is_production":        true,
		"type":                 pkg.OrderTypeOrder,
		"status":               recurringpb.OrderPublicStatusProcessed,
		"payment_gross_revenue_origin": bson.M{
			"$ne": nil,
		},
		"payment_gross_revenue_local": bson.M{
			"$ne": nil,
		},
	}
	if countryCode != "" {
		matchQuery["country_code"] = countryCode
	} else {
		matchQuery["country_code"] = bson.M{"$ne": ""}
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
	}

	switch currencyPolicy {
	case pkg.VatCurrencyRatesPolicyOnDay:
		query = append(query, bson.M{
			"$group": bson.M{
				"_id": "$payment_gross_revenue_local.currency",
				"amount": bson.M{
					"$sum": "$payment_gross_revenue_local.amount",
				},
			},
		})
		break

	case pkg.VatCurrencyRatesPolicyLastDay:
		query = append(query, bson.M{
			"$group": bson.M{
				"_id": "$payment_gross_revenue_origin.currency",
				"amount": bson.M{
					"$sum": "$payment_gross_revenue_origin.amount",
				},
			},
		})
		break

	default:
		return -1
	}

	var res []*turnoverQueryResItem

	cursor, err := suite.service.db.Collection(collectionOrderView).Aggregate(context.TODO(), query)
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

		req := &currenciespb.ExchangeCurrencyByDateCommonRequest{
			From:              v.Id,
			To:                targetCurrency,
			RateType:          currenciespb.RateTypeOxr,
			ExchangeDirection: currenciespb.ExchangeDirectionBuy,
			Amount:            v.Amount,
			Datetime:          nil,
		}

		rsp, err := suite.service.curService.ExchangeCurrencyByDateCommon(context.TODO(), req)
		assert.NoError(suite.T(), err)
		amount += rsp.ExchangedAmount
	}

	return amount
}
