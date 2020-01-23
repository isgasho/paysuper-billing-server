package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type MoneyBackCostMerchantTestSuite struct {
	suite.Suite
	service                 *Service
	log                     *zap.Logger
	cache                   database.CacheInterface
	moneyBackCostMerchantId string
	merchantId              string
}

func Test_MoneyBackCostMerchant(t *testing.T) {
	suite.Run(t, new(MoneyBackCostMerchantTestSuite))
}

func (suite *MoneyBackCostMerchantTestSuite) SetupTest() {
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	countryAz := &billingpb.Country{
		Id:                primitive.NewObjectID().Hex(),
		IsoCodeA2:         "AZ",
		Region:            "CIS",
		Currency:          "AZN",
		PaymentsAllowed:   true,
		ChangeAllowed:     true,
		VatEnabled:        true,
		PriceGroupId:      "",
		VatCurrency:       "AZN",
		PayerTariffRegion: billingpb.TariffRegionRussiaAndCis,
	}
	countryUs := &billingpb.Country{
		Id:                primitive.NewObjectID().Hex(),
		IsoCodeA2:         "US",
		Region:            "US",
		Currency:          "USD",
		PaymentsAllowed:   true,
		ChangeAllowed:     true,
		VatEnabled:        true,
		PriceGroupId:      "",
		VatCurrency:       "USD",
		PayerTariffRegion: billingpb.TariffRegionWorldwide,
	}
	countries := []*billingpb.Country{countryAz, countryUs}
	if err := suite.service.country.MultipleInsert(ctx, countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.moneyBackCostMerchantId = primitive.NewObjectID().Hex()
	suite.merchantId = primitive.NewObjectID().Hex()

	pmBankCard := &billingpb.PaymentMethod{
		Id:   primitive.NewObjectID().Hex(),
		Name: "Bank card",
	}
	merchant := &billingpb.Merchant{
		Id: suite.merchantId,
		PaymentMethods: map[string]*billingpb.MerchantPaymentMethod{
			pmBankCard.Id: {
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   pmBankCard.Id,
					Name: pmBankCard.Name,
				},
				Commission: &billingpb.MerchantPaymentMethodCommissions{
					Fee: 2.5,
					PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
						Fee:      30,
						Currency: "RUB",
					},
				},
				Integration: &billingpb.MerchantPaymentMethodIntegration{
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
	}
	if err := suite.service.merchantRepository.Insert(ctx, merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	moneyBackCostMerchant := &billingpb.MoneyBackCostMerchant{
		Id:                suite.moneyBackCostMerchantId,
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	moneyBackCostMerchant2 := &billingpb.MoneyBackCostMerchant{
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	anotherMoneyBackCostMerchant := &billingpb.MoneyBackCostMerchant{
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}
	pucs := []*billingpb.MoneyBackCostMerchant{moneyBackCostMerchant, moneyBackCostMerchant2, anotherMoneyBackCostMerchant}
	if err := suite.service.moneyBackCostMerchantRepository.MultipleInsert(ctx, pucs); err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}
}

func (suite *MoneyBackCostMerchantTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GrpcGet_Ok() {
	req := &billingpb.MoneyBackCostMerchantRequest{
		MerchantId:     suite.merchantId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         billingpb.TariffRegionRussiaAndCis,
		Country:        "AZ",
		Days:           10,
		PaymentStage:   1,
		MccCode:        billingpb.MccCodeLowRisk,
	}

	res := &billingpb.MoneyBackCostMerchantResponse{}

	err := suite.service.GetMoneyBackCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(5))
	assert.Equal(suite.T(), res.Item.Percent, float64(3))

	req.Country = ""
	err = suite.service.GetMoneyBackCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(3))
	assert.Equal(suite.T(), res.Item.Percent, float64(2))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GrpcSet_Ok() {
	req := &billingpb.MoneyBackCostMerchant{
		Id:                suite.moneyBackCostMerchantId,
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3.33,
		FixAmount:         7.5,
		FixAmountCurrency: "USD",
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	res := billingpb.MoneyBackCostMerchantResponse{}

	err := suite.service.SetMoneyBackCostMerchant(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(7.5))
	assert.Equal(suite.T(), res.Item.Id, suite.moneyBackCostMerchantId)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_getMoneyBackCostMerchant() {
	req := &billingpb.MoneyBackCostMerchantRequest{
		MerchantId:     suite.merchantId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         billingpb.TariffRegionRussiaAndCis,
		Country:        "AZ",
		Days:           5,
		PaymentStage:   1,
		MccCode:        billingpb.MccCodeLowRisk,
	}

	val, err := suite.service.getMoneyBackCostMerchant(ctx, req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(0))
	assert.Equal(suite.T(), val.Percent, float64(3))
	assert.Equal(suite.T(), val.FixAmount, float64(5))

	req.Days = 45
	val, err = suite.service.getMoneyBackCostMerchant(ctx, req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(30))
	assert.Equal(suite.T(), val.Percent, float64(10))
	assert.Equal(suite.T(), val.FixAmount, float64(15))
}
