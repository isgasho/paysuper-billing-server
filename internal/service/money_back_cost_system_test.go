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

type MoneyBackCostSystemTestSuite struct {
	suite.Suite
	service               *Service
	log                   *zap.Logger
	cache                 database.CacheInterface
	moneyBackCostSystemId string
	merchantId            string
	operatingCompany      *billingpb.OperatingCompany
}

func Test_MoneyBackCostSystem(t *testing.T) {
	suite.Run(t, new(MoneyBackCostSystemTestSuite))
}

func (suite *MoneyBackCostSystemTestSuite) SetupTest() {
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
		suite.cache, mocks.NewCurrencyServiceMockOk(),
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

	suite.moneyBackCostSystemId = primitive.NewObjectID().Hex()
	suite.merchantId = primitive.NewObjectID().Hex()

	moneyBackCostSystem := &billingpb.MoneyBackCostSystem{
		Id:                 suite.moneyBackCostSystemId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            3,
		FixAmount:          5,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	moneyBackCostSystem2 := &billingpb.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           30,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	anotherMoneyBackCostSystem := &billingpb.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            2,
		FixAmount:          3,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	pucs := []*billingpb.MoneyBackCostSystem{moneyBackCostSystem, moneyBackCostSystem2, anotherMoneyBackCostSystem}
	if err := suite.service.moneyBackCostSystemRepository.MultipleInsert(ctx, pucs); err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}
}

func (suite *MoneyBackCostSystemTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GrpcGet_Ok() {
	req := &billingpb.MoneyBackCostSystemRequest{
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Days:               10,
		PaymentStage:       1,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	res := &billingpb.MoneyBackCostSystemResponse{}

	err := suite.service.GetMoneyBackCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(5))
	assert.Equal(suite.T(), res.Item.Percent, float64(3))

	req.Country = ""
	err = suite.service.GetMoneyBackCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(3))
	assert.Equal(suite.T(), res.Item.Percent, float64(2))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GrpcSet_Ok() {
	req := &billingpb.MoneyBackCostSystem{
		Id:                 suite.moneyBackCostSystemId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            3.33,
		FixAmount:          7.5,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	res := billingpb.MoneyBackCostSystemResponse{}

	err := suite.service.SetMoneyBackCostSystem(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.EqualValues(suite.T(), res.Item.FixAmount, 7.5)
	assert.Equal(suite.T(), res.Item.Id, suite.moneyBackCostSystemId)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_getMoneyBackCostSystem() {
	req := &billingpb.MoneyBackCostSystemRequest{
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Days:               5,
		PaymentStage:       1,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	val, err := suite.service.getMoneyBackCostSystem(ctx, req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(0))
	assert.Equal(suite.T(), val.Percent, float64(3))
	assert.Equal(suite.T(), val.FixAmount, float64(5))

	req.Days = 45
	val, err = suite.service.getMoneyBackCostSystem(ctx, req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(30))
	assert.Equal(suite.T(), val.Percent, float64(10))
	assert.Equal(suite.T(), val.FixAmount, float64(15))
}
