package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type PaymentChannelCostSystemTestSuite struct {
	suite.Suite
	service                    *Service
	log                        *zap.Logger
	cache                      database.CacheInterface
	paymentChannelCostSystemId string
	operatingCompany           *billingpb.OperatingCompany
}

func Test_PaymentChannelCostSystem(t *testing.T) {
	suite.Run(t, new(PaymentChannelCostSystemTestSuite))
}

func (suite *PaymentChannelCostSystemTestSuite) SetupTest() {
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
	if err := suite.service.country.MultipleInsert(context.TODO(), countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.paymentChannelCostSystemId = primitive.NewObjectID().Hex()

	paymentChannelCostSystem := &billingpb.PaymentChannelCostSystem{
		Id:                 suite.paymentChannelCostSystemId,
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Percent:            1.5,
		FixAmount:          5,
		FixAmountCurrency:  "USD",
		IsActive:           false,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	anotherPaymentChannelCostSystem := &billingpb.PaymentChannelCostSystem{
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "",
		Percent:            2.2,
		FixAmount:          0,
		FixAmountCurrency:  "USD",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	pccs := []*billingpb.PaymentChannelCostSystem{paymentChannelCostSystem, anotherPaymentChannelCostSystem}
	if err := suite.service.paymentChannelCostSystem.MultipleInsert(context.TODO(), pccs); err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}
}

func (suite *PaymentChannelCostSystemTestSuite) TearDownTest() {
	suite.cache.FlushAll()
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GrpcGet_Ok() {
	req := &billingpb.PaymentChannelCostSystemRequest{
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	res := &billingpb.PaymentChannelCostSystemResponse{}

	err := suite.service.GetPaymentChannelCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(5))

	req.Country = ""
	err = suite.service.GetPaymentChannelCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(0))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GrpcSet_Ok() {
	req := &billingpb.PaymentChannelCostSystem{
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Percent:            1.7,
		FixAmount:          4,
		FixAmountCurrency:  "USD",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	res := billingpb.PaymentChannelCostSystemResponse{}

	err := suite.service.SetPaymentChannelCostSystem(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(4))
	assert.Equal(suite.T(), res.Item.Id, suite.paymentChannelCostSystemId)

	req2 := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "",
		Percent:            2.2,
		FixAmount:          1,
		FixAmountCurrency:  "USD",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	res2 := billingpb.PaymentChannelCostSystemResponse{}
	err = suite.service.SetPaymentChannelCostSystem(context.TODO(), req2, &res2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res2.Item.Country, "")
	assert.Equal(suite.T(), res2.Item.Region, billingpb.TariffRegionWorldwide)
	assert.Equal(suite.T(), res2.Item.FixAmount, float64(1))
	assert.NotEqual(suite.T(), res2.Item.Id, suite.paymentChannelCostSystemId)
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Insert_Ok() {
	req := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "",
		Percent:            2.2,
		FixAmount:          0,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostSystem.Insert(context.TODO(), req))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	ci.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("service unavailable"))
	ci.On("Delete", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("service unavailable"))
	suite.service.cacher = ci

	obj := &billingpb.PaymentChannelCostSystem{
		Name:               "Mastercard",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "",
		Percent:            2.1,
		FixAmount:          0,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	err := suite.service.paymentChannelCostSystem.Insert(context.TODO(), obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_UpdateOk() {
	obj := &billingpb.PaymentChannelCostSystem{
		Id:                 suite.paymentChannelCostSystemId,
		Name:               "Mastercard",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "",
		Percent:            2.1,
		FixAmount:          0,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostSystem.Update(context.TODO(), obj))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Get_Ok() {
	val, err := suite.service.paymentChannelCostSystem.Get(context.TODO(), "VISA", billingpb.TariffRegionRussiaAndCis, "AZ", billingpb.MccCodeLowRisk, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "AZ")
	assert.EqualValues(suite.T(), val.FixAmount, float64(5))

	val, err = suite.service.paymentChannelCostSystem.Get(context.TODO(), "VISA", billingpb.TariffRegionRussiaAndCis, "", billingpb.MccCodeLowRisk, suite.operatingCompany.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "")
	assert.EqualValues(suite.T(), val.FixAmount, float64(0))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Delete_Ok() {
	req := &billingpb.PaymentCostDeleteRequest{
		Id: suite.paymentChannelCostSystemId,
	}

	res := &billingpb.ResponseError{}
	err := suite.service.DeletePaymentChannelCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	_, err = suite.service.paymentChannelCostSystem.GetById(context.TODO(), suite.paymentChannelCostSystemId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GetAllPaymentChannelCostSystem_Ok() {
	res := &billingpb.PaymentChannelCostSystemListResponse{}
	err := suite.service.GetAllPaymentChannelCostSystem(context.TODO(), &billingpb.EmptyRequest{}, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), len(res.Item.Items), 2)
}
