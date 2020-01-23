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

type PaymentChannelCostMerchantTestSuite struct {
	suite.Suite
	service                      *Service
	log                          *zap.Logger
	cache                        database.CacheInterface
	paymentChannelCostMerchantId string
	merchantId                   string
}

func Test_PaymentChannelCostMerchant(t *testing.T) {
	suite.Run(t, new(PaymentChannelCostMerchantTestSuite))
}

func (suite *PaymentChannelCostMerchantTestSuite) SetupTest() {
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
	if err := suite.service.country.MultipleInsert(context.TODO(), countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.paymentChannelCostMerchantId = primitive.NewObjectID().Hex()
	suite.merchantId = primitive.NewObjectID().Hex()

	pmBankCard := &billingpb.PaymentMethod{
		Id:   primitive.NewObjectID().Hex(),
		Name: "Bank card",
	}
	merchant := &billingpb.Merchant{
		Id: suite.merchantId,
		Tariff: &billingpb.MerchantTariff{
			Payment: []*billingpb.MerchantTariffRatesPayment{
				{
					MinAmount:              0,
					MaxAmount:              100,
					MethodName:             "VISA",
					MethodPercentFee:       0,
					MethodFixedFee:         0,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           0,
					PsFixedFee:             0,
					PsFixedFeeCurrency:     "EUR",
					MerchantHomeRegion:     "",
					PayerRegion:            billingpb.TariffRegionRussiaAndCis,
					MccCode:                billingpb.MccCodeLowRisk,
					IsActive:               false,
				},
				{
					MinAmount:              0,
					MaxAmount:              100,
					MethodName:             "MASTERCARD",
					MethodPercentFee:       0,
					MethodFixedFee:         0,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           0,
					PsFixedFee:             0,
					PsFixedFeeCurrency:     "EUR",
					MerchantHomeRegion:     "",
					PayerRegion:            billingpb.TariffRegionRussiaAndCis,
					MccCode:                billingpb.MccCodeLowRisk,
					IsActive:               false,
				},
			},
		},
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
	if err := suite.service.merchantRepository.Insert(context.TODO(), merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	paymentChannelCostMerchant := &billingpb.PaymentChannelCostMerchant{
		Id:                      suite.paymentChannelCostMerchantId,
		MerchantId:              suite.merchantId,
		Name:                    "VISA",
		PayoutCurrency:          "USD",
		MinAmount:               0.75,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "AZ",
		MethodPercent:           1.5,
		MethodFixAmount:         0.01,
		MethodFixAmountCurrency: "USD",
		PsPercent:               3,
		PsFixedFee:              0.01,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	paymentChannelCostMerchant2 := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              suite.merchantId,
		Name:                    "VISA",
		PayoutCurrency:          "USD",
		MinAmount:               5,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "AZ",
		MethodPercent:           2.5,
		MethodFixAmount:         2,
		MethodFixAmountCurrency: "USD",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	anotherPaymentChannelCostMerchant := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              suite.merchantId,
		Name:                    "VISA",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "",
		MethodPercent:           2.2,
		MethodFixAmount:         0,
		MethodFixAmountCurrency: "USD",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}
	pccm := []*billingpb.PaymentChannelCostMerchant{paymentChannelCostMerchant, paymentChannelCostMerchant2, anotherPaymentChannelCostMerchant}
	if err := suite.service.paymentChannelCostMerchant.MultipleInsert(context.TODO(), pccm); err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}
}

func (suite *PaymentChannelCostMerchantTestSuite) TearDownTest() {
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

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GrpcGet_Ok() {
	req := &billingpb.PaymentChannelCostMerchantRequest{
		MerchantId:     suite.merchantId,
		PayoutCurrency: "USD",
		Name:           "VISA",
		Region:         billingpb.TariffRegionRussiaAndCis,
		Country:        "AZ",
		Amount:         10,
		MccCode:        billingpb.MccCodeLowRisk,
	}

	res := &billingpb.PaymentChannelCostMerchantResponse{}

	err := suite.service.GetPaymentChannelCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.MethodFixAmount, float64(2))
	assert.Equal(suite.T(), res.Item.MinAmount, float64(5))

	req.Country = ""
	err = suite.service.GetPaymentChannelCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.MethodFixAmount, float64(0))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GrpcSet_Ok() {
	req := &billingpb.PaymentChannelCostMerchant{
		Id:                      suite.paymentChannelCostMerchantId,
		MerchantId:              suite.merchantId,
		Name:                    "VISA",
		PayoutCurrency:          "USD",
		MinAmount:               1.75,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "AZ",
		MethodPercent:           2.5,
		MethodFixAmount:         1.01,
		MethodFixAmountCurrency: "USD",
		PsPercent:               2,
		PsFixedFee:              0.01,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
		IsActive:                true,
	}

	res := billingpb.PaymentChannelCostMerchantResponse{}

	err := suite.service.SetPaymentChannelCostMerchant(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.EqualValues(suite.T(), res.Item.MethodFixAmount, 1.01)
	assert.Equal(suite.T(), res.Item.Id, suite.paymentChannelCostMerchantId)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), suite.merchantId)
	assert.NoError(suite.T(), err)

	payment := merchant.Tariff.Payment[0]

	assert.EqualValues(suite.T(), req.MinAmount, payment.MinAmount)
	assert.EqualValues(suite.T(), req.MethodFixAmountCurrency, payment.MethodFixedFeeCurrency)
	assert.EqualValues(suite.T(), req.PsFixedFeeCurrency, payment.PsFixedFeeCurrency)
	assert.EqualValues(suite.T(), req.IsActive, payment.IsActive)
	assert.EqualValues(suite.T(), req.Name, payment.MethodName)
	assert.EqualValues(suite.T(), req.MccCode, payment.MccCode)
	assert.EqualValues(suite.T(), req.PsPercent, payment.PsPercentFee)
	assert.EqualValues(suite.T(), req.MethodFixAmount, payment.MethodFixedFee)
	assert.EqualValues(suite.T(), req.MethodPercent, payment.MethodPercentFee)
	assert.EqualValues(suite.T(), req.Region, payment.PayerRegion)
	assert.EqualValues(suite.T(), 100, payment.MaxAmount)

	payment = merchant.Tariff.Payment[1]

	assert.EqualValues(suite.T(), 0, payment.MinAmount)
	assert.EqualValues(suite.T(), "USD", payment.MethodFixedFeeCurrency)
	assert.EqualValues(suite.T(), "EUR", payment.PsFixedFeeCurrency)
	assert.EqualValues(suite.T(), false, payment.IsActive)
	assert.EqualValues(suite.T(), "MASTERCARD", payment.MethodName)
	assert.EqualValues(suite.T(), 0, payment.PsPercentFee)
	assert.EqualValues(suite.T(), 0, payment.MethodFixedFee)
	assert.EqualValues(suite.T(), 0, payment.MethodPercentFee)
	assert.EqualValues(suite.T(), 100, payment.MaxAmount)
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Insert_Ok() {
	req := &billingpb.PaymentChannelCostMerchant{
		MerchantId:      suite.merchantId,
		Name:            "MASTERCARD",
		Region:          "US",
		Country:         "",
		MethodPercent:   2.2,
		MethodFixAmount: 0,
		MccCode:         billingpb.MccCodeLowRisk,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostMerchant.Insert(context.TODO(), req))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	ci.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("service unavailable"))
	ci.On("Delete", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("service unavailable"))
	suite.service.cacher = ci

	obj := &billingpb.PaymentChannelCostMerchant{
		MerchantId:      suite.merchantId,
		Name:            "Mastercard",
		Region:          billingpb.TariffRegionWorldwide,
		Country:         "",
		MethodPercent:   2.1,
		MethodFixAmount: 0,
		MccCode:         billingpb.MccCodeLowRisk,
	}
	err := suite.service.paymentChannelCostMerchant.Insert(context.TODO(), obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_UpdateOk() {
	obj := &billingpb.PaymentChannelCostMerchant{
		Id:              suite.paymentChannelCostMerchantId,
		MerchantId:      suite.merchantId,
		Name:            "Mastercard",
		Region:          billingpb.TariffRegionWorldwide,
		Country:         "",
		MethodPercent:   2.1,
		MethodFixAmount: 0,
		MccCode:         billingpb.MccCodeLowRisk,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostMerchant.Update(context.TODO(), obj))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Get_Ok() {
	val, err := suite.service.paymentChannelCostMerchant.Get(context.TODO(), suite.merchantId, "VISA", "USD", billingpb.TariffRegionRussiaAndCis, "AZ", billingpb.MccCodeLowRisk)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val), 2)
	assert.Equal(suite.T(), val[0].Set[0].Country, "AZ")
	assert.EqualValues(suite.T(), val[0].Set[0].MethodFixAmount, 0.01)
	assert.Equal(suite.T(), val[1].Set[0].Country, "")

	val, err = suite.service.paymentChannelCostMerchant.Get(context.TODO(), suite.merchantId, "VISA", "USD", billingpb.TariffRegionRussiaAndCis, "", billingpb.MccCodeLowRisk)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val), 1)
	assert.Equal(suite.T(), val[0].Set[0].Country, "")
	assert.Equal(suite.T(), val[0].Set[0].MethodFixAmount, float64(0))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_getPaymentChannelCostMerchant() {
	req := &billingpb.PaymentChannelCostMerchantRequest{
		MerchantId:     suite.merchantId,
		PayoutCurrency: "USD",
		Name:           "VISA",
		Region:         billingpb.TariffRegionRussiaAndCis,
		Country:        "AZ",
		Amount:         0,
		MccCode:        billingpb.MccCodeLowRisk,
	}

	val, err := suite.service.getPaymentChannelCostMerchant(context.TODO(), req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "")
	assert.Equal(suite.T(), val.MinAmount, float64(0))
	assert.EqualValues(suite.T(), val.MethodPercent, 2.2)
	assert.EqualValues(suite.T(), val.MethodFixAmount, 0.)
	assert.EqualValues(suite.T(), val.PsPercent, 5)
	assert.EqualValues(suite.T(), val.PsFixedFee, 0.05)

	req.Amount = 1
	val, err = suite.service.getPaymentChannelCostMerchant(context.TODO(), req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "AZ")
	assert.EqualValues(suite.T(), val.MinAmount, 0.75)
	assert.EqualValues(suite.T(), val.MethodPercent, 1.5)
	assert.EqualValues(suite.T(), val.MethodFixAmount, 0.01)
	assert.EqualValues(suite.T(), val.PsPercent, 3)
	assert.EqualValues(suite.T(), val.PsFixedFee, 0.01)

	req.Amount = 10
	val, err = suite.service.getPaymentChannelCostMerchant(context.TODO(), req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "AZ")
	assert.EqualValues(suite.T(), val.MinAmount, 5)
	assert.EqualValues(suite.T(), val.MethodPercent, 2.5)
	assert.EqualValues(suite.T(), val.MethodFixAmount, 2)
	assert.EqualValues(suite.T(), val.PsPercent, 5)
	assert.EqualValues(suite.T(), val.PsFixedFee, 0.05)
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Delete_Ok() {
	req := &billingpb.PaymentCostDeleteRequest{
		Id: suite.paymentChannelCostMerchantId,
	}

	res := &billingpb.ResponseError{}
	err := suite.service.DeletePaymentChannelCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	_, err = suite.service.paymentChannelCostMerchant.GetById(context.TODO(), suite.paymentChannelCostMerchantId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentChannelCostMerchant))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GetAllPaymentChannelCostMerchant_Ok() {
	req := &billingpb.PaymentChannelCostMerchantListRequest{
		MerchantId: suite.merchantId,
	}
	res := &billingpb.PaymentChannelCostMerchantListResponse{}
	err := suite.service.GetAllPaymentChannelCostMerchant(context.TODO(), req, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), len(res.Item.Items), 3)
}
