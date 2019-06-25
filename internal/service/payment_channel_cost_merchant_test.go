package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PaymentChannelCostMerchantTestSuite struct {
	suite.Suite
	service                      *Service
	log                          *zap.Logger
	cache                        CacheInterface
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
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, nil)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	countryAz := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "AZ",
		Region:          "CIS",
		Currency:        "AZN",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "AZN",
	}
	countryUs := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "US",
		Region:          "US",
		Currency:        "USD",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "USD",
	}
	countries := []*billing.Country{countryAz, countryUs}
	if err := suite.service.country.MultipleInsert(countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.paymentChannelCostMerchantId = bson.NewObjectId().Hex()
	suite.merchantId = bson.NewObjectId().Hex()

	pmBankCard := &billing.PaymentMethod{
		Id:   bson.NewObjectId().Hex(),
		Name: "Bank card",
	}
	merchant := &billing.Merchant{
		Id: suite.merchantId,
		PaymentMethods: map[string]*billing.MerchantPaymentMethod{
			pmBankCard.Id: {
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   pmBankCard.Id,
					Name: pmBankCard.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: 2.5,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      30,
						Currency: rub.CodeA3,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
	}
	if err := suite.service.merchant.Insert(merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	paymentChannelCostMerchant := &billing.PaymentChannelCostMerchant{
		Id:                 suite.paymentChannelCostMerchantId,
		MerchantId:         suite.merchantId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          0.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      1.5,
		MethodFixAmount:    0.01,
		PsPercent:          3,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}

	paymentChannelCostMerchant2 := &billing.PaymentChannelCostMerchant{
		MerchantId:         suite.merchantId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          5,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}

	anotherPaymentChannelCostMerchant := &billing.PaymentChannelCostMerchant{
		MerchantId:         suite.merchantId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          0,
		Region:             "CIS",
		Country:            "",
		MethodPercent:      2.2,
		MethodFixAmount:    0,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	pccm := []*billing.PaymentChannelCostMerchant{paymentChannelCostMerchant, paymentChannelCostMerchant2, anotherPaymentChannelCostMerchant}
	if err := suite.service.paymentChannelCostMerchant.MultipleInsert(pccm); err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}
}

func (suite *PaymentChannelCostMerchantTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GrpcGet_Ok() {
	req := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     suite.merchantId,
		PayoutCurrency: "USD",
		Name:           "VISA",
		Region:         "CIS",
		Country:        "AZ",
		Amount:         10,
	}

	res := &billing.PaymentChannelCostMerchant{}

	err := suite.service.GetPaymentChannelCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "AZ")
	assert.Equal(suite.T(), res.MethodFixAmount, float64(2))
	assert.Equal(suite.T(), res.MinAmount, float64(5))

	req.Country = ""
	err = suite.service.GetPaymentChannelCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "")
	assert.Equal(suite.T(), res.MethodFixAmount, float64(0))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GrpcSet_Ok() {
	req := &billing.PaymentChannelCostMerchant{
		Id:                 suite.paymentChannelCostMerchantId,
		MerchantId:         suite.merchantId,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          1.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      2.5,
		MethodFixAmount:    1.01,
		PsPercent:          2,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}

	res := billing.PaymentChannelCostMerchant{}

	err := suite.service.SetPaymentChannelCostMerchant(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "AZ")
	assert.Equal(suite.T(), res.MethodFixAmount, float64(1.01))
	assert.Equal(suite.T(), res.Id, suite.paymentChannelCostMerchantId)
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Insert_Ok() {
	req := &billing.PaymentChannelCostMerchant{
		MerchantId:      suite.merchantId,
		Name:            "MASTERCARD",
		Region:          "US",
		Country:         "",
		MethodPercent:   2.2,
		MethodFixAmount: 0,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostMerchant.Insert(req))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Insert_ErrorCacheUpdate() {
	ci := &mock.CacheInterface{}
	obj := &billing.PaymentChannelCostMerchant{
		MerchantId:      suite.merchantId,
		Name:            "Mastercard",
		Region:          "US",
		Country:         "",
		MethodPercent:   2.1,
		MethodFixAmount: 0,
	}
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, obj.Country)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.paymentChannelCostMerchant.Insert(obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_UpdateOk() {
	obj := &billing.PaymentChannelCostMerchant{
		Id:              suite.paymentChannelCostMerchantId,
		MerchantId:      suite.merchantId,
		Name:            "Mastercard",
		Region:          "US",
		Country:         "",
		MethodPercent:   2.1,
		MethodFixAmount: 0,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostMerchant.Update(obj))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Get_Ok() {
	val, err := suite.service.paymentChannelCostMerchant.Get(suite.merchantId, "VISA", "USD", "CIS", "AZ")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 2)
	assert.Equal(suite.T(), val.Items[0].Country, "AZ")
	assert.Equal(suite.T(), val.Items[0].MethodFixAmount, float64(0.01))

	val, err = suite.service.paymentChannelCostMerchant.Get(suite.merchantId, "VISA", "USD", "CIS", "")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 1)
	assert.Equal(suite.T(), val.Items[0].Country, "")
	assert.Equal(suite.T(), val.Items[0].MethodFixAmount, float64(0))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_getPaymentChannelCostMerchant() {
	req := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     suite.merchantId,
		PayoutCurrency: "USD",
		Name:           "VISA",
		Region:         "CIS",
		Country:        "AZ",
		Amount:         0,
	}

	_, err := suite.service.getPaymentChannelCostMerchant(req)
	assert.Error(suite.T(), err)

	req.Amount = 1
	val, err := suite.service.getPaymentChannelCostMerchant(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.MinAmount, float64(0.75))
	assert.Equal(suite.T(), val.MethodPercent, float64(1.5))
	assert.Equal(suite.T(), val.MethodFixAmount, float64(0.01))
	assert.Equal(suite.T(), val.PsPercent, float64(3))
	assert.Equal(suite.T(), val.PsFixedFee, float64(0.01))

	req.Amount = 10
	val, err = suite.service.getPaymentChannelCostMerchant(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.MinAmount, float64(5))
	assert.Equal(suite.T(), val.MethodPercent, float64(2.5))
	assert.Equal(suite.T(), val.MethodFixAmount, float64(2))
	assert.Equal(suite.T(), val.PsPercent, float64(5))
	assert.Equal(suite.T(), val.PsFixedFee, float64(0.05))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_Delete_Ok() {
	req := &billing.PaymentCostDeleteRequest{
		Id: suite.paymentChannelCostMerchantId,
	}
	assert.NoError(suite.T(), suite.service.DeletePaymentChannelCostMerchant(context.TODO(), req, &grpc.EmptyResponse{}))

	_, err := suite.service.paymentChannelCostMerchant.GetById(suite.paymentChannelCostMerchantId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentChannelCostMerchant))
}

func (suite *PaymentChannelCostMerchantTestSuite) TestPaymentChannelCostMerchant_GetAllPaymentChannelCostMerchant_Ok() {
	req := &billing.PaymentChannelCostMerchantListRequest{
		MerchantId: suite.merchantId,
	}
	res := &billing.PaymentChannelCostMerchantList{}
	err := suite.service.GetAllPaymentChannelCostMerchant(context.TODO(), req, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(res.Items), 3)
}
