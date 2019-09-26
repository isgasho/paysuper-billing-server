package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type MoneyBackCostMerchantTestSuite struct {
	suite.Suite
	service                 *Service
	log                     *zap.Logger
	cache                   CacheInterface
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
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), nil, nil)

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

	suite.moneyBackCostMerchantId = bson.NewObjectId().Hex()
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
						Currency: "RUB",
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

	moneyBackCostMerchant := &billing.MoneyBackCostMerchant{
		Id:                suite.moneyBackCostMerchantId,
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}

	moneyBackCostMerchant2 := &billing.MoneyBackCostMerchant{
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}

	anotherMoneyBackCostMerchant := &billing.MoneyBackCostMerchant{
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	pucs := []*billing.MoneyBackCostMerchant{moneyBackCostMerchant, moneyBackCostMerchant2, anotherMoneyBackCostMerchant}
	if err := suite.service.moneyBackCostMerchant.MultipleInsert(pucs); err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}
}

func (suite *MoneyBackCostMerchantTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GrpcGet_Ok() {
	req := &billing.MoneyBackCostMerchantRequest{
		MerchantId:     suite.merchantId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		Days:           10,
		PaymentStage:   1,
	}

	res := &grpc.MoneyBackCostMerchantResponse{}

	err := suite.service.GetMoneyBackCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(5))
	assert.Equal(suite.T(), res.Item.Percent, float64(3))

	req.Country = ""
	err = suite.service.GetMoneyBackCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(3))
	assert.Equal(suite.T(), res.Item.Percent, float64(2))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GrpcSet_Ok() {
	req := &billing.MoneyBackCostMerchant{
		Id:                suite.moneyBackCostMerchantId,
		MerchantId:        suite.merchantId,
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3.33,
		FixAmount:         7.5,
		FixAmountCurrency: "USD",
	}

	res := grpc.MoneyBackCostMerchantResponse{}

	err := suite.service.SetMoneyBackCostMerchant(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(7.5))
	assert.Equal(suite.T(), res.Item.Id, suite.moneyBackCostMerchantId)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Insert_Ok() {
	req := &billing.MoneyBackCostMerchant{
		MerchantId:     suite.merchantId,
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "US",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3.33,
		FixAmount:      7.5,
	}

	assert.NoError(suite.T(), suite.service.moneyBackCostMerchant.Insert(req))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	obj := &billing.MoneyBackCostMerchant{
		MerchantId:     suite.merchantId,
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "US",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3.33,
		FixAmount:      7.5,
	}
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.moneyBackCostMerchant.Insert(obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_UpdateOk() {
	obj := &billing.MoneyBackCostMerchant{
		Id:             suite.moneyBackCostMerchantId,
		MerchantId:     suite.merchantId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       0,
		PaymentStage:   2,
		Percent:        4,
		FixAmount:      7,
	}

	assert.NoError(suite.T(), suite.service.moneyBackCostMerchant.Update(obj))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Get_Ok() {
	val, err := suite.service.moneyBackCostMerchant.Get(suite.merchantId, "VISA", "USD", "chargeback", "CIS", "AZ", 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 2)
	assert.Equal(suite.T(), val.Items[0].Country, "AZ")
	assert.Equal(suite.T(), val.Items[0].FixAmount, float64(5))

	val, err = suite.service.moneyBackCostMerchant.Get(suite.merchantId, "VISA", "USD", "chargeback", "CIS", "", 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 1)
	assert.Equal(suite.T(), val.Items[0].Country, "")
	assert.Equal(suite.T(), val.Items[0].FixAmount, float64(3))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_getMoneyBackCostMerchant() {
	req := &billing.MoneyBackCostMerchantRequest{
		MerchantId:     suite.merchantId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		Days:           5,
		PaymentStage:   1,
	}

	val, err := suite.service.getMoneyBackCostMerchant(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(0))
	assert.Equal(suite.T(), val.Percent, float64(3))
	assert.Equal(suite.T(), val.FixAmount, float64(5))

	req.Days = 45
	val, err = suite.service.getMoneyBackCostMerchant(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(30))
	assert.Equal(suite.T(), val.Percent, float64(10))
	assert.Equal(suite.T(), val.FixAmount, float64(15))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Delete_Ok() {
	req := &billing.PaymentCostDeleteRequest{
		Id: suite.moneyBackCostMerchantId,
	}

	res := &grpc.ResponseError{}
	err := suite.service.DeleteMoneyBackCostMerchant(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	_, err = suite.service.moneyBackCostMerchant.GetById(suite.moneyBackCostMerchantId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionMoneyBackCostMerchant))
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllMoneyBackCostMerchant_Ok() {
	req := &billing.MoneyBackCostMerchantListRequest{
		MerchantId: suite.merchantId,
	}
	res := &grpc.MoneyBackCostMerchantListResponse{}
	err := suite.service.GetAllMoneyBackCostMerchant(context.TODO(), req, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), len(res.Item.Items), 3)
}
