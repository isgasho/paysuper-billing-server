package service

import (
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type MerchantTestSuite struct {
	suite.Suite
	service    *Service
	log        *zap.Logger
	cache      CacheInterface
	merchant   *billing.Merchant
	pmBankCard *billing.PaymentMethod
}

func Test_Merchant(t *testing.T) {
	suite.Run(t, new(MerchantTestSuite))
}

func (suite *MerchantTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	settings := database.Connection{
		Host:     cfg.MongoHost,
		Database: cfg.MongoDatabase,
		User:     cfg.MongoUser,
		Password: cfg.MongoPassword,
	}

	db, err := database.NewDatabase(settings)

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
	suite.service = NewBillingService(db, cfg, make(chan bool, 1), nil, nil, nil, nil, nil, suite.cache)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.pmBankCard = &billing.PaymentMethod{
		Id:   bson.NewObjectId().Hex(),
		Name: "Bank card",
	}
	suite.merchant = &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		PaymentMethods: map[string]*billing.MerchantPaymentMethod{
			suite.pmBankCard.Id: {
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   suite.pmBankCard.Id,
					Name: suite.pmBankCard.Name,
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
	if err := suite.service.merchant.Insert(suite.merchant); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *MerchantTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *MerchantTestSuite) TestMerchant_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.merchant.Insert(&billing.Merchant{}))
}

func (suite *MerchantTestSuite) TestMerchant_Insert_ErrorCacheUpdate() {
	id := bson.NewObjectId().Hex()
	ci := &mock.CacheInterface{}
	ci.On("Set", "merchant:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.merchant.Insert(&billing.Merchant{Id: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MerchantTestSuite) TestMerchant_Update_Ok() {
	assert.NoError(suite.T(), suite.service.merchant.Update(&billing.Merchant{Id: suite.merchant.Id}))
}

func (suite *MerchantTestSuite) TestMerchant_Update_NotFound() {
	err := suite.service.merchant.Update(&billing.Merchant{Id: bson.NewObjectId().Hex()})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "not found")
}

func (suite *MerchantTestSuite) TestMerchant_Update_ErrorCacheUpdate() {
	id := bson.NewObjectId().Hex()
	ci := &mock.CacheInterface{}
	ci.On("Set", "merchant:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	_ = suite.service.merchant.Insert(&billing.Merchant{Id: id})
	err := suite.service.merchant.Update(&billing.Merchant{Id: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MerchantTestSuite) TestMerchant_GetById_Ok() {
	merchant := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
	}
	if err := suite.service.merchant.Insert(merchant); err != nil {
		suite.Assert().NoError(err)
	}
	c, err := suite.service.merchant.GetById(merchant.Id)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), merchant.Id, c.Id)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_Ok_ByCache() {
	ci := &mock.CacheInterface{}
	ci.On("Get", "merchant:id:"+suite.merchant.Id, mock2.Anything).
		Return(nil)
	suite.service.cacher = ci
	c, err := suite.service.merchant.GetById(suite.merchant.Id)

	assert.Nil(suite.T(), err)
	assert.IsType(suite.T(), &billing.Merchant{}, c)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_Ok() {
	pm, err := suite.service.merchant.GetPaymentMethod(suite.merchant.Id, suite.pmBankCard.Id)

	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.MerchantPaymentMethod{}, pm)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_ErrorByMethodNotAllowed() {
	_, err := suite.service.merchant.GetPaymentMethod(bson.NewObjectId().Hex(), "")

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, orderErrorPaymentMethodNotAllowed)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalId_ErrorByGetPaymentMethod() {
	_, err := suite.service.merchant.GetPaymentMethodTerminalId(suite.merchant.Id, "")

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "[PAYONE_BILLING] merchant not found")
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalId_ErrorByTerminalId() {
	suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration = nil
	_ = suite.service.merchant.Update(suite.merchant)
	_, err := suite.service.merchant.GetPaymentMethodTerminalId(suite.merchant.Id, suite.pmBankCard.Id)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, orderErrorPaymentMethodEmptySettings)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalPassword_ErrorByGetPaymentMethod() {
	_, err := suite.service.merchant.GetPaymentMethodTerminalPassword(suite.merchant.Id, "")

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "[PAYONE_BILLING] merchant not found")
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalPassword_ErrorByTerminalId() {
	suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration = nil
	_ = suite.service.merchant.Update(suite.merchant)
	_, err := suite.service.merchant.GetPaymentMethodTerminalPassword(suite.merchant.Id, suite.pmBankCard.Id)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, orderErrorPaymentMethodEmptySettings)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalCallbackPassword_ErrorByGetPaymentMethod() {
	_, err := suite.service.merchant.GetPaymentMethodTerminalCallbackPassword(suite.merchant.Id, "")

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "[PAYONE_BILLING] merchant not found")
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethodTerminalCallbackPassword_ErrorByTerminalId() {
	suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration = nil
	_ = suite.service.merchant.Update(suite.merchant)
	_, err := suite.service.merchant.GetPaymentMethodTerminalCallbackPassword(suite.merchant.Id, suite.pmBankCard.Id)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, orderErrorPaymentMethodEmptySettings)
}
