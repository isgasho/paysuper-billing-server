package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type CommissionTestSuite struct {
	suite.Suite
	service        *Service
	log            *zap.Logger
	cache          CacheInterface
	merchant       *billing.Merchant
	project        *billing.Project
	paymentMethod1 string
	paymentMethod2 string
}

func Test_Commission(t *testing.T) {
	suite.Run(t, new(CommissionTestSuite))
}

func (suite *CommissionTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.paymentMethod1 = bson.NewObjectId().Hex()
	suite.paymentMethod2 = bson.NewObjectId().Hex()
	paymentMethods := map[string]*billing.MerchantPaymentMethod{
		suite.paymentMethod1: {
			Commission: &billing.MerchantPaymentMethodCommissions{
				Fee:            1,
				PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{},
			},
			PaymentMethod: &billing.MerchantPaymentMethodIdentification{
				Id: bson.NewObjectId().Hex(),
			},
		},
		suite.paymentMethod2: {
			Commission: &billing.MerchantPaymentMethodCommissions{
				Fee:            1,
				PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{},
			},
			PaymentMethod: &billing.MerchantPaymentMethodIdentification{
				Id: bson.NewObjectId().Hex(),
			},
		},
	}
	suite.merchant = &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		User: &billing.MerchantUser{
			Id:    bson.NewObjectId().Hex(),
			Email: "test@unit.test",
		},
		Name: "Unit test",
		Zip:  "190000",
		City: "St.Petersburg",
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{},
			Technical:  &billing.MerchantContactTechnical{},
		},
		Banking: &billing.MerchantBanking{
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusDraft,
		IsSigned:                  true,
		PaymentMethods:            paymentMethods,
	}

	suite.project = &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         pkg.ProjectCallbackProtocolEmpty,
		LimitsCurrency:           "RUB",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               suite.merchant.Id,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	suite.cache = NewCacheRedis(mock.NewTestRedis())
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	if err := suite.service.merchant.Insert(suite.merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(suite.project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}
}

func (suite *CommissionTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *CommissionTestSuite) TestCommission_GetByProjectIdAndMethod_NotFound() {
	_, err := suite.service.commission.GetByProjectIdAndMethod(bson.NewObjectId().Hex(), bson.NewObjectId().Hex())
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCommission))
}

func (suite *CommissionTestSuite) TestCommission_GetByProjectIdAndMethod_Ok() {
	a, err := suite.service.commission.GetByProjectIdAndMethod(suite.project.Id, suite.paymentMethod1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.paymentMethod1].Commission.Fee, a.Fee)
}

func (suite *CommissionTestSuite) TestCommission_CalculatePmCommission_NotFound() {
	a, err := suite.service.commission.CalculatePmCommission(bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), 1)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionCommission))
	assert.Equal(suite.T(), float64(0), a)
}

func (suite *CommissionTestSuite) TestCommission_CalculatePmCommission_Ok() {
	a, err := suite.service.commission.CalculatePmCommission(suite.project.Id, suite.paymentMethod1, 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(0.01), a)
}
