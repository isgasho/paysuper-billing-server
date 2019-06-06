package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type CommissionTestSuite struct {
	suite.Suite
	service        *Service
	log            *zap.Logger
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

	currency := []interface{}{rub}

	err = db.Collection(pkg.CollectionCurrency).Insert(currency...)
	if err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
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
			Currency: rub,
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusDraft,
		IsSigned:                  true,
		PaymentMethods:            paymentMethods,
	}

	err = db.Collection(pkg.CollectionMerchant).Insert(suite.merchant)
	assert.NoError(suite.T(), err, "Insert merchant test data failed")

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

	err = db.Collection(pkg.CollectionProject).Insert(suite.project)
	assert.NoError(suite.T(), err, "Insert project test data failed")

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        cfg.CacheRedis.Address,
		Password:     cfg.CacheRedis.Password,
		MaxRetries:   cfg.CacheRedis.MaxRetries,
		MaxRedirects: cfg.CacheRedis.MaxRedirects,
		PoolSize:     cfg.CacheRedis.PoolSize,
	})

	suite.service = NewBillingService(db, cfg, make(chan bool, 1), nil, nil, nil, nil, nil, NewCacheRedis(redisdb))
	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *CommissionTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *CommissionTestSuite) TestCommission_GetAll() {
	c := suite.service.commission.GetAll()

	assert.NotNil(suite.T(), c)
}

func (suite *CommissionTestSuite) TestCommission_GetByProject_Ok() {
	c := suite.service.commission.GetByProject(suite.project.Id)

	assert.NotNil(suite.T(), c)
}

func (suite *CommissionTestSuite) TestCommission_GetByProject_NotFound() {
	c := suite.service.commission.GetByProject(bson.NewObjectId().Hex())

	assert.Nil(suite.T(), c)
}

func (suite *CommissionTestSuite) TestCommission_Get_Ok() {
	c, err := suite.service.commission.Get(suite.project.Id, suite.paymentMethod1)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), c)
}

func (suite *CommissionTestSuite) TestCommission_Get_NotFound() {
	c, err := suite.service.commission.Get(suite.project.Id, bson.NewObjectId().Hex())
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionCommission))
	assert.Nil(suite.T(), c)

	c2, err := suite.service.commission.Get(bson.NewObjectId().Hex(), suite.paymentMethod1)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionCommission))
	assert.Nil(suite.T(), c2)
}

func (suite *CommissionTestSuite) TestCommission_Update_Ok() {
	projectId := bson.NewObjectId().Hex()
	payId := bson.NewObjectId().Hex()
	commission := &billing.MerchantPaymentMethodCommissions{Fee: 1}
	err := suite.service.commission.Update(projectId, payId, commission)
	assert.NoError(suite.T(), err)

	c, err := suite.service.commission.Get(projectId, payId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), commission.Fee, c.Fee)
}

func (suite *CommissionTestSuite) TestCommission_CalculatePmCommission_NotFound() {
	a, err := suite.service.commission.CalculatePmCommission(bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), 1)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionCommission))
	assert.Equal(suite.T(), float64(0), a)
}

func (suite *CommissionTestSuite) TestCommission_CalculatePmCommission_Ok() {
	a, err := suite.service.commission.CalculatePmCommission(suite.project.Id, suite.paymentMethod1, 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(0.01), a)
}
