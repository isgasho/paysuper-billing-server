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

type PaymentMethodTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	pmQiwi  *billing.PaymentMethod
}

func Test_PaymentMethod(t *testing.T) {
	suite.Run(t, new(PaymentMethodTestSuite))
}

func (suite *PaymentMethodTestSuite) SetupTest() {
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

	suite.pmQiwi = &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Qiwi",
		Group:            "QIWI",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		Currencies:       []int32{643, 840, 980},
		Params: &billing.PaymentMethodParams{
			Handler:    "cardpay",
			Terminal:   "15993",
			ExternalId: "QIWI",
		},
		Type:     "ewallet",
		IsActive: true,
	}
	pms := []interface{}{suite.pmQiwi}
	err = db.Collection(pkg.CollectionPaymentMethod).Insert(pms...)
	if err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

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

func (suite *PaymentMethodTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetAll() {
	c := suite.service.paymentMethod.GetAll()

	assert.NotNil(suite.T(), c)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodById_Ok() {
	pm, err := suite.service.paymentMethod.GetPaymentMethodById(suite.pmQiwi.Id)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pm)
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.Id)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodById_NotFound() {
	_, err := suite.service.paymentMethod.GetPaymentMethodById("unknown")

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionPaymentMethod))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodByGroupAndCurrency_Ok() {
	pm, err := suite.service.paymentMethod.GetPaymentMethodByGroupAndCurrency(suite.pmQiwi.Group, 643)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pm)
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.Id)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodByGroupAndCurrency_NotFound() {
	_, err := suite.service.paymentMethod.GetPaymentMethodByGroupAndCurrency("unknown", 643)
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionPaymentMethod))

	_, err = suite.service.paymentMethod.GetPaymentMethodByGroupAndCurrency(suite.pmQiwi.Group, 1)
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, pkg.CollectionPaymentMethod))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Groups() {
	pm := suite.service.paymentMethod.Groups()

	assert.NotNil(suite.T(), pm)
	assert.NotNil(suite.T(), pm["QIWI"])
	assert.NotNil(suite.T(), pm["QIWI"][643])
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm["QIWI"][643].Id)
}
