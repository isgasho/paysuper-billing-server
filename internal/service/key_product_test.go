package service

import (
	"context"
	"github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type KeyProductTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	project    *billing.Project
	pmBankCard *billing.PaymentMethod
}

func Test_KeyProduct(t *testing.T) {
	suite.Run(t, new(KeyProductTestSuite))
}

func (suite *KeyProductTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

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
	usd := &billing.Currency{
		CodeInt:  840,
		CodeA3:   "USD",
		Name:     &billing.Name{Ru: "Доллар США", En: "US Dollar"},
		IsActive: true,
	}

	if err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)
	assert.NoError(suite.T(), err, "Creating RabbitMQ publisher failed")

	if err := InitTestCurrency(db, []interface{}{rub, usd}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		broker,
		nil,
		suite.cache,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *KeyProductTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *KeyProductTestSuite) GetKeyProduct_Test() {
	shouldBe := require.New(suite.T())

	req := &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "ru_double_yeti",
		Name:            map[string]string{"en": initialName},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys"},
		Url:             "http://test.ru/dffdsfsfs",
		Images:          []string{"/home/image.jpg"},
		MerchantId:      merchantId,
		ProjectId:       projectId,
		Metadata: map[string]string{
			"SomeKey": "SomeValue",
		},
	}

	res := grpc.KeyProduct{}
	err := suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res)
	shouldBe.Nil(err)

	product := grpc.KeyProduct{}
	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: res.MerchantId}, &product)
	shouldBe.Nil(err)

	shouldBe.Equal(res.Name["en"], product.Name["en"])
	shouldBe.Equal(res.DefaultCurrency, product.DefaultCurrency)
	shouldBe.Equal(res.Sku, product.Sku)
	shouldBe.Equal(res.Object, product.Object)
	shouldBe.Equal(res.Enabled, product.Enabled)
	shouldBe.Equal(res.Description, product.Description)
	shouldBe.Equal(res.LongDescription, product.LongDescription)
	shouldBe.Equal(res.Url, product.Url)
	shouldBe.Equal(res.Images, product.Images)
	shouldBe.Equal(res.Metadata, product.Metadata)
	shouldBe.NotNil(product.UpdatedAt)
	shouldBe.NotNil(product.CreatedAt)
	shouldBe.Nil(product.PublishedAt)
	shouldBe.False(product.IsPublished)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: ""}, &product)
	shouldBe.NotNil(err)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: "", MerchantId: res.MerchantId}, &product)
	shouldBe.NotNil(err)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: bson.NewObjectId().Hex(), MerchantId: res.MerchantId}, &product)
	shouldBe.NotNil(err)
}

func (suite *KeyProductTestSuite) CreateOrUpdateKeyProduct_Test() {
	shouldBe := require.New(suite.T())

	req := &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "ru_double_yeti",
		Name:            map[string]string{"en": initialName},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys"},
		Url:             "http://test.ru/dffdsfsfs",
		Images:          []string{"/home/image.jpg"},
		MerchantId:      merchantId,
		ProjectId:       projectId,
		Metadata: map[string]string{
			"SomeKey": "SomeValue",
		},
	}

	res := grpc.KeyProduct{}

	err := suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res)

	shouldBe.Nil(err)
	shouldBe.Equal(res.Name["en"], req.Name["en"])
	shouldBe.Equal(res.DefaultCurrency, req.DefaultCurrency)
	shouldBe.Equal(res.Sku, req.Sku)
	shouldBe.Equal(res.Object, req.Object)
	shouldBe.Equal(res.Enabled, req.Enabled)
	shouldBe.Equal(res.Description, req.Description)
	shouldBe.Equal(res.LongDescription, req.LongDescription)
	shouldBe.Equal(res.Url, req.Url)
	shouldBe.Equal(res.Images, req.Images)
	shouldBe.Equal(res.Metadata, req.Metadata)
	shouldBe.NotNil(res.UpdatedAt)
	shouldBe.NotNil(res.CreatedAt)
	shouldBe.Nil(res.PublishedAt)
	shouldBe.False(res.IsPublished)
	shouldBe.NotEmpty(res.Id)

	req.Id = res.Id
	res2 := grpc.KeyProduct{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)

	req.Id = bson.NewObjectId().Hex()
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.NotNil(err)

	req.Sku = "NEW SKU"
	req.Id = res.Id
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.NotNil(err)

	req.Sku = res.Sku
	req.MerchantId = bson.NewObjectId().Hex()
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.NotNil(err)

	req.MerchantId = res.MerchantId
	req.ProjectId = bson.NewObjectId().Hex()
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.NotNil(err)
}
