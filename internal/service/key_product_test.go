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

	response := grpc.KeyProductResponse{}
	err := suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)
	res := response.Product

	response = grpc.KeyProductResponse{}
	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: res.MerchantId}, &response)
	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)

	product := response.Product

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
	shouldBe.False(product.Enabled)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: res.MerchantId}, &response)
	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: res.MerchantId}, &response)
	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)

	err = suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: res.Id, MerchantId: res.MerchantId}, &response)
	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)
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

	response := grpc.KeyProductResponse{}
	err := suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	res := response.Product

	shouldBe.Nil(err)
	shouldBe.Nil(response.Message)
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
	shouldBe.False(res.Enabled)
	shouldBe.NotEmpty(res.Id)

	req.Id = res.Id
	res2 := grpc.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)
	shouldBe.Nil(res2.Message)

	res2 = grpc.KeyProductResponse{}
	req.Id = bson.NewObjectId().Hex()
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)
	shouldBe.Nil(res2.Message)

	req.Sku = "NEW SKU"
	req.Id = res.Id
	res2 = grpc.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)
	shouldBe.Nil(res2.Message)

	req.Sku = res.Sku
	req.MerchantId = bson.NewObjectId().Hex()
	res2 = grpc.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)
	shouldBe.Nil(res2.Message)

	req.MerchantId = res.MerchantId
	req.ProjectId = bson.NewObjectId().Hex()
	res2 = grpc.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &res2)
	shouldBe.Nil(err)
	shouldBe.Nil(res2.Message)
}

func (suite *KeyProductTestSuite) GetKeyProducts_Test() {
	shouldBe := require.New(suite.T())

	req := &grpc.ListKeyProductsRequest{
		MerchantId: merchantId,
		ProjectId:  projectId,
	}
	res := &grpc.ListKeyProductsResponse{}
	err := suite.service.GetKeyProducts(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Equal(0, res.Count)
	shouldBe.Equal(0, res.Offset)
	shouldBe.Equal(0, len(res.Products))

	for i := 0; i < 10; i++ {
		suite.createKeyProduct()
	}

	err = suite.service.GetKeyProducts(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Equal(10, res.Count)
	shouldBe.Equal(0, res.Offset)
	shouldBe.Equal(10, len(res.Products))

	req.Offset = 9
	err = suite.service.GetKeyProducts(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Equal(10, res.Count)
	shouldBe.Equal(1, len(res.Products))

	req.Offset = 0
	req.Limit = 2
	err = suite.service.GetKeyProducts(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Equal(10, res.Count)
	shouldBe.Equal(2, len(res.Products))

	req.Offset = 0
	req.Limit = 0
	req.Sku = "some sku"
	req.Name = "some name"
	err = suite.service.GetKeyProducts(context.TODO(), req, res)
	shouldBe.Nil(err)

}

func (suite *KeyProductTestSuite) getKeyProduct(id string) *grpc.KeyProduct {
	suite.T().Helper()

	res := &grpc.KeyProductResponse{}
	err := suite.service.GetKeyProduct(context.TODO(), &grpc.RequestKeyProduct{MerchantId: merchantId, Id: id}, res)
	assert.Nil(suite.T(), err)
	assert.Nil(suite.T(), res.Message)
	return res.Product
}

func (suite *KeyProductTestSuite) createKeyProduct() *grpc.KeyProduct {
	suite.T().Helper()

	req := &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             bson.NewObjectId().Hex(),
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

	res := &grpc.KeyProductResponse{}
	err := suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, res)
	assert.Nil(suite.T(), err)
	assert.Nil(suite.T(), res.Message)
	return res.Product
}

func (suite *KeyProductTestSuite) UpdatePlatformPrices_Test() {
	shouldBe := require.New(suite.T())
	product := suite.createKeyProduct()
	req := &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id: "steam",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 66.66},
			},
		},
	}

	res := &grpc.UpdatePlatformPricesResponse{}
	err := suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Nil(res.Message)

	prices := res.Price
	shouldBe.Equal(1, len(prices.Prices))
	shouldBe.Equal(66.66, prices.Prices[0].Amount)
	shouldBe.Equal("RUB", prices.Prices[0].Currency)

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id: "steam",
			Prices: []*grpc.ProductPrice{
				{Currency: "EUR", Amount: 77.77},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Nil(res.Message)

	prices = res.Price
	shouldBe.Equal(1, len(prices.Prices))
	shouldBe.Equal(77.77, prices.Prices[0].Amount)
	shouldBe.Equal("EUR", prices.Prices[0].Currency)

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id: "gog",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 33.33},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.Nil(res.Message)
	shouldBe.Equal(1, len(res.Price.Prices))

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id:            "best_store_ever",
			EulaUrl:       "http://www.example.com",
			ActivationUrl: "http://www.example.com",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 0.01},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id:            "best_store_ever",
			Name:          "The Best Store EVER",
			ActivationUrl: "http://www.example.com",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 0.01},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id:      "best_store_ever",
			Name:    "The Best Store EVER",
			EulaUrl: "http://www.example.com",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 0.01},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id:            "best_store_ever",
			Name:          "The Best Store EVER",
			EulaUrl:       "http://www.example.com",
			ActivationUrl: "http://www.example.com",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 0.01},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)
	shouldBe.Equal(1, len(res.Price.Prices))

	req = &grpc.AddOrUpdatePlatformPricesRequest{
		MerchantId:   merchantId,
		KeyProductId: product.Id,
		Platform: &grpc.PlatformPrice{
			Id:            "best_store_ever_another",
			Name:          "The Best Store EVER",
			EulaUrl:       "http://www.example.com",
			ActivationUrl: "http://www.example.com",
			Prices: []*grpc.ProductPrice{
				{Currency: "RUB", Amount: 0.01},
			},
		},
	}
	res = &grpc.UpdatePlatformPricesResponse{}
	err = suite.service.UpdatePlatformPrices(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)
}

func (suite *KeyProductTestSuite) PublishKeyProduct_Test() {
	shouldBe := require.New(suite.T())

	product := suite.createKeyProduct()
	req := &grpc.PublishKeyProductRequest{
		KeyProductId: product.Id,
		MerchantId:   merchantId,
	}
	res := &grpc.KeyProductResponse{}
	err := suite.service.PublishKeyProduct(context.TODO(), req, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)
	shouldBe.True(res.Product.Enabled)
	shouldBe.NotNil(res.Product.PublishedAt)
}

func (suite *KeyProductTestSuite) DeleteKeyProduct_Test() {
	shouldBe := require.New(suite.T())
	product := suite.createKeyProduct()

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: product.Id, MerchantId: merchantId}, res)
	shouldBe.Nil(err)
	shouldBe.Nil(res.Message)

	res = &grpc.EmptyResponseWithStatus{}
	err = suite.service.DeleteKeyProduct(context.TODO(), &grpc.RequestKeyProduct{Id: product.Id, MerchantId: merchantId}, res)
	shouldBe.Nil(err)
	shouldBe.NotNil(res.Message)
}