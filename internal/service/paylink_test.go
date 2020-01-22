package service

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

type PaylinkTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   database.CacheInterface

	merchant           *billingpb.Merchant
	merchant2          *billingpb.Merchant
	projectFixedAmount *billingpb.Project
	paymentMethod      *billingpb.PaymentMethod
	paymentSystem      *billingpb.PaymentSystem
	product1           *billingpb.Product
	product2           *billingpb.Product
	product3           *billingpb.Product
	product4           *billingpb.Product
	keyProduct1        *billingpb.KeyProduct
	keyProduct2        *billingpb.KeyProduct
	keyProduct3        *billingpb.KeyProduct
	paylink1           *billingpb.Paylink // normal paylink
	paylink2           *billingpb.Paylink // deleted paylink
	paylink3           *billingpb.Paylink // expired paylink
	project2           *billingpb.Project
	project3           *billingpb.Project
}

func Test_Paylink(t *testing.T) {
	suite.Run(t, new(PaylinkTestSuite))
}

func (suite *PaylinkTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.RoyaltyReportPeriodEndHour = 0
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"
	cfg.OrderViewUpdateBatchSize = 20

	m, err := migrate.New(
		"file://../../migrations/tests",
		cfg.MongoDsn)
	assert.NoError(suite.T(), err, "Migrate init failed")

	err = m.Up()
	if err != nil && err.Error() != "no change" {
		suite.FailNow("Migrations failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)

	if err != nil {
		suite.FailNow("Creating RabbitMQ publisher failed", "%v", err)
	}

	redisClient := database.NewRedis(
		&redis.Options{
			Addr:     cfg.RedisHost,
			Password: cfg.RedisPassword,
		},
	)

	redisdb := mocks.NewTestRedis()
	suite.cache, err = database.NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		broker,
		redisClient,
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

	suite.merchant, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)
	suite.merchant2 = helperCreateMerchant(suite.Suite, suite.service, "USD", "RU", suite.paymentMethod, suite.merchant.MinPayoutAmount, suite.merchant.OperatingCompanyId)

	suite.product1 = &billingpb.Product{
		Id:              primitive.NewObjectID().Hex(),
		Object:          "product",
		Sku:             "ru_double_yeti",
		Name:            map[string]string{"en": "Double Yeti"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       suite.projectFixedAmount.Id,
		Prices: []*billingpb.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(context.TODO(), suite.product1); err != nil {
		suite.FailNow("Product1 insert failed", "%v", err)
	}

	suite.product2 = &billingpb.Product{
		Id:              primitive.NewObjectID().Hex(),
		Object:          "product",
		Sku:             "my_favorite_game",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       suite.projectFixedAmount.Id,
		Prices: []*billingpb.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(context.TODO(), suite.product2); err != nil {
		suite.FailNow("Product2 insert failed", "%v", err)
	}

	suite.product3 = &billingpb.Product{
		Id:              primitive.NewObjectID().Hex(),
		Object:          "product",
		Sku:             "doom 2",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       primitive.NewObjectID().Hex(),
		Prices: []*billingpb.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(context.TODO(), suite.product3); err != nil {
		suite.FailNow("Product3 insert failed", "%v", err)
	}

	suite.product4 = &billingpb.Product{
		Id:              primitive.NewObjectID().Hex(),
		Object:          "product",
		Sku:             "my_favorite_game",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      primitive.NewObjectID().Hex(),
		ProjectId:       primitive.NewObjectID().Hex(),
		Prices: []*billingpb.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(context.TODO(), suite.product4); err != nil {
		suite.FailNow("Product4 insert failed", "%v", err)
	}

	req := &billingpb.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_1",
		Name:            map[string]string{"en": "Super game steam keys 1"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 1"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billingpb.ImageCollection{
			UseOneForAll: false,
			Images: &billingpb.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
		Platforms: []*billingpb.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billingpb.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	response := billingpb.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), response.Status, billingpb.ResponseStatusOk)
	suite.keyProduct1 = response.Product

	req = &billingpb.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_2",
		Name:            map[string]string{"en": "Super game steam keys 2"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 2"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billingpb.ImageCollection{
			UseOneForAll: false,
			Images: &billingpb.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
		Platforms: []*billingpb.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billingpb.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), response.Status, billingpb.ResponseStatusOk)
	suite.keyProduct2 = response.Product

	req = &billingpb.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_3",
		Name:            map[string]string{"en": "Super game steam keys 3"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 3"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billingpb.ImageCollection{
			UseOneForAll: false,
			Images: &billingpb.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
		Platforms: []*billingpb.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billingpb.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, response.Status)
	suite.keyProduct3 = response.Product

	bod, _ := ptypes.TimestampProto(now.BeginningOfDay())
	expiresAt, _ := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))

	suite.paylink1 = &billingpb.Paylink{
		Id:                   primitive.NewObjectID().Hex(),
		Object:               "paylink",
		Products:             []string{suite.product1.Id, suite.product2.Id},
		ExpiresAt:            expiresAt,
		CreatedAt:            bod,
		UpdatedAt:            bod,
		MerchantId:           suite.merchant.Id,
		ProjectId:            suite.projectFixedAmount.Id,
		Name:                 "Willy Wonka Strikes Back",
		IsExpired:            false,
		Visits:               100,
		NoExpiryDate:         true,
		ProductsType:         "product",
		Deleted:              false,
		TotalTransactions:    0,
		SalesCount:           0,
		ReturnsCount:         0,
		Conversion:           0,
		GrossSalesAmount:     0,
		GrossReturnsAmount:   0,
		GrossTotalAmount:     0,
		TransactionsCurrency: "",
	}

	err = suite.service.paylinkService.Insert(context.TODO(), suite.paylink1)
	assert.NoError(suite.T(), err)

	suite.paylink2 = &billingpb.Paylink{
		Id:                   primitive.NewObjectID().Hex(),
		Object:               "paylink",
		Products:             []string{suite.product1.Id, suite.product2.Id},
		ExpiresAt:            expiresAt,
		CreatedAt:            bod,
		UpdatedAt:            bod,
		MerchantId:           suite.merchant.Id,
		ProjectId:            suite.projectFixedAmount.Id,
		Name:                 "Willy Wonka Strikes Back",
		IsExpired:            false,
		Visits:               0,
		NoExpiryDate:         true,
		ProductsType:         "product",
		Deleted:              true,
		TotalTransactions:    0,
		SalesCount:           0,
		ReturnsCount:         0,
		Conversion:           0,
		GrossSalesAmount:     0,
		GrossReturnsAmount:   0,
		GrossTotalAmount:     0,
		TransactionsCurrency: "",
	}

	err = suite.service.paylinkService.Insert(context.TODO(), suite.paylink2)
	assert.NoError(suite.T(), err)

	alreadyExpiredAt, _ := ptypes.TimestampProto(time.Now().Add(-25 * time.Hour))

	suite.paylink3 = &billingpb.Paylink{
		Id:                   primitive.NewObjectID().Hex(),
		Object:               "paylink",
		Products:             []string{suite.product1.Id, suite.product2.Id},
		ExpiresAt:            alreadyExpiredAt,
		CreatedAt:            bod,
		UpdatedAt:            bod,
		MerchantId:           suite.merchant.Id,
		ProjectId:            suite.projectFixedAmount.Id,
		Name:                 "Willy Wonka Strikes Back",
		IsExpired:            true,
		Visits:               0,
		NoExpiryDate:         false,
		ProductsType:         "product",
		Deleted:              false,
		TotalTransactions:    0,
		SalesCount:           0,
		ReturnsCount:         0,
		Conversion:           0,
		GrossSalesAmount:     0,
		GrossReturnsAmount:   0,
		GrossTotalAmount:     0,
		TransactionsCurrency: "",
	}

	err = suite.service.paylinkService.Insert(context.TODO(), suite.paylink3)
	assert.NoError(suite.T(), err)
}

func (suite *PaylinkTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_SimpleProducts() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			suite.product2.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_KeyProducts() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_NoProducts() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products:     []string{},
		MerchantId:   suite.merchant.Id,
		ProjectId:    suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductsLengthInvalid)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_TooMuchProducts() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
			suite.product1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductsLengthInvalid)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductNotFound() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			primitive.NewObjectID().Hex(),
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_MixedProductsTypes() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			suite.keyProduct1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductFromAnotherProject() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			suite.product3.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToProject)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductFromAnotherMerchant() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			suite.product4.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToMerchant)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductFromAnotherProject() {
	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   billingpb.ProjectStatusDraft,
		MerchantId:               suite.merchant.Id,
	}
	if err := suite.service.project.Insert(context.TODO(), project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
			suite.keyProduct2.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  project.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, res.Status)
	assert.Equal(suite.T(), errorPaylinkProductNotBelongToProject, res.Message)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductNotFound() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			primitive.NewObjectID().Hex(),
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductFromAnotherMerchant() {
	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   billingpb.ProjectStatusDraft,
		MerchantId:               suite.merchant2.Id,
	}
	if err := suite.service.project.Insert(context.TODO(), project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
			suite.keyProduct3.Id,
		},
		MerchantId: suite.merchant2.Id,
		ProjectId:  project.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, res.Status)
	assert.Equal(suite.T(), errorPaylinkProductNotBelongToMerchant, res.Message)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ExpiresInPast() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(-25 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkExpiresInPast)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_ExpiresInPastButNoExpirationDate() {
	req := &billingpb.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(-25 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
		},
		MerchantId:   suite.merchant.Id,
		ProjectId:    suite.projectFixedAmount.Id,
		NoExpiryDate: true,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Ok() {
	assert.Equal(suite.T(), suite.paylink1.Name, "Willy Wonka Strikes Back")
	assert.Equal(suite.T(), suite.paylink1.Visits, int32(100))
	assert.Len(suite.T(), suite.paylink1.Products, 2)

	req := &billingpb.CreatePaylinkRequest{
		Id:           suite.paylink1.Id,
		Name:         "Unit-test-update",
		ProductsType: suite.paylink1.ProductsType,
		ExpiresAt:    suite.paylink1.ExpiresAt.Seconds,
		Products: []string{
			suite.product1.Id,
		},
		MerchantId:   suite.paylink1.MerchantId,
		ProjectId:    suite.paylink1.ProjectId,
		NoExpiryDate: suite.paylink1.NoExpiryDate,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
	assert.Equal(suite.T(), res.Item.Name, "Unit-test-update")
	assert.Equal(suite.T(), res.Item.Visits, int32(100))
	assert.Len(suite.T(), res.Item.Products, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Fail_ProjectMismatch() {
	req := &billingpb.CreatePaylinkRequest{
		Id:           suite.paylink1.Id,
		Name:         suite.paylink1.Name,
		ProductsType: suite.paylink1.ProductsType,
		ExpiresAt:    suite.paylink1.ExpiresAt.Seconds,
		Products:     suite.paylink1.Products,
		MerchantId:   suite.paylink1.MerchantId,
		ProjectId:    primitive.NewObjectID().Hex(),
		NoExpiryDate: suite.paylink1.NoExpiryDate,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProjectMismatch)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Fail_NotFound() {
	req := &billingpb.CreatePaylinkRequest{
		Id:           primitive.NewObjectID().Hex(),
		Name:         suite.paylink1.Name,
		ProductsType: suite.paylink1.ProductsType,
		ExpiresAt:    suite.paylink1.ExpiresAt.Seconds,
		Products:     suite.paylink1.Products,
		MerchantId:   suite.paylink1.MerchantId,
		ProjectId:    suite.paylink1.ProjectId,
		NoExpiryDate: suite.paylink1.NoExpiryDate,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Ok() {
	paylinkOid, err := primitive.ObjectIDFromHex(suite.paylink1.Id)
	assert.NoError(suite.T(), err)
	merchantOid, err := primitive.ObjectIDFromHex(suite.paylink1.MerchantId)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"_id":         paylinkOid,
		"merchant_id": merchantOid,
		"deleted":     false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	n, err = suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)

	query["deleted"] = true
	n, err = suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	var pl1 billingpb.Paylink
	key := fmt.Sprintf(cacheKeyPaylink, suite.paylink1.Id)
	err = suite.service.cacher.Get(key, &pl1)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), pl1, billingpb.Paylink{})

	var pl2 billingpb.Paylink
	key = fmt.Sprintf(cacheKeyPaylinkMerchant, suite.paylink1.Id, suite.paylink1.MerchantId)
	err = suite.service.cacher.Get(key, &pl2)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), pl2, billingpb.Paylink{})
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_AlreadyDeleted() {
	paylinkOid, err := primitive.ObjectIDFromHex(suite.paylink2.Id)
	assert.NoError(suite.T(), err)
	merchantOid, err := primitive.ObjectIDFromHex(suite.paylink2.MerchantId)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"_id":         paylinkOid,
		"merchant_id": merchantOid,
		"deleted":     true,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)

	n, err = suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_MerchantInvalid() {
	paylinkOid, err := primitive.ObjectIDFromHex(suite.paylink1.Id)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"_id":     paylinkOid,
		"deleted": false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)

	n, err = suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_IdInvalid() {
	someId := primitive.NewObjectID()
	query := bson.M{
		"_id":     someId,
		"deleted": false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)

	req := &billingpb.PaylinkRequest{
		Id:         someId.Hex(),
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_IncrPaylinkVisits_Ok() {
	oid, err := primitive.ObjectIDFromHex(suite.paylink1.Id)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"paylink_id": oid,
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)

	req := &billingpb.PaylinkRequestById{
		Id: suite.paylink1.Id,
	}

	res := &billingpb.EmptyResponse{}
	err = suite.service.IncrPaylinkVisits(context.TODO(), req, res)
	assert.NoError(suite.T(), err)

	n, err = suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Ok() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:          suite.paylink1.Id,
		MerchantId:  suite.paylink1.MerchantId,
		UrlMask:     "/my_paylink/%s/unit-test",
		UtmSource:   "unit-test-source",
		UtmMedium:   "unit-test-medium",
		UtmCampaign: "unit-test-campaign",
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Url,
		"/my_paylink/"+
			suite.paylink1.Id+
			"/unit-test?utm_campaign=unit-test-campaign&utm_medium=unit-test-medium&utm_source=unit-test-medium")
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Ok_OnlyRequiredParams() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Url, "/paylink/"+suite.paylink1.Id)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_Deleted() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_FailExpired() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusGone)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:         suite.paylink2.Id,
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_NotExists() {
	req := &billingpb.GetPaylinkURLRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Ok() {
	suite.paylink1.Visits = 0 // fix visits actualization

	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	// fix time issues
	res.Item.CreatedAt = suite.paylink1.CreatedAt
	res.Item.UpdatedAt = suite.paylink1.UpdatedAt
	res.Item.ExpiresAt = suite.paylink1.ExpiresAt

	assert.Equal(suite.T(), res.Item, suite.paylink1)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Ok_Expired() {
	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	// fix time issues
	res.Item.CreatedAt = suite.paylink3.CreatedAt
	res.Item.UpdatedAt = suite.paylink3.UpdatedAt
	res.Item.ExpiresAt = suite.paylink3.ExpiresAt

	assert.Equal(suite.T(), res.Item, suite.paylink3)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_Deleted() {
	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_MerchantMismatch() {
	req := &billingpb.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_NotExists() {
	req := &billingpb.PaylinkRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinks_Ok() {
	req := &billingpb.GetPaylinksRequest{
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &billingpb.GetPaylinksResponse{}
	err := suite.service.GetPaylinks(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Data)
	assert.Equal(suite.T(), res.Data.Count, int32(2))
	assert.Len(suite.T(), res.Data.Items, 2)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinks_Ok_NoPaylinks() {
	req := &billingpb.GetPaylinksRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPaylinksResponse{}
	err := suite.service.GetPaylinks(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Data)
	assert.Equal(suite.T(), res.Data.Count, int32(0))
	assert.Len(suite.T(), res.Data.Items, 0)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Ok_ForNewPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item
	stat.Id = ""

	statForNewPaylink := &billingpb.StatCommon{
		PaylinkId: suite.paylink1.Id,
	}
	assert.Equal(suite.T(), stat, statForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Ok_ForNewExpiredPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item
	stat.Id = ""

	statForNewPaylink := &billingpb.StatCommon{
		PaylinkId: suite.paylink3.Id,
	}
	assert.Equal(suite.T(), stat, statForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_Deleted() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_NotExists() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Ok_ForNewPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Ok_ForNewExpiredPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_Deleted() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_NotExists() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Ok_ForNewPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Ok_ForNewExpiredPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_Deleted() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_NotExists() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Ok_ForNewPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Ok_ForNewExpiredPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_Deleted() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_NotExists() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Ok_ForNewPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Ok_ForNewExpiredPaylink() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &billingpb.GroupStatCommon{
		Top: []*billingpb.StatCommon{},
		Total: &billingpb.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_Deleted() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_MerchantMismatch() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_NotExists() {
	req := &billingpb.GetPaylinkStatCommonRequest{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &billingpb.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkVisits_Ok() {
	oid, err := primitive.ObjectIDFromHex(suite.paylink1.Id)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"paylink_id": oid,
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)

	req := &billingpb.PaylinkRequestById{
		Id: suite.paylink1.Id,
	}

	res := &billingpb.EmptyResponse{}
	err = suite.service.IncrPaylinkVisits(context.TODO(), req, res)
	assert.NoError(suite.T(), err)

	n, err = suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), query)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	n, err = suite.service.paylinkService.GetPaylinkVisits(context.TODO(), suite.paylink1.Id, 0, 0)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 1)

	oneHourBefore := time.Now().Add(-1 * time.Hour).Unix()
	twoHourBefore := time.Now().Add(-2 * time.Hour).Unix()
	n, err = suite.service.paylinkService.GetPaylinkVisits(context.TODO(), suite.paylink1.Id, twoHourBefore, oneHourBefore)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)
}
