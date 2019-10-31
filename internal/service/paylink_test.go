package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/paylink"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"testing"
	"time"
)

type PaylinkTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   internalPkg.CacheInterface

	merchant           *billing.Merchant
	merchant2          *billing.Merchant
	projectFixedAmount *billing.Project
	paymentMethod      *billing.PaymentMethod
	paymentSystem      *billing.PaymentSystem
	product1           *grpc.Product
	product2           *grpc.Product
	product3           *grpc.Product
	product4           *grpc.Product
	keyProduct1        *grpc.KeyProduct
	keyProduct2        *grpc.KeyProduct
	keyProduct3        *grpc.KeyProduct
	paylink1           *paylink.Paylink // normal paylink
	paylink2           *paylink.Paylink // deleted paylink
	paylink3           *paylink.Paylink // expired paylink
	project2           *billing.Project
	project3           *billing.Project
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
	suite.cache = NewCacheRedis(redisdb)
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
	suite.merchant2 = helperCreateMerchant(suite.Suite, suite.service, "USD", "RU", suite.paymentMethod, suite.merchant.MinPayoutAmount)

	suite.product1 = &grpc.Product{
		Id:              bson.NewObjectId().Hex(),
		Object:          "product",
		Sku:             "ru_double_yeti",
		Name:            map[string]string{"en": "Double Yeti"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       suite.projectFixedAmount.Id,
		Prices: []*billing.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(suite.product1); err != nil {
		suite.FailNow("Product1 insert failed", "%v", err)
	}

	suite.product2 = &grpc.Product{
		Id:              bson.NewObjectId().Hex(),
		Object:          "product",
		Sku:             "my_favorite_game",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       suite.projectFixedAmount.Id,
		Prices: []*billing.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(suite.product2); err != nil {
		suite.FailNow("Product2 insert failed", "%v", err)
	}

	suite.product3 = &grpc.Product{
		Id:              bson.NewObjectId().Hex(),
		Object:          "product",
		Sku:             "doom 2",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      suite.merchant.Id,
		ProjectId:       bson.NewObjectId().Hex(),
		Prices: []*billing.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(suite.product3); err != nil {
		suite.FailNow("Product3 insert failed", "%v", err)
	}

	suite.product4 = &grpc.Product{
		Id:              bson.NewObjectId().Hex(),
		Object:          "product",
		Sku:             "my_favorite_game",
		Name:            map[string]string{"en": "My Favorite Game"},
		DefaultCurrency: "USD",
		Enabled:         true,
		Description:     map[string]string{"en": "blah-blah-blah"},
		MerchantId:      bson.NewObjectId().Hex(),
		ProjectId:       bson.NewObjectId().Hex(),
		Prices: []*billing.ProductPrice{{
			Currency: "USD",
			Region:   "USD",
			Amount:   1005.00,
		}},
	}
	if err := suite.service.productService.Upsert(suite.product4); err != nil {
		suite.FailNow("Product4 insert failed", "%v", err)
	}

	req := &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_1",
		Name:            map[string]string{"en": "Super game steam keys 1"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 1"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billing.ImageCollection{
			UseOneForAll: false,
			Images: &billing.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
		Platforms: []*grpc.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billing.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	response := grpc.KeyProductResponse{}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), response.Status, pkg.ResponseStatusOk)
	suite.keyProduct1 = response.Product

	req = &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_2",
		Name:            map[string]string{"en": "Super game steam keys 2"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 2"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billing.ImageCollection{
			UseOneForAll: false,
			Images: &billing.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  bson.NewObjectId().Hex(),
		Platforms: []*grpc.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billing.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), response.Status, pkg.ResponseStatusOk)
	suite.keyProduct2 = response.Product

	req = &grpc.CreateOrUpdateKeyProductRequest{
		Object:          "product",
		Sku:             "Super_game_steam_keys_3",
		Name:            map[string]string{"en": "Super game steam keys 3"},
		DefaultCurrency: "USD",
		Description:     map[string]string{"en": "blah-blah-blah"},
		LongDescription: map[string]string{"en": "Super game steam keys 3"},
		Url:             "http://test.ru/dffdsfsfs",
		Cover: &billing.ImageCollection{
			UseOneForAll: false,
			Images: &billing.LocalizedUrl{
				En: "/home/image.jpg",
			},
		},
		MerchantId: suite.merchant2.Id,
		ProjectId:  bson.NewObjectId().Hex(),
		Platforms: []*grpc.PlatformPrice{
			{
				Id: "steam",
				Prices: []*billing.ProductPrice{
					{Region: "USD", Currency: "USD", Amount: 10},
					{Region: "EUR", Currency: "EUR", Amount: 20},
				},
			},
		},
	}
	err = suite.service.CreateOrUpdateKeyProduct(context.TODO(), req, &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, response.Status)
	suite.keyProduct3 = response.Product

	bod, _ := ptypes.TimestampProto(now.BeginningOfDay())
	expiresAt, _ := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))

	suite.paylink1 = &paylink.Paylink{
		Id:                   bson.NewObjectId().Hex(),
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

	err = suite.service.paylinkService.Insert(suite.paylink1)
	assert.NoError(suite.T(), err)

	suite.paylink2 = &paylink.Paylink{
		Id:                   bson.NewObjectId().Hex(),
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

	err = suite.service.paylinkService.Insert(suite.paylink2)
	assert.NoError(suite.T(), err)

	alreadyExpiredAt, _ := ptypes.TimestampProto(time.Now().Add(-25 * time.Hour))

	suite.paylink3 = &paylink.Paylink{
		Id:                   bson.NewObjectId().Hex(),
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

	err = suite.service.paylinkService.Insert(suite.paylink3)
	assert.NoError(suite.T(), err)
}

func (suite *PaylinkTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_SimpleProducts() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_KeyProducts() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_NoProducts() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products:     []string{},
		MerchantId:   suite.merchant.Id,
		ProjectId:    suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductsLengthInvalid)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_TooMuchProducts() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductsLengthInvalid)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductNotFound() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "product",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.product1.Id,
			bson.NewObjectId().Hex(),
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_MixedProductsTypes() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductFromAnotherProject() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToProject)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ProductFromAnotherMerchant() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToMerchant)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductFromAnotherProject() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
			suite.keyProduct2.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToProject)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductNotFound() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			bson.NewObjectId().Hex(),
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotFoundOrInvalidType)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_KeyProductFromAnotherMerchant() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(1 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
			suite.keyProduct3.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProductNotBelongToMerchant)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Fail_ExpiresInPast() {
	req := &paylink.CreatePaylinkRequest{
		Name:         "Unit-test",
		ProductsType: "key",
		ExpiresAt:    time.Now().Add(-25 * time.Hour).Unix(),
		Products: []string{
			suite.keyProduct1.Id,
		},
		MerchantId: suite.merchant.Id,
		ProjectId:  suite.projectFixedAmount.Id,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkExpiresInPast)
}

func (suite *PaylinkTestSuite) Test_Paylink_Create_Ok_ExpiresInPastButNoExpirationDate() {
	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Ok() {
	assert.Equal(suite.T(), suite.paylink1.Name, "Willy Wonka Strikes Back")
	assert.Equal(suite.T(), suite.paylink1.Visits, int32(100))
	assert.Len(suite.T(), suite.paylink1.Products, 2)

	req := &paylink.CreatePaylinkRequest{
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

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Item)
	assert.Equal(suite.T(), res.Item.Name, "Unit-test-update")
	assert.Equal(suite.T(), res.Item.Visits, int32(100))
	assert.Len(suite.T(), res.Item.Products, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Fail_ProjectMismatch() {
	req := &paylink.CreatePaylinkRequest{
		Id:           suite.paylink1.Id,
		Name:         suite.paylink1.Name,
		ProductsType: suite.paylink1.ProductsType,
		ExpiresAt:    suite.paylink1.ExpiresAt.Seconds,
		Products:     suite.paylink1.Products,
		MerchantId:   suite.paylink1.MerchantId,
		ProjectId:    bson.NewObjectId().Hex(),
		NoExpiryDate: suite.paylink1.NoExpiryDate,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPaylinkProjectMismatch)
}

func (suite *PaylinkTestSuite) Test_Paylink_Update_Fail_NotFound() {
	req := &paylink.CreatePaylinkRequest{
		Id:           bson.NewObjectId().Hex(),
		Name:         suite.paylink1.Name,
		ProductsType: suite.paylink1.ProductsType,
		ExpiresAt:    suite.paylink1.ExpiresAt.Seconds,
		Products:     suite.paylink1.Products,
		MerchantId:   suite.paylink1.MerchantId,
		ProjectId:    suite.paylink1.ProjectId,
		NoExpiryDate: suite.paylink1.NoExpiryDate,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.CreateOrUpdatePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPaylinkNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Ok() {
	query := bson.M{
		"_id":         bson.ObjectIdHex(suite.paylink1.Id),
		"merchant_id": bson.ObjectIdHex(suite.paylink1.MerchantId),
		"deleted":     false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	req := &grpc.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	n, err = suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)

	query["deleted"] = true
	n, err = suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	var pl1 paylink.Paylink
	key := fmt.Sprintf(cacheKeyPaylink, suite.paylink1.Id)
	err = suite.service.cacher.Get(key, &pl1)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), pl1, paylink.Paylink{})

	var pl2 paylink.Paylink
	key = fmt.Sprintf(cacheKeyPaylinkMerchant, suite.paylink1.Id, suite.paylink1.MerchantId)
	err = suite.service.cacher.Get(key, &pl2)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), pl2, paylink.Paylink{})
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_AlreadyDeleted() {
	query := bson.M{
		"_id":         bson.ObjectIdHex(suite.paylink2.Id),
		"merchant_id": bson.ObjectIdHex(suite.paylink2.MerchantId),
		"deleted":     true,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	req := &grpc.PaylinkRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)

	n, err = suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_MerchantInvalid() {
	query := bson.M{
		"_id":     bson.ObjectIdHex(suite.paylink1.Id),
		"deleted": false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	req := &grpc.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)

	n, err = suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_Delete_Fail_IdInvalid() {
	someId := bson.NewObjectId().Hex()
	query := bson.M{
		"_id":     bson.ObjectIdHex(someId),
		"deleted": false,
	}
	n, err := suite.service.db.Collection(collectionPaylinks).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)

	req := &grpc.PaylinkRequest{
		Id:         someId,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.DeletePaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_IncrPaylinkVisits_Ok() {
	query := bson.M{
		"paylink_id": bson.ObjectIdHex(suite.paylink1.Id),
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)

	req := &grpc.PaylinkRequestById{
		Id: suite.paylink1.Id,
	}

	res := &grpc.EmptyResponse{}
	err = suite.service.IncrPaylinkVisits(context.TODO(), req, res)
	assert.NoError(suite.T(), err)

	n, err = suite.service.db.Collection(collectionPaylinkVisits).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Ok() {
	req := &grpc.GetPaylinkURLRequest{
		Id:          suite.paylink1.Id,
		MerchantId:  suite.paylink1.MerchantId,
		UrlMask:     "/my_paylink/%s/unit-test",
		UtmSource:   "unit-test-source",
		UtmMedium:   "unit-test-medium",
		UtmCampaign: "unit-test-campaign",
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Url,
		"/my_paylink/"+
			suite.paylink1.Id+
			"/unit-test?utm_campaign=unit-test-campaign&utm_medium=unit-test-medium&utm_source=unit-test-medium")
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Ok_OnlyRequiredParams() {
	req := &grpc.GetPaylinkURLRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Url, "/paylink/"+suite.paylink1.Id)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_Deleted() {
	req := &grpc.GetPaylinkURLRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_FailExpired() {
	req := &grpc.GetPaylinkURLRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusGone)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkURLRequest{
		Id:         suite.paylink2.Id,
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkURL_Fail_NotExists() {
	req := &grpc.GetPaylinkURLRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPaylinkUrlResponse{}
	err := suite.service.GetPaylinkURL(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Empty(suite.T(), res.Url)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Ok() {
	suite.paylink1.Visits = 0 // fix visits actualization

	req := &grpc.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	// fix time issues
	res.Item.CreatedAt = suite.paylink1.CreatedAt
	res.Item.UpdatedAt = suite.paylink1.UpdatedAt
	res.Item.ExpiresAt = suite.paylink1.ExpiresAt

	assert.Equal(suite.T(), res.Item, suite.paylink1)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Ok_Expired() {
	req := &grpc.PaylinkRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	// fix time issues
	res.Item.CreatedAt = suite.paylink3.CreatedAt
	res.Item.UpdatedAt = suite.paylink3.UpdatedAt
	res.Item.ExpiresAt = suite.paylink3.ExpiresAt

	assert.Equal(suite.T(), res.Item, suite.paylink3)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_Deleted() {
	req := &grpc.PaylinkRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_MerchantMismatch() {
	req := &grpc.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylink_Fail_NotExists() {
	req := &grpc.PaylinkRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPaylinkResponse{}
	err := suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Nil(suite.T(), res.Item)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinks_Ok() {
	req := &grpc.GetPaylinksRequest{
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.GetPaylinksResponse{}
	err := suite.service.GetPaylinks(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Data)
	assert.Equal(suite.T(), res.Data.Count, int32(2))
	assert.Len(suite.T(), res.Data.Items, 2)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinks_Ok_NoPaylinks() {
	req := &grpc.GetPaylinksRequest{
		MerchantId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPaylinksResponse{}
	err := suite.service.GetPaylinks(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.NotNil(suite.T(), res.Data)
	assert.Equal(suite.T(), res.Data.Count, int32(0))
	assert.Len(suite.T(), res.Data.Items, 0)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Ok_ForNewPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item
	stat.Id = ""

	statForNewPaylink := &paylink.StatCommon{
		PaylinkId: suite.paylink1.Id,
	}
	assert.Equal(suite.T(), stat, statForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Ok_ForNewExpiredPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item
	stat.Id = ""

	statForNewPaylink := &paylink.StatCommon{
		PaylinkId: suite.paylink3.Id,
	}
	assert.Equal(suite.T(), stat, statForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_Deleted() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatTotal_Fail_NotExists() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonResponse{}
	err := suite.service.GetPaylinkStatTotal(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Ok_ForNewPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Ok_ForNewExpiredPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_Deleted() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByCountry_Fail_NotExists() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByCountry(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Ok_ForNewPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Ok_ForNewExpiredPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_Deleted() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByReferrer_Fail_NotExists() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByReferrer(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Ok_ForNewPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Ok_ForNewExpiredPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_Deleted() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByDate_Fail_NotExists() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByDate(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Ok_ForNewPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink1.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Ok_ForNewExpiredPaylink() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink3.Id,
		MerchantId: suite.paylink3.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	stat := res.Item

	statGroupForNewPaylink := &paylink.GroupStatCommon{
		Top: []*paylink.StatCommon{},
		Total: &paylink.StatCommon{
			PaylinkId: suite.paylink3.Id,
		},
	}

	assert.Equal(suite.T(), stat, statGroupForNewPaylink)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_Deleted() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink2.Id,
		MerchantId: suite.paylink2.MerchantId,
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_MerchantMismatch() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         suite.paylink1.Id,
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkStatByUtm_Fail_NotExists() {
	req := &grpc.GetPaylinkStatCommonRequest{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		PeriodFrom: time.Now().Add(-1 * time.Hour).Unix(),
		PeriodTo:   time.Now().Add(1 * time.Hour).Unix(),
	}

	res := &grpc.GetPaylinkStatCommonGroupResponse{}
	err := suite.service.GetPaylinkStatByUtm(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PaylinkTestSuite) Test_Paylink_GetPaylinkVisits_Ok() {
	query := bson.M{
		"paylink_id": bson.ObjectIdHex(suite.paylink1.Id),
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)

	req := &grpc.PaylinkRequestById{
		Id: suite.paylink1.Id,
	}

	res := &grpc.EmptyResponse{}
	err = suite.service.IncrPaylinkVisits(context.TODO(), req, res)
	assert.NoError(suite.T(), err)

	n, err = suite.service.db.Collection(collectionPaylinkVisits).Find(query).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	n, err = suite.service.paylinkService.GetPaylinkVisits(suite.paylink1.Id, 0, 0)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 1)

	oneHourBefore := time.Now().Add(-1 * time.Hour).Unix()
	twoHourBefore := time.Now().Add(-2 * time.Hour).Unix()
	n, err = suite.service.paylinkService.GetPaylinkVisits(suite.paylink1.Id, twoHourBefore, oneHourBefore)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)
}
