package service

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

type ReportTestSuite struct {
	suite.Suite
	service *Service
	cache   database.CacheInterface
	log     *zap.Logger

	currencyRub             string
	currencyUsd             string
	project                 *billingpb.Project
	project1                *billingpb.Project
	pmBankCard              *billingpb.PaymentMethod
	pmBitcoin1              *billingpb.PaymentMethod
	productIds              []string
	merchantDefaultCurrency string
}

func Test_Report(t *testing.T) {
	suite.Run(t, new(ReportTestSuite))
}

func (suite *ReportTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

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
	assert.NoError(suite.T(), err, "Database connection failed")

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)
	assert.NoError(suite.T(), err, "Creating RabbitMQ publisher failed")

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

	_, suite.project, suite.pmBankCard, _ = helperCreateEntitiesForTests(suite.Suite, suite.service)
}

func (suite *ReportTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *ReportTestSuite) TestReport_ReturnEmptyList() {
	req := &billingpb.ListOrdersRequest{}
	rsp := &billingpb.ListOrdersPublicResponse{}

	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)
	assert.Empty(suite.T(), rsp.Item.Items)

	rsp1 := &billingpb.ListOrdersPrivateResponse{}
	err = suite.service.FindAllOrdersPrivate(context.TODO(), req, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.EqualValues(suite.T(), int32(0), rsp1.Item.Count)
	assert.Empty(suite.T(), rsp1.Item.Items)

	rsp2 := &billingpb.ListOrdersResponse{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.EqualValues(suite.T(), int32(0), rsp1.Item.Count)
	assert.Empty(suite.T(), rsp1.Item.Items)
}

func (suite *ReportTestSuite) TestReport_FindById() {
	req := &billingpb.ListOrdersRequest{Id: uuid.New().String()}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)

	req = &billingpb.ListOrdersRequest{Id: order.Uuid}
	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), int32(1), rsp.Item.Count)
	assert.Equal(suite.T(), order.Id, rsp.Item.Items[0].Id)

	rsp1 := &billingpb.ListOrdersPrivateResponse{}
	err = suite.service.FindAllOrdersPrivate(context.TODO(), req, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.EqualValues(suite.T(), int32(1), rsp1.Item.Count)
	assert.Equal(suite.T(), order.Id, rsp1.Item.Items[0].Id)

	rsp2 := &billingpb.ListOrdersResponse{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.EqualValues(suite.T(), int32(1), rsp1.Item.Count)
	assert.Equal(suite.T(), order.Id, rsp1.Item.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByMerchantId() {
	req := &billingpb.ListOrdersRequest{Merchant: []string{suite.project.MerchantId}}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	order1 := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
	order2 := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
	order3 := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)

	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 3, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	var orderIds []string

	for _, v := range rsp.Item.Items {
		orderIds = append(orderIds, v.Id)
	}

	assert.Contains(suite.T(), orderIds, order1.Id)
	assert.Contains(suite.T(), orderIds, order2.Id)
	assert.Contains(suite.T(), orderIds, order3.Id)
}

func (suite *ReportTestSuite) TestReport_FindByProject() {
	req := &billingpb.ListOrdersRequest{Project: []string{suite.project.Id}}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByCountry() {
	req := &billingpb.ListOrdersRequest{Country: []string{"RU"}}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 4; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 4, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByPaymentMethod() {
	req := &billingpb.ListOrdersRequest{PaymentMethod: []string{suite.pmBankCard.Id}}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByStatus() {
	req := &billingpb.ListOrdersRequest{Status: []string{recurringpb.OrderPublicStatusProcessed}}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByAccount() {
	req := &billingpb.ListOrdersRequest{Account: "test@unit.unit"}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), 0, rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	req.Account = "400000"
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}

	req = &billingpb.ListOrdersRequest{QuickSearch: suite.project.Name["en"]}
	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByPmDateFrom() {
	req := &billingpb.ListOrdersRequest{PmDateFrom: time.Now().Unix() - 10}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByPmDateTo() {
	req := &billingpb.ListOrdersRequest{PmDateTo: time.Now().Unix() + 1000}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string
	date := &timestamp.Timestamp{}

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
		date = order.PaymentMethodOrderClosedAt
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	req.PmDateTo = date.Seconds + 100
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByProjectDateFrom() {
	req := &billingpb.ListOrdersRequest{ProjectDateFrom: time.Now().Unix() - 10}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_FindByProjectDateTo() {
	req := &billingpb.ListOrdersRequest{ProjectDateTo: time.Now().Unix() + 100}
	rsp := &billingpb.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.Count)

	var orderIds []string

	for i := 0; i < 5; i++ {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)
		orderIds = append(orderIds, order.Id)
	}

	req.Merchant = append(req.Merchant, suite.project.MerchantId)
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.EqualValues(suite.T(), 5, rsp.Item.Count)
	assert.Len(suite.T(), rsp.Item.Items, int(rsp.Item.Count))

	for _, v := range rsp.Item.Items {
		assert.Contains(suite.T(), orderIds, v.Id)
	}
}

func (suite *ReportTestSuite) TestReport_GetOrder() {
	req := &billingpb.GetOrderRequest{
		OrderId:    primitive.NewObjectID().Hex(),
		MerchantId: suite.project.MerchantId,
	}
	rsp := &billingpb.GetOrderPublicResponse{}
	err := suite.service.GetOrderPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), orderErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	rsp1 := &billingpb.GetOrderPrivateResponse{}
	err = suite.service.GetOrderPrivate(context.TODO(), req, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), orderErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)

	req.OrderId = order.Uuid
	err = suite.service.GetOrderPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)

	err = suite.service.GetOrderPrivate(context.TODO(), req, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
}
