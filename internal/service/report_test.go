package service

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"testing"
)

type ReportTestSuite struct {
	suite.Suite
	service *Service
	cache   CacheInterface
	log     *zap.Logger

	currencyRub             string
	currencyUsd             string
	project                 *billing.Project
	project1                *billing.Project
	pmBankCard              *billing.PaymentMethod
	pmBitcoin1              *billing.PaymentMethod
	productIds              []string
	merchantDefaultCurrency string
}

func Test_Report(t *testing.T) {
	suite.Run(t, new(ReportTestSuite))
}

func (suite *ReportTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	cfg.AccountingCurrency = "RUB"
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

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		broker,
		redisClient,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
		mock.NewDocumentSignerMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	_, suite.project, suite.pmBankCard, _ = helperCreateEntitiesForTests(suite.Suite, suite.service)
}

func (suite *ReportTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *ReportTestSuite) TestReport_ReturnEmptyList() {
	req := &grpc.ListOrdersRequest{}
	rsp := &grpc.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.Equal(suite.T(), int32(0), rsp.Item.Count)
	assert.Empty(suite.T(), rsp.Item.Items)
}

func (suite *ReportTestSuite) TestReport_FindById() {
	req := &grpc.ListOrdersRequest{Id: uuid.New().String()}
	rsp := &grpc.ListOrdersPublicResponse{}
	err := suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)
	assert.Equal(suite.T(), int32(0), rsp.Item.Count)

	order := helperCreateAndPayOrder(suite.Suite, suite.service, 555.55, "RUB", "RU", suite.project, suite.pmBankCard)

	req = &grpc.ListOrdersRequest{Id: order.Uuid}
	err = suite.service.FindAllOrdersPublic(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Equal(suite.T(), int32(1), rsp.Item.Count)
	assert.Equal(suite.T(), order.Id, rsp.Item.Items[0].Id)
}
