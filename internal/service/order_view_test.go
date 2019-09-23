package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"testing"
)

type OrderViewTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	projectFixedAmount *billing.Project
	paymentMethod      *billing.PaymentMethod
	paymentSystem      *billing.PaymentSystem
}

func Test_OrderView(t *testing.T) {
	suite.Run(t, new(OrderViewTestSuite))
}

func (suite *OrderViewTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	_, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)
}

func (suite *OrderViewTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *OrderViewTestSuite) Test_OrderView_updateOrderView() {
	amounts := []float64{100, 10}
	currencies := []string{"RUB", "USD"}
	countries := []string{"RU", "FI"}
	var orders []*billing.Order

	count := 0
	for count < suite.service.cfg.OrderViewUpdateBatchSize+10 {
		order := helperCreateAndPayOrder(
			suite.Suite,
			suite.service,
			amounts[count%2],
			currencies[count%2],
			countries[count%2],
			suite.projectFixedAmount,
			suite.paymentMethod,
		)
		assert.NotNil(suite.T(), order)
		orders = append(orders, order)

		count++
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetOrderFromViewPublic_Ok() {
	order := helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"USD",
		"RU",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)

	orderPublic, err := suite.service.orderView.GetOrderFromViewPublic(order.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), orderPublic)
	assert.IsType(suite.T(), &billing.OrderViewPublic{}, orderPublic)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetOrderFromViewPrivate_Ok() {
	order := helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"USD",
		"RU",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)

	orderPrivate, err := suite.service.orderView.GetOrderFromViewPrivate(order.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), orderPrivate)
	assert.IsType(suite.T(), &billing.OrderViewPrivate{}, orderPrivate)
}

func (suite *OrderViewTestSuite) Test_OrderView_CountTransactions_Ok() {
	_ = helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"RUB",
		"RU",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)
	_ = helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		200,
		"USD",
		"FI",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)

	count, err := suite.service.orderView.CountTransactions(bson.M{})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 2)

	count, err = suite.service.orderView.CountTransactions(bson.M{"country_code": "FI"})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 1)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetTransactionsPublic_Ok() {
	transactions, err := suite.service.orderView.GetTransactionsPublic(bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 0)

	_ = helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"USD",
		"RU",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)

	transactions, err = suite.service.orderView.GetTransactionsPublic(bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 1)
	assert.IsType(suite.T(), &billing.OrderViewPublic{}, transactions[0])
}

func (suite *OrderViewTestSuite) Test_OrderView_GetTransactionsPrivate_Ok() {
	transactions, err := suite.service.orderView.GetTransactionsPrivate(bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 0)

	_ = helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"USD",
		"RU",
		suite.projectFixedAmount,
		suite.paymentMethod,
	)

	transactions, err = suite.service.orderView.GetTransactionsPrivate(bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 1)
	assert.IsType(suite.T(), &billing.OrderViewPrivate{}, transactions[0])
}
