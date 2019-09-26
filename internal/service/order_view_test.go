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
	"github.com/paysuper/paysuper-recurring-repository/tools"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"testing"
	"time"
)

type OrderViewTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	merchant           *billing.Merchant
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)
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
	orderPublic, err := suite.service.orderView.GetOrderBy(order.Id, "", "", new(billing.OrderViewPublic))
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

	orderPrivate, err := suite.service.orderView.GetOrderBy(order.Id, "", "", new(billing.OrderViewPrivate))
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

func (suite *OrderViewTestSuite) Test_OrderView_GetRoyaltySummary_Ok_NoTransactions() {
	to := time.Now().Add(time.Duration(5) * time.Hour)
	from := to.Add(-time.Duration(10) * time.Hour)

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
	assert.NoError(suite.T(), err)

	assert.Len(suite.T(), summaryItems, 0)

	assert.NotNil(suite.T(), summaryTotal)
	assert.Equal(suite.T(), summaryTotal.Product, "")
	assert.Equal(suite.T(), summaryTotal.Region, "")
	assert.Equal(suite.T(), summaryTotal.TotalTransactions, int32(0))
	assert.Equal(suite.T(), summaryTotal.SalesCount, int32(0))
	assert.Equal(suite.T(), summaryTotal.ReturnsCount, int32(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossSalesAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossReturnsAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossTotalAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), float64(0))

	controlTotal := summaryTotal.GrossSalesAmount - summaryTotal.TotalFees - summaryTotal.TotalVat
	assert.Equal(suite.T(), summaryTotal.PayoutAmount, controlTotal)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetRoyaltySummary_Ok_OnlySales() {
	countries := []string{"RU", "FI"}
	var orders []*billing.Order
	numberOfOrders := 3

	count := 0
	for count < numberOfOrders {
		order := helperCreateAndPayOrder(
			suite.Suite,
			suite.service,
			10,
			"USD",
			countries[count%2],
			suite.projectFixedAmount,
			suite.paymentMethod,
		)
		assert.NotNil(suite.T(), order)
		orders = append(orders, order)

		count++
	}

	to := time.Now().Add(time.Duration(5) * time.Hour)
	from := to.Add(-time.Duration(10) * time.Hour)

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
	assert.NoError(suite.T(), err)

	assert.Len(suite.T(), summaryItems, 2)

	assert.Equal(suite.T(), summaryItems[0].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[0].Region, "FI")
	assert.Equal(suite.T(), summaryItems[0].TotalTransactions, int32(1))
	assert.Equal(suite.T(), summaryItems[0].SalesCount, int32(1))
	assert.Equal(suite.T(), summaryItems[0].ReturnsCount, int32(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossSalesAmount), float64(12))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossReturnsAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossTotalAmount), float64(12))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), float64(0.66))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), float64(2.01))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), float64(9.34))

	controlTotal0 := summaryItems[0].GrossSalesAmount - summaryItems[0].TotalFees - summaryItems[0].TotalVat
	assert.Equal(suite.T(), summaryItems[0].PayoutAmount, controlTotal0)

	assert.Equal(suite.T(), summaryItems[1].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[1].Region, "RU")
	assert.Equal(suite.T(), summaryItems[1].TotalTransactions, int32(2))
	assert.Equal(suite.T(), summaryItems[1].SalesCount, int32(2))
	assert.Equal(suite.T(), summaryItems[1].ReturnsCount, int32(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossSalesAmount), float64(24))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossReturnsAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossTotalAmount), float64(24))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), float64(1.32))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), float64(4.1))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), float64(18.6))

	controlTotal1 := summaryItems[1].GrossSalesAmount - summaryItems[1].TotalFees - summaryItems[1].TotalVat
	assert.Equal(suite.T(), summaryItems[1].PayoutAmount, controlTotal1)

	assert.NotNil(suite.T(), summaryTotal)
	assert.Equal(suite.T(), summaryTotal.Product, "")
	assert.Equal(suite.T(), summaryTotal.Region, "")
	assert.Equal(suite.T(), summaryTotal.TotalTransactions, int32(3))
	assert.Equal(suite.T(), summaryTotal.SalesCount, int32(3))
	assert.Equal(suite.T(), summaryTotal.ReturnsCount, int32(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossSalesAmount), float64(36))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossReturnsAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossTotalAmount), float64(36))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), float64(1.97))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), float64(6.10))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), float64(27.94))

	controlTotal := summaryTotal.GrossSalesAmount - summaryTotal.TotalFees - summaryTotal.TotalVat
	assert.Equal(suite.T(), summaryTotal.PayoutAmount, controlTotal)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetRoyaltySummary_Ok_SalesAndRefunds() {
	countries := []string{"RU", "FI"}
	var orders []*billing.Order
	numberOfOrders := 3

	count := 0
	for count < numberOfOrders {
		order := helperCreateAndPayOrder(
			suite.Suite,
			suite.service,
			10,
			"USD",
			countries[count%2],
			suite.projectFixedAmount,
			suite.paymentMethod,
		)
		assert.NotNil(suite.T(), order)
		orders = append(orders, order)

		count++
	}

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	for _, order := range orders {
		refund := helperMakeRefund(suite.Suite, suite.service, order, order.TotalPaymentAmount, false)
		assert.NotNil(suite.T(), refund)
	}

	to := time.Now().Add(time.Duration(5) * time.Hour)
	from := to.Add(-time.Duration(10) * time.Hour)

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
	assert.NoError(suite.T(), err)

	assert.Len(suite.T(), summaryItems, 2)

	assert.Equal(suite.T(), summaryItems[0].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[0].Region, "FI")
	assert.Equal(suite.T(), summaryItems[0].TotalTransactions, int32(2))
	assert.Equal(suite.T(), summaryItems[0].SalesCount, int32(1))
	assert.Equal(suite.T(), summaryItems[0].ReturnsCount, int32(1))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossSalesAmount), float64(12))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossReturnsAmount), float64(12))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].GrossTotalAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), float64(0.66))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), float64(-0.65))

	controlTotal0 := summaryItems[0].GrossTotalAmount - summaryItems[0].TotalFees - summaryItems[0].TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), tools.FormatAmount(controlTotal0))

	assert.Equal(suite.T(), summaryItems[1].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[1].Region, "RU")
	assert.Equal(suite.T(), summaryItems[1].TotalTransactions, int32(4))
	assert.Equal(suite.T(), summaryItems[1].SalesCount, int32(2))
	assert.Equal(suite.T(), summaryItems[1].ReturnsCount, int32(2))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossSalesAmount), float64(24))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossReturnsAmount), float64(24))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].GrossTotalAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), float64(1.32))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), float64(-1.31))

	controlTotal1 := summaryItems[1].GrossTotalAmount - summaryItems[1].TotalFees - summaryItems[1].TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), tools.FormatAmount(controlTotal1))

	assert.NotNil(suite.T(), summaryTotal)
	assert.Equal(suite.T(), summaryTotal.Product, "")
	assert.Equal(suite.T(), summaryTotal.Region, "")
	assert.Equal(suite.T(), summaryTotal.TotalTransactions, int32(6))
	assert.Equal(suite.T(), summaryTotal.SalesCount, int32(3))
	assert.Equal(suite.T(), summaryTotal.ReturnsCount, int32(3))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossSalesAmount), float64(36))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossReturnsAmount), float64(36))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.GrossTotalAmount), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), float64(1.97))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), float64(-1.96))

	controlTotal := summaryTotal.GrossTotalAmount - summaryTotal.TotalFees - summaryTotal.TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), tools.FormatAmount(controlTotal))
}
