package service

import (
	"context"
	rabbitmq "github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type VatReportsTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	projectFixedAmount *billing.Project
	paymentMethod      *billing.PaymentMethod
	paymentSystem      *billing.PaymentSystem
}

func Test_VatReports(t *testing.T) {
	suite.Run(t, new(VatReportsTestSuite))
}

func (suite *VatReportsTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

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
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	_, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)
}

func (suite *VatReportsTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *VatReportsTestSuite) TestVatReports_getLastVatReportTime() {
	_, _, err := suite.service.getLastVatReportTime(0)
	assert.Error(suite.T(), err)

	from, to, err := suite.service.getLastVatReportTime(int32(3))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfQuarter())
	assert.Equal(suite.T(), to, now.EndOfQuarter())

	from, to, err = suite.service.getLastVatReportTime(int32(1))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, now.BeginningOfMonth())
	assert.Equal(suite.T(), to, now.EndOfMonth())

	fromRef := now.BeginningOfMonth()
	toRef := now.EndOfMonth()

	if fromRef.Month()%2 == 0 {
		fromRef = fromRef.AddDate(0, -1, 0)
	} else {
		toRef = toRef.AddDate(0, 1, 0)
	}

	from, to, err = suite.service.getLastVatReportTime(int32(2))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from, fromRef)
	assert.Equal(suite.T(), to, toRef)
	assert.Equal(suite.T(), fromRef.Month()%2, time.Month(1))
	assert.Equal(suite.T(), toRef.Month()%2, time.Month(0))
}

func (suite *VatReportsTestSuite) TestVatReports_getVatReportTimeForDate() {

	t, err := time.Parse(time.RFC3339, "2019-06-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)

	from, to, err := suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	from, to, err = suite.service.getVatReportTimeForDate(int32(1), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-06-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-05-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-05-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-05-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-07-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-07-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-08-31T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-08-29T11:45:26.371Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(2), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-07-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-08-31T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-04-01T00:00:00Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")

	t, err = time.Parse(time.RFC3339, "2019-06-30T23:59:59Z")
	assert.NoError(suite.T(), err)
	from, to, err = suite.service.getVatReportTimeForDate(int32(3), t)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), from.Format(time.RFC3339), "2019-04-01T00:00:00Z")
	assert.Equal(suite.T(), to.Format(time.RFC3339), "2019-06-30T23:59:59Z")
}

func (suite *VatReportsTestSuite) TestVatReports_ProcessVatReports() {

	amounts := []float64{100, 10}
	currencies := []string{"RUB", "USD"}
	countries := []string{"RU", "FI"}
	orders := []*billing.Order{}

	count := 0
	for count < 10 {
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

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	for _, order := range orders {
		refund := helperMakeRefund(suite.Suite, suite.service, order, order.TotalPaymentAmount*0.5, false)
		assert.NotNil(suite.T(), refund)
	}

	req := &grpc.ProcessVatReportsRequest{
		Date: ptypes.TimestampNow(),
	}

	err = suite.service.ProcessVatReports(context.TODO(), req, &grpc.EmptyResponse{})
	assert.NoError(suite.T(), err)
}
