package service

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
	"time"
)

type AccountingEntryTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   internalPkg.CacheInterface

	projectFixedAmount *billing.Project
	paymentMethod      *billing.PaymentMethod
	paymentSystem      *billing.PaymentSystem
	merchant           *billing.Merchant
}

var ctx = context.TODO()

func Test_AccountingEntry(t *testing.T) {
	suite.Run(t, new(AccountingEntryTestSuite))
}

func (suite *AccountingEntryTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
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
		broker,
		&casbinMocks.CasbinService{},
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)
}

func (suite *AccountingEntryTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_RUB_RUB_RUB() {
	// Order currency RUB
	// Royalty currency RUB
	// VAT currency RUB

	req := &grpc.GetMerchantByRequest{
		MerchantId: suite.projectFixedAmount.MerchantId,
	}
	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(ctx, req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)

	merchant := rsp.Item
	merchant.Banking.Currency = "RUB"
	err = suite.service.merchant.Update(ctx, merchant)
	assert.Nil(suite.T(), err)

	orderAmount := float64(100)
	orderCountry := "RU"
	orderCurrency := "RUB"
	orderControlResults := map[string]float64{
		"real_gross_revenue":                        120,
		"real_tax_fee":                              20,
		"central_bank_tax_fee":                      0,
		"real_tax_fee_total":                        20,
		"ps_gross_revenue_fx":                       0,
		"ps_gross_revenue_fx_tax_fee":               0,
		"ps_gross_revenue_fx_profit":                0,
		"merchant_gross_revenue":                    120,
		"merchant_tax_fee_cost_value":               20,
		"merchant_tax_fee_central_bank_fx":          0,
		"merchant_tax_fee":                          20,
		"ps_method_fee":                             6,
		"merchant_method_fee":                       3,
		"merchant_method_fee_cost_value":            1.8,
		"ps_markup_merchant_method_fee":             1.2,
		"merchant_method_fixed_fee":                 1.4688,
		"real_merchant_method_fixed_fee":            1.44,
		"markup_merchant_method_fixed_fee_fx":       0.0288,
		"real_merchant_method_fixed_fee_cost_value": 0.65,
		"ps_method_fixed_fee_profit":                0.79,
		"merchant_ps_fixed_fee":                     3.672,
		"real_merchant_ps_fixed_fee":                3.6,
		"markup_merchant_ps_fixed_fee":              0.072,
		"ps_method_profit":                          7.222,
		"merchant_net_revenue":                      90.328,
		"ps_profit_total":                           7.222,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          120,
		"real_refund_tax_fee":                  20,
		"real_refund_fee":                      12,
		"real_refund_fixed_fee":                10.8,
		"merchant_refund":                      120,
		"ps_merchant_refund_fx":                0,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -12,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      20,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             20,
		"merchant_reverse_revenue":             100,
		"ps_refund_profit":                     -12.15,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err = suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, false)
	assert.NotNil(suite.T(), refund)

	accountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(accountingEntries), len(orderControlResults)-11)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range accountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, orderControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], tools.ToPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], tools.ToPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	suite.helperCheckOrderView(order.Id, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, orderControlResults)

	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_RUB_USD_RUB() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency RUB

	orderAmount := float64(650)
	orderCountry := "RU"
	orderCurrency := "RUB"
	orderControlResults := map[string]float64{
		"real_gross_revenue":                        12,
		"real_tax_fee":                              2,
		"central_bank_tax_fee":                      0,
		"real_tax_fee_total":                        2,
		"ps_gross_revenue_fx":                       0.24,
		"ps_gross_revenue_fx_tax_fee":               0.04,
		"ps_gross_revenue_fx_profit":                0.2,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               1.96,
		"merchant_tax_fee_central_bank_fx":          0.045938,
		"merchant_tax_fee":                          2.005938,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.294,
		"merchant_method_fee_cost_value":            0.18,
		"ps_markup_merchant_method_fee":             0.114,
		"merchant_method_fixed_fee":                 0.022597,
		"real_merchant_method_fixed_fee":            0.022154,
		"markup_merchant_method_fixed_fee_fx":       0.000443,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.012154,
		"merchant_ps_fixed_fee":                     0.056492,
		"real_merchant_ps_fixed_fee":                0.055385,
		"markup_merchant_ps_fixed_fee":              0.001107,
		"ps_method_profit":                          0.454492,
		"merchant_net_revenue":                      9.10957,
		"ps_profit_total":                           0.654492,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      2.005938,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.001875,
		"merchant_reverse_tax_fee":             2.005937,
		"merchant_reverse_revenue":             10.2340625,
		"ps_refund_profit":                     -1.3159375,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults)-11)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, orderControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], tools.ToPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], tools.ToPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	suite.helperCheckOrderView(order.Id, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, orderControlResults)

	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_RUB_USD_USD() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency USD

	orderAmount := float64(650)
	orderCountry := "US"
	orderCurrency := "RUB"
	orderControlResults := map[string]float64{
		"real_gross_revenue":                        12,
		"real_tax_fee":                              2,
		"central_bank_tax_fee":                      0,
		"real_tax_fee_total":                        2,
		"ps_gross_revenue_fx":                       0.24,
		"ps_gross_revenue_fx_tax_fee":               0.04,
		"ps_gross_revenue_fx_profit":                0.2,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               1.96,
		"merchant_tax_fee_central_bank_fx":          0,
		"merchant_tax_fee":                          1.96,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.294,
		"merchant_method_fee_cost_value":            0.18,
		"ps_markup_merchant_method_fee":             0.114,
		"merchant_method_fixed_fee":                 0.022597,
		"real_merchant_method_fixed_fee":            0.022154,
		"markup_merchant_method_fixed_fee_fx":       0.000443,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.012154,
		"merchant_ps_fixed_fee":                     0.056492,
		"real_merchant_ps_fixed_fee":                0.055385,
		"markup_merchant_ps_fixed_fee":              0.001107,
		"ps_method_profit":                          0.454492,
		"merchant_net_revenue":                      9.155508,
		"ps_profit_total":                           0.654492,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      1.96,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             1.96,
		"merchant_reverse_revenue":             10.28,
		"ps_refund_profit":                     -1.27,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults)-11)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, orderControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], tools.ToPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], tools.ToPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	suite.helperCheckOrderView(order.Id, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, orderControlResults)

	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_RUB_USD_EUR() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency EUR

	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"
	orderControlResults := map[string]float64{
		"real_gross_revenue":                        12,
		"real_tax_fee":                              2,
		"central_bank_tax_fee":                      0,
		"real_tax_fee_total":                        2,
		"ps_gross_revenue_fx":                       0.24,
		"ps_gross_revenue_fx_tax_fee":               0.04,
		"ps_gross_revenue_fx_profit":                0.2,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               1.96,
		"merchant_tax_fee_central_bank_fx":          0.004436,
		"merchant_tax_fee":                          1.964436,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.294,
		"merchant_method_fee_cost_value":            0.18,
		"ps_markup_merchant_method_fee":             0.114,
		"merchant_method_fixed_fee":                 0.022597,
		"real_merchant_method_fixed_fee":            0.022154,
		"markup_merchant_method_fixed_fee_fx":       0.000443,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.012154,
		"merchant_ps_fixed_fee":                     0.056492,
		"real_merchant_ps_fixed_fee":                0.055385,
		"markup_merchant_ps_fixed_fee":              0.001107,
		"ps_method_profit":                          0.454492,
		"merchant_net_revenue":                      9.151072,
		"ps_profit_total":                           0.654492,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      1.964436,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.00018,
		"merchant_reverse_tax_fee":             1.964435,
		"merchant_reverse_revenue":             10.2755646552,
		"ps_refund_profit":                     -1.2744353448,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults)-11)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, orderControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], tools.ToPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], tools.ToPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	suite.helperCheckOrderView(order.Id, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, orderControlResults)

	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PartialRefund_Ok_RUB_USD_EUR() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"
	refundControlResults := map[string]float64{
		"real_refund":                          6,
		"real_refund_tax_fee":                  1,
		"real_refund_fee":                      0.6,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      6.12,
		"ps_merchant_refund_fx":                0.12,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -0.6,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      0.982218,
		"reverse_tax_fee_delta":                0.002128,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             0.982218,
		"merchant_reverse_revenue":             5.137782,
		"ps_refund_profit":                     -0.712218,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount*0.5, false)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_Ok_RUB_RUB_RUB() {
	// Order currency RUB
	// Royalty currency RUB
	// VAT currency RUB

	req := &grpc.GetMerchantByRequest{
		MerchantId: suite.projectFixedAmount.MerchantId,
	}
	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.NotNil(suite.T(), rsp.Item)

	merchant := rsp.Item
	merchant.Banking.Currency = "RUB"
	err = suite.service.merchant.Update(ctx, merchant)
	assert.Nil(suite.T(), err)

	orderAmount := float64(100)
	orderCountry := "RU"
	orderCurrency := "RUB"

	refundControlResults := map[string]float64{
		"real_refund":                          120,
		"real_refund_tax_fee":                  20,
		"real_refund_fee":                      12,
		"real_refund_fixed_fee":                10.8,
		"merchant_refund":                      120,
		"ps_merchant_refund_fx":                0,
		"merchant_refund_fee":                  24,
		"ps_markup_merchant_refund_fee":        12,
		"merchant_refund_fixed_fee_cost_value": 10.8,
		"merchant_refund_fixed_fee":            11.016,
		"ps_merchant_refund_fixed_fee_fx":      0.216,
		"ps_merchant_refund_fixed_fee_profit":  10.866,
		"reverse_tax_fee":                      20,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             20,
		"merchant_reverse_revenue":             135.016,
		"ps_refund_profit":                     22.866,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err = suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_Ok_RUB_USD_RUB() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency RUB

	orderAmount := float64(650)
	orderCountry := "RU"
	orderCurrency := "RUB"

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.166154,
		"merchant_refund_fixed_fee":            0.169477,
		"ps_merchant_refund_fixed_fee_fx":      0.0033230769,
		"ps_merchant_refund_fixed_fee_profit":  0.0194769231,
		"reverse_tax_fee":                      2.005938,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.001875,
		"merchant_reverse_tax_fee":             2.005938,
		"merchant_reverse_revenue":             12.8515394231,
		"ps_refund_profit":                     1.2693519231,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_Ok_RUB_USD_USD() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency USD

	orderAmount := float64(650)
	orderCountry := "US"
	orderCurrency := "RUB"

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.166154,
		"merchant_refund_fixed_fee":            0.169477,
		"ps_merchant_refund_fixed_fee_fx":      0.0033230769,
		"ps_merchant_refund_fixed_fee_profit":  0.0194769231,
		"reverse_tax_fee":                      1.96,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             1.96,
		"merchant_reverse_revenue":             12.8974769231,
		"ps_refund_profit":                     1.3474769231,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_Ok_RUB_USD_EUR() {
	// Order currency RUB
	// Royalty currency USD
	// VAT currency EUR

	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_tax_fee":                  2,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.166154,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.166154,
		"merchant_refund_fixed_fee":            0.169477,
		"ps_merchant_refund_fixed_fee_fx":      0.003323,
		"ps_merchant_refund_fixed_fee_profit":  0.019477,
		"reverse_tax_fee":                      1.964436,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.00018,
		"merchant_reverse_tax_fee":             1.964435,
		"merchant_reverse_revenue":             12.893042,
		"ps_refund_profit":                     1.343042,
	}

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.CreatedOrderId, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults)-7)
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			fmt.Println(entry.Type, entry.Amount, refundControlResults[entry.Type])
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], tools.ToPrecise(controlRealRefund))

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	err = suite.service.db.Collection(collectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)
	assert.NoError(suite.T(), err)
	suite.helperCheckRefundView(refund.CreatedOrderId, orderCurrency, merchantRoyaltyCurrency, country.VatCurrency, refundControlResults)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Ok() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)

	req := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypeRealGrossRevenue,
		OrderId:    order.Id,
		RefundId:   refund.Id,
		MerchantId: order.GetMerchantId(),
		Amount:     10,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err = suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	oid, _ := primitive.ObjectIDFromHex(rsp.Item.Id)
	err = suite.service.db.Collection(collectionAccountingEntry).FindOne(ctx, bson.M{"_id": oid}).Decode(&accountingEntry)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), accountingEntry)

	assert.Equal(suite.T(), req.Type, accountingEntry.Type)
	assert.Equal(suite.T(), req.MerchantId, accountingEntry.Source.Id)
	assert.Equal(suite.T(), collectionMerchant, accountingEntry.Source.Type)
	assert.Equal(suite.T(), req.Amount, accountingEntry.Amount)
	assert.Equal(suite.T(), req.Currency, accountingEntry.Currency)
	assert.Equal(suite.T(), req.Status, accountingEntry.Status)
	assert.Equal(suite.T(), req.Reason, accountingEntry.Reason)

	t, err := ptypes.Timestamp(accountingEntry.CreatedAt)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), req.Date, t.Unix())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_MerchantNotFound_Error() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)

	req := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypeRealGrossRevenue,
		OrderId:    order.Id,
		RefundId:   refund.Id,
		MerchantId: primitive.NewObjectID().Hex(),
		Amount:     10,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}

	rsp := &grpc.CreateAccountingEntryResponse{}
	err = suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorMerchantNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		FindOne(ctx, bson.M{"source.id": req.MerchantId, "source.type": collectionMerchant}).Decode(&accountingEntry)
	assert.Error(suite.T(), mongo.ErrNoDocuments, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_OrderNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypeRealGrossRevenue,
		OrderId:  primitive.NewObjectID().Hex(),
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		FindOne(ctx, bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).Decode(&accountingEntry)
	assert.Error(suite.T(), mongo.ErrNoDocuments, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_RefundNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypeRealGrossRevenue,
		RefundId: primitive.NewObjectID().Hex(),
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		FindOne(ctx, bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).Decode(&accountingEntry)
	assert.Error(suite.T(), mongo.ErrNoDocuments, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Refund_OrderNotFound_Error() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(ctx, suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, true)
	assert.NotNil(suite.T(), refund)

	refund.OriginalOrder.Id = primitive.NewObjectID().Hex()
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	_, err = suite.service.db.Collection(collectionRefund).ReplaceOne(ctx, bson.M{"_id": oid}, refund)
	assert.NoError(suite.T(), err)

	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypeRealGrossRevenue,
		RefundId: refund.Id,
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err = suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		FindOne(ctx, bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).Decode(&accountingEntry)
	assert.Error(suite.T(), mongo.ErrNoDocuments, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_EntryNotExist_Error() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, suite.projectFixedAmount, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	req := &grpc.CreateAccountingEntryRequest{
		Type:     "not_exist_accounting_entry_name",
		OrderId:  order.Id,
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorUnknownEntry, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		FindOne(ctx, bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).Decode(&accountingEntry)
	assert.Error(suite.T(), mongo.ErrNoDocuments, err)
}

func (suite *AccountingEntryTestSuite) helperGetAccountingEntries(orderId, collection string) []*billing.AccountingEntry {
	var accountingEntries []*billing.AccountingEntry
	oid, err := primitive.ObjectIDFromHex(orderId)
	assert.NoError(suite.T(), err)
	cursor, err := suite.service.db.Collection(collectionAccountingEntry).
		Find(ctx, bson.M{"source.id": oid, "source.type": collection})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &accountingEntries)
	assert.NoError(suite.T(), err)

	return accountingEntries
}

func (suite *AccountingEntryTestSuite) helperCheckOrderView(orderId, orderCurrency, royaltyCurrency, vatCurrency string, orderControlResults map[string]float64) {
	ow, err := suite.service.orderView.GetOrderBy(ctx, orderId, "", "", new(billing.OrderViewPrivate))

	orderView := ow.(*billing.OrderViewPrivate)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), orderView)

	assert.Equal(suite.T(), orderView.PaymentGrossRevenueOrigin.Currency, orderCurrency)
	assert.Equal(suite.T(), orderView.PaymentGrossRevenue.Currency, royaltyCurrency)
	assert.Equal(suite.T(), orderView.PaymentGrossRevenueLocal.Currency, vatCurrency)

	assert.Equal(suite.T(), orderView.PaymentTaxFeeOrigin.Currency, orderCurrency)
	assert.Equal(suite.T(), orderView.PaymentTaxFee.Currency, royaltyCurrency)
	assert.Equal(suite.T(), orderView.PaymentTaxFeeLocal.Currency, vatCurrency)

	a := orderView.PaymentTaxFeeTotal.Amount
	b := orderControlResults["real_tax_fee"] + orderControlResults["central_bank_tax_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["real_tax_fee_total"])

	a = orderView.TaxFeeTotal.Amount
	b = orderControlResults["merchant_tax_fee_cost_value"] + orderControlResults["merchant_tax_fee_central_bank_fx"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["merchant_tax_fee"])

	a = orderView.FeesTotal.Amount
	b = orderControlResults["ps_method_fee"] + orderControlResults["merchant_ps_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

	a = orderView.PaymentGrossRevenueFxProfit.Amount
	b = orderControlResults["ps_gross_revenue_fx"] - orderControlResults["ps_gross_revenue_fx_tax_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["ps_gross_revenue_fx_profit"])

	a = orderView.GrossRevenue.Amount
	b = orderControlResults["real_gross_revenue"] - orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["merchant_gross_revenue"])

	a = orderView.PaysuperMethodFeeProfit.Amount
	b = orderControlResults["merchant_method_fee"] - orderControlResults["merchant_method_fee_cost_value"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["ps_markup_merchant_method_fee"])

	a = orderView.PaysuperMethodFixedFeeTariffFxProfit.Amount
	b = orderControlResults["merchant_method_fixed_fee"] - orderControlResults["real_merchant_method_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["markup_merchant_method_fixed_fee_fx"])

	a = orderView.PaysuperMethodFixedFeeTariffTotalProfit.Amount
	b = orderControlResults["real_merchant_method_fixed_fee"] - orderControlResults["real_merchant_method_fixed_fee_cost_value"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["ps_method_fixed_fee_profit"])

	a = orderView.PaysuperFixedFeeFxProfit.Amount
	b = orderControlResults["merchant_ps_fixed_fee"] - orderControlResults["real_merchant_ps_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["markup_merchant_ps_fixed_fee"])

	a = orderView.NetRevenue.Amount
	b = orderControlResults["real_gross_revenue"] -
		orderControlResults["merchant_tax_fee_central_bank_fx"] -
		orderControlResults["ps_gross_revenue_fx"] -
		orderControlResults["merchant_tax_fee_cost_value"] -
		orderControlResults["ps_method_fee"] -
		orderControlResults["merchant_ps_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["merchant_net_revenue"])

	a = orderView.PaysuperMethodTotalProfit.Amount
	b = orderControlResults["ps_method_fee"] +
		orderControlResults["merchant_ps_fixed_fee"] -
		orderControlResults["merchant_method_fee_cost_value"] -
		orderControlResults["real_merchant_method_fixed_fee_cost_value"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, orderControlResults["ps_method_profit"])

	a = orderView.PaysuperTotalProfit.Amount
	b = orderControlResults["ps_gross_revenue_fx"] +
		orderControlResults["ps_method_fee"] +
		orderControlResults["merchant_ps_fixed_fee"] -
		orderControlResults["central_bank_tax_fee"] -
		orderControlResults["ps_gross_revenue_fx_tax_fee"] -
		orderControlResults["merchant_method_fee_cost_value"] -
		orderControlResults["real_merchant_method_fixed_fee_cost_value"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))
	assert.Equal(suite.T(), a, tools.ToPrecise(orderControlResults["ps_profit_total"]))
}

func (suite *AccountingEntryTestSuite) helperCheckRefundView(orderId, orderCurrency, royaltyCurrency, vatCurrency string, refundControlResults map[string]float64) {
	order, err := suite.service.orderView.GetOrderBy(ctx, orderId, "", "", new(billing.OrderViewPrivate))
	assert.NoError(suite.T(), err)
	orderView := order.(*billing.OrderViewPrivate)
	assert.NotNil(suite.T(), orderView)

	assert.Equal(suite.T(), orderView.PaymentRefundGrossRevenueOrigin.Currency, orderCurrency)
	assert.Equal(suite.T(), orderView.PaymentRefundGrossRevenue.Currency, royaltyCurrency)
	assert.Equal(suite.T(), orderView.PaymentRefundGrossRevenueLocal.Currency, vatCurrency)

	assert.Equal(suite.T(), orderView.PaymentRefundTaxFeeOrigin.Currency, orderCurrency)
	assert.Equal(suite.T(), orderView.PaymentRefundTaxFee.Currency, royaltyCurrency)
	assert.Equal(suite.T(), orderView.PaymentRefundTaxFeeLocal.Currency, vatCurrency)

	a := orderView.RefundTaxFeeTotal.Amount
	b := refundControlResults["reverse_tax_fee"] + refundControlResults["reverse_tax_fee_delta"]
	assert.Equal(suite.T(), tools.ToPrecise(a), tools.ToPrecise(b))

	a = orderView.RefundFeesTotal.Amount
	b = refundControlResults["merchant_refund_fee"] + refundControlResults["merchant_refund_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

	a = orderView.RefundGrossRevenueFx.Amount
	b = refundControlResults["merchant_refund"] - refundControlResults["real_refund"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

	a = orderView.PaysuperMethodRefundFeeTariffProfit.Amount
	b = refundControlResults["merchant_refund_fee"] - refundControlResults["real_refund_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

	a = orderView.PaysuperMethodRefundFixedFeeTariffProfit.Amount
	b = refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["real_refund_fixed_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

	a = orderView.RefundReverseRevenue.Amount
	b = refundControlResults["merchant_refund"] + refundControlResults["merchant_refund_fee"] + refundControlResults["merchant_refund_fixed_fee"] + refundControlResults["reverse_tax_fee_delta"] - refundControlResults["reverse_tax_fee"]
	assert.Equal(suite.T(), tools.ToPrecise(a), tools.ToPrecise(b))

	a = orderView.PaysuperRefundTotalProfit.Amount
	b = refundControlResults["merchant_refund_fee"] + refundControlResults["merchant_refund_fixed_fee"] + refundControlResults["ps_reverse_tax_fee_delta"] - refundControlResults["real_refund_fixed_fee"] - refundControlResults["real_refund_fee"]
	assert.Equal(suite.T(), a, tools.ToPrecise(b))

}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_USD_EUR_None() {
	// Order currency USD
	// Royalty currency EUR
	// VAT currency NONE

	orderAmount := float64(650)
	orderCountry := "AO"
	orderCurrency := "USD"
	royaltyCurrency := "EUR"
	merchantCountry := "DE"

	merchant := helperCreateMerchant(suite.Suite, suite.service, royaltyCurrency, merchantCountry, suite.paymentMethod, 0, suite.merchant.OperatingCompanyId)
	project := helperCreateProject(suite.Suite, suite.service, merchant.Id)

	country, err := suite.service.country.GetByIsoCodeA2(ctx, orderCountry)
	assert.NoError(suite.T(), err)

	paymentMerCost := &billing.PaymentChannelCostMerchant{
		MerchantId:              merchant.Id,
		Name:                    "MASTERCARD",
		PayoutCurrency:          royaltyCurrency,
		MinAmount:               0,
		Region:                  country.PayerTariffRegion,
		Country:                 country.IsoCodeA2,
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 pkg.MccCodeLowRisk,
	}

	err = suite.service.paymentChannelCostMerchant.Insert(ctx, paymentMerCost)
	assert.NoError(suite.T(), err)

	order := helperCreateAndPayOrder(suite.Suite, suite.service, orderAmount, orderCurrency, orderCountry, project, suite.paymentMethod)
	assert.NotNil(suite.T(), order)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), 15)
}
