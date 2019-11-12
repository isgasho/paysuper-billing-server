package service

import (
	"context"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"net/http"
	"testing"
	"time"
)

type MerchantBalanceTestSuite struct {
	suite.Suite
	service    *Service
	log        *zap.Logger
	cache      internalPkg.CacheInterface
	httpClient *http.Client

	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs

	merchant  *billing.Merchant
	merchant2 *billing.Merchant
}

func Test_MerchantBalance(t *testing.T) {
	suite.Run(t, new(MerchantBalanceTestSuite))
}

func (suite *MerchantBalanceTestSuite) SetupTest() {
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
	suite.httpClient = mocks.NewClientStatusOk()
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	var core zapcore.Core

	lvl := zap.NewAtomicLevel()
	core, suite.zapRecorder = observer.New(lvl)
	suite.logObserver = zap.New(core)

	suite.merchant = helperCreateMerchant(suite.Suite, suite.service, "RUB", "RU", nil, 13000)
	suite.merchant2 = helperCreateMerchant(suite.Suite, suite.service, "", "RU", nil, 0)
}

func (suite *MerchantBalanceTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetMerchantBalance_Ok_AutoCreateBalance() {
	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	req := &grpc.GetMerchantBalanceRequest{
		MerchantId: suite.merchant.Id,
	}

	res := &grpc.GetMerchantBalanceResponse{}

	err := suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), res.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), res.Item.Debit, float64(0))
	assert.Equal(suite.T(), res.Item.Credit, float64(0))
	assert.Equal(suite.T(), res.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), res.Item.Total, float64(0))

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetMerchantBalance_Ok_NoAutoCreateBalance() {
	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	mb, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mb.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mb.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mb.Debit, float64(0))
	assert.Equal(suite.T(), mb.Credit, float64(0))
	assert.Equal(suite.T(), mb.RollingReserve, float64(0))
	assert.Equal(suite.T(), mb.Total, float64(0))

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	req := &grpc.GetMerchantBalanceRequest{
		MerchantId: suite.merchant.Id,
	}

	res := &grpc.GetMerchantBalanceResponse{}

	err = suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	// ugly cheat, because Nanos are slightly differs
	// example:
	// mb:       seconds: 1568218155 nanos: 740067400
	// res.Item: seconds: 1568218155 nanos: 740000000
	// wtf?
	mb.CreatedAt.Nanos = 0
	res.Item.CreatedAt.Nanos = 0

	assert.Equal(suite.T(), res.Item, mb)

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetMerchantBalance_Failed_MerchantNotFound() {
	merchantId = bson.NewObjectId().Hex()

	count := suite.mbRecordsCount(merchantId, "")
	assert.Equal(suite.T(), count, 0)

	req := &grpc.GetMerchantBalanceRequest{
		MerchantId: merchantId,
	}

	res := &grpc.GetMerchantBalanceResponse{}

	err := suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.EqualError(suite.T(), err, "merchant not found")

	count = suite.mbRecordsCount(merchantId, "")
	assert.Equal(suite.T(), count, 0)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetMerchantBalance_Failed_NoPayoutCurrency() {
	count := suite.mbRecordsCount(suite.merchant2.Id, suite.merchant2.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	req := &grpc.GetMerchantBalanceRequest{
		MerchantId: suite.merchant2.Id,
	}

	res := &grpc.GetMerchantBalanceResponse{}

	err := suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.Equal(suite.T(), res.Message, errorMerchantPayoutCurrencyNotSet)

	count = suite.mbRecordsCount(suite.merchant2.Id, suite.merchant2.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_updateMerchantBalance_Failed_MerchantNotFound() {
	merchantId = bson.NewObjectId().Hex()
	count := suite.mbRecordsCount(merchantId, "")
	assert.Equal(suite.T(), count, 0)

	mb, err := suite.service.updateMerchantBalance(merchantId)
	assert.EqualError(suite.T(), err, "merchant not found")
	assert.Nil(suite.T(), mb)

	count = suite.mbRecordsCount(merchantId, "")
	assert.Equal(suite.T(), count, 0)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_updateMerchantBalance_Failed_NoPayoutCurrency() {
	count := suite.mbRecordsCount(suite.merchant2.Id, suite.merchant2.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	mb, err := suite.service.updateMerchantBalance(suite.merchant2.Id)
	assert.EqualError(suite.T(), err, errorMerchantPayoutCurrencyNotSet.Error())
	assert.Nil(suite.T(), mb)

	count = suite.mbRecordsCount(suite.merchant2.Id, suite.merchant2.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_updateMerchantBalance_Ok() {

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}
	err := suite.service.db.Collection(collectionRoyaltyReport).Insert(report)
	assert.NoError(suite.T(), err)

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate PayoutDocument date failed")

	payout := &billing.PayoutDocument{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{bson.NewObjectId().Hex()},
		TotalFees:          1000,
		Balance:            1000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          date,
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
	}
	err = suite.service.payoutDocument.Insert(payout, "127.0.0.1", payoutChangeSourceAdmin)
	assert.NoError(suite.T(), err)

	ae1 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Type:   pkg.AccountingEntryTypeMerchantRollingReserveCreate,
		Object: pkg.ObjectTypeBalanceTransaction,
		Source: &billing.AccountingEntrySource{
			Type: "merchant",
			Id:   suite.merchant.Id,
		},
		MerchantId: suite.merchant.Id,
		Amount:     150,
		Currency:   "RUB",
		CreatedAt:  ptypes.TimestampNow(),
	}

	ae2 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Type:   pkg.AccountingEntryTypeMerchantRollingReserveRelease,
		Object: pkg.ObjectTypeBalanceTransaction,
		Source: &billing.AccountingEntrySource{
			Type: "merchant",
			Id:   suite.merchant.Id,
		},
		MerchantId: suite.merchant.Id,
		Amount:     50,
		Currency:   "RUB",
		CreatedAt:  ptypes.TimestampNow(),
	}

	accountingEntries := []interface{}{ae1, ae2}
	err = suite.service.db.Collection(collectionAccountingEntry).Insert(accountingEntries...)
	assert.NoError(suite.T(), err)

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	mb, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mb.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mb.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mb.Debit, float64(1234.5))
	assert.Equal(suite.T(), mb.Credit, float64(1000))
	assert.Equal(suite.T(), mb.RollingReserve, float64(100))
	assert.Equal(suite.T(), mb.Total, float64(134.5))

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_getRollingReserveForBalance_Ok() {

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate PayoutDocument date failed")

	payout := &billing.PayoutDocument{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{bson.NewObjectId().Hex()},
		TotalFees:          1000,
		Balance:            1000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          date,
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
	}
	err = suite.service.payoutDocument.Insert(payout, "127.0.0.1", payoutChangeSourceAdmin)
	assert.NoError(suite.T(), err)

	rr, err := suite.service.getRollingReserveForBalance(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rr, float64(0))
}

// 1. trigger for merchant accepting royalty report
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_1() {
	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusPending,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}
	err := suite.service.db.Collection(collectionRoyaltyReport).Insert(report)
	assert.NoError(suite.T(), err)

	req1 := &grpc.MerchantReviewRoyaltyReportRequest{
		ReportId:   report.Id,
		IsAccepted: true,
		Ip:         "127.0.0.1",
	}
	rsp1 := &grpc.ResponseError{}
	err = suite.service.MerchantReviewRoyaltyReport(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(1234.5))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(1234.5))
}

// 2. Triggering on payout status change
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_3() {

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 100,
			PayoutAmount:      alreadyPaidRoyalty,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	err := suite.service.db.Collection(collectionRoyaltyReport).Insert(report)
	assert.NoError(suite.T(), err)

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate PayoutDocument date failed")

	payout := &billing.PayoutDocument{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{report.Id},
		TotalFees:          1000,
		Balance:            1000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          date,
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
	}
	err = suite.service.payoutDocument.Insert(payout, "127.0.0.1", payoutChangeSourceAdmin)
	assert.NoError(suite.T(), err)

	req3 := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: payout.Id,
		Status:           pkg.PayoutDocumentStatusPaid,
		Transaction:      "transaction123",
		Ip:               "192.168.1.1",
	}

	res3 := &grpc.PayoutDocumentResponse{}

	err = suite.service.UpdatePayoutDocument(context.TODO(), req3, res3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res3.Status, pkg.ResponseStatusOk)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(10432))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(1000))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(9432))
}

// 3. Triggering on rolling reserve create
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_4() {

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	req4 := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypeMerchantRollingReserveCreate,
		MerchantId: suite.merchant.Id,
		Amount:     150,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}
	rsp4 := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req4, rsp4)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp4.Status)
	assert.Empty(suite.T(), rsp4.Message)
	assert.NotNil(suite.T(), rsp4.Item)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(150))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(-150))
}

// 4. Triggering on rolling reserve create
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_5() {

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	req5 := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypeMerchantRollingReserveRelease,
		MerchantId: suite.merchant.Id,
		Amount:     50,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}
	rsp5 := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req5, rsp5)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp5.Status)
	assert.Empty(suite.T(), rsp5.Message)
	assert.NotNil(suite.T(), rsp5.Item)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(-50))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(50))
}

// 5. trigger on auto-accepting royalty report
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_6() {

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate PayoutDocument date failed")

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1000,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusPending,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: date,
		Currency:       suite.merchant.GetPayoutCurrency(),
	}
	err = suite.service.db.Collection(collectionRoyaltyReport).Insert(report)
	assert.NoError(suite.T(), err)

	req6 := &grpc.EmptyRequest{}
	rsp6 := &grpc.EmptyResponse{}
	err = suite.service.AutoAcceptRoyaltyReports(context.TODO(), req6, rsp6)
	assert.NoError(suite.T(), err)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(1000))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(1000))
}

// 6. trigger on change royalty report with accepting
func (suite *MerchantBalanceTestSuite) TestMerchantBalance_UpdateBalanceTriggeringOk_7() {

	count := suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 0)

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate PayoutDocument date failed")

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      500,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusPending,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: date,
		Currency:       suite.merchant.GetPayoutCurrency(),
	}
	err = suite.service.db.Collection(collectionRoyaltyReport).Insert(report)
	assert.NoError(suite.T(), err)

	req7 := &grpc.ChangeRoyaltyReportRequest{
		ReportId: report.Id,
		Status:   pkg.RoyaltyReportStatusAccepted,
		Ip:       "127.0.0.1",
	}
	rsp7 := &grpc.ResponseError{}
	err = suite.service.ChangeRoyaltyReport(context.TODO(), req7, rsp7)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp7.Status)
	assert.Empty(suite.T(), rsp7.Message)

	// control

	count = suite.mbRecordsCount(suite.merchant.Id, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), count, 1)

	mbReq := &grpc.GetMerchantBalanceRequest{MerchantId: suite.merchant.Id}
	mbRes := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), mbReq, mbRes)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), mbRes.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), mbRes.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), mbRes.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), mbRes.Item.Debit, float64(500))
	assert.Equal(suite.T(), mbRes.Item.Credit, float64(0))
	assert.Equal(suite.T(), mbRes.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), mbRes.Item.Total, float64(500))
}

func (suite *MerchantBalanceTestSuite) mbRecordsCount(merchantId, currency string) int {
	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"currency":    currency,
	}
	count, err := suite.service.db.Collection(collectionMerchantBalances).Find(query).Count()
	if err == nil {
		return count
	}
	if err == mgo.ErrNotFound {
		return 0
	}
	return -1
}
