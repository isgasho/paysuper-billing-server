package service

import (
	"context"
	"github.com/centrifugal/gocent"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
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
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"net/http"
	"testing"
	"time"
)

type RoyaltyReportTestSuite struct {
	suite.Suite
	service    *Service
	log        *zap.Logger
	cache      CacheInterface
	httpClient *http.Client

	project   *billing.Project
	project1  *billing.Project
	project2  *billing.Project
	merchant  *billing.Merchant
	merchant1 *billing.Merchant
	merchant2 *billing.Merchant

	paymentMethod *billing.PaymentMethod
	paymentSystem *billing.PaymentSystem
}

func Test_RoyaltyReport(t *testing.T) {
	suite.Run(t, new(RoyaltyReportTestSuite))
}

func (suite *RoyaltyReportTestSuite) SetupTest() {
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
	suite.httpClient = mock.NewClientStatusOk()
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
		mock.NewSmtpSenderMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant, suite.project, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)

	suite.service.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       cfg.CentrifugoURL,
			Key:        cfg.CentrifugoSecret,
			HTTPClient: suite.httpClient,
		},
	)

	suite.merchant1 = helperCreateMerchant(suite.Suite, suite.service, "USD", "RU", suite.paymentMethod)
	suite.merchant2 = helperCreateMerchant(suite.Suite, suite.service, "USD", "RU", suite.paymentMethod)

	suite.project1 = &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusDraft,
		MerchantId:               suite.merchant1.Id,
	}
	suite.project2 = &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 2"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 2 secret key",
		Status:                   pkg.ProjectStatusDraft,
		MerchantId:               suite.merchant2.Id,
	}

	projects := []*billing.Project{suite.project1, suite.project2}
	err = suite.service.project.MultipleInsert(projects)

	if err != nil {
		suite.FailNow("Insert projects test data failed", "%v", err)
	}
}

func (suite *RoyaltyReportTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_CreateRoyaltyReport_AllMerchants_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	assert.NoError(suite.T(), err)

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	from := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{"period_from": from, "period_to": to}).All(&reports)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), reports)
	assert.Len(suite.T(), reports, 3)

	var existMerchants []string

	for _, v := range reports {
		assert.NotZero(suite.T(), v.Id)
		assert.NotZero(suite.T(), v.Amounts)
		assert.NotZero(suite.T(), v.Status)
		assert.NotZero(suite.T(), v.MerchantId)
		assert.NotZero(suite.T(), v.PeriodFrom)
		assert.NotZero(suite.T(), v.PeriodTo)
		assert.NotZero(suite.T(), v.AcceptExpireAt)
		assert.NotZero(suite.T(), v.Amounts.PayoutAmount)
		assert.NotZero(suite.T(), v.Amounts.Currency)
		assert.NotZero(suite.T(), v.Amounts.VatAmount)
		assert.NotZero(suite.T(), v.Amounts.FeeAmount)
		assert.NotZero(suite.T(), v.Amounts.GrossAmount)
		assert.NotZero(suite.T(), v.Amounts.TransactionsCount)

		t, err := ptypes.Timestamp(v.PeriodFrom)
		assert.NoError(suite.T(), err)
		t1, err := ptypes.Timestamp(v.PeriodTo)
		assert.NoError(suite.T(), err)

		assert.Equal(suite.T(), t.In(loc), from)
		assert.Equal(suite.T(), t1.In(loc), to)
		assert.InDelta(suite.T(), suite.service.cfg.RoyaltyReportAcceptTimeout, v.AcceptExpireAt.Seconds-time.Now().Unix(), 10)

		existMerchants = append(existMerchants, v.MerchantId)
	}

	assert.Contains(suite.T(), existMerchants, suite.merchant.Id)
	assert.Contains(suite.T(), existMerchants, suite.merchant1.Id)
	assert.Contains(suite.T(), existMerchants, suite.merchant2.Id)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_CreateRoyaltyReport_SelectedMerchants_Ok() {
	projects := []*billing.Project{suite.project, suite.project1}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{
		Merchants: []string{suite.project.GetMerchantId(), suite.project1.GetMerchantId()},
	}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).All(&reports)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), reports)
	assert.Len(suite.T(), reports, len(req.Merchants))

	var existMerchants []string

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	assert.NoError(suite.T(), err)

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	from := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	for _, v := range reports {
		assert.NotZero(suite.T(), v.Id)
		assert.NotZero(suite.T(), v.Amounts)
		assert.NotZero(suite.T(), v.Status)
		assert.NotZero(suite.T(), v.MerchantId)
		assert.NotZero(suite.T(), v.PeriodFrom)
		assert.NotZero(suite.T(), v.PeriodTo)
		assert.NotZero(suite.T(), v.AcceptExpireAt)
		assert.NotZero(suite.T(), v.Amounts.PayoutAmount)
		assert.NotZero(suite.T(), v.Amounts.Currency)
		assert.NotZero(suite.T(), v.Amounts.VatAmount)
		assert.NotZero(suite.T(), v.Amounts.FeeAmount)
		assert.NotZero(suite.T(), v.Amounts.GrossAmount)
		assert.NotZero(suite.T(), v.Amounts.TransactionsCount)

		t, err := ptypes.Timestamp(v.PeriodFrom)
		assert.NoError(suite.T(), err)
		t1, err := ptypes.Timestamp(v.PeriodTo)
		assert.NoError(suite.T(), err)

		assert.Equal(suite.T(), t.In(loc), from)
		assert.Equal(suite.T(), t1.In(loc), to)
		assert.InDelta(suite.T(), suite.service.cfg.RoyaltyReportAcceptTimeout, v.AcceptExpireAt.Seconds-time.Now().Unix(), 10)

		existMerchants = append(existMerchants, v.MerchantId)
	}

	assert.Contains(suite.T(), existMerchants, suite.merchant.Id)
	assert.Contains(suite.T(), existMerchants, suite.merchant1.Id)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_CreateRoyaltyReport_EmptyMerchants_Error() {
	req := &grpc.CreateRoyaltyReportRequest{
		Merchants: []string{"incorrect_hex"},
	}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, royaltyReportErrorNoTransactions)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).All(&reports)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), reports)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_CreateRoyaltyReport_NotExistMerchant_Error() {
	req := &grpc.CreateRoyaltyReportRequest{
		Merchants: []string{bson.NewObjectId().Hex()},
	}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), rsp.Merchants)
	assert.Len(suite.T(), rsp.Merchants, 1)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).All(&reports)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), reports)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_CreateRoyaltyReport_UnknownTimeZone_Error() {
	suite.service.cfg.RoyaltyReportTimeZone = "incorrect_timezone"
	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, royaltyReportErrorTimezoneIncorrect)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).All(&reports)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), reports)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	req1 := &grpc.ListRoyaltyReportsRequest{}
	rsp1 := &grpc.ListRoyaltyReportsResponse{}
	err = suite.service.ListRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp1.Data.Count)
	assert.Len(suite.T(), rsp1.Data.Items, int(rsp1.Data.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Data.Count)
	assert.Empty(suite.T(), rsp.Data.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindById_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	req1 := &grpc.ListRoyaltyReportsRequest{Id: report.Id}
	rsp1 := &grpc.ListRoyaltyReportsResponse{}
	err = suite.service.ListRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp1.Data.Count)
	assert.Len(suite.T(), rsp1.Data.Items, int(rsp1.Data.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindById_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{Id: bson.NewObjectId().Hex()}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Data.Count)
	assert.Len(suite.T(), rsp.Data.Items, int(rsp.Data.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByMerchantId_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	assert.NoError(suite.T(), err)

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour).Add(-time.Duration(168) * time.Hour)
	from := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	query := bson.M{"merchant_id": bson.ObjectIdHex(suite.project.GetMerchantId())}
	set := bson.M{"$set": bson.M{"period_from": from, "period_to": to}}
	_, err = suite.service.db.Collection(collectionRoyaltyReport).UpdateAll(query, set)

	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	req1 := &grpc.ListRoyaltyReportsRequest{MerchantId: suite.project.GetMerchantId()}
	rsp1 := &grpc.ListRoyaltyReportsResponse{}
	err = suite.service.ListRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 2, rsp1.Data.Count)
	assert.Len(suite.T(), rsp1.Data.Items, int(rsp1.Data.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByMerchantId_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{MerchantId: bson.NewObjectId().Hex()}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Data.Count)
	assert.Empty(suite.T(), rsp.Data.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByPeriod_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	assert.NoError(suite.T(), err)

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour).Add(-time.Duration(168) * time.Hour)
	from := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	query := bson.M{"merchant_id": bson.ObjectIdHex(suite.project.GetMerchantId())}
	set := bson.M{"$set": bson.M{"period_from": from, "period_to": to}}
	_, err = suite.service.db.Collection(collectionRoyaltyReport).UpdateAll(query, set)

	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	to = now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	from = to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	req1 := &grpc.ListRoyaltyReportsRequest{
		PeriodFrom: from.Unix(),
		PeriodTo:   to.Unix(),
	}
	rsp1 := &grpc.ListRoyaltyReportsResponse{}
	err = suite.service.ListRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp1.Data.Count)
	assert.Len(suite.T(), rsp1.Data.Items, int(rsp1.Data.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByPeriod_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{
		PeriodFrom: time.Now().Unix(),
		PeriodTo:   time.Now().Unix(),
	}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Data.Count)
	assert.Empty(suite.T(), rsp.Data.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_Ok() {
	suite.createOrder(suite.project)
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	req1 := &grpc.ChangeRoyaltyReportRequest{
		ReportId: report.Id,
		Status:   pkg.RoyaltyReportStatusPending,
		Ip:       "127.0.0.1",
		Source:   "user",
	}
	rsp1 := &grpc.ResponseError{}
	err = suite.service.ChangeRoyaltyReport(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	err = suite.service.db.Collection(collectionRoyaltyReport).FindId(bson.ObjectIdHex(report.Id)).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusPending, report.Status)

	var changes []*billing.RoyaltyReportChanges
	err = suite.service.db.Collection(collectionRoyaltyReportChanges).
		Find(bson.M{"royalty_report_id": bson.ObjectIdHex(report.Id)}).Sort("-created_at").All(&changes)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), changes, 2)
	assert.Equal(suite.T(), req1.Ip, changes[0].Ip)
	assert.Equal(suite.T(), req1.Source, changes[0].Source)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, changes[0].Before.Status)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusPending, changes[0].After.Status)

	smtpCl, ok := suite.service.smtpCl.(*mock.SendCloserMock)
	assert.True(suite.T(), ok)
	assert.NoError(suite.T(), smtpCl.Err)

	centrifugoCl, ok := suite.httpClient.Transport.(*mock.TransportStatusOk)
	assert.True(suite.T(), ok)
	assert.NoError(suite.T(), centrifugoCl.Err)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_Accepted_Ok() {
	suite.createOrder(suite.project)
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)
	assert.EqualValues(suite.T(), -62135596800, report.AcceptedAt.Seconds)

	report.Status = pkg.RoyaltyReportStatusPending
	err = suite.service.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)
	assert.NoError(suite.T(), err)

	req1 := &grpc.ChangeRoyaltyReportRequest{
		ReportId: report.Id,
		Status:   pkg.RoyaltyReportStatusAccepted,
		Ip:       "127.0.0.1",
		Source:   "user",
	}
	rsp1 := &grpc.ResponseError{}
	err = suite.service.ChangeRoyaltyReport(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	err = suite.service.db.Collection(collectionRoyaltyReport).FindId(bson.ObjectIdHex(report.Id)).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusAccepted, report.Status)
	assert.NotEqual(suite.T(), int64(-62135596800), report.AcceptedAt.Seconds)

	var changes []*billing.RoyaltyReportChanges
	err = suite.service.db.Collection(collectionRoyaltyReportChanges).
		Find(bson.M{"royalty_report_id": bson.ObjectIdHex(report.Id)}).Sort("-created_at").All(&changes)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), changes, 2)
	assert.Equal(suite.T(), req1.Ip, changes[0].Ip)
	assert.Equal(suite.T(), req1.Source, changes[0].Source)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusPending, changes[0].Before.Status)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusAccepted, changes[0].After.Status)

	smtpCl, ok := suite.service.smtpCl.(*mock.SendCloserMock)
	assert.True(suite.T(), ok)
	assert.NoError(suite.T(), smtpCl.Err)

	centrifugoCl, ok := suite.httpClient.Transport.(*mock.TransportStatusOk)
	assert.True(suite.T(), ok)
	assert.NoError(suite.T(), centrifugoCl.Err)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_ReportNotFound_Error() {
	req := &grpc.ChangeRoyaltyReportRequest{
		ReportId: bson.NewObjectId().Hex(),
		Status:   pkg.RoyaltyReportStatusPending,
		Ip:       "127.0.0.1",
		Source:   "user",
	}
	rsp := &grpc.ResponseError{}
	err := suite.service.ChangeRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), royaltyReportErrorReportNotFound, rsp.Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_ChangeNotAllowed_Error() {
	suite.createOrder(suite.project)
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	req1 := &grpc.ChangeRoyaltyReportRequest{
		ReportId: report.Id,
		Status:   pkg.RoyaltyReportStatusAccepted,
		Ip:       "127.0.0.1",
		Source:   "user",
	}
	rsp1 := &grpc.ResponseError{}
	err = suite.service.ChangeRoyaltyReport(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp1.Status)
	assert.Equal(suite.T(), royaltyReportErrorReportStatusChangeDenied, rsp1.Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_StatusPaymentError_Error() {
	suite.createOrder(suite.project)
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	report.Status = pkg.RoyaltyReportStatusPending
	err = suite.service.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)
	assert.NoError(suite.T(), err)

	req1 := &grpc.ChangeRoyaltyReportRequest{
		ReportId: report.Id,
		Status:   pkg.RoyaltyReportStatusDispute,
		Ip:       "127.0.0.1",
		Source:   "user",
	}
	rsp1 := &grpc.ResponseError{}
	err = suite.service.ChangeRoyaltyReport(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp1.Status)
	assert.Equal(suite.T(), royaltyReportErrorReportDisputeCorrectionRequired, rsp1.Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReportOrders_Ok() {
	for i := 0; i < 10; i++ {
		suite.createOrder(suite.project)
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	req1 := &grpc.ListRoyaltyReportOrdersRequest{ReportId: report.Id, Limit: 5, Offset: 0}
	rsp1 := &grpc.TransactionsResponse{}
	err = suite.service.ListRoyaltyReportOrders(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.NotEmpty(suite.T(), rsp1.Data)
	assert.Equal(suite.T(), rsp1.Data.Count, req1.Limit)
	assert.Len(suite.T(), rsp1.Data.Items, int(req1.Limit))

	for _, v := range rsp1.Data.Items {
		assert.NotZero(suite.T(), v.CreatedAt)
		assert.NotZero(suite.T(), v.CountryCode)
		assert.NotZero(suite.T(), v.Transaction)
		assert.NotZero(suite.T(), v.PaymentMethod)
		assert.NotZero(suite.T(), v.TotalPaymentAmount)
		assert.NotZero(suite.T(), v.Currency)
	}
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReportOrders_ReportNotFound_Error() {
	req := &grpc.ListRoyaltyReportOrdersRequest{ReportId: bson.NewObjectId().Hex(), Limit: 5, Offset: 0}
	rsp := &grpc.TransactionsResponse{}
	err := suite.service.ListRoyaltyReportOrders(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReportOrders_OrdersNotFound_Error() {
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.EqualError(suite.T(), err, royaltyReportErrorNoTransactions)

}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_SendRoyaltyReportNotification_MerchantNotFound_Error() {
	core, recorded := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	report := &billing.RoyaltyReport{
		MerchantId: bson.NewObjectId().Hex(),
	}
	suite.service.sendRoyaltyReportNotification(report)
	assert.True(suite.T(), recorded.Len() == 1)

	messages := recorded.All()
	assert.Equal(suite.T(), "Merchant not found", messages[0].Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_SendRoyaltyReportNotification_EmailSendError() {
	for i := 0; i < 10; i++ {
		suite.createOrder(suite.project)
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	core, recorded := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	suite.service.smtpCl = mock.NewSmtpSenderMockError()

	suite.service.sendRoyaltyReportNotification(report)
	assert.True(suite.T(), recorded.Len() == 1)

	messages := recorded.All()
	assert.Equal(suite.T(), "[SMTP] Send merchant notification about new royalty report failed", messages[0].Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_SendRoyaltyReportNotification_CentrifugoSendError() {
	for i := 0; i < 10; i++ {
		suite.createOrder(suite.project)
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	core, recorded := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	suite.service.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       suite.service.cfg.CentrifugoURL,
			Key:        suite.service.cfg.CentrifugoSecret,
			HTTPClient: mock.NewClientStatusError(),
		},
	)

	suite.service.sendRoyaltyReportNotification(report)
	assert.True(suite.T(), recorded.Len() == 1)

	messages := recorded.All()
	assert.Equal(suite.T(), "[Centrifugo] Send merchant notification about new royalty report failed", messages[0].Message)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_AutoAcceptRoyaltyReports_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}
	err := suite.service.updateOrderView([]string{})
	assert.NoError(suite.T(), err)

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	_, err = suite.service.db.Collection(collectionRoyaltyReport).
		UpdateAll(
			bson.M{"merchant_id": bson.ObjectIdHex(suite.project.GetMerchantId())},
			bson.M{
				"$set": bson.M{
					"accept_expire_at": time.Now().Add(-time.Duration(336) * time.Hour),
					"status":           pkg.RoyaltyReportStatusPending,
				},
			},
		)
	assert.NoError(suite.T(), err)

	req1 := &grpc.EmptyRequest{}
	rsp1 := &grpc.EmptyResponse{}
	err = suite.service.AutoAcceptRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)

	var reports []*billing.RoyaltyReport
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).All(&reports)
	assert.NoError(suite.T(), err)

	for _, v := range reports {
		if v.MerchantId == suite.project.GetMerchantId() {
			assert.True(suite.T(), v.IsAutoAccepted)
			assert.Equal(suite.T(), pkg.RoyaltyReportStatusAccepted, v.Status)
		} else {
			assert.False(suite.T(), v.IsAutoAccepted)
			assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, v.Status)
		}
	}
}

func (suite *RoyaltyReportTestSuite) createOrder(project *billing.Project) *billing.Order {
	order := helperCreateAndPayOrder(
		suite.Suite,
		suite.service,
		100,
		"RUB",
		"RU",
		project,
		suite.paymentMethod,
	)

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	if !assert.NoError(suite.T(), err) {
		suite.FailNow("time.LoadLocation failed", "%v", err)
	}
	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	date := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod/2) * time.Second).In(loc)

	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(date)
	err = suite.service.updateOrder(order)
	if !assert.NoError(suite.T(), err) {
		suite.FailNow("update order failed", "%v", err)
	}

	query := bson.M{"merchant_id": bson.ObjectIdHex(project.GetMerchantId())}
	set := bson.M{"$set": bson.M{"created_at": date}}
	_, err = suite.service.db.Collection(collectionAccountingEntry).UpdateAll(query, set)
	if !assert.NoError(suite.T(), err) {
		suite.FailNow("accounting entries update failed", "%v", err)
	}

	return order
}
