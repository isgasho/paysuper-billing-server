package service

import (
	"context"
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
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/paylink"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
	"time"
)

type OrderViewTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   database.CacheInterface

	merchant            *billing.Merchant
	projectFixedAmount  *billing.Project
	projectWithProducts *billing.Project
	paymentMethod       *billing.PaymentMethod
	paymentSystem       *billing.PaymentSystem
	paylink1            *paylink.Paylink
}

func Test_OrderView(t *testing.T) {
	suite.Run(t, new(OrderViewTestSuite))
}

func (suite *OrderViewTestSuite) SetupTest() {
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

	ctx, _ := context.WithTimeout(context.Background(), 50*time.Second)
	opts := []mongodb.Option{mongodb.Context(ctx)}
	db, err := mongodb.NewDatabase(opts...)
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

	suite.projectWithProducts = &billing.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       true,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusDraft,
		MerchantId:               suite.merchant.Id,
		VatPayer:                 pkg.VatPayerBuyer,
	}

	if err := suite.service.project.Insert(context.TODO(), suite.projectWithProducts); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	products := createProductsForProject(suite.Suite, suite.service, suite.projectWithProducts, 1)
	assert.Len(suite.T(), products, 1)

	paylinkBod, _ := ptypes.TimestampProto(now.BeginningOfDay())
	paylinkExpiresAt, _ := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))

	suite.paylink1 = &paylink.Paylink{
		Id:                   primitive.NewObjectID().Hex(),
		Object:               "paylink",
		Products:             []string{products[0].Id},
		ExpiresAt:            paylinkExpiresAt,
		CreatedAt:            paylinkBod,
		UpdatedAt:            paylinkBod,
		MerchantId:           suite.projectWithProducts.MerchantId,
		ProjectId:            suite.projectWithProducts.Id,
		Name:                 "Willy Wonka Strikes Back",
		IsExpired:            false,
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

	err = suite.service.paylinkService.Insert(context.TODO(), suite.paylink1)
	assert.NoError(suite.T(), err)
}

func (suite *OrderViewTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
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
	err := suite.service.updateOrderView(context.TODO(), []string{})
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
	orderPublic, err := suite.service.orderView.GetOrderBy(context.TODO(), order.Id, "", "", new(billing.OrderViewPublic))
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

	orderPrivate, err := suite.service.orderView.GetOrderBy(context.TODO(), order.Id, "", "", new(billing.OrderViewPrivate))
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

	count, err := suite.service.orderView.CountTransactions(context.TODO(), bson.M{})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), count, 2)

	count, err = suite.service.orderView.CountTransactions(context.TODO(), bson.M{"country_code": "FI"})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), count, 1)
}

func (suite *OrderViewTestSuite) Test_OrderView_GetTransactionsPublic_Ok() {
	transactions, err := suite.service.orderView.GetTransactionsPublic(context.TODO(), bson.M{}, 10, 0)
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

	transactions, err = suite.service.orderView.GetTransactionsPublic(context.TODO(), bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 1)
	assert.IsType(suite.T(), &billing.OrderViewPublic{}, transactions[0])
}

func (suite *OrderViewTestSuite) Test_OrderView_GetTransactionsPrivate_Ok() {
	transactions, err := suite.service.orderView.GetTransactionsPrivate(context.TODO(), bson.M{}, 10, 0)
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

	transactions, err = suite.service.orderView.GetTransactionsPrivate(context.TODO(), bson.M{}, 10, 0)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), transactions, 1)
	assert.IsType(suite.T(), &billing.OrderViewPrivate{}, transactions[0])
}

func (suite *OrderViewTestSuite) Test_OrderView_GetRoyaltySummary_Ok_NoTransactions() {
	to := time.Now().Add(time.Duration(5) * time.Hour)
	from := to.Add(-time.Duration(10) * time.Hour)

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(context.TODO(), suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
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

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(context.TODO(), suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
	assert.NoError(suite.T(), err)

	assert.Len(suite.T(), summaryItems, 2)

	assert.Equal(suite.T(), summaryItems[0].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[0].Region, "FI")
	assert.EqualValues(suite.T(), summaryItems[0].TotalTransactions, 1)
	assert.EqualValues(suite.T(), summaryItems[0].SalesCount, 1)
	assert.EqualValues(suite.T(), summaryItems[0].ReturnsCount, 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossSalesAmount), 12)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossReturnsAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossTotalAmount), 12)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), 0.65)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), 2.0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), 9.33)

	controlTotal0 := summaryItems[0].GrossSalesAmount - summaryItems[0].TotalFees - summaryItems[0].TotalVat
	assert.Equal(suite.T(), summaryItems[0].PayoutAmount, controlTotal0)

	assert.Equal(suite.T(), summaryItems[1].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[1].Region, "RU")
	assert.EqualValues(suite.T(), summaryItems[1].TotalTransactions, 2)
	assert.EqualValues(suite.T(), summaryItems[1].SalesCount, 2)
	assert.EqualValues(suite.T(), summaryItems[1].ReturnsCount, 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossSalesAmount), 24)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossReturnsAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossTotalAmount), 24)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), 1.31)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), 4.09)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), 18.59)

	controlTotal1 := summaryItems[1].GrossSalesAmount - summaryItems[1].TotalFees - summaryItems[1].TotalVat
	assert.Equal(suite.T(), summaryItems[1].PayoutAmount, controlTotal1)

	assert.NotNil(suite.T(), summaryTotal)
	assert.Equal(suite.T(), summaryTotal.Product, "")
	assert.Equal(suite.T(), summaryTotal.Region, "")
	assert.EqualValues(suite.T(), summaryTotal.TotalTransactions, 3)
	assert.EqualValues(suite.T(), summaryTotal.SalesCount, 3)
	assert.EqualValues(suite.T(), summaryTotal.ReturnsCount, 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossSalesAmount), 36)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossReturnsAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossTotalAmount), 36)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), 1.96)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), 6.09)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), 27.93)

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
	err := suite.service.paymentSystem.Update(context.TODO(), suite.paymentSystem)
	assert.NoError(suite.T(), err)

	for _, order := range orders {
		refund := helperMakeRefund(suite.Suite, suite.service, order, order.ChargeAmount, false)
		assert.NotNil(suite.T(), refund)
	}

	to := time.Now().Add(time.Duration(5) * time.Hour)
	from := to.Add(-time.Duration(10) * time.Hour)

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(context.TODO(), suite.merchant.Id, suite.merchant.GetPayoutCurrency(), from, to)
	assert.NoError(suite.T(), err)

	assert.Len(suite.T(), summaryItems, 2)

	assert.Equal(suite.T(), summaryItems[0].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[0].Region, "FI")
	assert.EqualValues(suite.T(), summaryItems[0].TotalTransactions, 2)
	assert.EqualValues(suite.T(), summaryItems[0].SalesCount, 1)
	assert.EqualValues(suite.T(), summaryItems[0].ReturnsCount, 1)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossSalesAmount), 12)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossReturnsAmount), 12)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].GrossTotalAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), 0.65)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), -0.65)

	controlTotal0 := summaryItems[0].GrossTotalAmount - summaryItems[0].TotalFees - summaryItems[0].TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), tools.FormatAmount(controlTotal0))

	assert.Equal(suite.T(), summaryItems[1].Product, suite.projectFixedAmount.Name["en"])
	assert.Equal(suite.T(), summaryItems[1].Region, "RU")
	assert.EqualValues(suite.T(), summaryItems[1].TotalTransactions, 4)
	assert.EqualValues(suite.T(), summaryItems[1].SalesCount, 2)
	assert.EqualValues(suite.T(), summaryItems[1].ReturnsCount, 2)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossSalesAmount), 24)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossReturnsAmount), 24)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].GrossTotalAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), 1.31)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), -1.31)

	controlTotal1 := summaryItems[1].GrossTotalAmount - summaryItems[1].TotalFees - summaryItems[1].TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), tools.FormatAmount(controlTotal1))

	assert.NotNil(suite.T(), summaryTotal)
	assert.Equal(suite.T(), summaryTotal.Product, "")
	assert.Equal(suite.T(), summaryTotal.Region, "")
	assert.EqualValues(suite.T(), summaryTotal.TotalTransactions, 6)
	assert.EqualValues(suite.T(), summaryTotal.SalesCount, 3)
	assert.EqualValues(suite.T(), summaryTotal.ReturnsCount, 3)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossSalesAmount), 36)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossReturnsAmount), 36)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.GrossTotalAmount), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), 1.96)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), 0)
	assert.EqualValues(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), -1.96)

	controlTotal := summaryTotal.GrossTotalAmount - summaryTotal.TotalFees - summaryTotal.TotalVat
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), tools.FormatAmount(controlTotal))
}

func (suite *OrderViewTestSuite) Test_OrderView_PaylinkStat() {
	countries := []string{"RU", "FI"}
	referrers := []string{"http://steam.com", "http://games.mail.ru/someurl"}
	utmSources := []string{"yandex", "google"}
	utmMedias := []string{"cpc", ""}
	utmCampaign := []string{"45249779", "dfsdf"}
	yesterday := time.Now().Add(-24 * time.Hour).Unix()
	tomorrow := time.Now().Add(24 * time.Hour).Unix()
	maxVisits := 3
	maxOrders := 4
	maxRefunds := 1
	var orders []*billing.Order

	oid, err := primitive.ObjectIDFromHex(suite.paylink1.Id)
	assert.NoError(suite.T(), err)
	visitsQuery := bson.M{
		"paylink_id": oid,
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), visitsQuery)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, 0)

	visitsReq := &grpc.PaylinkRequestById{
		Id: suite.paylink1.Id,
	}

	count := 0
	for count < maxVisits {
		err = suite.service.IncrPaylinkVisits(context.TODO(), visitsReq, &grpc.EmptyResponse{})
		assert.NoError(suite.T(), err)
		count++
	}

	count = 0
	for count < maxOrders {
		err = suite.service.IncrPaylinkVisits(context.TODO(), visitsReq, &grpc.EmptyResponse{})
		assert.NoError(suite.T(), err)

		order := helperCreateAndPayPaylinkOrder(
			suite.Suite,
			suite.service,
			suite.paylink1.Id,
			countries[count%2],
			suite.paymentMethod,
			&billing.OrderIssuer{
				Url:         referrers[count%2],
				UtmSource:   utmSources[count%2],
				UtmMedium:   utmMedias[count%2],
				UtmCampaign: utmCampaign[count%2],
			},
		)
		assert.NotNil(suite.T(), order)
		orders = append(orders, order)
		count++
	}

	count = 0
	for count < maxRefunds {
		refund := helperMakeRefund(suite.Suite, suite.service, orders[count], orders[count].ChargeAmount, false)
		assert.NotNil(suite.T(), refund)
		count++
	}

	n, err = suite.service.db.Collection(collectionPaylinkVisits).CountDocuments(context.TODO(), visitsQuery)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), n, maxVisits+maxOrders)

	req := &grpc.PaylinkRequest{
		Id:         suite.paylink1.Id,
		MerchantId: suite.paylink1.MerchantId,
	}

	res := &grpc.GetPaylinkResponse{}
	err = suite.service.GetPaylink(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Visits, int32(maxVisits+maxOrders))
	assert.Equal(suite.T(), res.Item.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.EqualValues(suite.T(), res.Item.SalesCount, maxOrders)
	assert.EqualValues(suite.T(), res.Item.ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), res.Item.Conversion, tools.ToPrecise(float64(maxOrders)/float64(maxVisits+maxOrders)))
	assert.Equal(suite.T(), res.Item.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), res.Item.GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), res.Item.GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), res.Item.GrossTotalAmount, 132.125763)

	// stat by country
	stat, err := suite.service.orderView.GetPaylinkStatByCountry(context.TODO(), suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, len(countries))

	assert.Equal(suite.T(), stat.Top[0].CountryCode, "FI")
	assert.EqualValues(suite.T(), stat.Top[0].TotalTransactions, 2)
	assert.EqualValues(suite.T(), stat.Top[0].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[0].ReturnsCount, 0)
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[0].GrossSalesAmount, 88.52677)
	assert.EqualValues(suite.T(), stat.Top[0].GrossReturnsAmount, 0)
	assert.EqualValues(suite.T(), stat.Top[0].GrossTotalAmount, 88.52677)

	assert.Equal(suite.T(), stat.Top[1].CountryCode, "RU")
	assert.EqualValues(suite.T(), stat.Top[1].TotalTransactions, 3)
	assert.EqualValues(suite.T(), stat.Top[1].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[1].ReturnsCount, 1)
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[1].GrossSalesAmount, 88.977538)
	assert.EqualValues(suite.T(), stat.Top[1].GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Top[1].GrossTotalAmount, 43.598993)

	assert.EqualValues(suite.T(), stat.Total.TotalTransactions, maxOrders+maxRefunds)
	assert.EqualValues(suite.T(), stat.Total.SalesCount, maxOrders)
	assert.EqualValues(suite.T(), stat.Total.ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Total.GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), stat.Total.GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Total.GrossTotalAmount, 132.125763)

	// stat by referrer

	stat, err = suite.service.orderView.GetPaylinkStatByReferrer(context.TODO(), suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, len(referrers))

	assert.Equal(suite.T(), stat.Top[0].ReferrerHost, "games.mail.ru")
	assert.EqualValues(suite.T(), stat.Top[0].TotalTransactions, 2)
	assert.EqualValues(suite.T(), stat.Top[0].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[0].ReturnsCount, 0)
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[0].GrossSalesAmount, 88.52677)
	assert.EqualValues(suite.T(), stat.Top[0].GrossReturnsAmount, 0)
	assert.EqualValues(suite.T(), stat.Top[0].GrossTotalAmount, 88.52677)

	assert.Equal(suite.T(), stat.Top[1].ReferrerHost, "steam.com")
	assert.EqualValues(suite.T(), stat.Top[1].TotalTransactions, 3)
	assert.EqualValues(suite.T(), stat.Top[1].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[1].ReturnsCount, 1)
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[1].GrossSalesAmount, 88.977538)
	assert.EqualValues(suite.T(), stat.Top[1].GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Top[1].GrossTotalAmount, 43.598993)

	assert.EqualValues(suite.T(), stat.Total.TotalTransactions, maxOrders+maxRefunds)
	assert.EqualValues(suite.T(), stat.Total.SalesCount, maxOrders)
	assert.EqualValues(suite.T(), stat.Total.ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Total.GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), stat.Total.GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Total.GrossTotalAmount, 132.125763)

	// stat by date

	stat, err = suite.service.orderView.GetPaylinkStatByDate(context.TODO(), suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, 1)

	assert.Equal(suite.T(), stat.Top[0].Date, time.Now().Format("2006-01-02"))
	assert.Equal(suite.T(), stat.Top[0].TotalTransactions, int32(maxOrders+maxRefunds))
	assert.EqualValues(suite.T(), stat.Top[0].SalesCount, maxOrders)
	assert.EqualValues(suite.T(), stat.Top[0].ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[0].GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), stat.Top[0].GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Top[0].GrossTotalAmount, 132.125763)

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.EqualValues(suite.T(), stat.Total.SalesCount, maxOrders)
	assert.EqualValues(suite.T(), stat.Total.ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Total.GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), stat.Total.GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Total.GrossTotalAmount, 132.125763)

	// stat by utm
	stat, err = suite.service.orderView.GetPaylinkStatByUtm(context.TODO(), suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, 2)

	assert.NotNil(suite.T(), stat.Top[0].Utm)
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmSource, "google")
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmMedium, "")
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmCampaign, "dfsdf")
	assert.EqualValues(suite.T(), stat.Top[0].TotalTransactions, 2)
	assert.EqualValues(suite.T(), stat.Top[0].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[0].ReturnsCount, 0)
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[0].GrossSalesAmount, 88.52677)
	assert.EqualValues(suite.T(), stat.Top[0].GrossReturnsAmount, 0)
	assert.EqualValues(suite.T(), stat.Top[0].GrossTotalAmount, 88.52677)

	assert.NotNil(suite.T(), stat.Top[1].Utm)
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmSource, "yandex")
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmMedium, "cpc")
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmCampaign, "45249779")
	assert.EqualValues(suite.T(), stat.Top[1].TotalTransactions, 3)
	assert.EqualValues(suite.T(), stat.Top[1].SalesCount, 2)
	assert.EqualValues(suite.T(), stat.Top[1].ReturnsCount, 1)
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Top[1].GrossSalesAmount, 88.977538)
	assert.EqualValues(suite.T(), stat.Top[1].GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Top[1].GrossTotalAmount, 43.598993)

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.EqualValues(suite.T(), stat.Total.SalesCount, maxOrders)
	assert.EqualValues(suite.T(), stat.Total.ReturnsCount, maxRefunds)
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.EqualValues(suite.T(), stat.Total.GrossSalesAmount, 177.504308)
	assert.EqualValues(suite.T(), stat.Total.GrossReturnsAmount, 45.378545)
	assert.EqualValues(suite.T(), stat.Total.GrossTotalAmount, 132.125763)
}
