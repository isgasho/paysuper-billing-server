package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/paylink"
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
	cache   internalPkg.CacheInterface

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
	suite.service = NewBillingService(db, cfg, mocks.NewGeoIpServiceTestOk(), mocks.NewRepositoryServiceOk(), mocks.NewTaxServiceOkMock(), broker, redisClient, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), &reportingMocks.ReporterService{}, mocks.NewFormatterOK(), mocks.NewBrokerMockOk(), mocks.NewNotifierOk(), )

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant, suite.projectFixedAmount, suite.paymentMethod, suite.paymentSystem = helperCreateEntitiesForTests(suite.Suite, suite.service)

	suite.projectWithProducts = &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
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
		WebhookTesting: &billing.WebHookTesting {
			Products:             &billing.ProductsTesting{
				NonExistingUser:      true,
				ExistingUser:         true,
				CorrectPayment:       true,
				IncorrectPayment:     true,
			},
			VirtualCurrency:      &billing.VirtualCurrencyTesting{
				NonExistingUser:      true,
				ExistingUser:         true,
				CorrectPayment:       true,
				IncorrectPayment:     true,
			},
			Keys:                 &billing.KeysTesting{IsPassed: true},
		},
	}

	if err := suite.service.project.Insert(suite.projectWithProducts); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	products := createProductsForProject(suite.Suite, suite.service, suite.projectWithProducts, 1)
	assert.Len(suite.T(), products, 1)

	paylinkBod, _ := ptypes.TimestampProto(now.BeginningOfDay())
	paylinkExpiresAt, _ := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))

	suite.paylink1 = &paylink.Paylink{
		Id:                   bson.NewObjectId().Hex(),
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

	err = suite.service.paylinkService.Insert(suite.paylink1)
	assert.NoError(suite.T(), err)
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

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.OperatingCompanyId, suite.merchant.GetPayoutCurrency(), from, to)
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

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.OperatingCompanyId, suite.merchant.GetPayoutCurrency(), from, to)
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
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), float64(0.65))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), float64(2.0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), float64(9.33))

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
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), float64(1.31))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), float64(4.09))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), float64(18.59))

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
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), float64(1.96))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), float64(6.09))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), float64(27.93))

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

	summaryItems, summaryTotal, err := suite.service.orderView.GetRoyaltySummary(suite.merchant.Id, suite.merchant.OperatingCompanyId, suite.merchant.GetPayoutCurrency(), from, to)
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
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalFees), float64(0.65))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[0].PayoutAmount), float64(-0.66))

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
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalFees), float64(1.31))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryItems[1].PayoutAmount), float64(-1.32))

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
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalFees), float64(1.96))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.TotalVat), float64(0))
	assert.Equal(suite.T(), tools.FormatAmount(summaryTotal.PayoutAmount), float64(-1.97))

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

	visitsQuery := bson.M{
		"paylink_id": bson.ObjectIdHex(suite.paylink1.Id),
	}
	n, err := suite.service.db.Collection(collectionPaylinkVisits).Find(visitsQuery).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, 0)

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
		refund := helperMakeRefund(suite.Suite, suite.service, orders[count], orders[count].TotalPaymentAmount, false)
		assert.NotNil(suite.T(), refund)
		count++
	}

	n, err = suite.service.db.Collection(collectionPaylinkVisits).Find(visitsQuery).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, maxVisits+maxOrders)

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
	assert.Equal(suite.T(), res.Item.SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), res.Item.ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), res.Item.Conversion, tools.ToPrecise(float64(maxOrders)/float64(maxVisits+maxOrders)))
	assert.Equal(suite.T(), res.Item.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), res.Item.GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), res.Item.GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), res.Item.GrossTotalAmount, float64(132.398993))

	// stat by country

	stat, err := suite.service.orderView.GetPaylinkStatByCountry(suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, len(countries))

	assert.Equal(suite.T(), stat.Top[0].CountryCode, "FI")
	assert.Equal(suite.T(), stat.Top[0].TotalTransactions, int32(2))
	assert.Equal(suite.T(), stat.Top[0].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[0].ReturnsCount, int32(0))
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[0].GrossSalesAmount, float64(88.8))
	assert.Equal(suite.T(), stat.Top[0].GrossReturnsAmount, float64(0))
	assert.Equal(suite.T(), stat.Top[0].GrossTotalAmount, float64(88.8))

	assert.Equal(suite.T(), stat.Top[1].CountryCode, "RU")
	assert.Equal(suite.T(), stat.Top[1].TotalTransactions, int32(3))
	assert.Equal(suite.T(), stat.Top[1].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[1].ReturnsCount, int32(1))
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[1].GrossSalesAmount, float64(88.977538))
	assert.Equal(suite.T(), stat.Top[1].GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Top[1].GrossTotalAmount, float64(43.598993))

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.Equal(suite.T(), stat.Total.SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), stat.Total.ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Total.GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), stat.Total.GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Total.GrossTotalAmount, float64(132.398993))

	// stat by referrer

	stat, err = suite.service.orderView.GetPaylinkStatByReferrer(suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, len(referrers))

	assert.Equal(suite.T(), stat.Top[0].ReferrerHost, "games.mail.ru")
	assert.Equal(suite.T(), stat.Top[0].TotalTransactions, int32(2))
	assert.Equal(suite.T(), stat.Top[0].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[0].ReturnsCount, int32(0))
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[0].GrossSalesAmount, float64(88.8))
	assert.Equal(suite.T(), stat.Top[0].GrossReturnsAmount, float64(0))
	assert.Equal(suite.T(), stat.Top[0].GrossTotalAmount, float64(88.8))

	assert.Equal(suite.T(), stat.Top[1].ReferrerHost, "steam.com")
	assert.Equal(suite.T(), stat.Top[1].TotalTransactions, int32(3))
	assert.Equal(suite.T(), stat.Top[1].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[1].ReturnsCount, int32(1))
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[1].GrossSalesAmount, float64(88.977538))
	assert.Equal(suite.T(), stat.Top[1].GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Top[1].GrossTotalAmount, float64(43.598993))

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.Equal(suite.T(), stat.Total.SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), stat.Total.ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Total.GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), stat.Total.GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Total.GrossTotalAmount, float64(132.398993))

	// stat by date

	stat, err = suite.service.orderView.GetPaylinkStatByDate(suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, 1)

	assert.Equal(suite.T(), stat.Top[0].Date, time.Now().Format("2006-01-02"))
	assert.Equal(suite.T(), stat.Top[0].TotalTransactions, int32(maxOrders+maxRefunds))
	assert.Equal(suite.T(), stat.Top[0].SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), stat.Top[0].ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[0].GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), stat.Top[0].GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Top[0].GrossTotalAmount, float64(132.398993))

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.Equal(suite.T(), stat.Total.SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), stat.Total.ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Total.GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), stat.Total.GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Total.GrossTotalAmount, float64(132.398993))

	// stat by utm

	stat, err = suite.service.orderView.GetPaylinkStatByUtm(suite.paylink1.Id, suite.paylink1.MerchantId, yesterday, tomorrow)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stat.Total)
	assert.Len(suite.T(), stat.Top, 2)

	assert.NotNil(suite.T(), stat.Top[0].Utm)
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmSource, "google")
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmMedium, "")
	assert.Equal(suite.T(), stat.Top[0].Utm.UtmCampaign, "dfsdf")
	assert.Equal(suite.T(), stat.Top[0].TotalTransactions, int32(2))
	assert.Equal(suite.T(), stat.Top[0].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[0].ReturnsCount, int32(0))
	assert.Equal(suite.T(), stat.Top[0].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[0].GrossSalesAmount, float64(88.8))
	assert.Equal(suite.T(), stat.Top[0].GrossReturnsAmount, float64(0))
	assert.Equal(suite.T(), stat.Top[0].GrossTotalAmount, float64(88.8))

	assert.NotNil(suite.T(), stat.Top[1].Utm)
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmSource, "yandex")
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmMedium, "cpc")
	assert.Equal(suite.T(), stat.Top[1].Utm.UtmCampaign, "45249779")
	assert.Equal(suite.T(), stat.Top[1].TotalTransactions, int32(3))
	assert.Equal(suite.T(), stat.Top[1].SalesCount, int32(2))
	assert.Equal(suite.T(), stat.Top[1].ReturnsCount, int32(1))
	assert.Equal(suite.T(), stat.Top[1].TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Top[1].GrossSalesAmount, float64(88.977538))
	assert.Equal(suite.T(), stat.Top[1].GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Top[1].GrossTotalAmount, float64(43.598993))

	assert.Equal(suite.T(), stat.Total.TotalTransactions, int32(maxOrders+maxRefunds))
	assert.Equal(suite.T(), stat.Total.SalesCount, int32(maxOrders))
	assert.Equal(suite.T(), stat.Total.ReturnsCount, int32(maxRefunds))
	assert.Equal(suite.T(), stat.Total.TransactionsCurrency, suite.merchant.Banking.Currency)
	assert.Equal(suite.T(), stat.Total.GrossSalesAmount, float64(177.777538))
	assert.Equal(suite.T(), stat.Total.GrossReturnsAmount, float64(45.378545))
	assert.Equal(suite.T(), stat.Total.GrossTotalAmount, float64(132.398993))
}
