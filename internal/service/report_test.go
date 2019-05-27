package service

import (
	"context"
	"github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/stoewer/go-strcase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"strconv"
	"testing"
	"time"
)

type ReportTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger

	currencyRub             *billing.Currency
	currencyUsd             *billing.Currency
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
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	settings := database.Connection{
		Host:     cfg.MongoHost,
		Database: cfg.MongoDatabase,
		User:     cfg.MongoUser,
		Password: cfg.MongoPassword,
	}

	db, err := database.NewDatabase(settings)

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.currencyRub = &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}
	suite.currencyUsd = &billing.Currency{
		CodeInt:  840,
		CodeA3:   "USD",
		Name:     &billing.Name{Ru: "Доллар США", En: "US Dollar"},
		IsActive: true,
	}

	currency := []interface{}{suite.currencyRub, suite.currencyUsd}

	err = db.Collection(pkg.CollectionCurrency).Insert(currency...)

	if err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	rate := []interface{}{
		&billing.CurrencyRate{
			CurrencyFrom: 840,
			CurrencyTo:   643,
			Rate:         0.015625,
			Date:         ptypes.TimestampNow(),
			IsActive:     true,
		},
		&billing.CurrencyRate{
			CurrencyFrom: 643,
			CurrencyTo:   840,
			Rate:         64,
			Date:         ptypes.TimestampNow(),
			IsActive:     true,
		},
		&billing.CurrencyRate{
			CurrencyFrom: 643,
			CurrencyTo:   643,
			Rate:         1,
			Date:         ptypes.TimestampNow(),
			IsActive:     true,
		},
		&billing.CurrencyRate{
			CurrencyFrom: 643,
			CurrencyTo:   51,
			Rate:         1,
			Date:         ptypes.TimestampNow(),
			IsActive:     true,
		},
	}

	err = db.Collection(pkg.CollectionCurrencyRate).Insert(rate...)

	if err != nil {
		suite.FailNow("Insert rates test data failed", "%v", err)
	}

	ru := &billing.Country{
		CodeInt:  643,
		CodeA2:   "RU",
		CodeA3:   "RUS",
		Name:     &billing.Name{Ru: "Россия", En: "Russia (Russian Federation)"},
		IsActive: true,
	}
	us := &billing.Country{
		CodeInt:  840,
		CodeA2:   "US",
		CodeA3:   "USA",
		Name:     &billing.Name{Ru: "США", En: "USA"},
		IsActive: true,
	}

	err = db.Collection(pkg.CollectionCountry).Insert([]interface{}{ru, us}...)
	assert.NoError(suite.T(), err, "Insert country test data failed")

	pmBankCard := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		Currency:         suite.currencyRub,
		Currencies:       []int32{643, 840, 980},
		Params: &billing.PaymentMethodParams{
			Handler:          "cardpay",
			Terminal:         "15985",
			Password:         "A1tph4I6BD0f",
			CallbackPassword: "0V1rJ7t4jCRv",
			ExternalId:       "BANKCARD",
		},
		Type:          "bank_card",
		IsActive:      true,
		AccountRegexp: "^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$",
		PaymentSystem: &billing.PaymentSystem{
			Id:                 bson.NewObjectId().Hex(),
			Name:               "CardPay",
			AccountingCurrency: suite.currencyRub,
			AccountingPeriod:   "every-day",
			Country:            &billing.Country{},
			IsActive:           true,
		},
	}

	pmBitcoin1 := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bitcoin",
		Group:            "BITCOIN_1",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		Currency:         suite.currencyRub,
		Currencies:       []int32{643, 840, 980},
		Params: &billing.PaymentMethodParams{
			Handler:    "unit_test",
			Terminal:   "16007",
			ExternalId: "BITCOIN",
		},
		Type:     "crypto",
		IsActive: true,
		PaymentSystem: &billing.PaymentSystem{
			Id:                 bson.NewObjectId().Hex(),
			Name:               "CardPay",
			AccountingCurrency: suite.currencyRub,
			AccountingPeriod:   "every-day",
			Country:            &billing.Country{},
			IsActive:           true,
		},
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))

	if err != nil {
		suite.FailNow("Generate merchant date failed", "%v", err)
	}

	merchant := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: ru,
		Zip:     "190000",
		City:    "St.Petersburg",
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency: suite.currencyRub,
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusDraft,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned: true,
		PaymentMethods: map[string]*billing.MerchantPaymentMethod{
			pmBankCard.Id: {
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   pmBankCard.Id,
					Name: pmBankCard.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: 2.5,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      30,
						Currency: suite.currencyRub.CodeA3,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{
					TerminalId:               "15985",
					TerminalPassword:         "A1tph4I6BD0f",
					TerminalCallbackPassword: "0V1rJ7t4jCRv",
					Integrated:               true,
				},
				IsActive: true,
			},
			pmBitcoin1.Id: {
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   pmBitcoin1.Id,
					Name: pmBitcoin1.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: 3.5,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      300,
						Currency: suite.currencyRub.CodeA3,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
	}

	err = db.Collection(pkg.CollectionMerchant).Insert([]interface{}{merchant}...)

	if err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	project := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         suite.currencyRub.CodeA3,
		CallbackProtocol:         "default",
		LimitsCurrency:           suite.currencyUsd.CodeA3,
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
	}
	project1 := &billing.Project{
		Id:                 bson.NewObjectId().Hex(),
		CallbackCurrency:   suite.currencyRub.CodeA3,
		CallbackProtocol:   "default",
		LimitsCurrency:     suite.currencyRub.CodeA3,
		MaxPaymentAmount:   15000,
		MinPaymentAmount:   0,
		Name:               map[string]string{"en": "project incorrect payment method id"},
		IsProductsCheckout: true,
		SecretKey:          "project incorrect payment method id secret key",
		Status:             pkg.ProjectStatusInProduction,
		MerchantId:         merchant.Id,
	}

	projects := []interface{}{
		project,
		project1,
	}

	err = db.Collection(pkg.CollectionProject).Insert(projects...)

	if err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	pms := []interface{}{pmBankCard, pmBitcoin1}

	err = db.Collection(pkg.CollectionPaymentMethod).Insert(pms...)

	if err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	commissionStartDate, err := ptypes.TimestampProto(time.Now().Add(time.Minute * -10))

	if err != nil {
		suite.FailNow("Commission start date conversion failed", "%v", err)
	}

	commissions := []interface{}{
		&billing.Commission{
			PaymentMethodId:         pmBankCard.Id,
			ProjectId:               project.Id,
			PaymentMethodCommission: 1,
			PspCommission:           2,
			TotalCommissionToUser:   1,
			StartDate:               commissionStartDate,
		},
		&billing.Commission{
			PaymentMethodId:         pmBitcoin1.Id,
			ProjectId:               project.Id,
			PaymentMethodCommission: 1,
			PspCommission:           2,
			TotalCommissionToUser:   3,
			StartDate:               commissionStartDate,
		},
	}

	err = db.Collection(pkg.CollectionCommission).Insert(commissions...)

	if err != nil {
		suite.FailNow("Insert commission test data failed", "%v", err)
	}

	bin := &BinData{
		Id:                bson.NewObjectId(),
		CardBin:           400000,
		CardBrand:         "MASTERCARD",
		CardType:          "DEBIT",
		CardCategory:      "WORLD",
		BankName:          "ALFA BANK",
		BankCountryName:   "UKRAINE",
		BankCountryCodeA2: "US",
	}

	err = db.Collection(pkg.CollectionBinData).Insert(bin)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
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

	suite.service = NewBillingService(
		db,
		cfg,
		make(chan bool, 1),
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		broker,
		redisClient,
	)
	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	var productIds []string
	names := []string{"Madalin Stunt Cars M2", "Plants vs Zombies"}

	for i, n := range names {
		req := &grpc.Product{
			Object:          "product",
			Type:            "simple_product",
			Sku:             "ru_" + strconv.Itoa(i) + "_" + strcase.SnakeCase(n),
			Name:            map[string]string{"en": n},
			DefaultCurrency: "USD",
			Enabled:         true,
			Description:     map[string]string{"en": n + " description"},
			MerchantId:      project.MerchantId,
			ProjectId:       project.Id,
		}

		baseAmount := 37.00 * float64(i+1) // base amount in product's default currency

		req.Prices = append(req.Prices, &grpc.ProductPrice{
			Currency: "USD",
			Amount:   baseAmount,
		})
		req.Prices = append(req.Prices, &grpc.ProductPrice{
			Currency: "RUB",
			Amount:   baseAmount * 65.13,
		})

		prod := grpc.Product{}

		assert.NoError(suite.T(), suite.service.CreateOrUpdateProduct(context.TODO(), req, &prod))

		productIds = append(productIds, prod.Id)
	}

	suite.project = project
	suite.pmBankCard = pmBankCard
	suite.pmBitcoin1 = pmBitcoin1
	suite.productIds = productIds
	suite.merchantDefaultCurrency = "USD"
}

func (suite *ReportTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *ReportTestSuite) TestReport_ReturnEmptyList() {
	req := &grpc.ListOrdersRequest{}
	rsp := &billing.OrderPaginate{}
	err := suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *ReportTestSuite) TestReport_FindById() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{Id: bson.NewObjectId().Hex()}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{Id: oRsp.Id}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByProject() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{Project: []string{bson.NewObjectId().Hex()}}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{Project: []string{suite.project.Id}}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Project.Id, rsp.Items[0].Project.Id)
}

func (suite *ReportTestSuite) TestReport_FindByCountry() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		User: &billing.OrderUser{
			Address: &billing.OrderBillingAddress{
				Country: "RUS",
			},
		},
		Amount: 100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{Country: []string{"USA"}}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{Country: []string{"RUS"}}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Project.Id, rsp.Items[0].Project.Id)
}

func (suite *ReportTestSuite) TestReport_FindByPaymentMethod() {
	oReq := &billing.OrderCreateRequest{
		ProjectId:     suite.project.Id,
		Currency:      suite.currencyRub.CodeA3,
		Amount:        100,
		PaymentMethod: suite.pmBankCard.Group,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{PaymentMethod: []string{bson.NewObjectId().Hex()}}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{PaymentMethod: []string{oRsp.PaymentMethod.Id}}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.PaymentMethod.Id, rsp.Items[0].PaymentMethod.Id)
}

func (suite *ReportTestSuite) TestReport_FindByStatus() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	oRsp.Status = OrderStatusPaymentSystemRejectOnCreate
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{Status: []int32{OrderStatusPaymentSystemCreate}}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{Status: []int32{OrderStatusPaymentSystemRejectOnCreate}}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Status, rsp.Items[0].Status)
}

func (suite *ReportTestSuite) TestReport_FindByAccount() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
		Account:   "account",
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{Account: "unexists"}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{Account: "account"}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.ProjectAccount, rsp.Items[0].ProjectAccount)
}

func (suite *ReportTestSuite) TestReport_FindByPmDateFrom() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	t := time.Now()
	oRsp.PaymentMethodOrderClosedAt = &timestamp.Timestamp{Seconds: t.Unix()}
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{PmDateFrom: t.Unix() + 3}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{PmDateFrom: t.Unix() - 3}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByPmDateTo() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	t := time.Now()
	oRsp.PaymentMethodOrderClosedAt = &timestamp.Timestamp{Seconds: t.Unix()}
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{PmDateTo: t.Unix() - 3}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{PmDateTo: t.Unix() + 3}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByProjectDateFrom() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	t := time.Now()
	oRsp.CreatedAt = &timestamp.Timestamp{Seconds: t.Unix()}
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{ProjectDateFrom: t.Unix() + 3}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{ProjectDateFrom: t.Unix() - 3}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByProjectDateTo() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	t := time.Now()
	oRsp.CreatedAt = &timestamp.Timestamp{Seconds: t.Unix()}
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{ProjectDateTo: t.Unix() - 3}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{ProjectDateTo: t.Unix() + 3}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByQuickSearch_Id() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	req := &grpc.ListOrdersRequest{QuickSearch: bson.NewObjectId().Hex()}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{QuickSearch: oRsp.Id}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}

func (suite *ReportTestSuite) TestReport_FindByQuickSearch_ProjectOrderId() {
	oReq := &billing.OrderCreateRequest{
		ProjectId: suite.project.Id,
		Currency:  suite.currencyRub.CodeA3,
		Amount:    100,
	}
	oRsp := &billing.Order{}
	err := suite.service.OrderCreateProcess(context.TODO(), oReq, oRsp)
	if err != nil {
		suite.FailNow("Unable to create order", "%v", err)
	}

	oRsp.ProjectOrderId = "project_order_id"
	suite.service.updateOrder(oRsp)

	req := &grpc.ListOrdersRequest{QuickSearch: "unknown"}
	rsp := &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)

	req = &grpc.ListOrdersRequest{QuickSearch: "project_order_id"}
	rsp = &billing.OrderPaginate{}
	err = suite.service.FindAllOrders(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), oRsp.Id, rsp.Items[0].Id)
}
