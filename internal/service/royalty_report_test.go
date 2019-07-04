package service

import (
	"context"
	rabbitmq "github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/centrifugal/gocent"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"net/http"
	"testing"
	"time"
)

type RoyaltyReportTestSuite struct {
	suite.Suite
	service    *Service
	httpClient *http.Client

	project    *billing.Project
	project1   *billing.Project
	project2   *billing.Project
	merchant   *billing.Merchant
	merchant1  *billing.Merchant
	merchant2  *billing.Merchant
	pmBankCard *billing.PaymentMethod
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

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	rub := "RUB"

	country := &billing.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
	}

	paymentSystem := &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: rub,
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "mock_ok",
	}
	pmBankCard := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		Currencies:       []string{"RUB", "USD", "EUR"},
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: paymentSystem.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchant := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: country.IsoCodeA2,
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
			Currency: rub,
			Name:     "Bank name",
		},
		IsVatEnabled:              false,
		IsCommissionToUserEnabled: false,
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
						Currency: rub,
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
	merchant1 := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: country.IsoCodeA2,
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
			Currency: rub,
			Name:     "Bank name",
		},
		IsVatEnabled:              false,
		IsCommissionToUserEnabled: false,
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
						Currency: rub,
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
	merchant2 := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: country.IsoCodeA2,
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
			Currency: rub,
			Name:     "Bank name",
		},
		IsVatEnabled:              false,
		IsCommissionToUserEnabled: false,
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
						Currency: rub,
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

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)

	if err != nil {
		suite.FailNow("Creating RabbitMQ publisher failed", "%v", err)
	}

	suite.httpClient = mock.NewClientStatusOk()

	redisdb := mock.NewTestRedis()
	cache := NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		broker,
		nil,
		cache,
		mock.NewCurrencyServiceMockOk(),
		mock.NewSmtpSenderMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.service.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       cfg.CentrifugoURL,
			Key:        cfg.CentrifugoSecret,
			HTTPClient: suite.httpClient,
		},
	)

	err = suite.service.country.Insert(country)

	if err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	pms := []*billing.PaymentMethod{pmBankCard}
	err = suite.service.paymentMethod.MultipleInsert(pms)

	if err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	err = suite.service.merchant.MultipleInsert([]*billing.Merchant{merchant, merchant1, merchant2})

	if err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	project := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         rub,
		CallbackProtocol:         "default",
		LimitsCurrency:           rub,
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
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         rub,
		CallbackProtocol:         "default",
		LimitsCurrency:           rub,
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant1.Id,
	}
	project2 := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         rub,
		CallbackProtocol:         "default",
		LimitsCurrency:           rub,
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant2.Id,
	}

	projects := []*billing.Project{project, project1, project2}
	err = suite.service.project.MultipleInsert(projects)

	if err != nil {
		suite.FailNow("Insert projects test data failed", "%v", err)
	}

	err = suite.service.paymentSystem.MultipleInsert([]*billing.PaymentSystem{paymentSystem})

	if err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	refundSysCost := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3,
		FixAmount:      5,
	}
	refundSysCost1 := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       30,
		PaymentStage:   1,
		Percent:        10,
		FixAmount:      15,
	}
	refundSysCost2 := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        2,
		FixAmount:      3,
	}

	err = suite.service.moneyBackCostSystem.MultipleInsert([]*billing.MoneyBackCostSystem{refundSysCost, refundSysCost1, refundSysCost2})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	refundMerCost := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	refundMerCost1 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "RUB",
		IsPaidByMerchant:  true,
	}
	refundMerCost2 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	refundMerCost3 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	refundMerCost4 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}
	refundMerCost5 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "CIS",
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}

	err = suite.service.moneyBackCostMerchant.MultipleInsert([]*billing.MoneyBackCostMerchant{refundMerCost, refundMerCost1, refundMerCost2, refundMerCost3, refundMerCost4, refundMerCost5})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	paymentSysCost := &billing.PaymentChannelCostSystem{
		Id:        bson.NewObjectId().Hex(),
		Name:      "VISA",
		Region:    "CIS",
		Country:   "AZ",
		Percent:   1.5,
		FixAmount: 5,
	}
	paymentSysCost1 := &billing.PaymentChannelCostSystem{
		Name:      "VISA",
		Region:    "CIS",
		Country:   "",
		Percent:   2.2,
		FixAmount: 0,
	}

	err = suite.service.paymentChannelCostSystem.MultipleInsert([]*billing.PaymentChannelCostSystem{paymentSysCost, paymentSysCost1})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	paymentMerCost := &billing.PaymentChannelCostMerchant{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      1.5,
		MethodFixAmount:    0.01,
		PsPercent:          3,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost1 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost2 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0,
		Region:             "CIS",
		Country:            "",
		MethodPercent:      2.2,
		MethodFixAmount:    0,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost3 := &billing.PaymentChannelCostMerchant{
		MerchantId:         mock.MerchantIdMock,
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost4 := &billing.PaymentChannelCostMerchant{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         project1.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      1.5,
		MethodFixAmount:    0.01,
		PsPercent:          3,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost5 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project1.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost6 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project1.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0,
		Region:             "CIS",
		Country:            "",
		MethodPercent:      2.2,
		MethodFixAmount:    0,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost7 := &billing.PaymentChannelCostMerchant{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         project2.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      1.5,
		MethodFixAmount:    0.01,
		PsPercent:          3,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost8 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project2.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost9 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project2.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		MinAmount:          0,
		Region:             "CIS",
		Country:            "",
		MethodPercent:      2.2,
		MethodFixAmount:    0,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}

	err = suite.service.paymentChannelCostMerchant.MultipleInsert([]*billing.PaymentChannelCostMerchant{paymentMerCost, paymentMerCost1, paymentMerCost2, paymentMerCost3, paymentMerCost4, paymentMerCost5, paymentMerCost6, paymentMerCost7, paymentMerCost8, paymentMerCost9})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}

	bin := &BinData{
		Id:                 bson.NewObjectId(),
		CardBin:            400000,
		CardBrand:          "VISA",
		CardType:           "DEBIT",
		CardCategory:       "WORLD",
		BankName:           "ALFA BANK",
		BankCountryName:    "UKRAINE",
		BankCountryIsoCode: "US",
	}

	err = db.Collection(collectionBinData).Insert(bin)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
	}

	suite.merchant = merchant
	suite.merchant1 = merchant1
	suite.merchant2 = merchant2

	suite.project = project
	suite.project1 = project1
	suite.project2 = project2

	suite.pmBankCard = pmBankCard
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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

	req := &grpc.CreateRoyaltyReportRequest{
		Merchants: []string{suite.project.GetMerchantId(), suite.project1.GetMerchantId()},
	}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	err = suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	req1 := &grpc.ListRoyaltyReportsRequest{}
	rsp1 := &grpc.ListRoyaltyReportsResponse{}
	err = suite.service.ListRoyaltyReports(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, int(rsp1.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindById_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	assert.EqualValues(suite.T(), 1, rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, int(rsp1.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindById_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{Id: bson.NewObjectId().Hex()}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
	assert.Len(suite.T(), rsp.Items, int(rsp.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByMerchantId_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	assert.EqualValues(suite.T(), 2, rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, int(rsp1.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByMerchantId_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{MerchantId: bson.NewObjectId().Hex()}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByPeriod_Ok() {
	projects := []*billing.Project{suite.project, suite.project1, suite.project2}

	for _, v := range projects {
		for i := 0; i < 10; i++ {
			suite.createOrder(v)
		}
	}

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	assert.EqualValues(suite.T(), 3, rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, int(rsp1.Count))
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReports_FindByPeriod_NotFound() {
	req := &grpc.ListRoyaltyReportsRequest{
		PeriodFrom: time.Now().Unix(),
		PeriodTo:   time.Now().Unix(),
	}
	rsp := &grpc.ListRoyaltyReportsResponse{}
	err := suite.service.ListRoyaltyReports(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ChangeRoyaltyReport_Ok() {
	suite.createOrder(suite.project)
	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	req1 := &grpc.ListRoyaltyReportOrdersRequest{ReportId: report.Id, Limit: 5, Offset: 0}
	rsp1 := &grpc.ListRoyaltyReportOrdersResponse{}
	err = suite.service.ListRoyaltyReportOrders(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), rsp1.Items)
	assert.EqualValues(suite.T(), 10, rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, int(req1.Limit))

	for _, v := range rsp1.Items {
		assert.NotZero(suite.T(), v.Date)
		assert.NotZero(suite.T(), v.Country)
		assert.NotZero(suite.T(), v.PaymentId)
		assert.NotZero(suite.T(), v.Method)
		assert.NotZero(suite.T(), v.Amount)
		assert.NotZero(suite.T(), v.Vat)
		assert.NotZero(suite.T(), v.Commission)
	}
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReportOrders_ReportNotFound_Error() {
	req := &grpc.ListRoyaltyReportOrdersRequest{ReportId: bson.NewObjectId().Hex(), Limit: 5, Offset: 0}
	rsp := &grpc.ListRoyaltyReportOrdersResponse{}
	err := suite.service.ListRoyaltyReportOrders(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *RoyaltyReportTestSuite) TestRoyaltyReport_ListRoyaltyReportOrders_OrdersNotFound_Error() {
	for i := 0; i < 10; i++ {
		suite.createOrder(suite.project)
	}

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), rsp.Merchants)

	report := new(billing.RoyaltyReport)
	err = suite.service.db.Collection(collectionRoyaltyReport).Find(bson.M{}).One(&report)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Equal(suite.T(), pkg.RoyaltyReportStatusNew, report.Status)

	query := bson.M{"royalty_report_id": report.Id}
	set := bson.M{"$set": bson.M{"royalty_report_id": ""}}
	_, err = suite.service.db.Collection(collectionOrder).UpdateAll(query, set)
	assert.NoError(suite.T(), err)

	req1 := &grpc.ListRoyaltyReportOrdersRequest{ReportId: report.Id, Limit: 10, Offset: 0}
	rsp1 := &grpc.ListRoyaltyReportOrdersResponse{}
	err = suite.service.ListRoyaltyReportOrders(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp1.Count)
	assert.Empty(suite.T(), rsp1.Items)
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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

	req := &grpc.CreateRoyaltyReportRequest{}
	rsp := &grpc.CreateRoyaltyReportRequest{}
	err := suite.service.CreateRoyaltyReport(context.TODO(), req, rsp)
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
	req := &billing.OrderCreateRequest{
		ProjectId:   project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     bson.NewObjectId().Hex(),
		User: &billing.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &grpc.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, pkg.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &grpc.PaymentCreateRequest{
		Data: map[string]string{
			pkg.PaymentCreateFieldOrderId:         rsp.Uuid,
			pkg.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			pkg.PaymentCreateFieldEmail:           "test@unit.unit",
			pkg.PaymentCreateFieldPan:             "4000000000000002",
			pkg.PaymentCreateFieldCvv:             "123",
			pkg.PaymentCreateFieldMonth:           "02",
			pkg.PaymentCreateFieldYear:            expireYear.Format("2006"),
			pkg.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &grpc.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	var order *billing.Order
	err = suite.service.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(rsp.Id)).One(&order)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = constant.OrderStatusPaymentSystemComplete
	order.Tax = &billing.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}

	loc, err := time.LoadLocation(suite.service.cfg.RoyaltyReportTimeZone)
	assert.NoError(suite.T(), err)

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	date := to.Add(-time.Duration(suite.service.cfg.RoyaltyReportPeriod/2) * time.Second).In(loc)

	query := bson.M{"project.merchant_id": bson.ObjectIdHex(project.GetMerchantId())}
	set := bson.M{"$set": bson.M{"pm_order_close_date": date}}
	_, err = suite.service.db.Collection(collectionOrder).UpdateAll(query, set)
	assert.NoError(suite.T(), err)

	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(date)
	order.Status = constant.OrderPublicStatusProcessed
	order.PaymentMethod.Params.Currency = "RUB"
	err = suite.service.updateOrder(order)
	assert.NoError(suite.T(), err)

	err = suite.service.onPaymentNotify(context.TODO(), order)
	assert.NoError(suite.T(), err)

	query = bson.M{"merchant_id": bson.ObjectIdHex(project.GetMerchantId())}
	set = bson.M{"$set": bson.M{"created_at": date}}
	_, err = suite.service.db.Collection(collectionAccountingEntry).UpdateAll(query, set)
	assert.NoError(suite.T(), err)

	return order
}
