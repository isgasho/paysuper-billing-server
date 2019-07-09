package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	rabbitmq "github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type AccountingEntryTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	projectFixedAmount      *billing.Project
	paymentMethod           *billing.PaymentMethod
	paymentSystem           *billing.PaymentSystem
	merchantDefaultCurrency string
}

func Test_AccountingEntry(t *testing.T) {
	suite.Run(t, new(AccountingEntryTestSuite))
}

func (suite *AccountingEntryTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	ru := &billing.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
		VatRate:         0.20,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         3,
		VatDeadlineDays:        25,
		VatStoreYears:          5,
		VatCurrencyRatesPolicy: "last-day",
		VatCurrencyRatesSource: "",
	}

	us := &billing.Country{
		IsoCodeA2:       "US",
		Region:          "North America",
		Currency:        "USD",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "USD",
		VatRate:         0.20,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         3,
		VatDeadlineDays:        25,
		VatStoreYears:          5,
		VatCurrencyRatesPolicy: "last-day",
		VatCurrencyRatesSource: "",
	}

	fi := &billing.Country{
		IsoCodeA2:       "FI",
		Region:          "EU",
		Currency:        "EUR",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "EUR",
		VatRate:         0.20,
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:         3,
		VatDeadlineDays:        25,
		VatStoreYears:          5,
		VatCurrencyRatesPolicy: "last-day",
		VatCurrencyRatesSource: "",
	}

	suite.paymentSystem = &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}

	pmBankCard := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 10,
		MaxPaymentAmount: 15000,
		Currencies:       []string{"RUB", "USD", "EUR"},
		ExternalId:       "BANKCARD",
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
			"USD": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
			"USD": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		Type:            "bank_card",
		IsActive:        true,
		AccountRegexp:   "^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$",
		PaymentSystemId: suite.paymentSystem.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))

	if err != nil {
		suite.FailNow("Generate merchant date failed", "%v", err)
	}

	merchant := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: ru.IsoCodeA2,
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
			Currency: "USD",
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
						Currency: "RUB",
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
		},
	}

	projectFixedAmount := &billing.Project{
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
		MerchantId:               merchant.Id,
	}

	projects := []*billing.Project{
		projectFixedAmount,
	}
	bin := &BinData{
		Id:                 bson.NewObjectId(),
		CardBin:            400000,
		CardBrand:          "MASTERCARD",
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

	pms := []*billing.PaymentMethod{pmBankCard}
	if err := suite.service.paymentMethod.MultipleInsert(pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	merchants := []*billing.Merchant{merchant}
	if err := suite.service.merchant.MultipleInsert(merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	country := []*billing.Country{ru, us, fi}
	if err := suite.service.country.MultipleInsert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	if err := suite.service.project.MultipleInsert(projects); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	ps := []*billing.PaymentSystem{suite.paymentSystem}
	if err := suite.service.paymentSystem.MultipleInsert(ps); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.projectFixedAmount = projectFixedAmount
	suite.paymentMethod = pmBankCard
	suite.merchantDefaultCurrency = "USD"

	sysCost := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "reversal",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost2 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "RUB",
		UndoReason:     "reversal",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost3 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "reversal",
		Region:         "North America",
		Country:        "US",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost4 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "reversal",
		Region:         "EU",
		Country:        "FI",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost5 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost6 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "RUB",
		UndoReason:     "chargeback",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost7 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "North America",
		Country:        "US",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	sysCost8 := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "EU",
		Country:        "FI",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        0.10,
		FixAmount:      0.15,
	}

	err = suite.service.moneyBackCostSystem.MultipleInsert([]*billing.MoneyBackCostSystem{sysCost, sysCost2, sysCost3, sysCost4, sysCost5, sysCost6, sysCost7, sysCost8})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	merCost1 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
	}

	merCost2 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
	}

	merCost3 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            "North America",
		Country:           "US",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
	}

	merCost4 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            "EU",
		Country:           "FI",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
	}

	merCost5 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}

	merCost6 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}

	merCost7 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "North America",
		Country:           "US",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}

	merCost8 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        projectFixedAmount.GetMerchantId(),
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "EU",
		Country:           "FI",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}

	err = suite.service.moneyBackCostMerchant.MultipleInsert([]*billing.MoneyBackCostMerchant{merCost1, merCost2, merCost3, merCost4, merCost5, merCost6, merCost7, merCost8})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	paymentSysCost1 := &billing.PaymentChannelCostSystem{
		Name:              "MASTERCARD",
		Region:            "Russia",
		Country:           "RU",
		Percent:           0.015,
		FixAmount:         0.01,
		FixAmountCurrency: "USD",
	}
	paymentSysCost2 := &billing.PaymentChannelCostSystem{
		Name:              "MASTERCARD",
		Region:            "North America",
		Country:           "US",
		Percent:           0.015,
		FixAmount:         0.01,
		FixAmountCurrency: "USD",
	}
	paymentSysCost3 := &billing.PaymentChannelCostSystem{
		Name:              "MASTERCARD",
		Region:            "EU",
		Country:           "FI",
		Percent:           0.015,
		FixAmount:         0.01,
		FixAmountCurrency: "USD",
	}

	err = suite.service.paymentChannelCostSystem.MultipleInsert([]*billing.PaymentChannelCostSystem{paymentSysCost1, paymentSysCost2, paymentSysCost3})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	paymentMerCost1 := &billing.PaymentChannelCostMerchant{
		MerchantId:              projectFixedAmount.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  "Russia",
		Country:                 "RU",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
	}
	paymentMerCost2 := &billing.PaymentChannelCostMerchant{
		MerchantId:              projectFixedAmount.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "RUB",
		MinAmount:               0,
		Region:                  "Russia",
		Country:                 "RU",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
	}
	paymentMerCost3 := &billing.PaymentChannelCostMerchant{
		MerchantId:              projectFixedAmount.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  "North America",
		Country:                 "US",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
	}
	paymentMerCost4 := &billing.PaymentChannelCostMerchant{
		MerchantId:              projectFixedAmount.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  "EU",
		Country:                 "FI",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
	}

	err = suite.service.paymentChannelCostMerchant.MultipleInsert([]*billing.PaymentChannelCostMerchant{paymentMerCost1, paymentMerCost2, paymentMerCost3, paymentMerCost4})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}
}

func (suite *AccountingEntryTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Ok_RUB_RUB_RUB() {
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
	err = suite.service.merchant.Update(merchant)
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
		"merchant_tax_fee_cost_value":               24,
		"merchant_tax_fee_central_bank_fx":          0,
		"merchant_tax_fee":                          24,
		"ps_method_fee":                             6,
		"merchant_method_fee":                       3.6,
		"merchant_method_fee_cost_value":            2.4,
		"ps_markup_merchant_method_fee":             1.2,
		"merchant_method_fixed_fee":                 1.4688,
		"real_merchant_method_fixed_fee":            1.44,
		"markup_merchant_method_fixed_fee_fx":       0.0288,
		"real_merchant_method_fixed_fee_cost_value": 0.65,
		"ps_method_fixed_fee_profit":                0.79,
		"merchant_ps_fixed_fee":                     3.672,
		"real_merchant_ps_fixed_fee":                3.6,
		"markup_merchant_ps_fixed_fee":              0.072,
		"ps_method_profit":                          6.622,
		"merchant_net_revenue":                      86.328,
		"ps_profit_total":                           6.622,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          120,
		"real_refund_fee":                      12,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      120,
		"ps_merchant_refund_fx":                0,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -12,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      24,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             24,
		"merchant_reverse_revenue":             96,
		"ps_refund_profit":                     -12.15,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err = suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, false)
	assert.NotNil(suite.T(), refund)

	accountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(accountingEntries), len(orderControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range accountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], toPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], toPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"ps_gross_revenue_fx_tax_fee":               0.048,
		"ps_gross_revenue_fx_profit":                0.192,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               2.352,
		"merchant_tax_fee_central_bank_fx":          0.055125,
		"merchant_tax_fee":                          2.407125,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.3528,
		"merchant_method_fee_cost_value":            0.24,
		"ps_markup_merchant_method_fee":             0.1128,
		"merchant_method_fixed_fee":                 0.0225969231,
		"real_merchant_method_fixed_fee":            0.0221538462,
		"markup_merchant_method_fixed_fee_fx":       0.0004430769,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.0121538462,
		"merchant_ps_fixed_fee":                     0.0564923077,
		"real_merchant_ps_fixed_fee":                0.0553846154,
		"markup_merchant_ps_fixed_fee":              0.0011076923,
		"ps_method_profit":                          0.3944923077,
		"merchant_net_revenue":                      8.7083826923,
		"ps_profit_total":                           0.5864923077,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      2.407125,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.040875,
		"merchant_reverse_tax_fee":             2.407125,
		"merchant_reverse_revenue":             9.832875,
		"ps_refund_profit":                     -1.309125,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type])
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], toPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], toPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"ps_gross_revenue_fx_tax_fee":               0.048,
		"ps_gross_revenue_fx_profit":                0.192,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               2.352,
		"merchant_tax_fee_central_bank_fx":          0,
		"merchant_tax_fee":                          2.352,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.3528,
		"merchant_method_fee_cost_value":            0.24,
		"ps_markup_merchant_method_fee":             0.1128,
		"merchant_method_fixed_fee":                 0.0225969231,
		"real_merchant_method_fixed_fee":            0.0221538462,
		"markup_merchant_method_fixed_fee_fx":       0.0004430769,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.0121538462,
		"merchant_ps_fixed_fee":                     0.0564923077,
		"real_merchant_ps_fixed_fee":                0.0553846154,
		"markup_merchant_ps_fixed_fee":              0.0011076923,
		"ps_method_profit":                          0.3944923077,
		"merchant_net_revenue":                      8.7635076923,
		"ps_profit_total":                           0.5864923077,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      2.352,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.096,
		"merchant_reverse_tax_fee":             2.352,
		"merchant_reverse_revenue":             9.888,
		"ps_refund_profit":                     -1.254,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], toPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], toPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"ps_gross_revenue_fx_tax_fee":               0.048,
		"ps_gross_revenue_fx_profit":                0.192,
		"merchant_gross_revenue":                    11.76,
		"merchant_tax_fee_cost_value":               2.352,
		"merchant_tax_fee_central_bank_fx":          0.0053224138,
		"merchant_tax_fee":                          2.3573224138,
		"ps_method_fee":                             0.588,
		"merchant_method_fee":                       0.3528,
		"merchant_method_fee_cost_value":            0.24,
		"ps_markup_merchant_method_fee":             0.1128,
		"merchant_method_fixed_fee":                 0.0225969231,
		"real_merchant_method_fixed_fee":            0.0221538462,
		"markup_merchant_method_fixed_fee_fx":       0.0004430769,
		"real_merchant_method_fixed_fee_cost_value": 0.01,
		"ps_method_fixed_fee_profit":                0.0121538462,
		"merchant_ps_fixed_fee":                     0.0564923077,
		"real_merchant_ps_fixed_fee":                0.0553846154,
		"markup_merchant_ps_fixed_fee":              0.0011076923,
		"ps_method_profit":                          0.3944923077,
		"merchant_net_revenue":                      8.7581852785,
		"ps_profit_total":                           0.5864923077,
	}

	refundControlResults := map[string]float64{
		"real_refund":                          12,
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -1.2,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      2.3573224138,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.0906775862,
		"merchant_reverse_tax_fee":             2.3573224138,
		"merchant_reverse_revenue":             9.8826775862,
		"ps_refund_profit":                     -1.2593224138,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, false)
	assert.NotNil(suite.T(), refund)

	orderAccountingEntries := suite.helperGetAccountingEntries(order.Id, collectionOrder)
	assert.Equal(suite.T(), len(orderAccountingEntries), len(orderControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range orderAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, orderControlResults[entry.Type]) {
			fmt.Println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"] + orderControlResults["ps_gross_revenue_fx"]
	assert.Equal(suite.T(), orderControlResults["real_gross_revenue"], toPrecise(controlRealGrossRevenue))

	controlMerchantGrossRevenue := orderControlResults["merchant_net_revenue"] + orderControlResults["merchant_ps_fixed_fee"] +
		orderControlResults["ps_method_fee"] + orderControlResults["merchant_tax_fee"]
	assert.Equal(suite.T(), orderControlResults["merchant_gross_revenue"], toPrecise(controlMerchantGrossRevenue))

	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PartialRefund_Ok_RUB_USD_EUR() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"
	refundControlResults := map[string]float64{
		"real_refund":                          6,
		"real_refund_fee":                      0.6,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      6.12,
		"ps_merchant_refund_fx":                0.12,
		"merchant_refund_fee":                  0,
		"ps_markup_merchant_refund_fee":        -0.6,
		"merchant_refund_fixed_fee_cost_value": 0,
		"merchant_refund_fixed_fee":            0,
		"ps_merchant_refund_fixed_fee_fx":      0,
		"ps_merchant_refund_fixed_fee_profit":  -0.15,
		"reverse_tax_fee":                      1.1786612069,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.0453387931,
		"merchant_reverse_tax_fee":             1.1786612069,
		"merchant_reverse_revenue":             4.9413387931,
		"ps_refund_profit":                     -0.7046612069,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount*0.5, false)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
	err = suite.service.merchant.Update(merchant)
	assert.Nil(suite.T(), err)

	orderAmount := float64(100)
	orderCountry := "RU"
	orderCurrency := "RUB"

	refundControlResults := map[string]float64{
		"real_refund":                          120,
		"real_refund_fee":                      12,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      120,
		"ps_merchant_refund_fx":                0,
		"merchant_refund_fee":                  24,
		"ps_markup_merchant_refund_fee":        12,
		"merchant_refund_fixed_fee_cost_value": 10.8,
		"merchant_refund_fixed_fee":            11.016,
		"ps_merchant_refund_fixed_fee_fx":      0.216,
		"ps_merchant_refund_fixed_fee_profit":  10.866,
		"reverse_tax_fee":                      24,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0,
		"merchant_reverse_tax_fee":             24,
		"merchant_reverse_revenue":             131.016,
		"ps_refund_profit":                     22.866,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err = suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "RUB")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.1661538462,
		"merchant_refund_fixed_fee":            0.1694769231,
		"ps_merchant_refund_fixed_fee_fx":      0.0033230769,
		"ps_merchant_refund_fixed_fee_profit":  0.0194769231,
		"reverse_tax_fee":                      2.407125,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.040875,
		"merchant_reverse_tax_fee":             2.407125,
		"merchant_reverse_revenue":             12.4503519231,
		"ps_refund_profit":                     1.3083519231,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.1661538462,
		"merchant_refund_fixed_fee":            0.1694769231,
		"ps_merchant_refund_fixed_fee_fx":      0.0033230769,
		"ps_merchant_refund_fixed_fee_profit":  0.0194769231,
		"reverse_tax_fee":                      2.352,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.096,
		"merchant_reverse_tax_fee":             2.352,
		"merchant_reverse_revenue":             12.5054769231,
		"ps_refund_profit":                     1.3634769231,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
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
		"real_refund_fee":                      1.2,
		"real_refund_fixed_fee":                0.15,
		"merchant_refund":                      12.24,
		"ps_merchant_refund_fx":                0.24,
		"merchant_refund_fee":                  2.448,
		"ps_markup_merchant_refund_fee":        1.248,
		"merchant_refund_fixed_fee_cost_value": 0.1661538462,
		"merchant_refund_fixed_fee":            0.1694769231,
		"ps_merchant_refund_fixed_fee_fx":      0.0033230769,
		"ps_merchant_refund_fixed_fee_profit":  0.0194769231,
		"reverse_tax_fee":                      2.3573224138,
		"reverse_tax_fee_delta":                0,
		"ps_reverse_tax_fee_delta":             0.0906775862,
		"merchant_reverse_tax_fee":             2.3573224138,
		"merchant_reverse_revenue":             12.5001545093,
		"ps_refund_profit":                     1.3581545093,
	}

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)
	refundAccountingEntries := suite.helperGetAccountingEntries(refund.Id, collectionRefund)
	assert.Equal(suite.T(), len(refundAccountingEntries), len(refundControlResults))
	merchantRoyaltyCurrency := order.GetMerchantRoyaltyCurrency()
	assert.Equal(suite.T(), merchantRoyaltyCurrency, "USD")
	for _, entry := range refundAccountingEntries {
		if !assert.Equal(suite.T(), entry.Amount, refundControlResults[entry.Type]) {
			println(entry.Type)
		}
		assert.Equal(suite.T(), entry.Currency, merchantRoyaltyCurrency)
	}

	controlRealRefund := refundControlResults["merchant_reverse_revenue"] + refundControlResults["merchant_reverse_tax_fee"] -
		refundControlResults["merchant_refund_fixed_fee"] - refundControlResults["merchant_refund_fee"] - refundControlResults["ps_merchant_refund_fx"]
	assert.Equal(suite.T(), refundControlResults["real_refund"], toPrecise(controlRealRefund))
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Ok() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
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
	err = suite.service.db.Collection(collectionAccountingEntry).FindId(bson.ObjectIdHex(rsp.Item.Id)).One(&accountingEntry)
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

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)

	req := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypeRealGrossRevenue,
		OrderId:    order.Id,
		RefundId:   refund.Id,
		MerchantId: bson.NewObjectId().Hex(),
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
		Find(bson.M{"source.id": req.MerchantId, "source.type": collectionMerchant}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_OrderNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypeRealGrossRevenue,
		OrderId:  bson.NewObjectId().Hex(),
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
		Find(bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_RefundNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypeRealGrossRevenue,
		RefundId: bson.NewObjectId().Hex(),
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
		Find(bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Refund_OrderNotFound_Error() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
	assert.NotNil(suite.T(), order)

	suite.paymentSystem.Handler = "mock_ok"
	err := suite.service.paymentSystem.Update(suite.paymentSystem)
	assert.NoError(suite.T(), err)

	refund := suite.helperMakeRefund(order, order.TotalPaymentAmount, true)
	assert.NotNil(suite.T(), refund)

	refund.Order.Id = bson.NewObjectId().Hex()
	err = suite.service.db.Collection(collectionRefund).UpdateId(bson.ObjectIdHex(refund.Id), refund)
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
		Find(bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_EntryNotExist_Error() {
	orderAmount := float64(650)
	orderCountry := "FI"
	orderCurrency := "RUB"

	order := suite.helperCreateAndPayOrder(orderAmount, orderCurrency, orderCountry)
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
		Find(bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) helperCreateAndPayOrder(amount float64, currency, country string) *billing.Order {
	req := &billing.OrderCreateRequest{
		ProjectId:   suite.projectFixedAmount.Id,
		Amount:      amount,
		Currency:    currency,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     bson.NewObjectId().Hex(),
		User: &billing.OrderUser{
			Email: "test@unit.unit",
			Ip:    "127.0.0.1",
			Address: &billing.OrderBillingAddress{
				Country: country,
			},
		},
	}

	rsp := &grpc.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)

	req1 := &grpc.PaymentCreateRequest{
		Data: map[string]string{
			pkg.PaymentCreateFieldOrderId:         rsp.Item.Uuid,
			pkg.PaymentCreateFieldPaymentMethodId: suite.paymentMethod.Id,
			pkg.PaymentCreateFieldEmail:           "test@unit.unit",
			pkg.PaymentCreateFieldPan:             "4000000000000002",
			pkg.PaymentCreateFieldCvv:             "123",
			pkg.PaymentCreateFieldMonth:           "02",
			pkg.PaymentCreateFieldYear:            time.Now().AddDate(1, 0, 0).Format("2006"),
			pkg.PaymentCreateFieldHolder:          "MR. CARD HOLDER",
		},
		Ip: "127.0.0.1",
	}

	rsp1 := &grpc.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)

	var order *billing.Order
	err = suite.service.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(rsp.Item.Id)).One(&order)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billing.Order{}, order)

	callbackRequest := &billing.CardPayPaymentCallback{
		PaymentMethod: suite.paymentMethod.ExternalId,
		CallbackTime:  time.Now().Format("2006-01-02T15:04:05Z"),
		MerchantOrder: &billing.CardPayMerchantOrder{
			Id:          rsp.Item.Id,
			Description: rsp.Item.Description,
		},
		CardAccount: &billing.CallbackCardPayBankCardAccount{
			Holder:             order.PaymentRequisites[pkg.PaymentCreateFieldHolder],
			IssuingCountryCode: country,
			MaskedPan:          order.PaymentRequisites[pkg.PaymentCreateFieldPan],
			Token:              bson.NewObjectId().Hex(),
		},
		Customer: &billing.CardPayCustomer{
			Email:  rsp.Item.User.Email,
			Ip:     rsp.Item.User.Ip,
			Id:     rsp.Item.ProjectAccount,
			Locale: "Europe/Moscow",
		},
		PaymentData: &billing.CallbackCardPayPaymentData{
			Id:          bson.NewObjectId().Hex(),
			Amount:      order.TotalPaymentAmount,
			Currency:    order.Currency,
			Description: order.Description,
			Is_3D:       true,
			Rrn:         bson.NewObjectId().Hex(),
			Status:      pkg.CardPayPaymentResponseStatusCompleted,
		},
	}

	buf, err := json.Marshal(callbackRequest)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(buf) + order.PaymentMethod.Params.SecretCallback))

	callbackData := &grpc.PaymentNotifyRequest{
		OrderId:   order.Id,
		Request:   buf,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}

	callbackResponse := &grpc.PaymentNotifyResponse{}
	err = suite.service.PaymentCallbackProcess(context.TODO(), callbackData, callbackResponse)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, callbackResponse.Status)

	err = suite.service.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(order.Id)).One(&order)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billing.Order{}, order)
	assert.Equal(suite.T(), int32(constant.OrderStatusPaymentSystemComplete), order.PrivateStatus)

	return order
}

func (suite *AccountingEntryTestSuite) helperMakeRefund(order *billing.Order, amount float64, isChargeback bool) *billing.Refund {
	req2 := &grpc.CreateRefundRequest{
		OrderId:      order.Uuid,
		Amount:       amount,
		CreatorId:    bson.NewObjectId().Hex(),
		Reason:       "unit test",
		IsChargeback: isChargeback,
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err := suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)
	assert.NoError(suite.T(), err)

	refundReq := &billing.CardPayRefundCallback{
		MerchantOrder: &billing.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billing.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billing.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       bson.NewObjectId().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   pkg.CardPayPaymentResponseStatusCompleted,
			AuthCode: bson.NewObjectId().Hex(),
			Is_3D:    true,
			Rrn:      bson.NewObjectId().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billing.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &grpc.CallbackRequest{
		Handler:   pkg.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &grpc.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Error)

	var refund *billing.Refund
	err = suite.service.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(rsp2.Item.Id)).One(&refund)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusCompleted, refund.Status)

	return refund
}

func (suite *AccountingEntryTestSuite) helperGetAccountingEntries(orderId, collection string) []*billing.AccountingEntry {
	var accountingEntries []*billing.AccountingEntry
	err := suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": bson.ObjectIdHex(orderId), "source.type": collection}).All(&accountingEntries)
	assert.NoError(suite.T(), err)

	return accountingEntries
}
