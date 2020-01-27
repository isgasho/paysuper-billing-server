package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/repository"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

type RefundTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   database.CacheInterface

	paySys           *billingpb.PaymentSystem
	project          *billingpb.Project
	pmBankCard       *billingpb.PaymentMethod
	operatingCompany *billingpb.OperatingCompany
}

func Test_Refund(t *testing.T) {
	suite.Run(t, new(RefundTestSuite))
}

func (suite *RefundTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	paymentMinLimitSystem1 := &billingpb.PaymentMinLimitSystem{
		Id:        primitive.NewObjectID().Hex(),
		Currency:  "RUB",
		Amount:    0.01,
		CreatedAt: ptypes.TimestampNow(),
		UpdatedAt: ptypes.TimestampNow(),
	}

	suite.operatingCompany = &billingpb.OperatingCompany{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "Legal name",
		Country:            "RU",
		RegistrationNumber: "some number",
		VatNumber:          "some vat number",
		Address:            "Home, home 0",
		VatAddress:         "Address for VAT purposes",
		SignatoryName:      "Vassiliy Poupkine",
		SignatoryPosition:  "CEO",
		BankingDetails:     "bank details including bank, bank address, account number, swift/ bic, intermediary bank",
		PaymentCountries:   []string{},
	}

	_, err = db.Collection(collectionOperatingCompanies).InsertOne(context.TODO(), suite.operatingCompany)
	if err != nil {
		suite.FailNow("Insert operatingCompany test data failed", "%v", err)
	}

	keyRubVisa := billingpb.GetPaymentMethodKey("RUB", billingpb.MccCodeLowRisk, suite.operatingCompany.Id, "Visa")
	keyRubBitcoin := billingpb.GetPaymentMethodKey("RUB", billingpb.MccCodeLowRisk, suite.operatingCompany.Id, "Bitcoin")
	keyRubQiwi := billingpb.GetPaymentMethodKey("RUB", billingpb.MccCodeLowRisk, suite.operatingCompany.Id, "Qiwi")

	countryRu := &billingpb.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
		VatThreshold: &billingpb.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:          3,
		VatDeadlineDays:         25,
		VatStoreYears:           5,
		VatCurrencyRatesPolicy:  "last-day",
		VatCurrencyRatesSource:  "cbrf",
		PayerTariffRegion:       billingpb.TariffRegionRussiaAndCis,
		HighRiskPaymentsAllowed: false,
		HighRiskChangeAllowed:   false,
	}

	countryUa := &billingpb.Country{
		IsoCodeA2:       "UA",
		Region:          "CIS",
		Currency:        "UAH",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      false,
		PriceGroupId:    "",
		VatCurrency:     "",
		VatThreshold: &billingpb.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:          0,
		VatDeadlineDays:         0,
		VatStoreYears:           0,
		VatCurrencyRatesPolicy:  "",
		VatCurrencyRatesSource:  "",
		PayerTariffRegion:       billingpb.TariffRegionRussiaAndCis,
		HighRiskPaymentsAllowed: false,
		HighRiskChangeAllowed:   false,
	}

	suite.paySys = &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            paymentSystemHandlerCardPayMock,
	}
	pmBankCard := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			keyRubVisa: {
				Currency:           "RUB",
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
		},
		ProductionSettings: map[string]*billingpb.PaymentMethodParams{
			keyRubVisa: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "RUB",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			}},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: suite.paySys.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchant := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "Unit test",
			AlternativeName:    "merchant1",
			Website:            "http://localhost",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Address:            "address",
			AddressAdditional:  "address_additional",
			RegistrationNumber: "registration_number",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:             "RUB",
			Name:                 "Bank name",
			Address:              "address",
			AccountNumber:        "0000001",
			Swift:                "swift",
			CorrespondentAccount: "correspondent_account",
			Details:              "details",
		},
		IsVatEnabled:              false,
		IsCommissionToUserEnabled: false,
		Status:                    billingpb.MerchantStatusDraft,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned: true,
		PaymentMethods: map[string]*billingpb.MerchantPaymentMethod{
			pmBankCard.Id: {
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   pmBankCard.Id,
					Name: pmBankCard.Name,
				},
				Commission: &billingpb.MerchantPaymentMethodCommissions{
					Fee: 2.5,
					PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
						Fee:      30,
						Currency: "RUB",
					},
				},
				Integration: &billingpb.MerchantPaymentMethodIntegration{
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
		Tariff: &billingpb.MerchantTariff{
			Payment: []*billingpb.MerchantTariffRatesPayment{
				{
					MinAmount:              0,
					MaxAmount:              4.99,
					MethodName:             "VISA",
					MethodPercentFee:       1.8,
					MethodFixedFee:         0.2,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           3.0,
					PsFixedFee:             0.3,
					PsFixedFeeCurrency:     "USD",
					MerchantHomeRegion:     "russia_and_cis",
					PayerRegion:            "europe",
				},
				{
					MinAmount:              5,
					MaxAmount:              999999999.99,
					MethodName:             "MasterCard",
					MethodPercentFee:       1.8,
					MethodFixedFee:         0.2,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           3.0,
					PsFixedFee:             0.3,
					PsFixedFeeCurrency:     "USD",
					MerchantHomeRegion:     "russia_and_cis",
					PayerRegion:            "europe",
				},
			},
			Payout: &billingpb.MerchantTariffRatesSettingsItem{
				MethodPercentFee:       0,
				MethodFixedFee:         25.0,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       true,
			},
			HomeRegion: "russia_and_cis",
		},
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
		DontChargeVat:      false,
	}

	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "RUB",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   billingpb.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
		VatPayer:                 billingpb.VatPayerBuyer,
	}
	psErr := &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "MockError",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "mock_error",
	}
	pmQiwi := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Qiwi",
		Group:            "QIWI",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "QIWI",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			keyRubQiwi: {
				Currency:           "RUB",
				TerminalId:         "15993",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
				Brand:              []string{"QIWI"},
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: psErr.Id,
	}
	pmBitcoin := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Bitcoin",
		Group:            "BITCOIN",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "BITCOIN",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			keyRubBitcoin: {
				Currency:           "RUB",
				TerminalId:         "16007",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
				Brand:              []string{"BITCOIN"},
			},
		},
		ProductionSettings: map[string]*billingpb.PaymentMethodParams{
			keyRubBitcoin: {
				TerminalId:         "16007",
				Currency:           "RUB",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
				Brand:              []string{"BITCOIN"},
			}},
		Type:            "crypto",
		IsActive:        true,
		PaymentSystemId: suite.paySys.Id,
	}

	merchantAgreement := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "Unit test",
			AlternativeName:    "merchant1",
			Website:            "http://localhost",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Address:            "address",
			AddressAdditional:  "address_additional",
			RegistrationNumber: "registration_number",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:             "RUB",
			Name:                 "Bank name",
			Address:              "address",
			AccountNumber:        "0000001",
			Swift:                "swift",
			CorrespondentAccount: "correspondent_account",
			Details:              "details",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusAgreementSigning,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 10000,
		},
		IsSigned:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
		DontChargeVat:      false,
	}
	merchant1 := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "Unit test",
			AlternativeName:    "merchant1",
			Website:            "http://localhost",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Address:            "address",
			AddressAdditional:  "address_additional",
			RegistrationNumber: "registration_number",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:             "RUB",
			Name:                 "Bank name",
			Address:              "address",
			AccountNumber:        "0000001",
			Swift:                "swift",
			CorrespondentAccount: "correspondent_account",
			Details:              "details",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusDraft,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 100000,
		},
		IsSigned:           false,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
		DontChargeVat:      false,
	}

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)
	assert.NoError(suite.T(), err, "Creating RabbitMQ publisher failed")

	redisdb := mocks.NewTestRedis()
	suite.cache, err = database.NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		broker,
		nil,
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

	limits := []interface{}{paymentMinLimitSystem1}
	_, err = suite.service.db.Collection(collectionPaymentMinLimitSystem).InsertMany(context.TODO(), limits)
	assert.NoError(suite.T(), err)

	pms := []*billingpb.PaymentMethod{pmBankCard, pmQiwi, pmBitcoin}
	if err := suite.service.paymentMethod.MultipleInsert(context.TODO(), pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	merchants := []*billingpb.Merchant{merchant, merchantAgreement, merchant1}
	if err := suite.service.merchantRepository.MultipleInsert(context.TODO(), merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(context.TODO(), project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(context.TODO(), countryRu); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
	if err := suite.service.country.Insert(context.TODO(), countryUa); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	if err := suite.service.paymentSystem.MultipleInsert(context.TODO(), []*billingpb.PaymentSystem{suite.paySys, psErr}); err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	sysCost := &billingpb.PaymentChannelCostSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Percent:            1.5,
		FixAmount:          5,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	sysCost1 := &billingpb.PaymentChannelCostSystem{
		Id:                 "",
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "",
		Percent:            2.2,
		FixAmount:          0,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	err = suite.service.paymentChannelCostSystem.MultipleInsert(context.TODO(), []*billingpb.PaymentChannelCostSystem{sysCost, sysCost1})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	merCost := &billingpb.PaymentChannelCostMerchant{
		Id:                      primitive.NewObjectID().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "VISA",
		PayoutCurrency:          "RUB",
		MinAmount:               0.75,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "RU",
		MethodPercent:           1.5,
		MethodFixAmount:         0.01,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               3,
		PsFixedFee:              0.01,
		PsFixedFeeCurrency:      "EUR",
		CreatedAt:               nil,
		UpdatedAt:               nil,
		IsActive:                true,
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	merCost1 := &billingpb.PaymentChannelCostMerchant{
		Id:                      primitive.NewObjectID().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               5,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "RU",
		MethodPercent:           2.5,
		MethodFixAmount:         2,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		IsActive:                true,
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	merCost2 := &billingpb.PaymentChannelCostMerchant{
		Id:                      primitive.NewObjectID().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "",
		MethodPercent:           2.2,
		MethodFixAmount:         0,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		IsActive:                true,
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	err = suite.service.paymentChannelCostMerchant.MultipleInsert(context.TODO(), []*billingpb.PaymentChannelCostMerchant{merCost, merCost1, merCost2})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}

	mbSysCost := &billingpb.MoneyBackCostSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            3,
		FixAmount:          5,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost1 := &billingpb.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost2 := &billingpb.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost3 := &billingpb.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "reversal",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	err = suite.service.moneyBackCostSystemRepository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostSystem{mbSysCost, mbSysCost1, mbSysCost2, mbSysCost3})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	mbMerCost := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	mbMerCost1 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	mbMerCost2 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}
	mbMerCost3 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}
	mbMerCost4 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}
	mbMerCost5 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	err = suite.service.moneyBackCostMerchantRepository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostMerchant{mbMerCost, mbMerCost1, mbMerCost2, mbMerCost3, mbMerCost4, mbMerCost5})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	bins := []interface{}{
		&BinData{
			Id:                 primitive.NewObjectID(),
			CardBin:            400000,
			CardBrand:          "VISA",
			CardType:           "DEBIT",
			CardCategory:       "WORLD",
			BankName:           "ALFA BANK",
			BankCountryName:    "UKRAINE",
			BankCountryIsoCode: "UA",
		},
		&BinData{
			Id:                 primitive.NewObjectID(),
			CardBin:            500000,
			CardBrand:          "JCB",
			CardType:           "DEBIT",
			CardCategory:       "WORLD",
			BankName:           "ALFA BANK",
			BankCountryName:    "UKRAINE",
			BankCountryIsoCode: "UA",
		},
	}

	_, err = db.Collection(collectionBinData).InsertMany(context.TODO(), bins)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
	}

	suite.project = project
	suite.pmBankCard = pmBankCard
}

func (suite *RefundTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotNil(suite.T(), rsp2.Item)
	assert.NotEmpty(suite.T(), rsp2.Item.Id)
	assert.NotEmpty(suite.T(), rsp2.Item.ExternalId)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, rsp2.Item.Status)

	refund, err := suite.service.refundRepository.GetById(context.TODO(), rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, refund.Status)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_PaymentSystemNotExists_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.PaymentMethod.Handler = "not_exist_payment_system"
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), paymentSystemErrorHandlerNotFound.Error(), rsp2.Message.Message)
	assert.Empty(suite.T(), rsp2.Item)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_PaymentSystemReturnError_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.PaymentMethod.Handler = "mock_error"
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), pkg.PaymentSystemErrorCreateRefundFailed, rsp2.Message.Message)
	assert.Empty(suite.T(), rsp2.Item)
}

func (suite *RefundTestSuite) TestRefund_CreateRefundProcessor_ProcessOrder_OrderNotFound_Error() {
	processor := &createRefundProcessor{
		service: suite.service,
		request: &billingpb.CreateRefundRequest{
			OrderId:   primitive.NewObjectID().Hex(),
			Amount:    10,
			CreatorId: primitive.NewObjectID().Hex(),
			Reason:    "unit test",
		},
		checked: &createRefundChecked{},
	}

	err := processor.processOrder()
	assert.Error(suite.T(), err)

	err1, ok := err.(*billingpb.ResponseError)
	assert.True(suite.T(), ok)
	assert.Equal(suite.T(), newBillingServerResponseError(billingpb.ResponseStatusNotFound, refundErrorNotFound), err1)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_RefundNotAllowed_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: primitive.NewObjectID().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorNotAllowed, rsp2.Message)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_WasRefunded_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusRefund
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: primitive.NewObjectID().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorAlreadyRefunded, rsp2.Message)
}

func (suite *RefundTestSuite) TestRefund_ListRefunds_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusProjectComplete
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.NotEmpty(suite.T(), rsp2.Message)

	req3 := &billingpb.ListRefundsRequest{
		MerchantId: order.GetMerchantId(),
		OrderId:    order.Uuid,
		Limit:      100,
		Offset:     0,
	}
	rsp3 := &billingpb.ListRefundsResponse{}
	err = suite.service.ListRefunds(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int64(1), rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, int(rsp3.Count))
}

func (suite *RefundTestSuite) TestRefund_ListRefunds_NoResults_Ok() {
	req3 := &billingpb.ListRefundsRequest{
		OrderId: primitive.NewObjectID().Hex(),
		Limit:   100,
		Offset:  0,
	}
	rsp3 := &billingpb.ListRefundsResponse{}
	err := suite.service.ListRefunds(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(0), rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, 0)
}

func (suite *RefundTestSuite) TestRefund_GetRefund_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusProjectComplete
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	req3 := &billingpb.GetRefundRequest{
		OrderId:    order.Uuid,
		RefundId:   rsp2.Item.Id,
		MerchantId: order.GetMerchantId(),
	}
	rsp3 := &billingpb.CreateRefundResponse{}
	err = suite.service.GetRefund(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Message)
	assert.Equal(suite.T(), req3.OrderId, rsp3.Item.OriginalOrder.Uuid)
	assert.Equal(suite.T(), req3.RefundId, rsp3.Item.Id)
}

func (suite *RefundTestSuite) TestRefund_GetRefund_NotFound_Error() {
	req3 := &billingpb.GetRefundRequest{
		OrderId:  uuid.New().String(),
		RefundId: primitive.NewObjectID().Hex(),
	}
	rsp3 := &billingpb.CreateRefundResponse{}
	err := suite.service.GetRefund(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorNotFound, rsp3.Message)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   20,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(context.TODO(), order)

	ae := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	_, err = suite.service.db.Collection(collectionAccountingEntry).InsertMany(context.TODO(), accountingEntries)
	assert.NoError(suite.T(), err)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Error)

	refund, err := suite.service.refundRepository.GetById(context.TODO(), rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusCompleted, refund.Status)
	assert.False(suite.T(), refund.IsChargeback)

	oid, err := primitive.ObjectIDFromHex(refund.CreatedOrderId)
	assert.NoError(suite.T(), err)
	filter := bson.M{"source.id": oid, "source.type": repository.CollectionRefund}

	cursor, err := suite.service.db.Collection(collectionAccountingEntry).Find(context.TODO(), filter)
	assert.NoError(suite.T(), err)
	err = cursor.All(context.TODO(), &accountingEntries)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), accountingEntries)

	refundOrder, err := suite.service.orderRepository.GetById(context.TODO(), refund.CreatedOrderId)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refundOrder)
	assert.Equal(suite.T(), rsp.Id, refundOrder.ParentOrder.Id)
	assert.EqualValues(suite.T(), recurringpb.OrderStatusRefund, refundOrder.PrivateStatus)
	assert.Equal(suite.T(), recurringpb.OrderPublicStatusRefunded, refundOrder.Status)
	assert.EqualValues(suite.T(), refund.Amount, refundOrder.ChargeAmount)
	assert.Equal(suite.T(), refund.Currency, refundOrder.Currency)
	assert.Equal(suite.T(), pkg.OrderTypeRefund, refundOrder.Type)
	assert.False(suite.T(), refundOrder.IsRefundAllowed)

	// check RefundAllowed flag for original order has correct value in order
	originalOrder, err := suite.service.orderRepository.GetById(context.TODO(), refund.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), originalOrder)
	assert.False(suite.T(), originalOrder.IsRefundAllowed)

	oid, err = primitive.ObjectIDFromHex(refund.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	filter = bson.M{"_id": oid}

	// check RefundAllowed flag for original order has correct value on order view
	originalOrderViewPublic := new(billingpb.OrderViewPublic)
	err = suite.service.db.Collection(collectionOrderView).FindOne(context.TODO(), filter).Decode(&originalOrderViewPublic)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), originalOrder)
	assert.False(suite.T(), originalOrderViewPublic.RefundAllowed)

	// check that OrderCharge exists in json
	originalOrderViewPublicJson, err := json.Marshal(originalOrderViewPublic)
	assert.NoError(suite.T(), err)
	originalOrderViewPublicFromJson := new(billingpb.OrderViewPublic)
	err = json.Unmarshal(originalOrderViewPublicJson, &originalOrderViewPublicFromJson)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), originalOrderViewPublicFromJson)
	assert.NotNil(suite.T(), originalOrderViewPublicFromJson.OrderCharge)
	assert.Equal(suite.T(), originalOrderViewPublic.OrderCharge, originalOrderViewPublicFromJson.OrderCharge)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnmarshalError() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := `{"some_field": "some_value"}`

	hash := sha512.New()
	hash.Write([]byte(refundReq + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      []byte(refundReq),
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), callbackRequestIncorrect, rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnknownHandler_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   "fake_payment_system_handler",
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), callbackHandlerIncorrect, rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_RefundNotFound_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: primitive.NewObjectID().Hex(),
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              primitive.NewObjectID().Hex(),
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorNotFound.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_OrderNotFound_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refund, err := suite.service.refundRepository.GetById(context.TODO(), rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refund)

	refund.OriginalOrder = &billingpb.RefundOrder{Id: primitive.NewObjectID().Hex(), Uuid: uuid.New().String()}
	err = suite.service.refundRepository.Update(context.TODO(), refund)
	assert.NoError(suite.T(), err)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorOrderNotFound.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnknownPaymentSystemHandler_Error() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	suite.paySys.Handler = "fake_payment_system_handler"
	err = suite.service.paymentSystem.Update(context.TODO(), suite.paySys)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp3.Status)
	assert.Equal(suite.T(), orderErrorUnknown.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_ProcessRefundError() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     10,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	suite.paySys.Handler = billingpb.PaymentSystemHandlerCardPay
	err = suite.service.paymentSystem.Update(context.TODO(), suite.paySys)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10000,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid.Error(), rsp3.Error)

	var accountingEntries []*billingpb.AccountingEntry
	oid, err := primitive.ObjectIDFromHex(rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	filter := bson.M{"source.id": oid, "source.type": repository.CollectionRefund}
	cursor, err := suite.service.db.Collection(collectionAccountingEntry).Find(context.TODO(), filter)
	assert.NoError(suite.T(), err)
	err = cursor.All(context.TODO(), &accountingEntries)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), accountingEntries)

	order, err = suite.service.getOrderById(context.TODO(), rsp2.Item.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_TemporaryStatus_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	suite.paySys.Handler = billingpb.PaymentSystemHandlerCardPay
	err = suite.service.paymentSystem.Update(context.TODO(), suite.paySys)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   order.ChargeAmount,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusAuthorized,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Equal(suite.T(), paymentSystemErrorRequestTemporarySkipped.Error(), rsp3.Error)

	refund, err := suite.service.refundRepository.GetById(context.TODO(), rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, refund.Status)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_OrderFullyRefunded_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(context.TODO(), order)

	ae := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	_, err = suite.service.db.Collection(collectionAccountingEntry).InsertMany(context.TODO(), accountingEntries)
	assert.NoError(suite.T(), err)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:    rsp.Uuid,
		Amount:     order.TotalPaymentAmount,
		CreatorId:  primitive.NewObjectID().Hex(),
		Reason:     "unit test",
		MerchantId: suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 0,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   order.TotalPaymentAmount,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Error)

	order, err = suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), recurringpb.OrderStatusRefund, order.PrivateStatus)
	assert.NotNil(suite.T(), order.Refund)
	assert.Equal(suite.T(), req2.Amount, order.Refund.Amount)
	assert.Equal(suite.T(), req2.Reason, order.Refund.Reason)
	assert.Equal(suite.T(), rsp2.Item.Id, order.Refund.ReceiptNumber)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_Chargeback_Ok() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   20,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(context.TODO(), order)

	ae := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billingpb.AccountingEntry{
		Id:     primitive.NewObjectID().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billingpb.AccountingEntrySource{
			Id:   order.Id,
			Type: repository.CollectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	_, err = suite.service.db.Collection(collectionAccountingEntry).InsertMany(context.TODO(), accountingEntries)
	assert.NoError(suite.T(), err)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:      rsp.Uuid,
		Amount:       10,
		CreatorId:    primitive.NewObjectID().Hex(),
		Reason:       "unit test",
		IsChargeback: true,
		MerchantId:   suite.project.MerchantId,
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(context.TODO(), order)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   billingpb.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Error)

	refund, err := suite.service.refundRepository.GetById(context.TODO(), rsp2.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusCompleted, refund.Status)
	assert.True(suite.T(), refund.IsChargeback)

	order, err = suite.service.getOrderById(context.TODO(), rsp2.Item.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.EqualValues(suite.T(), recurringpb.OrderStatusChargeback, order.PrivateStatus)
	assert.Equal(suite.T(), refund.Amount, order.TotalPaymentAmount)

	oid, err := primitive.ObjectIDFromHex(refund.CreatedOrderId)
	assert.NoError(suite.T(), err)
	filter := bson.M{"source.id": oid, "source.type": repository.CollectionRefund}

	cursor, err := suite.service.db.Collection(collectionAccountingEntry).Find(context.TODO(), filter)
	assert.NoError(suite.T(), err)
	err = cursor.All(context.TODO(), &accountingEntries)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), accountingEntries)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_NotHasCostsRates() {
	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &billingpb.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "5000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            expireYear.Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	order, err := suite.service.orderRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = recurringpb.OrderStatusPaymentSystemComplete
	order.Tax = &billingpb.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(context.TODO(), order)

	req2 := &billingpb.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: primitive.NewObjectID().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorCostsRatesNotFound, rsp2.Message)
	assert.Nil(suite.T(), rsp2.Item)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_OrderFullyRefunded_OtherOrders_Ok() {
	orderAmounts := []float64{100, 200, 300}
	orders := make([]*billingpb.Order, 0)

	for _, v := range orderAmounts {
		order := helperCreateAndPayOrder(suite.Suite, suite.service, v, "RUB", "RU", suite.project, suite.pmBankCard)
		assert.NotNil(suite.T(), order)

		orders = append(orders, order)
	}

	for _, v1 := range orders {
		_ = helperMakeRefund(suite.Suite, suite.service, v1, v1.ChargeAmount, false)
	}

	for _, v := range orders {
		order, err := suite.service.getOrderById(context.TODO(), v.Id)
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), order)

		assert.EqualValues(suite.T(), recurringpb.OrderStatusRefund, order.PrivateStatus)
		assert.Equal(suite.T(), recurringpb.OrderPublicStatusRefunded, order.Status)
		assert.NotNil(suite.T(), order.Refund)
		assert.NotZero(suite.T(), order.Refund.Amount)
		assert.NotZero(suite.T(), order.Refund.Reason)
		assert.NotZero(suite.T(), order.Refund.ReceiptNumber)

	}
}
