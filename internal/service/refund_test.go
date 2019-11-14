package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"testing"
	"time"
)

type RefundTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   internalPkg.CacheInterface

	paySys           *billing.PaymentSystem
	project          *billing.Project
	pmBankCard       *billing.PaymentMethod
	operatingCompany *billing.OperatingCompany
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

	paymentMinLimitSystem1 := &billing.PaymentMinLimitSystem{
		Id:        bson.NewObjectId().Hex(),
		Currency:  "RUB",
		Amount:    0.01,
		CreatedAt: ptypes.TimestampNow(),
		UpdatedAt: ptypes.TimestampNow(),
	}

	suite.operatingCompany = &billing.OperatingCompany{
		Id:                 bson.NewObjectId().Hex(),
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

	err = db.Collection(collectionOperatingCompanies).Insert(suite.operatingCompany)
	if err != nil {
		suite.FailNow("Insert operatingCompany test data failed", "%v", err)
	}

	keyRub := fmt.Sprintf(pkg.PaymentMethodKey, "RUB", pkg.MccCodeLowRisk, suite.operatingCompany.Id)

	country := &billing.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
		VatThreshold: &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		},
		VatPeriodMonth:          3,
		VatDeadlineDays:         25,
		VatStoreYears:           5,
		VatCurrencyRatesPolicy:  "last-day",
		VatCurrencyRatesSource:  "cbrf",
		PayerTariffRegion:       pkg.TariffRegionRussiaAndCis,
		HighRiskPaymentsAllowed: false,
		HighRiskChangeAllowed:   false,
	}

	suite.paySys = &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
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
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billing.PaymentMethodParams{
			keyRub: {
				Currency:           "RUB",
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			keyRub: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "RUB",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			}},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: suite.paySys.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchant := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		Company: &billing.MerchantCompanyInfo{
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
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
		Tariff: &billing.MerchantTariff{
			Payment: []*billing.MerchantTariffRatesPayment{
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
			Payout: &billing.MerchantTariffRatesSettingsItem{
				MethodPercentFee:       0,
				MethodFixedFee:         25.0,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       true,
			},
			HomeRegion: "russia_and_cis",
		},
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	project := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "RUB",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
	}
	psErr := &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "MockError",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "mock_error",
	}
	pmQiwi := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Qiwi",
		Group:            "QIWI",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "QIWI",
		TestSettings: map[string]*billing.PaymentMethodParams{
			keyRub: {
				Currency:           "RUB",
				TerminalId:         "15993",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: psErr.Id,
	}
	pmBitcoin := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bitcoin",
		Group:            "BITCOIN",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "BITCOIN",
		TestSettings: map[string]*billing.PaymentMethodParams{
			keyRub: {
				Currency:           "RUB",
				TerminalId:         "16007",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			keyRub: {
				TerminalId:         "16007",
				Currency:           "RUB",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			}},
		Type:            "crypto",
		IsActive:        true,
		PaymentSystemId: suite.paySys.Id,
	}

	merchantAgreement := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		Company: &billing.MerchantCompanyInfo{
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
		Status:                    pkg.MerchantStatusAgreementSigning,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 10000,
		},
		IsSigned:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	merchant1 := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		Company: &billing.MerchantCompanyInfo{
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
		Status:                    pkg.MerchantStatusDraft,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 100000,
		},
		IsSigned:           false,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)
	assert.NoError(suite.T(), err, "Creating RabbitMQ publisher failed")

	redisdb := mocks.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
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
	err = suite.service.db.Collection(collectionPaymentMinLimitSystem).Insert(limits...)
	assert.NoError(suite.T(), err)

	pms := []*billing.PaymentMethod{pmBankCard, pmQiwi, pmBitcoin}
	if err := suite.service.paymentMethod.MultipleInsert(pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	merchants := []*billing.Merchant{merchant, merchantAgreement, merchant1}
	if err := suite.service.merchant.MultipleInsert(merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	if err := suite.service.paymentSystem.MultipleInsert([]*billing.PaymentSystem{suite.paySys, psErr}); err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	sysCost := &billing.PaymentChannelCostSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "VISA",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "AZ",
		Percent:            1.5,
		FixAmount:          5,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	sysCost1 := &billing.PaymentChannelCostSystem{
		Id:                 "",
		Name:               "VISA",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "",
		Percent:            2.2,
		FixAmount:          0,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	err = suite.service.paymentChannelCostSystem.MultipleInsert([]*billing.PaymentChannelCostSystem{sysCost, sysCost1})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	merCost := &billing.PaymentChannelCostMerchant{
		Id:                      bson.NewObjectId().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "VISA",
		PayoutCurrency:          "RUB",
		MinAmount:               0.75,
		Region:                  pkg.TariffRegionRussiaAndCis,
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
		MccCode:                 pkg.MccCodeLowRisk,
	}

	merCost1 := &billing.PaymentChannelCostMerchant{
		Id:                      bson.NewObjectId().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               5,
		Region:                  pkg.TariffRegionRussiaAndCis,
		Country:                 "RU",
		MethodPercent:           2.5,
		MethodFixAmount:         2,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		IsActive:                true,
		MccCode:                 pkg.MccCodeLowRisk,
	}

	merCost2 := &billing.PaymentChannelCostMerchant{
		Id:                      bson.NewObjectId().Hex(),
		MerchantId:              project.GetMerchantId(),
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  pkg.TariffRegionRussiaAndCis,
		Country:                 "",
		MethodPercent:           2.2,
		MethodFixAmount:         0,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               5,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		IsActive:                true,
		MccCode:                 pkg.MccCodeLowRisk,
	}

	err = suite.service.paymentChannelCostMerchant.MultipleInsert([]*billing.PaymentChannelCostMerchant{merCost, merCost1, merCost2})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}

	mbSysCost := &billing.MoneyBackCostSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            3,
		FixAmount:          5,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost1 := &billing.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost2 := &billing.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	mbSysCost3 := &billing.MoneyBackCostSystem{
		Name:               "VISA",
		PayoutCurrency:     "RUB",
		UndoReason:         "reversal",
		Region:             pkg.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            10,
		FixAmount:          15,
		FixAmountCurrency:  "EUR",
		IsActive:           true,
		MccCode:            pkg.MccCodeLowRisk,
		OperatingCompanyId: merchant.OperatingCompanyId,
	}

	err = suite.service.moneyBackCostSystem.MultipleInsert([]*billing.MoneyBackCostSystem{mbSysCost, mbSysCost1, mbSysCost2, mbSysCost3})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	mbMerCost := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}

	mbMerCost1 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}

	mbMerCost2 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}
	mbMerCost3 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}
	mbMerCost4 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}
	mbMerCost5 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            pkg.TariffRegionRussiaAndCis,
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           pkg.MccCodeLowRisk,
	}

	err = suite.service.moneyBackCostMerchant.MultipleInsert([]*billing.MoneyBackCostMerchant{mbMerCost, mbMerCost1, mbMerCost2, mbMerCost3, mbMerCost4, mbMerCost5})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	bins := []interface{}{
		&BinData{
			Id:                 bson.NewObjectId(),
			CardBin:            400000,
			CardBrand:          "VISA",
			CardType:           "DEBIT",
			CardCategory:       "WORLD",
			BankName:           "ALFA BANK",
			BankCountryName:    "UKRAINE",
			BankCountryIsoCode: "US",
		},
		&BinData{
			Id:                 bson.NewObjectId(),
			CardBin:            500000,
			CardBrand:          "JCB",
			CardType:           "DEBIT",
			CardCategory:       "WORLD",
			BankName:           "ALFA BANK",
			BankCountryName:    "UKRAINE",
			BankCountryIsoCode: "US",
		},
	}

	err = db.Collection(collectionBinData).Insert(bins...)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
	}

	suite.project = project
	suite.pmBankCard = pmBankCard
}

func (suite *RefundTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotNil(suite.T(), rsp2.Item)
	assert.NotEmpty(suite.T(), rsp2.Item.Id)
	assert.NotEmpty(suite.T(), rsp2.Item.ExternalId)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, rsp2.Item.Status)

	var refund *billing.Refund
	err = suite.service.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(rsp2.Item.Id)).One(&refund)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, refund.Status)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_AmountLess_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   order.Uuid,
		Amount:    50,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, rsp2.Item.Status)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, rsp2.Item.Status)

	req2.Amount = order.TotalPaymentAmount
	rsp3 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), refundErrorPaymentAmountLess, rsp3.Message)
	assert.Empty(suite.T(), rsp3.Item)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_PaymentSystemNotExists_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	suite.paySys.Handler = "not_exist_payment_system"
	err = suite.service.paymentSystem.Update(suite.paySys)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), paymentSystemErrorHandlerNotFound.Error(), rsp2.Message.Message)
	assert.Empty(suite.T(), rsp2.Item)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_PaymentSystemReturnError_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	suite.paySys.Handler = "mock_error"
	err = suite.service.paymentSystem.Update(suite.paySys)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), pkg.PaymentSystemErrorCreateRefundFailed, rsp2.Message.Message)
	assert.Empty(suite.T(), rsp2.Item)
}

func (suite *RefundTestSuite) TestRefund_CreateRefundProcessor_ProcessOrder_OrderNotFound_Error() {
	processor := &createRefundProcessor{
		service: suite.service,
		request: &grpc.CreateRefundRequest{
			OrderId:   bson.NewObjectId().Hex(),
			Amount:    10,
			CreatorId: bson.NewObjectId().Hex(),
			Reason:    "unit test",
		},
		checked: &createRefundChecked{},
	}

	err := processor.processOrder()
	assert.Error(suite.T(), err)

	err1, ok := err.(*grpc.ResponseError)
	assert.True(suite.T(), ok)
	assert.Equal(suite.T(), newBillingServerResponseError(pkg.ResponseStatusNotFound, refundErrorNotFound), err1)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_RefundNotAllowed_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorNotAllowed, rsp2.Message)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_WasRefunded_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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

	order.PrivateStatus = constant.OrderStatusRefund
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorAlreadyRefunded, rsp2.Message)
}

func (suite *RefundTestSuite) TestRefund_ListRefunds_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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

	order.PrivateStatus = constant.OrderStatusProjectComplete
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	req3 := &grpc.ListRefundsRequest{
		OrderId: order.Uuid,
		Limit:   100,
		Offset:  0,
	}
	rsp3 := &grpc.ListRefundsResponse{}
	err = suite.service.ListRefunds(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, int(rsp3.Count))
	assert.Equal(suite.T(), rsp2.Item.Id, rsp3.Items[2].Id)
}

func (suite *RefundTestSuite) TestRefund_ListRefunds_Limit_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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

	order.PrivateStatus = constant.OrderStatusProjectComplete
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	req3 := &grpc.ListRefundsRequest{
		OrderId: order.Uuid,
		Limit:   1,
		Offset:  0,
	}
	rsp3 := &grpc.ListRefundsResponse{}
	err = suite.service.ListRefunds(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, int(req3.Limit))
}

func (suite *RefundTestSuite) TestRefund_ListRefunds_NoResults_Ok() {
	req3 := &grpc.ListRefundsRequest{
		OrderId: bson.NewObjectId().Hex(),
		Limit:   100,
		Offset:  0,
	}
	rsp3 := &grpc.ListRefundsResponse{}
	err := suite.service.ListRefunds(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, 0)
}

func (suite *RefundTestSuite) TestRefund_GetRefund_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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

	order.PrivateStatus = constant.OrderStatusProjectComplete
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)
	assert.NotEmpty(suite.T(), rsp2.Item)

	req3 := &grpc.GetRefundRequest{
		OrderId:    order.Uuid,
		RefundId:   rsp2.Item.Id,
		MerchantId: order.GetMerchantId(),
	}
	rsp3 := &grpc.CreateRefundResponse{}
	err = suite.service.GetRefund(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Message)
	assert.Equal(suite.T(), req3.OrderId, rsp3.Item.OriginalOrder.Uuid)
	assert.Equal(suite.T(), req3.RefundId, rsp3.Item.Id)
}

func (suite *RefundTestSuite) TestRefund_GetRefund_NotFound_Error() {
	req3 := &grpc.GetRefundRequest{
		OrderId:  uuid.New().String(),
		RefundId: bson.NewObjectId().Hex(),
	}
	rsp3 := &grpc.CreateRefundResponse{}
	err := suite.service.GetRefund(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorNotFound, rsp3.Message)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
		Amount:   20,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(order)

	ae := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	err = suite.service.db.Collection(collectionAccountingEntry).Insert(accountingEntries...)
	assert.NoError(suite.T(), err)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

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
	assert.False(suite.T(), refund.IsChargeback)

	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": bson.ObjectIdHex(refund.CreatedOrderId), "source.type": collectionRefund}).All(&accountingEntries)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), accountingEntries)

	refundOrder := new(billing.Order)
	err = suite.service.db.Collection(collectionOrder).Find(bson.M{"refund.receipt_number": refund.Id}).One(&refundOrder)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), refundOrder)
	assert.Equal(suite.T(), rsp.Id, refundOrder.ParentOrder.Id)
	assert.EqualValues(suite.T(), constant.OrderStatusRefund, refundOrder.PrivateStatus)
	assert.Equal(suite.T(), constant.OrderPublicStatusRefunded, refundOrder.Status)
	assert.EqualValues(suite.T(), refund.Amount, refundOrder.TotalPaymentAmount)
	assert.Equal(suite.T(), refund.Currency, refundOrder.Currency)
	assert.Equal(suite.T(), pkg.OrderTypeRefund, refundOrder.Type)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnmarshalError() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	refundReq := `{"some_field": "some_value"}`

	hash := sha512.New()
	hash.Write([]byte(refundReq + order.PaymentMethod.Params.SecretCallback))

	req3 := &grpc.CallbackRequest{
		Handler:   pkg.PaymentSystemHandlerCardPay,
		Body:      []byte(refundReq),
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &grpc.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), callbackRequestIncorrect, rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnknownHandler_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

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
		Handler:   "fake_payment_system_handler",
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &grpc.PaymentNotifyResponse{}
	err = suite.service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), callbackHandlerIncorrect, rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_RefundNotFound_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	refundReq := &billing.CardPayRefundCallback{
		MerchantOrder: &billing.CardPayMerchantOrder{
			Id: bson.NewObjectId().Hex(),
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billing.CardPayRefundCallbackPaymentData{
			Id:              bson.NewObjectId().Hex(),
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
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorNotFound.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_OrderNotFound_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	var refund *billing.Refund
	err = suite.service.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(rsp2.Item.Id)).One(&refund)
	assert.NotNil(suite.T(), refund)

	refund.OriginalOrder = &billing.RefundOrder{Id: bson.NewObjectId().Hex(), Uuid: uuid.New().String()}
	err = suite.service.db.Collection(collectionRefund).UpdateId(bson.ObjectIdHex(refund.Id), refund)

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
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp3.Status)
	assert.Equal(suite.T(), refundErrorOrderNotFound.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_UnknownPaymentSystemHandler_Error() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	suite.paySys.Handler = "fake_payment_system_handler"
	err = suite.service.paymentSystem.Update(suite.paySys)

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
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp3.Status)
	assert.Equal(suite.T(), orderErrorUnknown.Error(), rsp3.Error)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_ProcessRefundError() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	order.PaymentMethod.Params.Currency = "USD"
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	suite.paySys.Handler = pkg.PaymentSystemHandlerCardPay
	err = suite.service.paymentSystem.Update(suite.paySys)

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
			Amount:   10000,
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
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp3.Status)
	assert.Equal(suite.T(), paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid.Error(), rsp3.Error)

	var accountingEntries []*billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": bson.ObjectIdHex(rsp2.Item.Id), "source.type": collectionRefund}).All(&accountingEntries)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), accountingEntries)

	order, err = suite.service.getOrderById(rsp2.Item.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_TemporaryStatus_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	suite.paySys.Handler = pkg.PaymentSystemHandlerCardPay
	err = suite.service.paymentSystem.Update(suite.paySys)

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
			Status:   pkg.CardPayPaymentResponseStatusAuthorized,
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
	assert.Equal(suite.T(), paymentSystemErrorRequestTemporarySkipped.Error(), rsp3.Error)

	var refund *billing.Refund
	err = suite.service.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(rsp2.Item.Id)).One(&refund)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusInProgress, refund.Status)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_OrderFullyRefunded_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(order)

	ae := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	err = suite.service.db.Collection(collectionAccountingEntry).Insert(accountingEntries...)
	assert.NoError(suite.T(), err)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    order.TotalPaymentAmount,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

	refundReq := &billing.CardPayRefundCallback{
		MerchantOrder: &billing.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billing.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 0,
		},
		RefundData: &billing.CardPayRefundCallbackRefundData{
			Amount:   order.TotalPaymentAmount,
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

	err = suite.service.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(rsp.Id)).One(&order)
	assert.Equal(suite.T(), int32(constant.OrderStatusRefund), order.PrivateStatus)
}

func (suite *RefundTestSuite) TestRefund_ProcessRefundCallback_Chargeback_Ok() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
		Amount:   20,
		Currency: "RUB",
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.PaymentMethodOrderClosedAt, _ = ptypes.TimestampProto(time.Now().Add(-30 * time.Minute))
	err = suite.service.updateOrder(order)

	ae := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCostValue,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae2 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	ae3 := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRealTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   order.Id,
			Type: collectionOrder,
		},
		MerchantId: order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusAvailable,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    order.GetCountry(),
		Currency:   order.GetMerchantRoyaltyCurrency(),
	}

	accountingEntries := []interface{}{ae, ae2, ae3}
	err = suite.service.db.Collection(collectionAccountingEntry).Insert(accountingEntries...)
	assert.NoError(suite.T(), err)

	req2 := &grpc.CreateRefundRequest{
		OrderId:      rsp.Uuid,
		Amount:       10,
		CreatorId:    bson.NewObjectId().Hex(),
		Reason:       "unit test",
		IsChargeback: true,
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = suite.service.updateOrder(order)

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
	assert.True(suite.T(), refund.IsChargeback)

	order, err = suite.service.getOrderById(rsp2.Item.OriginalOrder.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.EqualValues(suite.T(), constant.OrderStatusChargeback, order.PrivateStatus)
	assert.Equal(suite.T(), refund.Amount, order.TotalPaymentAmount)

	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": bson.ObjectIdHex(refund.CreatedOrderId), "source.type": collectionRefund}).All(&accountingEntries)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), accountingEntries)
}

func (suite *RefundTestSuite) TestRefund_CreateRefund_NotHasCostsRates() {
	req := &billing.OrderCreateRequest{
		Type:        billing.OrderType_simple,
		ProjectId:   suite.project.Id,
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
			pkg.PaymentCreateFieldPan:             "5000000000000002",
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
	err = suite.service.updateOrder(order)

	req2 := &grpc.CreateRefundRequest{
		OrderId:   rsp.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp2 := &grpc.CreateRefundResponse{}
	err = suite.service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp2.Status)
	assert.Equal(suite.T(), refundErrorCostsRatesNotFound, rsp2.Message)
	assert.Nil(suite.T(), rsp2.Item)
}
