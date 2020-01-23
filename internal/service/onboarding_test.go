package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/centrifugal/gocent"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	"github.com/paysuper/paysuper-proto/go/reporterpb"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"strconv"
	"testing"
	"time"
)

type OnboardingTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   database.CacheInterface

	operatingCompany *billingpb.OperatingCompany

	merchant          *billingpb.Merchant
	merchantAgreement *billingpb.Merchant
	merchant1         *billingpb.Merchant

	project *billingpb.Project

	pmBankCard *billingpb.PaymentMethod
	pmQiwi     *billingpb.PaymentMethod

	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs

	euTariff   []*billingpb.MerchantTariffRatesPayment
	cisTariff  []*billingpb.MerchantTariffRatesPayment
	asiaTariff []*billingpb.MerchantTariffRatesPayment
}

func Test_Onboarding(t *testing.T) {
	suite.Run(t, new(OnboardingTestSuite))
}

func (suite *OnboardingTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	assert.NoError(suite.T(), err, "Config load failed")

	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

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

	_, err = db.Collection(collectionOperatingCompanies).InsertOne(ctx, suite.operatingCompany)
	if err != nil {
		suite.FailNow("Insert operatingCompany test data failed", "%v", err)
	}

	country := &billingpb.Country{
		IsoCodeA2:         "RU",
		Region:            "Russia",
		Currency:          "RUB",
		PaymentsAllowed:   true,
		ChangeAllowed:     true,
		VatEnabled:        true,
		PriceGroupId:      "",
		VatCurrency:       "RUB",
		PayerTariffRegion: "russia_and_cis",
	}

	ps := &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}

	pmBankCard := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			"RUB": {
				Currency:       "RUB",
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: ps.Id,
	}

	pmQiwi := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "QIWI",
		Group:            "QIWI",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		ExternalId:       "QIWI",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			"RUB": {
				Currency:       "RUB",
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: ps.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchant := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		User: &billingpb.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "Unit test",
			AlternativeName:    "merchant1",
			Country:            country.IsoCodeA2,
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
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
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
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
		MccCode: billingpb.MccCodeLowRisk,
	}

	date, err = ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchantAgreement := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		User: &billingpb.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test_agreement@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "Unit test status Agreement",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
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
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusAgreementSigning,
		HasMerchantSignature:      true,
		HasPspSignature:           true,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 10000,
		},
		IsSigned: true,
		MccCode:  billingpb.MccCodeLowRisk,
	}
	merchant1 := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		User: &billingpb.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test_merchant1@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
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
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusDraft,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 100000,
		},
		IsSigned: false,
		MccCode:  billingpb.MccCodeLowRisk,
	}

	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "RUB",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       true,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   billingpb.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
	}

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	casbin := &casbinMocks.CasbinService{}
	casbin.On("AddRoleForUser", mock2.Anything, mock2.Anything).Return(nil, nil)

	redisdb := mocks.NewTestRedis()
	suite.cache, err = database.NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		casbin,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billingpb.PaymentMethod{pmBankCard, pmQiwi}
	if err := suite.service.paymentMethod.MultipleInsert(ctx, pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	merchants := []*billingpb.Merchant{merchant, merchantAgreement, merchant1}
	if err := suite.service.merchantRepository.MultipleInsert(ctx, merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(ctx, project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	countries := []*billingpb.Country{
		country,
		{
			IsoCodeA2:         "DE",
			Region:            "EU",
			Currency:          "EUR",
			PaymentsAllowed:   true,
			ChangeAllowed:     true,
			VatEnabled:        true,
			PriceGroupId:      "",
			VatCurrency:       "EUR",
			PayerTariffRegion: "europe",
		},
		{
			IsoCodeA2:         "UK",
			Region:            "EU",
			Currency:          "EUR",
			PaymentsAllowed:   true,
			ChangeAllowed:     true,
			VatEnabled:        true,
			PriceGroupId:      "",
			VatCurrency:       "EUR",
			PayerTariffRegion: "europe",
		},
	}
	err = suite.service.country.MultipleInsert(ctx, countries)

	if err != nil {
		suite.FailNow("Insert countries test data failed", "%v", err)
	}

	euTariff := []*billingpb.MerchantTariffRatesPayment{
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
			MerchantHomeRegion:     "europe",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "MasterCard",
			MethodPercentFee:       1.8,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           3.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "europe",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "Bitcoin",
			MethodPercentFee:       2.5,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           5.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "europe",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
	}
	cisTariff := []*billingpb.MerchantTariffRatesPayment{
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
			PayerRegion:            "russia_and_cis",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "MasterCard",
			MethodPercentFee:       1.8,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           3.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "russia_and_cis",
			PayerRegion:            "russia_and_cis",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "Bitcoin",
			MethodPercentFee:       2.5,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           5.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "russia_and_cis",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
	}
	asiaTariff := []*billingpb.MerchantTariffRatesPayment{
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
			MerchantHomeRegion:     "asia",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "MasterCard",
			MethodPercentFee:       1.8,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           3.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "asia",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
		{
			MinAmount:              0,
			MaxAmount:              4.99,
			MethodName:             "Bitcoin",
			MethodPercentFee:       2.5,
			MethodFixedFee:         0.2,
			MethodFixedFeeCurrency: "USD",
			PsPercentFee:           5.0,
			PsFixedFee:             0.3,
			PsFixedFeeCurrency:     "USD",
			MerchantHomeRegion:     "asia",
			PayerRegion:            "europe",
			MccCode:                billingpb.MccCodeLowRisk,
			IsActive:               true,
		},
	}

	var tariffs []interface{}

	for _, v := range euTariff {
		tariffs = append(tariffs, v)
	}

	for _, v := range cisTariff {
		tariffs = append(tariffs, v)
	}

	for _, v := range asiaTariff {
		tariffs = append(tariffs, v)
	}

	_, err = suite.service.db.Collection(collectionMerchantsPaymentTariffs).InsertMany(ctx, tariffs)

	if err != nil {
		suite.FailNow("Insert merchant tariffs test data failed", "%v", err)
	}

	tariffsSettings := &billingpb.MerchantTariffRatesSettings{
		Refund: []*billingpb.MerchantTariffRatesSettingsItem{
			{
				MethodName:             "MasterCard",
				MethodPercentFee:       0.059757,
				MethodFixedFee:         0.03916,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       false,
			},
			{
				MethodName:             "VISA",
				MethodPercentFee:       0.00,
				MethodFixedFee:         0.27115,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       false,
			},
		},
		Chargeback: []*billingpb.MerchantTariffRatesSettingsItem{
			{
				MethodName:             "MasterCard",
				MethodPercentFee:       0.00,
				MethodFixedFee:         25.00,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       true,
			},
		},
		Payout: map[string]*billingpb.MerchantTariffRatesSettingsItem{
			"USD": {
				MethodPercentFee:       0.00,
				MethodFixedFee:         25,
				MethodFixedFeeCurrency: "USD",
				IsPaidByMerchant:       true,
			},
			"EUR": {
				MethodPercentFee:       0.00,
				MethodFixedFee:         25,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       true,
			},
			"RUB": {
				MethodPercentFee:       0.00,
				MethodFixedFee:         2500,
				MethodFixedFeeCurrency: "RUB",
				IsPaidByMerchant:       true,
			},
			"GBP": {
				MethodPercentFee:       0.00,
				MethodFixedFee:         20,
				MethodFixedFeeCurrency: "GBP",
				IsPaidByMerchant:       true,
			},
		},
		MinimalPayout: map[string]float32{
			"USD": 100,
			"EUR": 100,
			"RUB": 10000,
			"GBP": 100,
		},
		MccCode: billingpb.MccCodeLowRisk,
	}

	_, err = suite.service.db.Collection(collectionMerchantTariffsSettings).InsertOne(ctx, tariffsSettings)

	if err != nil {
		suite.FailNow("Insert merchant tariffs settings test data failed", "%v", err)
	}

	suite.merchant = merchant
	suite.merchantAgreement = merchantAgreement
	suite.merchant1 = merchant1

	suite.project = project

	suite.pmBankCard = pmBankCard
	suite.pmQiwi = pmQiwi

	var core zapcore.Core

	lvl := zap.NewAtomicLevel()
	core, suite.zapRecorder = observer.New(lvl)
	suite.logObserver = zap.New(core)

	suite.euTariff = euTariff
	suite.cisTariff = cisTariff
	suite.asiaTariff = asiaTariff

	reporterMock := &reportingMocks.ReporterService{}
	reporterMock.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).
		Return(&reporterpb.CreateFileResponse{Status: billingpb.ResponseStatusOk}, nil)
	suite.service.reporterService = reporterMock

	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock2.Anything, mock2.Anything).Return("token")
	centrifugoMock.On("Publish", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil)
	suite.service.centrifugoDashboard = centrifugoMock
}

func (suite *OnboardingTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_NewMerchant_Ok() {
	var merchant *billingpb.Merchant

	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "merchant1",
			AlternativeName:    "merchant1",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Status)
	assert.Equal(suite.T(), req.Company.Website, rsp.Company.Website)
	assert.Equal(suite.T(), req.Contacts.Authorized.Position, rsp.Contacts.Authorized.Position)
	assert.Equal(suite.T(), req.Banking.Name, rsp.Banking.Name)
	assert.Equal(suite.T(), req.Banking.Currency, rsp.Banking.Currency)
	assert.True(suite.T(), rsp.Steps.Company)
	assert.True(suite.T(), rsp.Steps.Contacts)
	assert.True(suite.T(), rsp.Steps.Banking)
	assert.False(suite.T(), rsp.Steps.Tariff)

	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant, err = suite.service.merchantRepository.GetById(ctx, rsp.Id)
	assert.NotNil(suite.T(), merchant)
	assert.Equal(suite.T(), billingpb.MerchantStatusPending, merchant.Status)
	assert.Equal(suite.T(), rsp.Contacts.Authorized.Position, merchant.Contacts.Authorized.Position)
	assert.Equal(suite.T(), rsp.Banking.Name, merchant.Banking.Name)
	assert.True(suite.T(), merchant.Steps.Banking)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_UpdateMerchant_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "0987654321",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "0987654321",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "0987654321",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Status)
	assert.Equal(suite.T(), req.Company.Website, rsp.Company.Website)
	assert.Equal(suite.T(), req.Contacts.Authorized.Phone, rsp.Contacts.Authorized.Phone)
	assert.Equal(suite.T(), req.Banking.AccountNumber, rsp.Banking.AccountNumber)
	assert.NotZero(suite.T(), rsp.CentrifugoToken)

	merchant, err := suite.service.merchantRepository.GetById(ctx, rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.Equal(suite.T(), rsp.Status, merchant.Status)
	assert.Equal(suite.T(), rsp.Contacts.Authorized.Phone, merchant.Contacts.Authorized.Phone)
	assert.Equal(suite.T(), rsp.Banking.AccountNumber, merchant.Banking.AccountNumber)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_UpdateMerchantNotAllowed_Error() {
	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchantAgreement.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	req := &billingpb.OnboardingRequest{
		Id: suite.merchantAgreement.Id,
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "0987654321",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "0987654321",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "0987654321",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err = suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusForbidden, cmres.Status)
	assert.Equal(suite.T(), merchantErrorChangeNotAllowed, cmres.Message)
	assert.Nil(suite.T(), cmres.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_CreateMerchant_CountryNotFound_Error() {
	req := &billingpb.OnboardingRequest{
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "XX",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorCountryNotFound, cmres.Message)
	assert.Nil(suite.T(), cmres.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_MerchantId_Ok() {
	req := &billingpb.GetMerchantByRequest{
		MerchantId: suite.merchant.Id,
	}

	rsp := &billingpb.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Item.Id)
	assert.Equal(suite.T(), suite.merchant.Company.Website, rsp.Item.Company.Website)
	assert.Equal(suite.T(), suite.merchant.Company.Name, rsp.Item.Company.Name)
	assert.NotEmpty(suite.T(), rsp.Item.CentrifugoToken)
	assert.True(suite.T(), rsp.Item.HasProjects)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_UserId_Ok() {
	req := &billingpb.GetMerchantByRequest{
		UserId: suite.merchant.User.Id,
	}

	rsp := &billingpb.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Item.Id)
	assert.Equal(suite.T(), suite.merchant.Company.Website, rsp.Item.Company.Website)
	assert.Equal(suite.T(), suite.merchant.Company.Name, rsp.Item.Company.Name)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_Error() {
	req := &billingpb.GetMerchantByRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}

	rsp := &billingpb.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantBy_IncorrectRequest_Error() {
	req := &billingpb.GetMerchantByRequest{}
	rsp := &billingpb.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantErrorBadData, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_EmptyQuery_Ok() {
	req := &billingpb.MerchantListingRequest{}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_NameQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		Name: "test",
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 2, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_StatusesQuery_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	// Create merchant with MerchantStatusDraft status
	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	// Create merchant with MerchantStatusAgreementSigned status
	req.User.Id = primitive.NewObjectID().Hex()
	req.Company.Name = req.Company.Name + "_1"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	merchant, err := suite.service.merchantRepository.GetById(ctx, rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigned
	err = suite.service.merchantRepository.Update(ctx, merchant)

	// Create merchant with MerchantStatusAgreementSigning status
	req.User.Id = primitive.NewObjectID().Hex()
	req.Company.Name = req.Company.Name + "_2"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	merchant, err = suite.service.merchantRepository.GetById(ctx, rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigning
	err = suite.service.merchantRepository.Update(ctx, merchant)

	// Create merchant with MerchantStatusAgreementSigned status
	req.User.Id = primitive.NewObjectID().Hex()
	req.Company.Name = req.Company.Name + "_3"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	merchant, err = suite.service.merchantRepository.GetById(ctx, rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigned
	err = suite.service.merchantRepository.Update(ctx, merchant)

	// Create merchant with MerchantStatusAgreementSigned status
	req.User.Id = primitive.NewObjectID().Hex()
	req.Company.Name = req.Company.Name + "_4"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	merchant, err = suite.service.merchantRepository.GetById(ctx, rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigned
	err = suite.service.merchantRepository.Update(ctx, merchant)

	// List merchants by status
	req1 := &billingpb.MerchantListingRequest{Statuses: []int32{billingpb.MerchantStatusDraft}}
	rsp1 := &billingpb.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), int64(3), rsp1.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp1.Items[0].Id)

	req1 = &billingpb.MerchantListingRequest{Statuses: []int32{billingpb.MerchantStatusAgreementSigning}}
	rsp1 = &billingpb.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), int64(2), rsp1.Count)

	req1 = &billingpb.MerchantListingRequest{Statuses: []int32{billingpb.MerchantStatusAgreementSigned}}
	rsp1 = &billingpb.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), int64(3), rsp1.Count)

	req1 = &billingpb.MerchantListingRequest{Statuses: []int32{billingpb.MerchantStatusAgreementSigning, billingpb.MerchantStatusAgreementSigned}}
	rsp1 = &billingpb.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), int64(5), rsp1.Count)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_QuickSearchQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		QuickSearch: "test_agreement",
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp.Count)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateFromQuery_Ok() {
	date := time.Now().Add(time.Hour * -450)

	req := &billingpb.MerchantListingRequest{
		LastPayoutDateFrom: date.Unix(),
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 2, rsp.Count)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateToQuery_Ok() {
	date := time.Now()

	req := &billingpb.MerchantListingRequest{
		LastPayoutDateTo: date.Unix(),
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateFromToQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		LastPayoutDateFrom: time.Now().Add(time.Hour * -500).Unix(),
		LastPayoutDateTo:   time.Now().Add(time.Hour * -400).Unix(),
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutAmountQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		LastPayoutAmount: 999999,
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_IsAgreementFalseQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		IsSigned: 1,
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp.Count)
	assert.Equal(suite.T(), suite.merchant1.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_IsAgreementTrueQuery_Ok() {
	req := &billingpb.MerchantListingRequest{
		IsSigned: 2,
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 2, rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Limit_Ok() {
	req := &billingpb.MerchantListingRequest{
		Limit: 2,
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Offset_Ok() {
	req := &billingpb.MerchantListingRequest{
		Offset: 1,
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Sort_Ok() {
	req := &billingpb.MerchantListingRequest{
		Limit: 2,
		Sort:  []string{"-_id"},
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchant1.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_EmptyResult_Ok() {
	req := &billingpb.MerchantListingRequest{
		Name: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp.Count)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Item.Status)

	merchant, err := suite.service.merchantRepository.GetById(ctx, rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigning
	err = suite.service.merchantRepository.Update(ctx, merchant)
	assert.NoError(suite.T(), err)

	reqChangeStatus := &billingpb.MerchantChangeStatusRequest{
		MerchantId: rsp.Item.Id,
		Status:     billingpb.MerchantStatusRejected,
	}

	rspChangeStatus := &billingpb.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), billingpb.MerchantStatusRejected, rspChangeStatus.Item.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_MerchantPaymentMethodsEmpty_Ok() {
	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), suite.merchant1.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.Len(suite.T(), merchant.PaymentMethods, 0)

	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant1.Id,
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rsp.PaymentMethods) > 0)
	pm, err := suite.service.paymentMethod.GetAll(context.TODO())
	assert.Len(suite.T(), rsp.PaymentMethods, len(pm))

	for _, v := range rsp.PaymentMethods {
		assert.True(suite.T(), v.PaymentMethod.Id != "")
		assert.True(suite.T(), v.PaymentMethod.Name != "")
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodFee, v.Commission.Fee)
		assert.NotNil(suite.T(), v.Commission.PerTransaction)
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodPerTransactionFee, v.Commission.PerTransaction.Fee)
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodCurrency, v.Commission.PerTransaction.Currency)
		assert.True(suite.T(), v.Integration.TerminalId == "")
		assert.True(suite.T(), v.Integration.TerminalPassword == "")
		assert.False(suite.T(), v.Integration.Integrated)
		assert.True(suite.T(), v.IsActive)
	}
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_ExistMerchantHasPaymentMethod_Ok() {
	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant.Id,
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rsp.PaymentMethods) > 0)
	pm, err := suite.service.paymentMethod.GetAll(context.TODO())
	assert.Len(suite.T(), rsp.PaymentMethods, len(pm))

	for _, v := range rsp.PaymentMethods {
		if v.PaymentMethod.Id != suite.pmBankCard.Id {
			continue
		}

		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].PaymentMethod.Id, v.PaymentMethod.Id)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].PaymentMethod.Name, v.PaymentMethod.Name)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Commission.Fee, v.Commission.Fee)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Commission.PerTransaction.Fee, v.Commission.PerTransaction.Fee)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Commission.PerTransaction.Currency, v.Commission.PerTransaction.Currency)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration.TerminalId, v.Integration.TerminalId)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration.TerminalPassword, v.Integration.TerminalPassword)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].Integration.Integrated, v.Integration.Integrated)
		assert.Equal(suite.T(), suite.merchant.PaymentMethods[suite.pmBankCard.Id].IsActive, v.IsActive)
	}
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_NewMerchant_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item

	assert.Nil(suite.T(), rsp.PaymentMethods)

	reqListMerchantPaymentMethods := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: rsp.Id,
	}
	rspListMerchantPaymentMethods := &billingpb.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), reqListMerchantPaymentMethods, rspListMerchantPaymentMethods)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rspListMerchantPaymentMethods.PaymentMethods) > 0)
	pma, err := suite.service.paymentMethod.GetAll(context.TODO())
	assert.Len(suite.T(), rspListMerchantPaymentMethods.PaymentMethods, len(pma))

	for _, v := range rspListMerchantPaymentMethods.PaymentMethods {
		assert.True(suite.T(), v.PaymentMethod.Id != "")
		assert.True(suite.T(), v.PaymentMethod.Name != "")
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodFee, v.Commission.Fee)
		assert.NotNil(suite.T(), v.Commission.PerTransaction)
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodPerTransactionFee, v.Commission.PerTransaction.Fee)
		assert.Equal(suite.T(), pkg.DefaultPaymentMethodCurrency, v.Commission.PerTransaction.Currency)
		assert.True(suite.T(), v.Integration.TerminalId == "")
		assert.True(suite.T(), v.Integration.TerminalPassword == "")
		assert.False(suite.T(), v.Integration.Integrated)
		assert.True(suite.T(), v.IsActive)
	}

	reqMerchantPaymentMethodAdd := &billingpb.MerchantPaymentMethodRequest{
		MerchantId: rsp.Id,
		PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
			Id:   suite.pmBankCard.Id,
			Name: suite.pmBankCard.Name,
		},
		Commission: &billingpb.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
				Fee:      100,
				Currency: "RUB",
			},
		},
		Integration: &billingpb.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
		UserId:   primitive.NewObjectID().Hex(),
	}
	rspMerchantPaymentMethodAdd := &billingpb.MerchantPaymentMethodResponse{}
	err = suite.service.ChangeMerchantPaymentMethod(context.TODO(), reqMerchantPaymentMethodAdd, rspMerchantPaymentMethodAdd)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rspMerchantPaymentMethodAdd.Status)
	assert.NotNil(suite.T(), rspMerchantPaymentMethodAdd.Item)
	assert.True(suite.T(), len(rspMerchantPaymentMethodAdd.Item.PaymentMethod.Id) > 0)

	pm, err := suite.service.getMerchantPaymentMethod(context.TODO(), rsp.Id, suite.pmBankCard.Id)
	assert.NoError(suite.T(), err)

	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.PaymentMethod.Id, pm.PaymentMethod.Id)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.PaymentMethod.Name, pm.PaymentMethod.Name)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.Fee, pm.Commission.Fee)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.PerTransaction.Fee, pm.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.PerTransaction.Currency, pm.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.TerminalId, pm.Integration.TerminalId)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.TerminalPassword, pm.Integration.TerminalPassword)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.Integrated, pm.Integration.Integrated)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.IsActive, pm.IsActive)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.True(suite.T(), len(merchant.PaymentMethods) > 0)

	pm1, ok := merchant.PaymentMethods[suite.pmBankCard.Id]
	assert.True(suite.T(), ok)

	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.PaymentMethod.Id, pm1.PaymentMethod.Id)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.PaymentMethod.Name, pm1.PaymentMethod.Name)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.Fee, pm1.Commission.Fee)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.PerTransaction.Fee, pm1.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Commission.PerTransaction.Currency, pm1.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.TerminalId, pm1.Integration.TerminalId)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.TerminalPassword, pm1.Integration.TerminalPassword)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.Integration.Integrated, pm1.Integration.Integrated)
	assert.Equal(suite.T(), reqMerchantPaymentMethodAdd.IsActive, pm1.IsActive)

	assert.Equal(suite.T(), pm.PaymentMethod.Id, pm1.PaymentMethod.Id)
	assert.Equal(suite.T(), pm.PaymentMethod.Name, pm1.PaymentMethod.Name)
	assert.Equal(suite.T(), pm.Commission.Fee, pm1.Commission.Fee)
	assert.Equal(suite.T(), pm.Commission.PerTransaction.Fee, pm1.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), pm.Commission.PerTransaction.Currency, pm1.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), pm.Integration.TerminalId, pm1.Integration.TerminalId)
	assert.Equal(suite.T(), pm.Integration.TerminalPassword, pm1.Integration.TerminalPassword)
	assert.Equal(suite.T(), pm.Integration.Integrated, pm1.Integration.Integrated)
	assert.Equal(suite.T(), pm.IsActive, pm1.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_UpdateMerchant_Ok() {
	reqMerchantPaymentMethodAdd := &billingpb.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
			Id:   suite.pmQiwi.Id,
			Name: suite.pmQiwi.Name,
		},
		Commission: &billingpb.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
				Fee:      100,
				Currency: "RUB",
			},
		},
		Integration: &billingpb.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
		UserId:   primitive.NewObjectID().Hex(),
	}
	rspMerchantPaymentMethodAdd := &billingpb.MerchantPaymentMethodResponse{}
	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), reqMerchantPaymentMethodAdd, rspMerchantPaymentMethodAdd)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rspMerchantPaymentMethodAdd.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_PaymentMethodsIsEmpty_Ok() {
	_, err := suite.service.db.Collection(collectionPaymentMethod).DeleteMany(context.TODO(), bson.M{})

	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant1.Id,
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 0)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_Filter_Ok() {
	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId:        suite.merchant.Id,
		PaymentMethodName: "iwi",
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 1)

	pm := rsp.PaymentMethods[0]

	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, pm.PaymentMethod.Name)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodFee, pm.Commission.Fee)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodPerTransactionFee, pm.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodCurrency, pm.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), "", pm.Integration.TerminalId)
	assert.Equal(suite.T(), "", pm.Integration.TerminalPassword)
	assert.False(suite.T(), pm.Integration.Integrated)
	assert.True(suite.T(), pm.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_Sort_Ok() {
	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant.Id,
		Sort:       []string{"-name"},
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 2)

	pm := rsp.PaymentMethods[0]

	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, pm.PaymentMethod.Name)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_MerchantNotFound_EmptyResult() {
	req := &billingpb.ListMerchantPaymentMethodsRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Empty(suite.T(), rsp.PaymentMethods)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_ExistPaymentMethod_Ok() {
	req := &billingpb.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: suite.pmBankCard.Id,
	}
	rsp := &billingpb.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.NotNil(suite.T(), rsp.Item.PaymentMethod)
	assert.NotNil(suite.T(), rsp.Item.Commission)
	assert.NotNil(suite.T(), rsp.Item.Commission.PerTransaction)
	assert.NotNil(suite.T(), rsp.Item.Integration)
	assert.True(suite.T(), rsp.Item.IsActive)

	pm, ok := suite.merchant.PaymentMethods[suite.pmBankCard.Id]
	assert.True(suite.T(), ok)

	assert.Equal(suite.T(), pm.PaymentMethod.Id, rsp.Item.PaymentMethod.Id)
	assert.Equal(suite.T(), pm.PaymentMethod.Name, rsp.Item.PaymentMethod.Name)
	assert.Equal(suite.T(), pm.Commission.Fee, rsp.Item.Commission.Fee)
	assert.Equal(suite.T(), pm.Commission.PerTransaction.Fee, rsp.Item.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), pm.Commission.PerTransaction.Currency, rsp.Item.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), pm.Integration.TerminalId, rsp.Item.Integration.TerminalId)
	assert.Equal(suite.T(), pm.Integration.TerminalPassword, rsp.Item.Integration.TerminalPassword)
	assert.Equal(suite.T(), pm.Integration.Integrated, rsp.Item.Integration.Integrated)
	assert.Equal(suite.T(), pm.IsActive, rsp.Item.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_NotExistPaymentMethod_Ok() {
	req := &billingpb.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: suite.pmQiwi.Id,
	}
	rsp := &billingpb.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.NotNil(suite.T(), rsp.Item.PaymentMethod)
	assert.NotNil(suite.T(), rsp.Item.Commission)
	assert.NotNil(suite.T(), rsp.Item.Commission.PerTransaction)
	assert.NotNil(suite.T(), rsp.Item.Integration)
	assert.True(suite.T(), rsp.Item.IsActive)

	assert.Equal(suite.T(), suite.pmQiwi.Id, rsp.Item.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, rsp.Item.PaymentMethod.Name)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodFee, rsp.Item.Commission.Fee)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodPerTransactionFee, rsp.Item.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), pkg.DefaultPaymentMethodCurrency, rsp.Item.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), "", rsp.Item.Integration.TerminalId)
	assert.Equal(suite.T(), "", rsp.Item.Integration.TerminalPassword)
	assert.False(suite.T(), rsp.Item.Integration.Integrated)
	assert.True(suite.T(), rsp.Item.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_PaymentMethodNotFound_Error() {
	req := &billingpb.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), orderErrorPaymentMethodNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_MerchantNotFound_Error() {
	req := &billingpb.GetMerchantPaymentMethodRequest{
		MerchantId:      primitive.NewObjectID().Hex(),
		PaymentMethodId: suite.pmBankCard.Id,
	}
	rsp := &billingpb.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantPaymentMethod_PaymentMethodNotFound_Error() {
	req := &billingpb.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
			Id:   primitive.NewObjectID().Hex(),
			Name: "Unit test",
		},
		Commission: &billingpb.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
				Fee:      10,
				Currency: "RUB",
			},
		},
		Integration: &billingpb.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
	}
	rsp := &billingpb.MerchantPaymentMethodResponse{}
	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), orderErrorPaymentMethodNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantPaymentMethod_CurrencyNotFound_Error() {
	req := &billingpb.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
			Id:   suite.pmBankCard.Id,
			Name: suite.pmBankCard.Name,
		},
		Commission: &billingpb.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
				Fee:      10,
				Currency: "USD",
			},
		},
		Integration: &billingpb.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
	}
	rsp := &billingpb.MerchantPaymentMethodResponse{}

	suite.service.curService = mocks.NewCurrencyServiceMockError()
	suite.service.supportedCurrencies = []string{}

	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), orderErrorCurrencyNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_Ok() {
	var notification *billingpb.Notification

	oid, err := primitive.ObjectIDFromHex(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	query := bson.M{
		"merchant_id": oid,
	}
	err = suite.service.db.Collection(collectionNotification).FindOne(context.TODO(), query).Decode(&notification)
	assert.Nil(suite.T(), notification)

	req := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     "",
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	cnres := &billingpb.CreateNotificationResponse{}

	err = suite.service.CreateNotification(context.TODO(), req, cnres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cnres.Status, billingpb.ResponseStatusOk)
	rsp := cnres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), req.MerchantId, rsp.MerchantId)
	assert.Equal(suite.T(), req.UserId, rsp.UserId)
	assert.Equal(suite.T(), req.Message, rsp.Message)

	err = suite.service.db.Collection(collectionNotification).FindOne(context.TODO(), query).Decode(&notification)
	assert.NotNil(suite.T(), notification)
	assert.Equal(suite.T(), rsp.Id, notification.Id)
	assert.Equal(suite.T(), rsp.MerchantId, notification.MerchantId)
	assert.Equal(suite.T(), rsp.UserId, notification.UserId)
	assert.Equal(suite.T(), rsp.Message, notification.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_MessageEmpty_Error() {
	req := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title",
	}
	rsp := &billingpb.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), notificationErrorMessageIsEmpty, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_AddNotification_Error() {
	req := &billingpb.NotificationRequest{
		MerchantId: "ffffffffffffffffffffffff",
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	rsp := &billingpb.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetNotification_Ok() {
	req := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	cmres := &billingpb.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, cmres)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item

	assert.True(suite.T(), len(rsp.Id) > 0)

	reqGetNotification := &billingpb.GetNotificationRequest{
		MerchantId:     suite.merchant.Id,
		NotificationId: rsp.Id,
	}
	rspGetNotification := &billingpb.Notification{}
	err = suite.service.GetNotification(context.TODO(), reqGetNotification, rspGetNotification)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Id, rspGetNotification.Id)
	assert.Equal(suite.T(), rsp.MerchantId, rspGetNotification.MerchantId)
	assert.Equal(suite.T(), rsp.UserId, rspGetNotification.UserId)
	assert.Equal(suite.T(), rsp.Message, rspGetNotification.Message)
	assert.NotNil(suite.T(), rspGetNotification.CreatedAt)
	assert.NotNil(suite.T(), rspGetNotification.UpdatedAt)
}

func (suite *OnboardingTestSuite) TestOnboarding_NotFound_Error() {
	reqGetNotification := &billingpb.GetNotificationRequest{
		MerchantId:     primitive.NewObjectID().Hex(),
		NotificationId: primitive.NewObjectID().Hex(),
	}
	rspGetNotification := &billingpb.Notification{}
	err := suite.service.GetNotification(context.TODO(), reqGetNotification, rspGetNotification)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), notificationErrorNotFound, err)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_Merchant_Ok() {
	req1 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &billingpb.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)

	req2 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp2 := &billingpb.CreateNotificationResponse{}

	err = suite.service.CreateNotification(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp2.Item.Id) > 0)

	req3 := &billingpb.ListingNotificationRequest{
		MerchantId: suite.merchant.Id,
		Limit:      10,
		Offset:     0,
	}
	rsp3 := &billingpb.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req3, rsp3)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp3.Items, 2)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp3.Items[0].Id)
	assert.Equal(suite.T(), rsp2.Item.Id, rsp3.Items[1].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_Sort_Ok() {
	req := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp := &billingpb.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req.Title = req.Title + "_1"
	err = suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req.Title = req.Title + "_2"
	err = suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req1 := &billingpb.ListingNotificationRequest{
		MerchantId: suite.merchant.Id,
		Sort:       []string{"-created_at"},
		Limit:      10,
		Offset:     0,
	}
	rsp1 := &billingpb.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp1.Items, 3)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_User_Ok() {
	userId := primitive.NewObjectID().Hex()

	req1 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &billingpb.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)

	req2 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 2",
		Message:    "Unit test message 2",
	}

	rsp2 := &billingpb.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp2.Item.Id) > 0)

	req3 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 3",
		Message:    "Unit test message 3",
	}

	rsp3 := &billingpb.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req3, rsp3)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp3.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp3.Item.Id) > 0)

	req4 := &billingpb.ListingNotificationRequest{
		UserId: userId,
		Limit:  10,
		Offset: 0,
	}
	rsp4 := &billingpb.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req4, rsp4)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp4.Items, 3)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp4.Items[0].Id)
	assert.Equal(suite.T(), rsp2.Item.Id, rsp4.Items[1].Id)
	assert.Equal(suite.T(), rsp3.Item.Id, rsp4.Items[2].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_MarkNotificationAsRead_Ok() {
	req1 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &billingpb.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)
	assert.False(suite.T(), rsp1.Item.IsRead)

	req2 := &billingpb.GetNotificationRequest{
		MerchantId:     req1.MerchantId,
		NotificationId: rsp1.Item.Id,
	}
	rsp2 := &billingpb.Notification{}
	err = suite.service.MarkNotificationAsRead(context.TODO(), req2, rsp2)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), rsp2.IsRead)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp2.Id)

	oid, err := primitive.ObjectIDFromHex(rsp1.Item.Id)
	assert.NoError(suite.T(), err)
	filter := bson.M{"_id": oid}
	var notification *billingpb.Notification
	err = suite.service.db.Collection(collectionNotification).FindOne(context.TODO(), filter).Decode(&notification)
	assert.NotNil(suite.T(), notification)

	assert.True(suite.T(), notification.IsRead)
}

func (suite *OnboardingTestSuite) TestOnboarding_MarkNotificationAsRead_NotFound_Error() {
	req1 := &billingpb.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}

	rsp1 := &billingpb.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, billingpb.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)
	assert.False(suite.T(), rsp1.Item.IsRead)

	req2 := &billingpb.GetNotificationRequest{
		MerchantId:     primitive.NewObjectID().Hex(),
		NotificationId: primitive.NewObjectID().Hex(),
	}
	rsp2 := &billingpb.Notification{}
	err = suite.service.MarkNotificationAsRead(context.TODO(), req2, rsp2)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), notificationErrorNotFound, err)
	assert.False(suite.T(), rsp2.IsRead)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Status)
	assert.Empty(suite.T(), rsp.ReceivedDate)
	assert.Empty(suite.T(), rsp.StatusLastUpdatedAt)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigning
	merchant.AgreementType = pkg.MerchantAgreementTypeESign
	err = suite.service.merchantRepository.Update(context.TODO(), merchant)

	req1 := &billingpb.ChangeMerchantDataRequest{
		MerchantId:           merchant.Id,
		HasPspSignature:      false,
		HasMerchantSignature: true,
	}
	rsp1 := &billingpb.ChangeMerchantDataResponse{}
	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant1, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.False(suite.T(), merchant1.HasPspSignature)
	assert.True(suite.T(), merchant1.HasMerchantSignature)
	assert.Equal(suite.T(), billingpb.MerchantStatusAgreementSigning, merchant1.Status)
	assert.NotEmpty(suite.T(), merchant1.ReceivedDate)
	assert.NotZero(suite.T(), merchant1.ReceivedDate.Seconds)
	assert.NotZero(suite.T(), merchant1.StatusLastUpdatedAt.Seconds)

	req1.HasPspSignature = true
	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant1, err = suite.service.merchantRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.True(suite.T(), merchant1.HasPspSignature)
	assert.True(suite.T(), merchant1.HasMerchantSignature)
	assert.Equal(suite.T(), billingpb.MerchantStatusAgreementSigned, merchant1.Status)
	assert.NotEmpty(suite.T(), merchant1.ReceivedDate)
	assert.NotZero(suite.T(), merchant1.ReceivedDate.Seconds)
	assert.NotZero(suite.T(), merchant1.StatusLastUpdatedAt.Seconds)

	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	req2 := &billingpb.ListingNotificationRequest{
		MerchantId: merchant.Id,
	}
	rsp2 := &billingpb.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), rsp2.Count, 2)
	assert.Len(suite.T(), rsp2.Items, 2)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_MerchantNotFound_Error() {
	req1 := &billingpb.ChangeMerchantDataRequest{
		MerchantId:           primitive.NewObjectID().Hex(),
		HasPspSignature:      true,
		HasMerchantSignature: true,
	}
	rsp1 := &billingpb.ChangeMerchantDataResponse{}
	err := suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantS3Agreement_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Status)
	assert.Empty(suite.T(), rsp.S3AgreementName)

	zap.ReplaceGlobals(suite.logObserver)
	suite.service.centrifugoDashboard = &Centrifugo{
		centrifugoClient: gocent.New(
			gocent.Config{
				Addr:       "http://localhost",
				Key:        "api_key",
				HTTPClient: mocks.NewClientStatusOk(),
			},
		),
	}

	ocRep := &mocks.OperatingCompanyInterface{}
	ocRep.On("GetById", mock2.Anything, mock2.Anything).Return(&billingpb.OperatingCompany{SignatoryName: "name", Email: "email"}, nil)
	suite.service.operatingCompany = ocRep

	req1 := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId:      rsp.Id,
		S3AgreementName: "agreement_" + rsp.Id + ".pdf",
	}
	rsp1 := &billingpb.ChangeMerchantDataResponse{}
	err = suite.service.SetMerchantS3Agreement(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.Equal(suite.T(), req1.S3AgreementName, rsp1.Item.S3AgreementName)

	messages := suite.zapRecorder.All()
	assert.Equal(suite.T(), zapcore.InfoLevel, messages[0].Level)
	assert.Len(suite.T(), messages[0].Context, 2)
	assert.Equal(suite.T(), "request_body", messages[0].Context[1].Key)

	msg := make(map[string]interface{})
	err = json.Unmarshal(messages[0].Context[1].Interface.([]byte), &msg)
	assert.NoError(suite.T(), err)
	assert.Contains(suite.T(), msg, "params")

	params := msg["params"].(map[string]interface{})
	assert.Contains(suite.T(), params, "channel")
	assert.Contains(suite.T(), params, "data")
	assert.Equal(suite.T(), suite.service.getMerchantCentrifugoChannel(rsp.Id), params["channel"])

	data := params["data"].(map[string]interface{})
	assert.Equal(suite.T(), merchantAgreementReadyToSignMessage, data)

	merchant1, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant1)
	assert.Equal(suite.T(), req1.S3AgreementName, merchant1.S3AgreementName)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantS3Agreement_MerchantNotFound_Error() {
	req1 := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId:      primitive.NewObjectID().Hex(),
		S3AgreementName: "agreement_" + primitive.NewObjectID().Hex() + ".pdf",
	}
	rsp1 := &billingpb.ChangeMerchantDataResponse{}
	err := suite.service.SetMerchantS3Agreement(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_UserNotifications_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, billingpb.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Status)

	req1 := &billingpb.NotificationRequest{
		MerchantId: rsp.Id,
		UserId:     primitive.NewObjectID().Hex(),
		Title:      "some title",
		Message:    "some message",
	}
	cnr := &billingpb.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, billingpb.ResponseStatusOk)
	rsp1 := cnr.Item
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 1"
	req1.Message = "some message 1"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, billingpb.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 2"
	req1.Message = "some message 2"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, billingpb.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 3"
	req1.Message = "some message 3"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, billingpb.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.False(suite.T(), rsp1.IsSystem)

	req2 := &billingpb.ListingNotificationRequest{MerchantId: rsp.Id, IsSystem: 1}
	rsp2 := &billingpb.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 4, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, 4)

	for _, v := range rsp2.Items {
		assert.Nil(suite.T(), v.Statuses)
	}

	req2.IsSystem = 0
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 4, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, 4)

	req2.IsSystem = 2
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, rsp2.Count)
	assert.Empty(suite.T(), rsp2.Items)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantOnboardingCompleteData_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "merchant1",
			AlternativeName:    "merchant1",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	req1 := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId: rsp.Item.Id,
	}
	rsp1 := &billingpb.GetMerchantOnboardingCompleteDataResponse{}
	err = suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)

	assert.True(suite.T(), rsp1.Item.Steps.Company)
	assert.True(suite.T(), rsp1.Item.Steps.Banking)
	assert.False(suite.T(), rsp1.Item.Steps.Contacts)
	assert.False(suite.T(), rsp1.Item.Steps.Tariff)
	assert.Equal(suite.T(), int32(2), rsp1.Item.CompleteStepsCount)
	assert.Equal(suite.T(), "draft", rsp1.Item.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantOnboardingCompleteData_FullyCompleteAndLive_Ok() {
	req0 := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
	}
	rsp0 := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req0, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	assert.Empty(suite.T(), rsp0.Message)

	req := &billingpb.OnboardingRequest{
		Id: rsp0.Item.Id,
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "merchant1",
			AlternativeName:    "merchant1",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	req2 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp0.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp2 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = billingpb.MerchantStatusAgreementSigned
	err = suite.service.merchantRepository.Update(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	req1 := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId: rsp.Item.Id,
	}
	rsp1 := &billingpb.GetMerchantOnboardingCompleteDataResponse{}
	err = suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)

	assert.True(suite.T(), rsp1.Item.Steps.Company)
	assert.True(suite.T(), rsp1.Item.Steps.Banking)
	assert.True(suite.T(), rsp1.Item.Steps.Contacts)
	assert.True(suite.T(), rsp1.Item.Steps.Tariff)
	assert.Equal(suite.T(), int32(4), rsp1.Item.CompleteStepsCount)
	assert.Equal(suite.T(), "life", rsp1.Item.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantOnboardingCompleteData_MerchantNotFound_Error() {
	req := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.GetMerchantOnboardingCompleteDataResponse{}
	err := suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_GetMerchantAgreementSignature_Error() {
	req0 := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
	}
	rsp0 := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req0, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	assert.Empty(suite.T(), rsp0.Message)

	rs := &reportingMocks.ReporterService{}
	rs.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil, errors.New(mocks.SomeError))
	suite.service.reporterService = rs

	req := &billingpb.OnboardingRequest{
		Id: rsp0.Item.Id,
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "merchant1",
			AlternativeName:    "merchant1",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req2 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp0.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp2 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	req3 := &billingpb.SetMerchantOperatingCompanyRequest{
		MerchantId:         req2.MerchantId,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	rsp3 := &billingpb.SetMerchantOperatingCompanyResponse{}
	err = suite.service.SetMerchantOperatingCompany(context.TODO(), req3, rsp3)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp3.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp3.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_Upsert_Error() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	rep := &mocks.MerchantRepositoryInterface{}
	rep.On("GetByUserId", mock2.Anything, mock2.Anything).Return(nil, mongo.ErrNoDocuments)
	rep.On("Upsert", mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	suite.service.merchantRepository = rep

	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_GetMerchantAgreementSignature_ResultError() {
	req0 := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
	}
	rsp0 := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req0, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, billingpb.ResponseStatusOk)
	assert.Empty(suite.T(), rsp0.Message)

	rs := &reportingMocks.ReporterService{}
	rs.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).
		Return(
			&reporterpb.CreateFileResponse{
				Status:  billingpb.ResponseStatusSystemError,
				Message: &reporterpb.ResponseErrorMessage{Message: mocks.SomeError},
			}, nil)
	suite.service.reporterService = rs

	req := &billingpb.OnboardingRequest{
		Id: rsp0.Item.Id,
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "merchant1",
			AlternativeName:    "merchant1",
			Country:            "RU",
			Zip:                "190000",
			City:               "St.Petersburg",
			Website:            "http://localhost",
			State:              "RU",
			Address:            "address",
			AddressAdditional:  "additional address",
			RegistrationNumber: "0000000000000000001",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp0.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	req3 := &billingpb.SetMerchantOperatingCompanyRequest{
		MerchantId:         req1.MerchantId,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	rsp3 := &billingpb.SetMerchantOperatingCompanyResponse{}
	err = suite.service.SetMerchantOperatingCompany(context.TODO(), req3, rsp3)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp3.Status)
	assert.Equal(suite.T(), mocks.SomeError, rsp3.Message.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantTariffRates_Ok() {
	req := &billingpb.GetMerchantTariffRatesRequest{
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.GetMerchantTariffRatesResponse{}
	err := suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotEmpty(suite.T(), rsp.Items)
	assert.NotEmpty(suite.T(), rsp.Items.Payment)
	assert.Len(suite.T(), rsp.Items.Payment, 2)
	assert.Equal(suite.T(), rsp.Items.Payment[0], suite.cisTariff[0])
	assert.Equal(suite.T(), rsp.Items.Payment[1], suite.cisTariff[1])
	assert.NotEmpty(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Payout)

	err = suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotEmpty(suite.T(), rsp.Items)
	assert.NotEmpty(suite.T(), rsp.Items.Payment)
	assert.Len(suite.T(), rsp.Items.Payment, 2)
	assert.Equal(suite.T(), rsp.Items.Payment[0], suite.cisTariff[0])
	assert.Equal(suite.T(), rsp.Items.Payment[1], suite.cisTariff[1])
	assert.NotEmpty(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Payout)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantTariffRates_WithoutRange_Ok() {
	req := &billingpb.GetMerchantTariffRatesRequest{
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.GetMerchantTariffRatesResponse{}
	err := suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Items)
	assert.NotEmpty(suite.T(), rsp.Items.Payment)
	assert.Len(suite.T(), rsp.Items.Payment, 2)
	assert.Equal(suite.T(), rsp.Items.Payment[0], suite.cisTariff[0])
	assert.Equal(suite.T(), rsp.Items.Payment[1], suite.cisTariff[1])
	assert.NotNil(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Payout)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantTariffRates_RepositoryError() {
	mtf := &mocks.MerchantTariffRatesInterface{}
	mtf.On("GetBy", mock2.Anything, mock2.Anything).Return(nil, merchantErrorUnknown)
	suite.service.merchantTariffRates = mtf

	req := &billingpb.GetMerchantTariffRatesRequest{HomeRegion: "russia_and_cis"}
	rsp := &billingpb.GetMerchantTariffRatesResponse{}
	err := suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
	assert.Nil(suite.T(), rsp.Items)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_Ok() {
	req0 := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp0 := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req0, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp0.Status)
	assert.NotNil(suite.T(), rsp0.Item)
	assert.NotNil(suite.T(), rsp0.Item.Banking)
	assert.Equal(suite.T(), req0.Banking.Currency, rsp0.Item.Banking.Currency)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.NotNil(suite.T(), merchant.Banking)
	assert.NotZero(suite.T(), merchant.Banking.Currency)

	paymentCosts, err := suite.service.paymentChannelCostMerchant.GetAllForMerchant(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), paymentCosts.Items, 0)

	moneyBackCosts, err := suite.service.moneyBackCostMerchantRepository.GetAllForMerchant(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), moneyBackCosts.Items, 0)

	req := &billingpb.GetMerchantTariffRatesRequest{
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.GetMerchantTariffRatesResponse{}
	err = suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Items)

	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp0.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	paymentCosts, err = suite.service.paymentChannelCostMerchant.GetAllForMerchant(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), paymentCosts.Items)
	assert.Len(suite.T(), paymentCosts.Items, 3)

	moneyBackCosts, err = suite.service.moneyBackCostMerchantRepository.GetAllForMerchant(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), moneyBackCosts.Items)
	assert.Len(suite.T(), moneyBackCosts.Items, 15)

	merchant, err = suite.service.merchantRepository.GetById(context.TODO(), rsp0.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.NotNil(suite.T(), merchant.Banking)
	assert.NotNil(suite.T(), merchant.Tariff)
	assert.NotNil(suite.T(), merchant.Tariff.Payment)
	assert.NotEmpty(suite.T(), merchant.Tariff.Payment)
	assert.NotNil(suite.T(), merchant.Tariff.Payout)
	assert.NotZero(suite.T(), merchant.Tariff.Payout.MethodFixedFee)
	assert.NotZero(suite.T(), merchant.Tariff.Payout.MethodFixedFeeCurrency)
	assert.Equal(suite.T(), req.HomeRegion, merchant.Tariff.HomeRegion)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_MerchantNotFound_Error() {
	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             primitive.NewObjectID().Hex(),
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_GetBy_Error() {
	mtf := &mocks.MerchantTariffRatesInterface{}
	mtf.On("GetBy", mock2.Anything, mock2.Anything).Return(nil, errors.New(mocks.SomeError))
	suite.service.merchantTariffRates = mtf

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_InsertPaymentCosts_Error() {
	ci := &mocks.CacheInterface{}
	ci.On("Get", mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	ci.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil)
	ci.On("Delete", mock2.Anything).Return(errors.New(mocks.SomeError))
	suite.service.cacher = ci

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_InsertMoneyBackCosts_Error() {
	ci := &mocks.CacheInterface{}
	ci.On("Get", mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	ci.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	ci.On("Delete", mock2.Anything).Return(errors.New(mocks.SomeError))
	suite.service.cacher = ci

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_ChangeTariffNotAllowed_Error() {
	suite.merchant.AgreementSignatureData = &billingpb.MerchantAgreementSignatureData{
		DetailsUrl:          "http://localhost",
		FilesUrl:            "http://localhost",
		SignatureRequestId:  primitive.NewObjectID().Hex(),
		MerchantSignatureId: primitive.NewObjectID().Hex(),
		PsSignatureId:       primitive.NewObjectID().Hex(),
	}
	err := suite.service.merchantRepository.Update(context.TODO(), suite.merchant)
	assert.NoError(suite.T(), err)

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantErrorChangeNotAllowed, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_MerchantUpdate_Error() {
	ci := &mocks.CacheInterface{}
	ci.On("Get", mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	ci.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil)
	ci.On("Delete", mock2.Anything).Return(errors.New(mocks.SomeError))
	suite.service.cacher = ci

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantOperatingCompany_GetMerchantAgreementSignature_Error() {
	rs := &reportingMocks.ReporterService{}
	rs.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).
		Return(
			&reporterpb.CreateFileResponse{
				Status:  billingpb.ResponseStatusBadData,
				Message: &reporterpb.ResponseErrorMessage{Message: mocks.SomeError},
			},
			nil,
		)
	suite.service.reporterService = rs

	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req3 := &billingpb.SetMerchantOperatingCompanyRequest{
		MerchantId:         suite.merchant.Id,
		OperatingCompanyId: suite.operatingCompany.Id,
	}
	rsp3 := &billingpb.SetMerchantOperatingCompanyResponse{}
	err = suite.service.SetMerchantOperatingCompany(context.TODO(), req3, rsp3)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp3.Status)
	assert.Equal(suite.T(), mocks.SomeError, rsp3.Message.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_SetStatusToPending() {
	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.MerchantStatusPending, merchant.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantTariffRates_MerchantHasTariff_Error() {
	req := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             suite.merchant.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.CheckProjectRequestSignatureResponse{}
	err := suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	err = suite.service.SetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantErrorOnboardingTariffAlreadyExist, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_NewMerchant_WithBeforeCreatedUserProfile_Ok() {
	req := &billingpb.UserProfile{
		UserId: primitive.NewObjectID().Hex(),
		Email: &billingpb.UserProfileEmail{
			Email: "test@unit.test",
		},
		Personal: &billingpb.UserProfilePersonal{
			FirstName: "Unit test",
			LastName:  "Unit Test",
			Position:  "test",
		},
		Help: &billingpb.UserProfileHelp{
			ProductPromotionAndDevelopment: false,
			ReleasedGamePromotion:          true,
			InternationalSales:             true,
			Other:                          false,
		},
		LastStep: "step2",
	}
	rsp := &billingpb.GetUserProfileResponse{}
	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	rsp.Item.Email.Confirmed = true
	rsp.Item.Email.ConfirmedAt = ptypes.TimestampNow()

	err = suite.service.userProfileRepository.Update(context.TODO(), rsp.Item)
	assert.NoError(suite.T(), err)

	req1 := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    rsp.Item.UserId,
			Email: rsp.Item.Email.Email,
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp1 := &billingpb.ChangeMerchantResponse{}
	err = suite.service.ChangeMerchant(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.Len(suite.T(), rsp1.Item.Id, 24)
	assert.NotZero(suite.T(), rsp.Item.CentrifugoToken)

	assert.NotNil(suite.T(), rsp1.Item.User)
	assert.Equal(suite.T(), rsp1.Item.User.Email, rsp.Item.Email.Email)
	assert.Equal(suite.T(), rsp1.Item.User.FirstName, rsp.Item.Personal.FirstName)
	assert.Equal(suite.T(), rsp1.Item.User.LastName, rsp.Item.Personal.LastName)
	assert.NotZero(suite.T(), rsp1.Item.User.ProfileId, rsp.Item.Id)
	assert.Equal(suite.T(), rsp1.Item.User.RegistrationDate.Seconds, req.Email.ConfirmedAt.Seconds)
	assert.NotNil(suite.T(), rsp1.Item.Banking)
	assert.NotZero(suite.T(), rsp1.Item.Banking.Currency)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_QuickSearchQuery_UserFirstNameLastName_Ok() {
	lastName := "LastName"

	req := &billingpb.UserProfile{
		UserId: primitive.NewObjectID().Hex(),
		Email: &billingpb.UserProfileEmail{
			Email: "test@unit.test",
		},
		Personal: &billingpb.UserProfilePersonal{
			FirstName: "FirstName",
			LastName:  lastName,
			Position:  "test",
		},
		Help: &billingpb.UserProfileHelp{
			ProductPromotionAndDevelopment: false,
			ReleasedGamePromotion:          true,
			InternationalSales:             true,
			Other:                          false,
		},
		LastStep: "step2",
	}
	rsp := &billingpb.GetUserProfileResponse{}

	for i := 0; i < 5; i++ {
		if i > 0 {
			req.UserId = primitive.NewObjectID().Hex()
			req.Personal.LastName = lastName + "_" + strconv.Itoa(i)
		}

		err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
		assert.NoError(suite.T(), err)
		assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

		req1 := &billingpb.OnboardingRequest{
			User: &billingpb.MerchantUser{
				Id:    rsp.Item.UserId,
				Email: rsp.Item.Email.Email,
			},
			Company: &billingpb.MerchantCompanyInfo{
				Name:    "merchant1",
				Country: "RU",
				Zip:     "190000",
				City:    "St.Petersburg",
			},
		}
		rsp1 := &billingpb.ChangeMerchantResponse{}
		err = suite.service.ChangeMerchant(context.TODO(), req1, rsp1)
		assert.Nil(suite.T(), err)
		assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	}

	req2 := &billingpb.MerchantListingRequest{QuickSearch: "first"}
	rsp2 := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 5, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	req2.QuickSearch = "name_1"
	err = suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 1, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_QuickSearchQuery_UserRegistrationDate_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
	}

	for i := 0; i < 10; i++ {
		req.User.Id = primitive.NewObjectID().Hex()
		req.User.Email = "test_" + strconv.Itoa(i) + "@unit.test"
		rsp := &billingpb.ChangeMerchantResponse{}
		err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
		assert.Nil(suite.T(), err)
		assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

		if i == 2 || i == 5 || i == 7 {
			rsp.Item.User.RegistrationDate, _ = ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))
		} else if i == 3 || i == 6 || i == 9 {
			rsp.Item.User.RegistrationDate, _ = ptypes.TimestampProto(time.Now().AddDate(0, 0, -5))
		} else {
			rsp.Item.User.RegistrationDate, _ = ptypes.TimestampProto(time.Now())
		}

		err = suite.service.merchantRepository.Upsert(context.TODO(), rsp.Item)
	}

	req2 := &billingpb.MerchantListingRequest{RegistrationDateFrom: time.Now().Add(-49 * time.Hour).Unix()}
	rsp2 := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 7, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	req2.RegistrationDateTo = time.Now().Add(-23 * time.Hour).Unix()
	err = suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	req2 = &billingpb.MerchantListingRequest{RegistrationDateTo: time.Now().Add(-48 * time.Hour).Unix()}
	err = suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 6, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_QuickSearchQuery_ReceivedDateFrom_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
	}

	for i := 0; i < 10; i++ {
		req.User.Id = primitive.NewObjectID().Hex()
		req.User.Email = "test_" + strconv.Itoa(i) + "@unit.test"
		rsp := &billingpb.ChangeMerchantResponse{}
		err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
		assert.Nil(suite.T(), err)
		assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

		if i == 2 || i == 5 || i == 7 {
			rsp.Item.ReceivedDate, _ = ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))
		} else if i == 3 || i == 6 || i == 9 {
			rsp.Item.ReceivedDate, _ = ptypes.TimestampProto(time.Now().AddDate(0, 0, -5))
		} else {
			rsp.Item.ReceivedDate, _ = ptypes.TimestampProto(time.Now())
		}

		err = suite.service.merchantRepository.Upsert(context.TODO(), rsp.Item)
	}

	req2 := &billingpb.MerchantListingRequest{ReceivedDateFrom: time.Now().Add(-49 * time.Hour).Unix()}
	rsp2 := &billingpb.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 7, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	req2.ReceivedDateTo = time.Now().Add(-23 * time.Hour).Unix()
	err = suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 3, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	req2 = &billingpb.MerchantListingRequest{ReceivedDateTo: time.Now().Add(-48 * time.Hour).Unix()}
	err = suite.service.ListMerchants(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.EqualValues(suite.T(), 6, rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_MerchantNotFound() {
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: primitive.NewObjectID().Hex(),
		Status:     billingpb.MerchantStatusRejected,
	}
	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err := suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_SetRejectedStatus_Error() {
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: suite.merchant.Id,
		Status:     billingpb.MerchantStatusRejected,
	}
	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err := suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantStatusChangeNotPossible, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_SetDeletedStatus_Error() {
	suite.merchant.Status = billingpb.MerchantStatusAgreementSigned
	err := suite.service.merchantRepository.Update(context.TODO(), suite.merchant)
	assert.NoError(suite.T(), err)
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: suite.merchant.Id,
		Status:     billingpb.MerchantStatusDeleted,
	}
	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantStatusChangeNotPossible, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_SetFromDraftToDeletedStatus_Ok() {
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: suite.merchant.Id,
		Status:     billingpb.MerchantStatusDeleted,
	}
	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err := suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AddNotification_Error() {
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: suite.merchantAgreement.Id,
		Status:     billingpb.MerchantStatusRejected,
	}

	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock2.Anything, mock2.Anything).Return("token")
	centrifugoMock.On("Publish", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("some error"))
	suite.service.centrifugoDashboard = centrifugoMock

	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err := suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_UpdateMerchant_Error() {
	req := &billingpb.MerchantChangeStatusRequest{
		MerchantId: suite.merchantAgreement.Id,
		Status:     billingpb.MerchantStatusRejected,
	}

	suite.merchant.Status = billingpb.MerchantStatusAgreementSigning
	merchantMock := &mocks.MerchantRepositoryInterface{}
	merchantMock.On("GetById", mock2.Anything, mock2.Anything).Return(suite.merchant, nil)
	merchantMock.On("Update", mock2.Anything, mock2.Anything).Return(errors.New("some error"))
	suite.service.merchantRepository = merchantMock

	rsp := &billingpb.ChangeMerchantStatusResponse{}
	err := suite.service.ChangeMerchantStatus(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_MessageNotFound_Error() {
	suite.merchant.Status = 999
	err := suite.service.merchantRepository.Update(context.TODO(), suite.merchant)
	assert.NoError(suite.T(), err)

	req := &billingpb.ChangeMerchantDataRequest{
		MerchantId: suite.merchant.Id,
	}
	rsp := &billingpb.ChangeMerchantDataResponse{}
	err = suite.service.ChangeMerchantData(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantNotificationSettingNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_AddNotification_Error() {
	req := &billingpb.ChangeMerchantDataRequest{
		MerchantId:           suite.merchant.Id,
		HasMerchantSignature: true,
	}
	rsp := &billingpb.ChangeMerchantDataResponse{}

	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock2.Anything, mock2.Anything).Return("token")
	centrifugoMock.On("Publish", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("some error"))
	suite.service.centrifugoDashboard = centrifugoMock

	err := suite.service.ChangeMerchantData(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_UpdateMerchant_Error() {
	req := &billingpb.ChangeMerchantDataRequest{
		MerchantId:           suite.merchant.Id,
		HasMerchantSignature: true,
	}
	rsp := &billingpb.ChangeMerchantDataResponse{}

	merchantMock := &mocks.MerchantRepositoryInterface{}
	merchantMock.On("GetById", mock2.Anything, mock2.Anything).Return(suite.merchant, nil)
	merchantMock.On("Update", mock2.Anything, mock2.Anything).Return(errors.New("some error"))
	suite.service.merchantRepository = merchantMock

	err := suite.service.ChangeMerchantData(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantManualPayouts_Ok() {
	merchant1, err := suite.service.merchantRepository.GetById(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)
	assert.False(suite.T(), merchant1.ManualPayoutsEnabled)

	req1 := &billingpb.ChangeMerchantManualPayoutsRequest{
		MerchantId:           suite.merchant.Id,
		ManualPayoutsEnabled: true,
	}
	rsp1 := &billingpb.ChangeMerchantManualPayoutsResponse{}
	err = suite.service.ChangeMerchantManualPayouts(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.True(suite.T(), rsp1.Item.ManualPayoutsEnabled)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantManualPayouts_MerchantNotFound_Error() {
	req1 := &billingpb.ChangeMerchantManualPayoutsRequest{
		MerchantId:           primitive.NewObjectID().Hex(),
		ManualPayoutsEnabled: true,
	}
	rsp1 := &billingpb.ChangeMerchantManualPayoutsResponse{}
	err := suite.service.ChangeMerchantManualPayouts(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantS3Agreement_AgreementReadyToSign_CentrifigoError() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Item.Status)
	assert.Empty(suite.T(), rsp.Item.S3AgreementName)

	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock2.Anything, mock2.Anything).Return("token")
	centrifugoMock.On("Publish", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New("some error"))
	suite.service.centrifugoDashboard = centrifugoMock

	ocRep := &mocks.OperatingCompanyInterface{}
	ocRep.On("GetById", mock2.Anything, mock2.Anything).Return(&billingpb.OperatingCompany{SignatoryName: "name", Email: "email"}, nil)
	suite.service.operatingCompany = ocRep

	req1 := &billingpb.SetMerchantS3AgreementRequest{
		MerchantId:      rsp.Item.Id,
		S3AgreementName: "agreement_" + rsp.Item.Id + ".pdf",
	}
	rsp1 := &billingpb.ChangeMerchantDataResponse{}
	err = suite.service.SetMerchantS3Agreement(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GenerateMerchantAgreement_CheckFullAddress_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:              "merchant1",
			Country:           "RU",
			Zip:               "190000",
			City:              "St.Petersburg",
			Address:           "address",
			AddressAdditional: "address_additional",
			State:             "SPE",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Item.Status)

	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	createFileMockFn := func(
		ctx context.Context,
		in *reporterpb.ReportFile,
		opts ...client.CallOption,
	) *reporterpb.CreateFileResponse {
		params := make(map[string]interface{})
		err := json.Unmarshal(in.Params, &params)

		if err != nil {
			return nil
		}

		zap.L().Info("message", zap.Any("address", params[reporterpb.RequestParameterAgreementAddress]))
		return &reporterpb.CreateFileResponse{Status: billingpb.ResponseStatusOk}
	}

	zap.ReplaceGlobals(suite.logObserver)
	reporterMock := &reportingMocks.ReporterService{}
	reporterMock.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).Return(createFileMockFn, nil)
	suite.service.reporterService = reporterMock

	ocMock := &mocks.OperatingCompanyInterface{}
	ocMock.
		On("GetById", mock2.Anything, mock2.Anything).
		Return(&billingpb.OperatingCompany{
			Name:               "name",
			Address:            "address",
			RegistrationNumber: "number",
			SignatoryName:      "sig name",
			SignatoryPosition:  "sig position",
		}, nil)
	suite.service.operatingCompany = ocMock

	err = suite.service.generateMerchantAgreement(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	messages := suite.zapRecorder.All()
	assert.Equal(suite.T(), zapcore.InfoLevel, messages[0].Level)
	assert.Contains(suite.T(), messages[0].Context[0].String, "address, address_additional, SPE, St.Petersburg, RU, 190000")
}

func (suite *OnboardingTestSuite) TestOnboarding_GenerateMerchantAgreement_CheckWithoutStateAddress_Ok() {
	req := &billingpb.OnboardingRequest{
		User: &billingpb.MerchantUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:              "merchant1",
			Country:           "RU",
			Zip:               "190000",
			City:              "St.Petersburg",
			Address:           "address",
			AddressAdditional: "address_additional",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &billingpb.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), billingpb.MerchantStatusDraft, rsp.Item.Status)

	req1 := &billingpb.SetMerchantTariffRatesRequest{
		MerchantId:             rsp.Item.Id,
		HomeRegion:             "russia_and_cis",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp1 := &billingpb.CheckProjectRequestSignatureResponse{}
	err = suite.service.SetMerchantTariffRates(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant, err := suite.service.merchantRepository.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	createFileMockFn := func(
		ctx context.Context,
		in *reporterpb.ReportFile,
		opts ...client.CallOption,
	) *reporterpb.CreateFileResponse {
		params := make(map[string]interface{})
		err := json.Unmarshal(in.Params, &params)

		if err != nil {
			return nil
		}

		zap.L().Info("message", zap.Any("address", params[reporterpb.RequestParameterAgreementAddress]))
		return &reporterpb.CreateFileResponse{Status: billingpb.ResponseStatusOk}
	}

	zap.ReplaceGlobals(suite.logObserver)
	reporterMock := &reportingMocks.ReporterService{}
	reporterMock.On("CreateFile", mock2.Anything, mock2.Anything, mock2.Anything).Return(createFileMockFn, nil)
	suite.service.reporterService = reporterMock

	ocMock := &mocks.OperatingCompanyInterface{}
	ocMock.
		On("GetById", mock2.Anything, mock2.Anything).
		Return(&billingpb.OperatingCompany{
			Name:               "name",
			Address:            "address",
			RegistrationNumber: "number",
			SignatoryName:      "sig name",
			SignatoryPosition:  "sig position",
		}, nil)
	suite.service.operatingCompany = ocMock

	err = suite.service.generateMerchantAgreement(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	messages := suite.zapRecorder.All()
	assert.Equal(suite.T(), zapcore.InfoLevel, messages[0].Level)
	assert.Contains(suite.T(), messages[0].Context[0].String, "address, address_additional, St.Petersburg, RU, 190000")
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantTariffRates_WithPayerRegion_Ok() {
	req := &billingpb.GetMerchantTariffRatesRequest{
		HomeRegion:             "russia_and_cis",
		PayerRegion:            "europe",
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
	}
	rsp := &billingpb.GetMerchantTariffRatesResponse{}
	err := suite.service.GetMerchantTariffRates(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotEmpty(suite.T(), rsp.Items)
	assert.NotEmpty(suite.T(), rsp.Items.Payment)
	assert.Len(suite.T(), rsp.Items.Payment, 1)
	assert.Equal(suite.T(), rsp.Items.Payment[0], suite.cisTariff[2])
	assert.NotEmpty(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Chargeback)
	assert.NotNil(suite.T(), rsp.Items.Payout)
}
