package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"testing"
	"time"
)

type OnboardingTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	merchant          *billing.Merchant
	merchantAgreement *billing.Merchant
	merchant1         *billing.Merchant

	project *billing.Project

	pmBankCard *billing.PaymentMethod
	pmQiwi     *billing.PaymentMethod

	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs
}

func Test_Onboarding(t *testing.T) {
	suite.Run(t, new(OnboardingTestSuite))
}

func (suite *OnboardingTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	assert.NoError(suite.T(), err, "Config load failed")

	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

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

	ps := &billing.PaymentSystem{
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
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		Currencies:       []string{"RUB", "USD", "EUR"},
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billing.PaymentMethodParams{
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

	pmQiwi := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "QIWI",
		Group:            "QIWI",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		Currencies:       []string{"RUB", "USD", "EUR"},
		ExternalId:       "QIWI",
		TestSettings: map[string]*billing.PaymentMethodParams{
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

	merchant := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		User: &billing.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "Unit test",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
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
			Currency: "RUB",
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
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
	}

	date, err = ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchantAgreement := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		User: &billing.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test_agreement@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "Unit test status Agreement",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
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
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusAgreementRequested,
		HasMerchantSignature:      true,
		HasPspSignature:           true,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 10000,
		},
		IsSigned: true,
	}
	merchant1 := &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		User: &billing.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test_merchant1@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
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
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusDraft,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 100000,
		},
		IsSigned: false,
	}

	project := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "RUB",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       true,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
	}

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
		mock.NewDocumentSignerMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billing.PaymentMethod{pmBankCard, pmQiwi}
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
}

func (suite *OnboardingTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_NewMerchant_Ok() {
	var merchant *billing.Merchant

	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)
	assert.Equal(suite.T(), req.Company.Website, rsp.Company.Website)
	assert.Equal(suite.T(), req.Contacts.Authorized.Position, rsp.Contacts.Authorized.Position)
	assert.Equal(suite.T(), req.Banking.Name, rsp.Banking.Name)
	assert.True(suite.T(), rsp.Steps.Company)
	assert.True(suite.T(), rsp.Steps.Contacts)
	assert.True(suite.T(), rsp.Steps.Banking)
	assert.False(suite.T(), rsp.Steps.Tariff)

	err = suite.service.db.Collection(collectionMerchant).Find(bson.M{"_id": bson.ObjectIdHex(rsp.Id)}).One(&merchant)

	assert.NotNil(suite.T(), merchant)
	assert.Equal(suite.T(), rsp.Status, merchant.Status)
	assert.Equal(suite.T(), rsp.Contacts.Authorized.Position, merchant.Contacts.Authorized.Position)
	assert.Equal(suite.T(), rsp.Banking.Name, merchant.Banking.Name)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_UpdateMerchant_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "0987654321",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "0987654321",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "0987654321",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)
	assert.Equal(suite.T(), req.Company.Website, rsp.Company.Website)
	assert.Equal(suite.T(), req.Contacts.Authorized.Phone, rsp.Contacts.Authorized.Phone)
	assert.Equal(suite.T(), req.Banking.AccountNumber, rsp.Banking.AccountNumber)

	var merchant *billing.Merchant
	err = suite.service.db.Collection(collectionMerchant).Find(bson.M{"_id": bson.ObjectIdHex(rsp.Id)}).One(&merchant)

	assert.NotNil(suite.T(), merchant)
	assert.Equal(suite.T(), rsp.Status, merchant.Status)
	assert.Equal(suite.T(), rsp.Contacts.Authorized.Phone, merchant.Contacts.Authorized.Phone)
	assert.Equal(suite.T(), rsp.Banking.AccountNumber, merchant.Banking.AccountNumber)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_UpdateMerchantNotAllowed_Error() {
	req := &grpc.OnboardingRequest{
		Id: suite.merchantAgreement.Id,
		User: &billing.MerchantUser{
			Id:    bson.NewObjectId().Hex(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "0987654321",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "0987654321",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "0987654321",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusForbidden)
	assert.Equal(suite.T(), merchantErrorChangeNotAllowed, cmres.Message)
	assert.Nil(suite.T(), cmres.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_CreateMerchant_CountryNotFound_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "XX",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorCountryNotFound, cmres.Message)
	assert.Nil(suite.T(), cmres.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_CreateMerchant_CurrencyNotFound_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "USD",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	suite.service.supportedCurrencies = []string{}

	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorCurrencyNotFound, cmres.Message)
	assert.Nil(suite.T(), cmres.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_MerchantId_Ok() {
	req := &grpc.GetMerchantByRequest{
		MerchantId: suite.merchant.Id,
	}

	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Item.Id)
	assert.Equal(suite.T(), suite.merchant.Company.Website, rsp.Item.Company.Website)
	assert.Equal(suite.T(), suite.merchant.Company.Name, rsp.Item.Company.Name)
	assert.NotEmpty(suite.T(), rsp.Item.CentrifugoToken)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_UserId_Ok() {
	req := &grpc.GetMerchantByRequest{
		UserId: suite.merchant.User.Id,
	}

	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Item.Id)
	assert.Equal(suite.T(), suite.merchant.Company.Website, rsp.Item.Company.Website)
	assert.Equal(suite.T(), suite.merchant.Company.Name, rsp.Item.Company.Name)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantById_Error() {
	req := &grpc.GetMerchantByRequest{
		MerchantId: bson.NewObjectId().Hex(),
	}

	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantBy_IncorrectRequest_Error() {
	req := &grpc.GetMerchantByRequest{}
	rsp := &grpc.GetMerchantResponse{}
	err := suite.service.GetMerchantBy(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), merchantErrorBadData, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_EmptyQuery_Ok() {
	req := &grpc.MerchantListingRequest{}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_NameQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		Name: "test",
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(2), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_StatusesQuery_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)

	req.Company.Name = req.Company.Name + "_1"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusAgreementSigned
	err = suite.service.merchant.Update(merchant)

	req.Company.Name = req.Company.Name + "_2"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)

	merchant, err = suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusOnReview
	err = suite.service.merchant.Update(merchant)

	req.Company.Name = req.Company.Name + "_3"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)

	merchant, err = suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusAgreementSigned
	err = suite.service.merchant.Update(merchant)

	req.Company.Name = req.Company.Name + "_4"
	err = suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)

	merchant, err = suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusAgreementSigned
	err = suite.service.merchant.Update(merchant)

	req1 := &grpc.MerchantListingRequest{Statuses: []int32{pkg.MerchantStatusDraft}}
	rsp1 := &grpc.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp1.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp1.Items[0].Id)

	req1 = &grpc.MerchantListingRequest{Statuses: []int32{pkg.MerchantStatusOnReview}}
	rsp1 = &grpc.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp1.Count)

	req1 = &grpc.MerchantListingRequest{Statuses: []int32{pkg.MerchantStatusAgreementSigned}}
	rsp1 = &grpc.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp1.Count)

	req1 = &grpc.MerchantListingRequest{Statuses: []int32{pkg.MerchantStatusOnReview, pkg.MerchantStatusAgreementSigned}}
	rsp1 = &grpc.MerchantListingResponse{}
	err = suite.service.ListMerchants(context.TODO(), req1, rsp1)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(4), rsp1.Count)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_QuickSearchQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		QuickSearch: "test_agreement",
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateFromQuery_Ok() {
	date := time.Now().Add(time.Hour * -450)

	req := &grpc.MerchantListingRequest{
		LastPayoutDateFrom: date.Unix(),
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(2), rsp.Count)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateToQuery_Ok() {
	date := time.Now()

	req := &grpc.MerchantListingRequest{
		LastPayoutDateTo: date.Unix(),
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutDateFromToQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		LastPayoutDateFrom: time.Now().Add(time.Hour * -500).Unix(),
		LastPayoutDateTo:   time.Now().Add(time.Hour * -400).Unix(),
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_PayoutAmountQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		LastPayoutAmount: 999999,
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_IsAgreementFalseQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		IsSigned: 1,
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Equal(suite.T(), suite.merchant1.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_IsAgreementTrueQuery_Ok() {
	req := &grpc.MerchantListingRequest{
		IsSigned: 2,
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(2), rsp.Count)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Limit_Ok() {
	req := &grpc.MerchantListingRequest{
		Limit: 2,
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchant.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Offset_Ok() {
	req := &grpc.MerchantListingRequest{
		Offset: 1,
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchantAgreement.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_Sort_Ok() {
	req := &grpc.MerchantListingRequest{
		Limit: 2,
		Sort:  []string{"-_id"},
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp.Count)
	assert.Len(suite.T(), rsp.Items, 2)
	assert.Equal(suite.T(), suite.merchant1.Id, rsp.Items[0].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchants_EmptyResult_Ok() {
	req := &grpc.MerchantListingRequest{
		Name: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.MerchantListingResponse{}

	err := suite.service.ListMerchants(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Item.Status)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Item.Id,
		Status:     pkg.MerchantStatusAgreementRequested,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), pkg.MerchantStatusAgreementRequested, rspChangeStatus.Item.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementRequested_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item

	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusOnReview
	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementRequested,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorAgreementRequested, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_OnReview_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementSigning
	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusOnReview,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorOnReview, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigning_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	req1 := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigning,
	}
	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), req1, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorSigning, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigned_IncorrectBeforeStatus_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusOnReview
	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigned,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorSigned, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigned_NotHaveTwoSignature_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementSigning
	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigned,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorSigned, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigned_NotHaveMerchantSignature_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementSigning
	rsp.HasPspSignature = true

	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigned,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorSigned, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigned_NotHavePspSignature_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementSigning
	rsp.HasMerchantSignature = true

	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigned,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rspChangeStatus.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorSigned, rspChangeStatus.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_AgreementSigned_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementSigning
	rsp.HasMerchantSignature = true
	rsp.HasPspSignature = true

	err = suite.service.merchant.Update(rsp)

	reqChangeStatus := &grpc.MerchantChangeStatusRequest{
		MerchantId: rsp.Id,
		Status:     pkg.MerchantStatusAgreementSigned,
	}

	rspChangeStatus := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), reqChangeStatus, rspChangeStatus)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), rspChangeStatus.Item.IsSigned)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_MerchantPaymentMethodsEmpty_Ok() {
	var merchant *billing.Merchant
	err := suite.service.db.Collection(collectionMerchant).FindId(bson.ObjectIdHex(suite.merchant1.Id)).One(&merchant)

	assert.NotNil(suite.T(), merchant)
	assert.Len(suite.T(), merchant.PaymentMethods, 0)

	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant1.Id,
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rsp.PaymentMethods) > 0)
	pm, err := suite.service.paymentMethod.GetAll()
	assert.Len(suite.T(), rsp.PaymentMethods, len(pm))

	for _, v := range rsp.PaymentMethods {
		assert.True(suite.T(), v.PaymentMethod.Id != "")
		assert.True(suite.T(), v.PaymentMethod.Name != "")
		assert.Equal(suite.T(), DefaultPaymentMethodFee, v.Commission.Fee)
		assert.NotNil(suite.T(), v.Commission.PerTransaction)
		assert.Equal(suite.T(), DefaultPaymentMethodPerTransactionFee, v.Commission.PerTransaction.Fee)
		assert.Equal(suite.T(), DefaultPaymentMethodCurrency, v.Commission.PerTransaction.Currency)
		assert.True(suite.T(), v.Integration.TerminalId == "")
		assert.True(suite.T(), v.Integration.TerminalPassword == "")
		assert.False(suite.T(), v.Integration.Integrated)
		assert.True(suite.T(), v.IsActive)
	}
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_ExistMerchantHasPaymentMethod_Ok() {
	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant.Id,
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rsp.PaymentMethods) > 0)
	pm, err := suite.service.paymentMethod.GetAll()
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
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item

	assert.Nil(suite.T(), rsp.PaymentMethods)

	reqListMerchantPaymentMethods := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: rsp.Id,
	}
	rspListMerchantPaymentMethods := &grpc.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), reqListMerchantPaymentMethods, rspListMerchantPaymentMethods)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), len(rspListMerchantPaymentMethods.PaymentMethods) > 0)
	pma, err := suite.service.paymentMethod.GetAll()
	assert.Len(suite.T(), rspListMerchantPaymentMethods.PaymentMethods, len(pma))

	for _, v := range rspListMerchantPaymentMethods.PaymentMethods {
		assert.True(suite.T(), v.PaymentMethod.Id != "")
		assert.True(suite.T(), v.PaymentMethod.Name != "")
		assert.Equal(suite.T(), DefaultPaymentMethodFee, v.Commission.Fee)
		assert.NotNil(suite.T(), v.Commission.PerTransaction)
		assert.Equal(suite.T(), DefaultPaymentMethodPerTransactionFee, v.Commission.PerTransaction.Fee)
		assert.Equal(suite.T(), DefaultPaymentMethodCurrency, v.Commission.PerTransaction.Currency)
		assert.True(suite.T(), v.Integration.TerminalId == "")
		assert.True(suite.T(), v.Integration.TerminalPassword == "")
		assert.False(suite.T(), v.Integration.Integrated)
		assert.True(suite.T(), v.IsActive)
	}

	reqMerchantPaymentMethodAdd := &grpc.MerchantPaymentMethodRequest{
		MerchantId: rsp.Id,
		PaymentMethod: &billing.MerchantPaymentMethodIdentification{
			Id:   suite.pmBankCard.Id,
			Name: suite.pmBankCard.Name,
		},
		Commission: &billing.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
				Fee:      100,
				Currency: "RUB",
			},
		},
		Integration: &billing.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
		UserId:   bson.NewObjectId().Hex(),
	}
	rspMerchantPaymentMethodAdd := &grpc.MerchantPaymentMethodResponse{}
	err = suite.service.ChangeMerchantPaymentMethod(context.TODO(), reqMerchantPaymentMethodAdd, rspMerchantPaymentMethodAdd)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rspMerchantPaymentMethodAdd.Status)
	assert.NotNil(suite.T(), rspMerchantPaymentMethodAdd.Item)
	assert.True(suite.T(), len(rspMerchantPaymentMethodAdd.Item.PaymentMethod.Id) > 0)

	pm, err := suite.service.merchant.GetPaymentMethod(rsp.Id, suite.pmBankCard.Id)
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

	var merchant *billing.Merchant
	err = suite.service.db.Collection(collectionMerchant).FindId(bson.ObjectIdHex(rsp.Id)).One(&merchant)
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
	reqMerchantPaymentMethodAdd := &grpc.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billing.MerchantPaymentMethodIdentification{
			Id:   suite.pmQiwi.Id,
			Name: suite.pmQiwi.Name,
		},
		Commission: &billing.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
				Fee:      100,
				Currency: "RUB",
			},
		},
		Integration: &billing.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
		UserId:   bson.NewObjectId().Hex(),
	}
	rspMerchantPaymentMethodAdd := &grpc.MerchantPaymentMethodResponse{}
	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), reqMerchantPaymentMethodAdd, rspMerchantPaymentMethodAdd)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rspMerchantPaymentMethodAdd.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_PaymentMethodsIsEmpty_Ok() {
	_, err := suite.service.db.Collection(collectionPaymentMethod).RemoveAll(bson.M{})

	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant1.Id,
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err = suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 0)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_Filter_Ok() {
	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId:        suite.merchant.Id,
		PaymentMethodName: "iwi",
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 1)

	pm := rsp.PaymentMethods[0]

	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, pm.PaymentMethod.Name)
	assert.Equal(suite.T(), DefaultPaymentMethodFee, pm.Commission.Fee)
	assert.Equal(suite.T(), DefaultPaymentMethodPerTransactionFee, pm.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), DefaultPaymentMethodCurrency, pm.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), "", pm.Integration.TerminalId)
	assert.Equal(suite.T(), "", pm.Integration.TerminalPassword)
	assert.False(suite.T(), pm.Integration.Integrated)
	assert.True(suite.T(), pm.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_Sort_Ok() {
	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: suite.merchant.Id,
		Sort:       []string{"-name"},
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp.PaymentMethods, 2)

	pm := rsp.PaymentMethods[0]

	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, pm.PaymentMethod.Name)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListMerchantPaymentMethods_MerchantNotFound_EmptyResult() {
	req := &grpc.ListMerchantPaymentMethodsRequest{
		MerchantId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ListingMerchantPaymentMethod{}
	err := suite.service.ListMerchantPaymentMethods(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Empty(suite.T(), rsp.PaymentMethods)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_ExistPaymentMethod_Ok() {
	req := &grpc.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: suite.pmBankCard.Id,
	}
	rsp := &grpc.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
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
	req := &grpc.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: suite.pmQiwi.Id,
	}
	rsp := &grpc.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.NotNil(suite.T(), rsp.Item.PaymentMethod)
	assert.NotNil(suite.T(), rsp.Item.Commission)
	assert.NotNil(suite.T(), rsp.Item.Commission.PerTransaction)
	assert.NotNil(suite.T(), rsp.Item.Integration)
	assert.True(suite.T(), rsp.Item.IsActive)

	assert.Equal(suite.T(), suite.pmQiwi.Id, rsp.Item.PaymentMethod.Id)
	assert.Equal(suite.T(), suite.pmQiwi.Name, rsp.Item.PaymentMethod.Name)
	assert.Equal(suite.T(), DefaultPaymentMethodFee, rsp.Item.Commission.Fee)
	assert.Equal(suite.T(), DefaultPaymentMethodPerTransactionFee, rsp.Item.Commission.PerTransaction.Fee)
	assert.Equal(suite.T(), DefaultPaymentMethodCurrency, rsp.Item.Commission.PerTransaction.Currency)
	assert.Equal(suite.T(), "", rsp.Item.Integration.TerminalId)
	assert.Equal(suite.T(), "", rsp.Item.Integration.TerminalPassword)
	assert.False(suite.T(), rsp.Item.Integration.Integrated)
	assert.True(suite.T(), rsp.Item.IsActive)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_PaymentMethodNotFound_Error() {
	req := &grpc.GetMerchantPaymentMethodRequest{
		MerchantId:      suite.merchant.Id,
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), orderErrorPaymentMethodNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantPaymentMethod_MerchantNotFound_Error() {
	req := &grpc.GetMerchantPaymentMethodRequest{
		MerchantId:      bson.NewObjectId().Hex(),
		PaymentMethodId: suite.pmBankCard.Id,
	}
	rsp := &grpc.GetMerchantPaymentMethodResponse{}
	err := suite.service.GetMerchantPaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantPaymentMethod_PaymentMethodNotFound_Error() {
	req := &grpc.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billing.MerchantPaymentMethodIdentification{
			Id:   bson.NewObjectId().Hex(),
			Name: "Unit test",
		},
		Commission: &billing.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
				Fee:      10,
				Currency: "RUB",
			},
		},
		Integration: &billing.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
	}
	rsp := &grpc.MerchantPaymentMethodResponse{}
	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), orderErrorPaymentMethodNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantPaymentMethod_CurrencyNotFound_Error() {
	req := &grpc.MerchantPaymentMethodRequest{
		MerchantId: suite.merchant.Id,
		PaymentMethod: &billing.MerchantPaymentMethodIdentification{
			Id:   suite.pmBankCard.Id,
			Name: suite.pmBankCard.Name,
		},
		Commission: &billing.MerchantPaymentMethodCommissions{
			Fee: 5,
			PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
				Fee:      10,
				Currency: "USD",
			},
		},
		Integration: &billing.MerchantPaymentMethodIntegration{
			TerminalId:       "1234567890",
			TerminalPassword: "0987654321",
			Integrated:       true,
		},
		IsActive: true,
	}
	rsp := &grpc.MerchantPaymentMethodResponse{}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	suite.service.supportedCurrencies = []string{}

	err := suite.service.ChangeMerchantPaymentMethod(context.TODO(), req, rsp)

	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), orderErrorCurrencyNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_Ok() {
	var notification *billing.Notification

	userId := bson.NewObjectId().Hex()

	query := bson.M{
		"merchant_id": bson.ObjectIdHex(suite.merchant.Id),
		"user_id":     bson.ObjectIdHex(userId),
	}
	err := suite.service.db.Collection(collectionNotification).Find(query).One(&notification)
	assert.Nil(suite.T(), notification)

	req := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	cnres := &grpc.CreateNotificationResponse{}

	err = suite.service.CreateNotification(context.TODO(), req, cnres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cnres.Status, pkg.ResponseStatusOk)
	rsp := cnres.Item
	assert.True(suite.T(), len(rsp.Id) > 0)
	assert.Equal(suite.T(), req.MerchantId, rsp.MerchantId)
	assert.Equal(suite.T(), req.UserId, rsp.UserId)
	assert.Equal(suite.T(), req.Title, rsp.Title)
	assert.Equal(suite.T(), req.Message, rsp.Message)

	err = suite.service.db.Collection(collectionNotification).Find(query).One(&notification)
	assert.NotNil(suite.T(), notification)
	assert.Equal(suite.T(), rsp.Id, notification.Id)
	assert.Equal(suite.T(), rsp.MerchantId, notification.MerchantId)
	assert.Equal(suite.T(), rsp.UserId, notification.UserId)
	assert.Equal(suite.T(), rsp.Title, notification.Title)
	assert.Equal(suite.T(), rsp.Message, notification.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_UserIdEmpty_Error() {
	req := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	rsp := &grpc.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), notificationErrorUserIdIncorrect, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_MessageEmpty_Error() {
	req := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title",
	}
	rsp := &grpc.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), notificationErrorMessageIsEmpty, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_CreateNotification_AddNotification_Error() {
	req := &grpc.NotificationRequest{
		MerchantId: "ffffffffffffffffffffffff",
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	rsp := &grpc.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetNotification_Ok() {
	req := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title",
		Message:    "Unit test message",
	}
	cmres := &grpc.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req, cmres)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item

	assert.True(suite.T(), len(rsp.Id) > 0)

	reqGetNotification := &grpc.GetNotificationRequest{
		MerchantId:     suite.merchant.Id,
		NotificationId: rsp.Id,
	}
	rspGetNotification := &billing.Notification{}
	err = suite.service.GetNotification(context.TODO(), reqGetNotification, rspGetNotification)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Id, rspGetNotification.Id)
	assert.Equal(suite.T(), rsp.MerchantId, rspGetNotification.MerchantId)
	assert.Equal(suite.T(), rsp.UserId, rspGetNotification.UserId)
	assert.Equal(suite.T(), rsp.Title, rspGetNotification.Title)
	assert.Equal(suite.T(), rsp.Message, rspGetNotification.Message)
	assert.NotNil(suite.T(), rspGetNotification.CreatedAt)
	assert.NotNil(suite.T(), rspGetNotification.UpdatedAt)
}

func (suite *OnboardingTestSuite) TestOnboarding_NotFound_Error() {
	reqGetNotification := &grpc.GetNotificationRequest{
		MerchantId:     bson.NewObjectId().Hex(),
		NotificationId: bson.NewObjectId().Hex(),
	}
	rspGetNotification := &billing.Notification{}
	err := suite.service.GetNotification(context.TODO(), reqGetNotification, rspGetNotification)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), notificationErrorNotFound, err)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_Merchant_Ok() {
	req1 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &grpc.CreateNotificationResponse{}

	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)

	req2 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp2 := &grpc.CreateNotificationResponse{}

	err = suite.service.CreateNotification(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp2.Item.Id) > 0)

	req3 := &grpc.ListingNotificationRequest{
		MerchantId: suite.merchant.Id,
		Limit:      10,
		Offset:     0,
	}
	rsp3 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req3, rsp3)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp3.Items, 2)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp3.Items[0].Id)
	assert.Equal(suite.T(), rsp2.Item.Id, rsp3.Items[1].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_Sort_Ok() {
	req := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp := &grpc.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req.Title = req.Title + "_1"
	err = suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req.Title = req.Title + "_2"
	err = suite.service.CreateNotification(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp.Item.Id) > 0)

	req1 := &grpc.ListingNotificationRequest{
		MerchantId: suite.merchant.Id,
		Sort:       []string{"-created_at"},
		Limit:      10,
		Offset:     0,
	}
	rsp1 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp1.Items, 3)
}

func (suite *OnboardingTestSuite) TestOnboarding_ListNotifications_User_Ok() {
	userId := bson.NewObjectId().Hex()

	req1 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &grpc.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)

	req2 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 2",
		Message:    "Unit test message 2",
	}

	rsp2 := &grpc.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req2, rsp2)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp2.Item.Id) > 0)

	req3 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     userId,
		Title:      "Unit test title 3",
		Message:    "Unit test message 3",
	}

	rsp3 := &grpc.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req3, rsp3)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp3.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp3.Item.Id) > 0)

	req4 := &grpc.ListingNotificationRequest{
		UserId: userId,
		Limit:  10,
		Offset: 0,
	}
	rsp4 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req4, rsp4)
	assert.Nil(suite.T(), err)
	assert.Len(suite.T(), rsp4.Items, 3)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp4.Items[0].Id)
	assert.Equal(suite.T(), rsp2.Item.Id, rsp4.Items[1].Id)
	assert.Equal(suite.T(), rsp3.Item.Id, rsp4.Items[2].Id)
}

func (suite *OnboardingTestSuite) TestOnboarding_MarkNotificationAsRead_Ok() {
	req1 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}
	rsp1 := &grpc.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)
	assert.False(suite.T(), rsp1.Item.IsRead)

	req2 := &grpc.GetNotificationRequest{
		MerchantId:     req1.MerchantId,
		NotificationId: rsp1.Item.Id,
	}
	rsp2 := &billing.Notification{}
	err = suite.service.MarkNotificationAsRead(context.TODO(), req2, rsp2)

	assert.Nil(suite.T(), err)
	assert.True(suite.T(), rsp2.IsRead)
	assert.Equal(suite.T(), rsp1.Item.Id, rsp2.Id)

	var notification *billing.Notification
	err = suite.service.db.Collection(collectionNotification).FindId(bson.ObjectIdHex(rsp1.Item.Id)).One(&notification)
	assert.NotNil(suite.T(), notification)

	assert.True(suite.T(), notification.IsRead)
}

func (suite *OnboardingTestSuite) TestOnboarding_MarkNotificationAsRead_NotFound_Error() {
	req1 := &grpc.NotificationRequest{
		MerchantId: suite.merchant.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "Unit test title 1",
		Message:    "Unit test message 1",
	}

	rsp1 := &grpc.CreateNotificationResponse{}
	err := suite.service.CreateNotification(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, pkg.ResponseStatusOk)
	assert.True(suite.T(), len(rsp1.Item.Id) > 0)
	assert.False(suite.T(), rsp1.Item.IsRead)

	req2 := &grpc.GetNotificationRequest{
		MerchantId:     bson.NewObjectId().Hex(),
		NotificationId: bson.NewObjectId().Hex(),
	}
	rsp2 := &billing.Notification{}
	err = suite.service.MarkNotificationAsRead(context.TODO(), req2, rsp2)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), notificationErrorNotFound, err)
	assert.False(suite.T(), rsp2.IsRead)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_Ok() {
	req := &grpc.OnboardingRequest{
		User: &billing.MerchantUser{
			Id:    bson.NewObjectId().Hex(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusAgreementSigning
	merchant.AgreementType = pkg.MerchantAgreementTypeESign
	err = suite.service.merchant.Update(merchant)

	req1 := &grpc.ChangeMerchantDataRequest{
		MerchantId:           merchant.Id,
		HasPspSignature:      true,
		HasMerchantSignature: true,
	}
	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)

	merchant1, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.True(suite.T(), merchant1.HasPspSignature)
	assert.True(suite.T(), merchant1.HasMerchantSignature)
	assert.Equal(suite.T(), pkg.MerchantStatusAgreementSigned, merchant1.Status)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_MerchantNotFound_Error() {
	req1 := &grpc.ChangeMerchantDataRequest{
		MerchantId:           bson.NewObjectId().Hex(),
		HasPspSignature:      true,
		HasMerchantSignature: true,
	}
	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err := suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantData_NotCorrectStatus_Error() {
	req := &grpc.OnboardingRequest{
		User: &billing.MerchantUser{
			Id:    bson.NewObjectId().Hex(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	rsp.Status = pkg.MerchantStatusAgreementRequested
	err = suite.service.merchant.Update(rsp)

	req1 := &grpc.ChangeMerchantDataRequest{
		MerchantId:      rsp.Id,
		HasPspSignature: true,
		AgreementType:   pkg.MerchantAgreementTypeESign,
	}
	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorAgreementTypeSelectNotAllow, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantS3Agreement_Ok() {
	req := &grpc.OnboardingRequest{
		User: &billing.MerchantUser{
			Id:    bson.NewObjectId().Hex(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)
	assert.Empty(suite.T(), rsp.S3AgreementName)

	req1 := &grpc.SetMerchantS3AgreementRequest{
		MerchantId:      rsp.Id,
		S3AgreementName: "agreement_" + rsp.Id + ".pdf",
	}
	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err = suite.service.SetMerchantS3Agreement(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.Equal(suite.T(), req1.S3AgreementName, rsp1.Item.S3AgreementName)

	merchant1, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant1)
	assert.Equal(suite.T(), req1.S3AgreementName, merchant1.S3AgreementName)
}

func (suite *OnboardingTestSuite) TestOnboarding_SetMerchantS3Agreement_MerchantNotFound_Error() {
	req1 := &grpc.SetMerchantS3AgreementRequest{
		MerchantId:      bson.NewObjectId().Hex(),
		S3AgreementName: "agreement_" + bson.NewObjectId().Hex() + ".pdf",
	}
	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err := suite.service.SetMerchantS3Agreement(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp1.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_SystemNotifications_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	req1 := &grpc.ChangeMerchantDataRequest{
		MerchantId:    rsp.Id,
		AgreementType: pkg.MerchantAgreementTypeESign,
	}

	rsp1 := &grpc.ChangeMerchantDataResponse{}
	err = suite.service.ChangeMerchantData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.Equal(suite.T(), pkg.MerchantStatusAgreementRequested, rsp1.Item.Status)

	req2 := &grpc.ListingNotificationRequest{MerchantId: rsp.Id}
	rsp2 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, 1)

	req3 := &grpc.ListingNotificationRequest{MerchantId: rsp.Id, IsSystem: 2}
	rsp3 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Count, rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, 1)
	assert.Equal(suite.T(), rsp2.Items[0].Id, rsp3.Items[0].Id)

	req4 := &grpc.MerchantChangeStatusRequest{MerchantId: rsp.Id, Status: pkg.MerchantStatusOnReview}
	rsp4 := &grpc.ChangeMerchantStatusResponse{}
	err = suite.service.ChangeMerchantStatus(context.TODO(), req4, rsp4)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp4.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), pkg.MerchantStatusOnReview, rsp4.Item.Status)

	req4.Status = pkg.MerchantStatusAgreementSigning
	err = suite.service.ChangeMerchantStatus(context.TODO(), req4, rsp4)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp4.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), pkg.MerchantStatusAgreementSigning, rsp4.Item.Status)

	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(3), rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, int(rsp2.Count))

	err = suite.service.ListNotifications(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp2.Count, rsp3.Count)
	assert.Len(suite.T(), rsp3.Items, int(rsp2.Count))
	assert.Equal(suite.T(), rsp2.Items[0].Id, rsp3.Items[0].Id)

	req5 := &grpc.ListingNotificationRequest{MerchantId: rsp.Id, IsSystem: 1}
	rsp5 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req5, rsp5)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp5.Count)
	assert.Len(suite.T(), rsp5.Items, 0)

	for _, v := range rsp3.Items {
		assert.NotNil(suite.T(), v.Statuses)
		assert.True(suite.T(), v.Statuses.To > v.Statuses.From)
		assert.Equal(suite.T(), pkg.SystemUserId, v.UserId)
		assert.True(suite.T(), v.IsSystem)
	}
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchantStatus_UserNotifications_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}

	cmres := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, cmres)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), cmres.Status, pkg.ResponseStatusOk)
	rsp := cmres.Item
	assert.Equal(suite.T(), pkg.MerchantStatusDraft, rsp.Status)

	req1 := &grpc.NotificationRequest{
		MerchantId: rsp.Id,
		UserId:     bson.NewObjectId().Hex(),
		Title:      "some title",
		Message:    "some message",
	}
	cnr := &grpc.CreateNotificationResponse{}
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, pkg.ResponseStatusOk)
	rsp1 := cnr.Item
	assert.True(suite.T(), bson.IsObjectIdHex(rsp1.Id))
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 1"
	req1.Message = "some message 1"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, pkg.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.True(suite.T(), bson.IsObjectIdHex(rsp1.Id))
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 2"
	req1.Message = "some message 2"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, pkg.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.True(suite.T(), bson.IsObjectIdHex(rsp1.Id))
	assert.False(suite.T(), rsp1.IsSystem)

	req1.Title = "some title 3"
	req1.Message = "some message 3"
	err = suite.service.CreateNotification(context.TODO(), req1, cnr)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cnr.Status, pkg.ResponseStatusOk)
	rsp1 = cnr.Item
	assert.True(suite.T(), bson.IsObjectIdHex(rsp1.Id))
	assert.False(suite.T(), rsp1.IsSystem)

	req2 := &grpc.ListingNotificationRequest{MerchantId: rsp.Id, IsSystem: 1}
	rsp2 := &grpc.Notifications{}
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(4), rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, 4)

	for _, v := range rsp2.Items {
		assert.Nil(suite.T(), v.Statuses)
	}

	req2.IsSystem = 0
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(4), rsp2.Count)
	assert.Len(suite.T(), rsp2.Items, 4)

	req2.IsSystem = 2
	err = suite.service.ListNotifications(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp2.Count)
	assert.Empty(suite.T(), rsp2.Items)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_Ok() {
	dsIsCalled := false
	mockResultFn := func(ctx context.Context, in *proto.CreateSignatureRequest, opts ...client.CallOption) *proto.CreateSignatureResponse {
		dsIsCalled = true
		return mock.CreateSignatureResponse
	}
	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).Return(mockResultFn, nil)
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(mock.GetSignatureUrlResponse, nil)
	suite.service.documentSigner = ds

	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.NotNil(suite.T(), merchant.AgreementSignatureData)
	assert.Equal(suite.T(), mock.CreateSignatureResponse.Item.DetailsUrl, merchant.AgreementSignatureData.DetailsUrl)
	assert.Equal(suite.T(), mock.CreateSignatureResponse.Item.FilesUrl, merchant.AgreementSignatureData.FilesUrl)
	assert.Equal(suite.T(), mock.CreateSignatureResponse.Item.SignatureRequestId, merchant.AgreementSignatureData.SignatureRequestId)
	assert.Equal(suite.T(), mock.CreateSignatureResponse.Item.MerchantSignatureId, merchant.AgreementSignatureData.MerchantSignatureId)
	assert.Equal(suite.T(), mock.CreateSignatureResponse.Item.PsSignatureId, merchant.AgreementSignatureData.PsSignatureId)
	assert.Nil(suite.T(), merchant.AgreementSignatureData.MerchantSignUrl)
	assert.Nil(suite.T(), merchant.AgreementSignatureData.PsSignUrl)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)

	assert.True(suite.T(), dsIsCalled)

	merchant, err = suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)
	assert.NotNil(suite.T(), merchant.AgreementSignatureData)
	assert.NotNil(suite.T(), merchant.AgreementSignatureData.MerchantSignUrl)
	assert.Equal(suite.T(), mock.GetSignatureUrlResponse.Item.SignUrl, merchant.AgreementSignatureData.MerchantSignUrl.SignUrl)
	assert.Equal(suite.T(), mock.GetSignatureUrlResponse.Item.ExpiresAt.Seconds, merchant.AgreementSignatureData.MerchantSignUrl.ExpiresAt.Seconds)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_MerchantNotFound_Error() {
	req := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetMerchantAgreementSignUrlResponse{}
	err := suite.service.GetMerchantAgreementSignUrl(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_AgreementAlreadySigned_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.HasPspSignature = true
	merchant.HasMerchantSignature = true
	err = suite.service.merchant.Update(merchant)
	assert.NoError(suite.T(), err)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorAlreadySigned, rsp1.Message)
	assert.Nil(suite.T(), rsp1.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_MerchantHasSignatureRequest_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.AgreementSignatureData.MerchantSignUrl = &billing.MerchantAgreementSignatureDataSignUrl{
		SignUrl:   mock.GetSignatureUrlResponse.Item.SignUrl,
		ExpiresAt: mock.GetSignatureUrlResponse.Item.ExpiresAt,
	}
	err = suite.service.merchant.Update(merchant)
	assert.NoError(suite.T(), err)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_DocumentSignerSystemError() {
	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).Return(mock.CreateSignatureResponse, nil)
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(nil, errors.New(mock.SomeError))
	suite.service.documentSigner = ds

	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	zap.ReplaceGlobals(suite.logObserver)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp1.Message)
	assert.Nil(suite.T(), rsp1.Item)

	messages := suite.zapRecorder.All()
	assert.Equal(suite.T(), pkg.ErrorGrpcServiceCallFailed, messages[0].Message)
	assert.Equal(suite.T(), zapcore.ErrorLevel, messages[0].Level)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_DocumentSignerResultError() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	ds := &mock.DocumentSignerService{}
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).
		Return(&proto.GetSignatureUrlResponse{Status: pkg.ResponseStatusBadData, Message: &proto.ResponseErrorMessage{Message: mock.SomeError}}, nil)
	suite.service.documentSigner = ds

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp1.Status)
	assert.Equal(suite.T(), mock.SomeError, rsp1.Message.Error())
	assert.Nil(suite.T(), rsp1.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_AgreementSign_UpdateError() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	cache := &mock.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheMerchantId, rsp.Item.Id), mock2.Anything, mock2.Anything).
		Return(errors.New(mock.SomeError))
	suite.service.cacher = cache
	zap.ReplaceGlobals(suite.logObserver)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp1.Message)
	assert.Nil(suite.T(), rsp1.Item)

	messages := suite.zapRecorder.All()
	assert.Equal(suite.T(), pkg.ErrorCacheQueryFailed, messages[0].Message)
	assert.Equal(suite.T(), zapcore.ErrorLevel, messages[0].Level)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantOnboardingCompleteData_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	req1 := &grpc.SetMerchantS3AgreementRequest{
		MerchantId: rsp.Item.Id,
	}
	rsp1 := &grpc.GetMerchantOnboardingCompleteDataResponse{}
	err = suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
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
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, pkg.ResponseStatusOk)
	assert.Empty(suite.T(), rsp.Message)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(rsp.Item.Id)})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	merchant.Status = pkg.MerchantStatusAgreementSigned
	err = suite.service.merchant.Update(merchant)
	assert.NoError(suite.T(), err)

	req1 := &grpc.SetMerchantS3AgreementRequest{
		MerchantId: rsp.Item.Id,
	}
	rsp1 := &grpc.GetMerchantOnboardingCompleteDataResponse{}
	err = suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
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
	req := &grpc.SetMerchantS3AgreementRequest{
		MerchantId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetMerchantOnboardingCompleteDataResponse{}
	err := suite.service.GetMerchantOnboardingCompleteData(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_GetMerchantAgreementSignature_Error() {
	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).Return(nil, errors.New(mock.SomeError))
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(mock.GetSignatureUrlResponse, nil)
	suite.service.documentSigner = ds

	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_Upsert_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}

	cache := &mock.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, req.Company.Country), mock2.Anything).Return(nil)
	cache.On("Set", mock2.Anything, mock2.Anything, mock2.Anything).
		Return(errors.New(mock.SomeError))
	suite.service.cacher = cache

	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), merchantErrorUnknown, rsp.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantAgreementSignUrl_AgreementSignatureDataIsNil_Error() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: signerTypeMerchant,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp1.Status)
	assert.Equal(suite.T(), merchantErrorOnboardingNotComplete, rsp1.Message)
	assert.Nil(suite.T(), rsp1.Item)
}

func (suite *OnboardingTestSuite) TestOnboarding_ChangeMerchant_GetMerchantAgreementSignature_ResultError() {
	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(
			&proto.CreateSignatureResponse{
				Status:  pkg.ResponseStatusBadData,
				Message: &proto.ResponseErrorMessage{Message: mock.SomeError},
			},
			nil,
		)
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(mock.GetSignatureUrlResponse, nil)
	suite.service.documentSigner = ds

	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), mock.SomeError, rsp.Message.Message)
}

func (suite *OnboardingTestSuite) TestOnboarding_GetMerchantAgreementSignUrl_PaysuperSign_Ok() {
	req := &grpc.OnboardingRequest{
		Company: &billing.MerchantCompanyInfo{
			Name:    "merchant1",
			Country: "RU",
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "1234567890",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "1234567890",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency:      "RUB",
			Name:          "Bank name",
			Address:       "Unknown",
			AccountNumber: "1234567890",
			Swift:         "TEST",
			Details:       "",
		},
		Tariff: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangeMerchantResponse{}
	err := suite.service.ChangeMerchant(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	req1 := &grpc.GetMerchantAgreementSignUrlRequest{
		MerchantId: rsp.Item.Id,
		SignerType: 1,
	}
	rsp1 := &grpc.GetMerchantAgreementSignUrlResponse{}
	err = suite.service.GetMerchantAgreementSignUrl(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)
}
