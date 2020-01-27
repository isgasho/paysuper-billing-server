package service

import (
	"context"
	"github.com/jinzhu/copier"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

var (
	merchantMock = &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
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
			Authorized: &billingpb.MerchantContactAuthorized{},
			Technical:  &billingpb.MerchantContactTechnical{},
		},
		Banking: &billingpb.MerchantBanking{
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusDraft,
		IsSigned:                  true,
		DontChargeVat:             false,
	}

	projectMock = &billingpb.Project{
		MerchantId:         merchantMock.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer:           billingpb.VatPayerSeller,
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
)

type ProjectCRUDTestSuite struct {
	suite.Suite
	service *Service
	cache   database.CacheInterface

	merchant *billingpb.Merchant
	project  *billingpb.Project
}

func Test_ProjectCRUD(t *testing.T) {
	suite.Run(t, new(ProjectCRUDTestSuite))
}

func (suite *ProjectCRUDTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")
	projectId := primitive.NewObjectID().Hex()

	ps1 := &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}
	pm1 := &billingpb.PaymentMethod{
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
		AccountRegexp:   "^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$",
		PaymentSystemId: ps1.Id,
	}

	ps2 := &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}
	pm2 := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Bitcoin",
		Group:            "BITCOIN_1",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "BITCOIN",
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			"RUB": {
				Currency:   "RUB",
				TerminalId: "16007",
			},
		},
		Type:            "crypto",
		IsActive:        true,
		PaymentSystemId: ps2.Id,
	}

	merchant := merchantMock

	project := &billingpb.Project{
		Id:                       projectId,
		CallbackCurrency:         "RUB",
		CallbackProtocol:         billingpb.ProjectCallbackProtocolEmpty,
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
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}

	products := []interface{}{
		&billingpb.Product{
			Object:          "product",
			Type:            "simple_product",
			Sku:             "ru_double_yeti",
			Name:            map[string]string{"en": initialName},
			DefaultCurrency: "USD",
			Enabled:         true,
			Description:     map[string]string{"en": "blah-blah-blah"},
			LongDescription: map[string]string{"en": "Super game steam keys"},
			Url:             "http://test.ru/dffdsfsfs",
			Images:          []string{"/home/image.jpg"},
			MerchantId:      merchant.Id,
			ProjectId:       project.Id,
			Metadata: map[string]string{
				"SomeKey": "SomeValue",
			},
			Prices: []*billingpb.ProductPrice{{Currency: "USD", Amount: 1005.00}},
		},
		&billingpb.Product{
			Object:          "product1",
			Type:            "simple_product",
			Sku:             "ru_double_yeti1",
			Name:            map[string]string{"en": initialName},
			DefaultCurrency: "USD",
			Enabled:         true,
			Description:     map[string]string{"en": "blah-blah-blah"},
			LongDescription: map[string]string{"en": "Super game steam keys"},
			Url:             "http://test.ru/dffdsfsfs",
			Images:          []string{"/home/image.jpg"},
			MerchantId:      merchant.Id,
			ProjectId:       project.Id,
			Metadata: map[string]string{
				"SomeKey": "SomeValue",
			},
			Prices: []*billingpb.ProductPrice{{Currency: "USD", Amount: 1005.00}},
		},
		&billingpb.Product{
			Object:          "product2",
			Type:            "simple_product",
			Sku:             "ru_double_yeti2",
			Name:            map[string]string{"en": initialName},
			DefaultCurrency: "USD",
			Enabled:         true,
			Description:     map[string]string{"en": "blah-blah-blah"},
			LongDescription: map[string]string{"en": "Super game steam keys"},
			Url:             "http://test.ru/dffdsfsfs",
			Images:          []string{"/home/image.jpg"},
			MerchantId:      merchant.Id,
			ProjectId:       project.Id,
			Metadata: map[string]string{
				"SomeKey": "SomeValue",
			},
			Prices: []*billingpb.ProductPrice{{Currency: "USD", Amount: 1005.00}},
		},
	}

	_, err = db.Collection(collectionProduct).InsertMany(context.TODO(), products)
	assert.NoError(suite.T(), err, "Insert product test data failed")
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
		&casbinMocks.CasbinService{},
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billingpb.PaymentMethod{pm1, pm2}
	if err := suite.service.paymentMethod.MultipleInsert(context.TODO(), pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	if err := suite.service.merchantRepository.Insert(context.TODO(), merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(context.TODO(), project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	suite.merchant = merchant
	suite.project = project
}

func (suite *ProjectCRUDTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerSeller,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	assert.NotEmpty(suite.T(), rsp.Item.Id)
	assert.Equal(suite.T(), req.MerchantId, rsp.Item.MerchantId)
	assert.Equal(suite.T(), req.Name, rsp.Item.Name)
	assert.Equal(suite.T(), req.CallbackCurrency, rsp.Item.CallbackCurrency)
	assert.Equal(suite.T(), req.CallbackProtocol, rsp.Item.CallbackProtocol)
	assert.Equal(suite.T(), req.LimitsCurrency, rsp.Item.LimitsCurrency)
	assert.Equal(suite.T(), req.MinPaymentAmount, rsp.Item.MinPaymentAmount)
	assert.Equal(suite.T(), req.MaxPaymentAmount, rsp.Item.MaxPaymentAmount)
	assert.Equal(suite.T(), req.IsProductsCheckout, rsp.Item.IsProductsCheckout)
	assert.Equal(suite.T(), billingpb.ProjectStatusDraft, rsp.Item.Status)
	assert.EqualValues(suite.T(), int32(0), rsp.Item.ProductsCount)
	assert.Equal(suite.T(), req.Localizations, rsp.Item.Localizations)
	assert.Equal(suite.T(), req.FullDescription, rsp.Item.FullDescription)
	assert.Equal(suite.T(), req.ShortDescription, rsp.Item.ShortDescription)
	assert.Equal(suite.T(), req.Currencies, rsp.Item.Currencies)
	assert.Equal(suite.T(), req.Cover, rsp.Item.Cover)
	assert.Equal(suite.T(), req.VirtualCurrency, rsp.Item.VirtualCurrency)
	assert.Equal(suite.T(), billingpb.VatPayerSeller, rsp.Item.VatPayer)

	project, err := suite.service.project.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), project)

	assert.Equal(suite.T(), project.Id, rsp.Item.Id)
	assert.Equal(suite.T(), project.MerchantId, rsp.Item.MerchantId)
	assert.Equal(suite.T(), project.Name, rsp.Item.Name)
	assert.Equal(suite.T(), project.CallbackCurrency, rsp.Item.CallbackCurrency)
	assert.Equal(suite.T(), project.CallbackProtocol, rsp.Item.CallbackProtocol)
	assert.Equal(suite.T(), project.LimitsCurrency, rsp.Item.LimitsCurrency)
	assert.Equal(suite.T(), project.MinPaymentAmount, rsp.Item.MinPaymentAmount)
	assert.Equal(suite.T(), project.MaxPaymentAmount, rsp.Item.MaxPaymentAmount)
	assert.Equal(suite.T(), project.IsProductsCheckout, rsp.Item.IsProductsCheckout)
	assert.Equal(suite.T(), project.Status, rsp.Item.Status)
	assert.Equal(suite.T(), project.Localizations, rsp.Item.Localizations)
	assert.Equal(suite.T(), project.FullDescription, rsp.Item.FullDescription)
	assert.Equal(suite.T(), project.ShortDescription, rsp.Item.ShortDescription)
	assert.Equal(suite.T(), project.Currencies, rsp.Item.Currencies)
	assert.Equal(suite.T(), project.Cover, rsp.Item.Cover)
	assert.Equal(suite.T(), project.VirtualCurrency, rsp.Item.VirtualCurrency)
	assert.Equal(suite.T(), project.VatPayer, rsp.Item.VatPayer)

	cProject, err := suite.service.project.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, cProject.Id)
	assert.Equal(suite.T(), project.MerchantId, cProject.MerchantId)
	assert.Equal(suite.T(), project.Name, cProject.Name)
	assert.Equal(suite.T(), project.CallbackCurrency, cProject.CallbackCurrency)
	assert.Equal(suite.T(), project.CallbackProtocol, cProject.CallbackProtocol)
	assert.Equal(suite.T(), project.LimitsCurrency, cProject.LimitsCurrency)
	assert.Equal(suite.T(), project.MinPaymentAmount, cProject.MinPaymentAmount)
	assert.Equal(suite.T(), project.MaxPaymentAmount, cProject.MaxPaymentAmount)
	assert.Equal(suite.T(), project.IsProductsCheckout, cProject.IsProductsCheckout)
	assert.Equal(suite.T(), project.Status, cProject.Status)
	assert.Equal(suite.T(), project.Localizations, cProject.Localizations)
	assert.Equal(suite.T(), project.FullDescription, cProject.FullDescription)
	assert.Equal(suite.T(), project.ShortDescription, cProject.ShortDescription)
	assert.Equal(suite.T(), project.Currencies, cProject.Currencies)
	assert.Equal(suite.T(), project.Cover, cProject.Cover)
	assert.Equal(suite.T(), project.VirtualCurrency, cProject.VirtualCurrency)
	assert.Equal(suite.T(), project.VatPayer, cProject.VatPayer)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_ExistProject_Ok() {
	req := suite.project
	assert.Equal(suite.T(), billingpb.VatPayerBuyer, suite.project.VatPayer)
	req.Name["ua"] = "модульний тест"
	req.CallbackProtocol = billingpb.ProjectCallbackProtocolDefault
	req.SecretKey = "qwerty"
	req.VatPayer = billingpb.VatPayerSeller

	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	assert.Equal(suite.T(), req.Id, rsp.Item.Id)
	assert.Equal(suite.T(), req.MerchantId, rsp.Item.MerchantId)
	assert.Equal(suite.T(), req.Name, rsp.Item.Name)
	assert.Equal(suite.T(), req.CallbackProtocol, rsp.Item.CallbackProtocol)
	assert.NotEqual(suite.T(), req.Status, rsp.Item.Status)
	assert.Equal(suite.T(), billingpb.ProjectStatusDraft, rsp.Item.Status)

	project, err := suite.service.project.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), project)

	assert.Equal(suite.T(), project.Id, rsp.Item.Id)
	assert.Equal(suite.T(), project.MerchantId, rsp.Item.MerchantId)
	assert.Equal(suite.T(), project.Name, rsp.Item.Name)
	assert.Equal(suite.T(), project.CallbackCurrency, rsp.Item.CallbackCurrency)
	assert.Equal(suite.T(), project.CallbackProtocol, rsp.Item.CallbackProtocol)
	assert.Equal(suite.T(), project.LimitsCurrency, rsp.Item.LimitsCurrency)
	assert.Equal(suite.T(), project.MinPaymentAmount, rsp.Item.MinPaymentAmount)
	assert.Equal(suite.T(), project.MaxPaymentAmount, rsp.Item.MaxPaymentAmount)
	assert.Equal(suite.T(), project.IsProductsCheckout, rsp.Item.IsProductsCheckout)
	assert.Equal(suite.T(), project.Status, rsp.Item.Status)
	assert.Equal(suite.T(), project.VatPayer, rsp.Item.VatPayer)
	assert.Equal(suite.T(), billingpb.VatPayerSeller, rsp.Item.VatPayer)

	cProject, err := suite.service.project.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, cProject.Id)
	assert.Equal(suite.T(), project.MerchantId, cProject.MerchantId)
	assert.Equal(suite.T(), project.Name, cProject.Name)
	assert.Equal(suite.T(), project.CallbackCurrency, cProject.CallbackCurrency)
	assert.Equal(suite.T(), project.CallbackProtocol, cProject.CallbackProtocol)
	assert.Equal(suite.T(), project.LimitsCurrency, cProject.LimitsCurrency)
	assert.Equal(suite.T(), project.MinPaymentAmount, cProject.MinPaymentAmount)
	assert.Equal(suite.T(), project.MaxPaymentAmount, cProject.MaxPaymentAmount)
	assert.Equal(suite.T(), project.IsProductsCheckout, cProject.IsProductsCheckout)
	assert.Equal(suite.T(), project.Status, cProject.Status)
	assert.Equal(suite.T(), project.VatPayer, cProject.VatPayer)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_MerchantNotFound_Error() {
	req := &billingpb.Project{
		MerchantId:         primitive.NewObjectID().Hex(),
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), merchantErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_ExistProjectIdNotFound_Error() {
	req := &billingpb.Project{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), projectErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NameInDefaultLanguageNotSet_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorNameDefaultLangRequired, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_CallbackCurrencyNotFound_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "USD",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}

	suite.service.curService = mocks.NewCurrencyServiceMockError()
	suite.service.supportedCurrencies = []string{}

	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorCallbackCurrencyIncorrect, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_LimitCurrencyNotFound_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "USD",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}

	suite.service.supportedCurrencies = []string{"RUB"}
	suite.service.curService = mocks.NewCurrencyServiceMockError()

	err := suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorLimitCurrencyIncorrect, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_GetProject_Ok() {
	req := &billingpb.GetProjectRequest{
		ProjectId:  suite.project.Id,
		MerchantId: suite.merchant.Id,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.GetProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	assert.Equal(suite.T(), suite.project.Id, rsp.Item.Id)
	assert.Equal(suite.T(), suite.project.MerchantId, rsp.Item.MerchantId)
	assert.Equal(suite.T(), suite.project.Name, rsp.Item.Name)
	assert.Equal(suite.T(), suite.project.CallbackCurrency, rsp.Item.CallbackCurrency)
	assert.Equal(suite.T(), suite.project.CallbackProtocol, rsp.Item.CallbackProtocol)
	assert.Equal(suite.T(), suite.project.LimitsCurrency, rsp.Item.LimitsCurrency)
	assert.Equal(suite.T(), suite.project.MinPaymentAmount, rsp.Item.MinPaymentAmount)
	assert.Equal(suite.T(), suite.project.MaxPaymentAmount, rsp.Item.MaxPaymentAmount)
	assert.Equal(suite.T(), suite.project.IsProductsCheckout, rsp.Item.IsProductsCheckout)
	assert.Equal(suite.T(), suite.project.Status, rsp.Item.Status)
	assert.EqualValues(suite.T(), int32(3), rsp.Item.ProductsCount)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_GetProject_NotFound_Error() {
	req := &billingpb.GetProjectRequest{
		ProjectId:  suite.project.Id,
		MerchantId: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.GetProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), projectErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ListProjects_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit1 test", "ru": "Юнит1 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit11 test", "ru": "Юнит11 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit2 test", "ru": "Юнит2 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req1 := &billingpb.ListProjectsRequest{
		MerchantId: suite.merchant.Id,
		Limit:      100,
	}
	rsp1 := &billingpb.ListProjectsResponse{}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(5), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 5)
	assert.EqualValues(suite.T(), int32(3), rsp1.Items[0].ProductsCount)
	assert.NotNil(suite.T(), rsp1.Items[0].RedirectSettings)
	assert.NotZero(suite.T(), rsp1.Items[0].RedirectSettings.Mode)
	assert.NotZero(suite.T(), rsp1.Items[0].RedirectSettings.Usage)
	assert.Zero(suite.T(), rsp1.Items[0].RedirectSettings.Delay)
	assert.Zero(suite.T(), rsp1.Items[0].RedirectSettings.ButtonCaption)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ListProjects_NameQuery_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit1 test", "ru": "Юнит1 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit11 test", "ru": "Юнит11 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit2 test", "ru": "Юнит2 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req1 := &billingpb.ListProjectsRequest{
		MerchantId:  suite.merchant.Id,
		QuickSearch: "nit1",
		Limit:       100,
	}
	rsp1 := &billingpb.ListProjectsResponse{}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(2), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 2)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ListProjects_StatusQuery_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	rsp.Item.Status = billingpb.ProjectStatusTestCompleted
	err = suite.service.ChangeProject(context.TODO(), rsp.Item, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit1 test", "ru": "Юнит1 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	rsp.Item.Status = billingpb.ProjectStatusTestCompleted
	err = suite.service.ChangeProject(context.TODO(), rsp.Item, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit11 test", "ru": "Юнит11 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	rsp.Item.Status = billingpb.ProjectStatusInProduction
	err = suite.service.ChangeProject(context.TODO(), rsp.Item, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "Unit2 test", "ru": "Юнит2 тест"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req1 := &billingpb.ListProjectsRequest{
		MerchantId: suite.merchant.Id,
		Statuses:   []int32{billingpb.ProjectStatusInProduction},
		Limit:      100,
	}
	rsp1 := &billingpb.ListProjectsResponse{}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(2), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 2)

	req1.Statuses = []int32{billingpb.ProjectStatusTestCompleted}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(2), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 2)

	req1.Statuses = []int32{billingpb.ProjectStatusDraft, billingpb.ProjectStatusTestCompleted}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(3), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 3)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ListProjects_SortQuery_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "A", "ru": "А"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "B", "ru": "Б"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "C", "ru": "В"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req.Name = map[string]string{"en": "D", "ru": "Г"}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)

	req1 := &billingpb.ListProjectsRequest{
		MerchantId: suite.merchant.Id,
		Sort:       []string{"name"},
		Limit:      100,
	}
	rsp1 := &billingpb.ListProjectsResponse{}
	err = suite.service.ListProjects(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int32(5), rsp1.Count)
	assert.Len(suite.T(), rsp1.Items, 5)
	assert.Equal(suite.T(), "A", rsp1.Items[0].Name["en"])
	assert.Equal(suite.T(), "А", rsp1.Items[0].Name["ru"])
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_DeleteProject_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "A", "ru": "А"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Equal(suite.T(), billingpb.ProjectStatusDraft, rsp.Item.Status)

	req1 := &billingpb.GetProjectRequest{
		MerchantId: req.MerchantId,
		ProjectId:  rsp.Item.Id,
	}
	rsp1 := &billingpb.ChangeProjectResponse{}
	err = suite.service.DeleteProject(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)

	project, err := suite.service.project.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ProjectStatusDeleted, project.Status)

	project1, err := suite.service.project.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Status, project1.Status)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_DeleteProject_NotFound_Error() {
	req := &billingpb.GetProjectRequest{
		MerchantId: suite.merchant.Id,
		ProjectId:  primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.DeleteProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), projectErrorNotFound, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_DeleteDeletedProject_Ok() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "A", "ru": "А"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		VatPayer:           billingpb.VatPayerBuyer,
		RedirectSettings: &billingpb.ProjectRedirectSettings{
			Mode:  pkg.ProjectRedirectModeAny,
			Usage: pkg.ProjectRedirectUsageAny,
		},
		UrlRedirectSuccess: "http://localhost?success",
		UrlRedirectFail:    "http://localhost?fail",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Equal(suite.T(), billingpb.ProjectStatusDraft, rsp.Item.Status)

	rsp.Item.Status = billingpb.ProjectStatusDeleted
	err = suite.service.ChangeProject(context.TODO(), rsp.Item, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Equal(suite.T(), billingpb.ProjectStatusDeleted, rsp.Item.Status)

	req1 := &billingpb.GetProjectRequest{
		MerchantId: req.MerchantId,
		ProjectId:  rsp.Item.Id,
	}
	rsp1 := &billingpb.ChangeProjectResponse{}
	err = suite.service.DeleteProject(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)
}

type ProjectTestSuite struct {
	suite.Suite
	service *Service
	cache   database.CacheInterface
	log     *zap.Logger
	project *billingpb.Project
}

func Test_Project(t *testing.T) {
	suite.Run(t, new(ProjectTestSuite))
}

func (suite *ProjectTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.project = &billingpb.Project{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         primitive.NewObjectID().Hex(),
		CallbackCurrency:   "RUB",
		CallbackProtocol:   "default",
		LimitsCurrency:     "RUB",
		MaxPaymentAmount:   15000,
		MinPaymentAmount:   0,
		Name:               map[string]string{"en": "test project 1"},
		IsProductsCheckout: true,
		SecretKey:          "test project 1 secret key",
		Status:             billingpb.ProjectStatusInProduction,
		VatPayer:           billingpb.VatPayerBuyer,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

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
		&casbinMocks.CasbinService{},
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	if err := suite.service.project.Insert(context.TODO(), suite.project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}
}

func (suite *ProjectTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_IncorrectCurrencies_Error() {
	suite.service.supportedCurrencies = []string{"RUB"}

	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorCurrencyIsNotSupport.Message, rsp.Message.Message)
	assert.Equal(suite.T(), projectErrorCurrencyIsNotSupport.Code, rsp.Message.Code)
	assert.Equal(suite.T(), "USD", rsp.Message.Details)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_ShortDescriptionNotHaveDefaultLanguage_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorShortDescriptionDefaultLangRequired, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_FullDescriptionNotHaveDefaultLanguage_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorFullDescriptionDefaultLangRequired, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_VirtualCurrencyNameNotHaveDefaultLanguage_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorVirtualCurrencyNameDefaultLangRequired, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_VirtualCurrencySuccessMessageNotHaveDefaultLanguage_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorVirtualCurrencySuccessMessageDefaultLangRequired, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_VirtualCurrencyPriceNotSupportedCurrency_Error() {
	suite.service.supportedCurrencies = []string{"RUB", "USD"}
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "KZT", Region: "CIS"},
			},
			MaxPurchaseValue: 1000000,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorVirtualCurrencyPriceCurrencyIsNotSupport.Message, rsp.Message.Message)
	assert.Equal(suite.T(), projectErrorVirtualCurrencyPriceCurrencyIsNotSupport.Code, rsp.Message.Code)
	assert.Equal(suite.T(), "KZT", rsp.Message.Details)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_VirtualCurrencyPurchasesLimit_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		LimitsCurrency:     "RUB",
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MinPurchaseValue: 1000,
			MaxPurchaseValue: 100,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorVirtualCurrencyLimitsIncorrect, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_LimitAmounts_Error() {
	req := &billingpb.Project{
		MerchantId:         suite.merchant.Id,
		Name:               map[string]string{"en": "Unit test", "ru": "Юнит тест"},
		CallbackCurrency:   "RUB",
		CallbackProtocol:   billingpb.ProjectCallbackProtocolEmpty,
		MinPaymentAmount:   0,
		MaxPaymentAmount:   15000,
		IsProductsCheckout: false,
		Localizations:      []string{"en", "ru"},
		FullDescription: map[string]string{
			"en": "It's english full description",
			"ru": "Это полное описание на русском языке",
		},
		ShortDescription: map[string]string{
			"en": "It's english short description",
			"ru": "Это короткое описание на русском языке",
		},
		Currencies: []*billingpb.HasCurrencyItem{
			{Currency: "USD", Region: "USD"},
			{Currency: "RUB", Region: "Russia"},
		},
		Cover: &billingpb.ImageCollection{
			Images: &billingpb.LocalizedUrl{
				En: "http://en.localhost",
				Ru: "http://ru.localhost",
			},
			UseOneForAll: true,
		},
		VirtualCurrency: &billingpb.ProjectVirtualCurrency{
			Logo: "http://localhost",
			Name: map[string]string{
				"en": "It's english virtual currency name",
				"ru": "Это название виртуальной валюты на русском языке",
			},
			SuccessMessage: map[string]string{
				"en": "It's english success message",
				"ru": "Это сообщение о успешной покупке на русском языке",
			},
			Prices: []*billingpb.ProductPrice{
				{Amount: 100, Currency: "USD", Region: "USD"},
				{Amount: 1000, Currency: "RUB", Region: "Russia"},
			},
			MaxPurchaseValue: 100,
			SellCountType:    "fractional",
		},
		VatPayer: billingpb.VatPayerBuyer,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err := suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), projectErrorLimitCurrencyRequired, rsp.Message)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_WithoutRedirectSettings_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)

	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectSettingsIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_WithRedirectSettings_WithoutRedirectMode_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Usage: pkg.ProjectRedirectUsageAny,
	}

	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_WithRedirectSettings_WithoutRedirectUsage_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode: pkg.ProjectRedirectModeAny,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectUsageIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_RedirectModeSuccess_RedirectUrlSuccessEmpty_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)

	req.UrlRedirectSuccess = ""
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:  pkg.ProjectRedirectModeSuccessful,
		Usage: pkg.ProjectRedirectUsageAny,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeSuccessfulUrlIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_RedirectModeFail_RedirectUrlFailEmpty_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)

	req.UrlRedirectFail = ""
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:  pkg.ProjectRedirectModeFail,
		Usage: pkg.ProjectRedirectUsageAny,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeFailUrlIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_RedirectModeAny_RedirectUrlsEmpty_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)

	req.UrlRedirectFail = ""
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:  pkg.ProjectRedirectModeAny,
		Usage: pkg.ProjectRedirectUsageAny,
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeBothRedirectUrlsIsRequired)
	assert.Nil(suite.T(), rsp.Item)

	req.UrlRedirectFail = "http://localhost?fail"
	req.UrlRedirectSuccess = ""

	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeBothRedirectUrlsIsRequired)
	assert.Nil(suite.T(), rsp.Item)

	req.UrlRedirectFail = ""
	req.UrlRedirectSuccess = ""

	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeBothRedirectUrlsIsRequired)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_ChangeProject_NewProject_RedirectButtonCaptionNotEmpty_NonZeroDelay_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &projectMock)
	assert.NoError(suite.T(), err)

	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:          pkg.ProjectRedirectModeAny,
		Usage:         pkg.ProjectRedirectUsageAny,
		Delay:         100,
		ButtonCaption: "button caption",
	}
	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorButtonCaptionAllowedOnlyForAfterRedirect)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_UpdateProject_WithoutRedirectSettings_Ok() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &suite.project)
	assert.NoError(suite.T(), err)

	req.SecretKey = "qwerty"
	req.RedirectSettings = nil

	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	assert.Equal(suite.T(), req.Id, rsp.Item.Id)
	assert.NotEqual(suite.T(), suite.project.SecretKey, rsp.Item.SecretKey)
	assert.Equal(suite.T(), req.SecretKey, rsp.Item.SecretKey)
	assert.NotNil(suite.T(), rsp.Item.RedirectSettings)
	assert.NotEqual(suite.T(), req.RedirectSettings, rsp.Item.RedirectSettings)
	assert.Equal(suite.T(), suite.project.RedirectSettings, rsp.Item.RedirectSettings)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_UpdateProject_WithRedirectSettings_Ok() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &suite.project)
	assert.NoError(suite.T(), err)

	req.SecretKey = "qwerty"
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:          pkg.ProjectRedirectModeDisable,
		Usage:         pkg.ProjectRedirectUsageAny,
		Delay:         0,
		ButtonCaption: "button_caption",
	}

	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	assert.Equal(suite.T(), req.Id, rsp.Item.Id)
	assert.NotEqual(suite.T(), suite.project.SecretKey, rsp.Item.SecretKey)
	assert.Equal(suite.T(), req.SecretKey, rsp.Item.SecretKey)
	assert.NotEqual(suite.T(), suite.project.RedirectSettings, rsp.Item.RedirectSettings)
	assert.Equal(suite.T(), req.RedirectSettings, rsp.Item.RedirectSettings)
}

func (suite *ProjectCRUDTestSuite) TestProjectCRUD_UpdateProject_WithIncorrectRedirectSettings_Error() {
	req := new(billingpb.Project)
	err := copier.Copy(&req, &suite.project)
	assert.NoError(suite.T(), err)

	req.SecretKey = "qwerty"
	req.RedirectSettings = &billingpb.ProjectRedirectSettings{
		Mode:          "",
		Delay:         100,
		ButtonCaption: "button_caption",
	}

	rsp := &billingpb.ChangeProjectResponse{}
	err = suite.service.ChangeProject(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), rsp.Message, projectErrorRedirectModeIsRequired)
}
