package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PaymentMethodTestSuite struct {
	suite.Suite
	service          *Service
	log              *zap.Logger
	cache            internalPkg.CacheInterface
	pmQiwi           *billing.PaymentMethod
	project          *billing.Project
	operatingCompany *billing.OperatingCompany
}

func Test_PaymentMethod(t *testing.T) {
	suite.Run(t, new(PaymentMethodTestSuite))
}

func (suite *PaymentMethodTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
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

	ps := &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}
	suite.pmQiwi = &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Qiwi",
		Group:            "QIWI",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		ExternalId:       "QIWI",
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Currency:           "RUB",
				TerminalId:         "15993",
				Secret:             "A1tph4I6BD0f",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: ps.Id,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
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
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billing.PaymentMethod{suite.pmQiwi}
	if err := suite.service.paymentMethod.MultipleInsert(pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	if err := suite.service.paymentSystem.Insert(ps); err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	project := &billing.Project{
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
		MerchantId:               bson.NewObjectId().Hex(),
	}
	suite.project = project
	err = suite.service.project.Insert(project)

	if err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}
}

func (suite *PaymentMethodTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetAll() {
	c, err := suite.service.paymentMethod.GetAll()

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), c)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetById_Ok() {
	pm, err := suite.service.paymentMethod.GetById(suite.pmQiwi.Id)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pm)
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.Id)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetById_NotFound() {
	_, err := suite.service.paymentMethod.GetById(bson.NewObjectId().Hex())

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentMethod))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetByGroupAndCurrency_Ok() {
	pm, err := suite.service.paymentMethod.GetByGroupAndCurrency(suite.project, suite.pmQiwi.Group, "RUB")

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pm)
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.Id)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetByGroupAndCurrency_NotFound() {
	_, err := suite.service.paymentMethod.GetByGroupAndCurrency(suite.project, "unknown", "RUB")
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentMethod))

	_, err = suite.service.paymentMethod.GetByGroupAndCurrency(suite.project, suite.pmQiwi.Group, "")
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentMethod))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Insert_Ok() {
	id := bson.NewObjectId().Hex()
	assert.NoError(suite.T(), suite.service.paymentMethod.Insert(&billing.PaymentMethod{
		Id:              id,
		PaymentSystemId: id,
	}))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Insert_ErrorCacheUpdate() {
	id := bson.NewObjectId().Hex()
	ci := &mocks.CacheInterface{}
	ci.On("Delete", "payment_method:all").Return(nil)
	ci.On("Set", "payment_method:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.paymentMethod.Insert(&billing.PaymentMethod{Id: id, PaymentSystemId: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Update_Ok() {
	assert.NoError(suite.T(), suite.service.paymentMethod.Update(&billing.PaymentMethod{
		Id:              suite.pmQiwi.Id,
		PaymentSystemId: suite.pmQiwi.Id,
	}))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_NotFound() {
	id := bson.NewObjectId().Hex()
	err := suite.service.paymentMethod.Update(&billing.PaymentMethod{Id: id, PaymentSystemId: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "not found")
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Update_ErrorCacheUpdate() {
	id := bson.NewObjectId().Hex()
	ci := &mocks.CacheInterface{}
	ci.On("Delete", "payment_method:all").Return(nil)
	ci.On("Set", "payment_method:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	_ = suite.service.paymentMethod.Insert(&billing.PaymentMethod{Id: id, PaymentSystemId: id})
	err := suite.service.paymentMethod.Update(&billing.PaymentMethod{Id: id, PaymentSystemId: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_ErrorNoTestSettings() {
	method := &billing.PaymentMethod{}
	project := &billing.Project{}
	_, err := suite.service.paymentMethod.GetPaymentSettings(method, "RUB", pkg.MccCodeLowRisk, suite.operatingCompany.Id, project)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, orderErrorPaymentMethodEmptySettings)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_OkTestSettings() {
	key := fmt.Sprintf(pkg.PaymentMethodKey, "RUB", pkg.MccCodeLowRisk, suite.operatingCompany.Id)

	method := &billing.PaymentMethod{
		Id:         bson.NewObjectId().Hex(),
		Name:       "Unit Test",
		Group:      "Unit",
		ExternalId: "Unit",
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			key: {
				Currency:           "RUB",
				TerminalId:         "15993",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "A1tph4I6BD0f",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: suite.pmQiwi.PaymentSystemId,
	}
	err := suite.service.paymentMethod.Insert(method)
	assert.NoError(suite.T(), err)

	suite.project.Status = pkg.ProjectStatusInProduction
	err = suite.service.project.Update(suite.project)
	assert.NoError(suite.T(), err)

	settings, err := suite.service.paymentMethod.GetPaymentSettings(method, "RUB", pkg.MccCodeLowRisk, suite.operatingCompany.Id, suite.project)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), method.ProductionSettings[key].Secret, settings.Secret)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_ErrorNoPaymentCurrency() {
	method := &billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {Currency: "RUB"},
		},
	}
	project := &billing.Project{
		Status: pkg.ProjectStatusInProduction,
	}
	_, err := suite.service.paymentMethod.GetPaymentSettings(method, "EUR", pkg.MccCodeLowRisk, suite.operatingCompany.Id, project)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, orderErrorPaymentMethodEmptySettings)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_Ok() {
	key := fmt.Sprintf(pkg.PaymentMethodKey, "EUR", pkg.MccCodeLowRisk, suite.operatingCompany.Id)

	method := &billing.PaymentMethod{
		Id:         bson.NewObjectId().Hex(),
		Name:       "Unit Test",
		Group:      "Unit",
		ExternalId: "Unit",
		TestSettings: map[string]*billing.PaymentMethodParams{
			key: {
				Currency:           "EUR",
				TerminalId:         "15993",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "A1tph4I6BD0f",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: suite.pmQiwi.PaymentSystemId,
	}
	err := suite.service.paymentMethod.Insert(method)
	assert.NoError(suite.T(), err)

	settings, err := suite.service.paymentMethod.GetPaymentSettings(method, "EUR", pkg.MccCodeLowRisk, suite.operatingCompany.Id, suite.project)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), method.TestSettings[key].Secret, settings.Secret)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethod_ErrorPaymentSystem() {
	req := &billing.PaymentMethod{
		PaymentSystemId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangePaymentMethodResponse{}
	paymentSystem := &mocks.PaymentSystemServiceInterface{}

	paymentSystem.On("GetById", req.PaymentSystemId).Return(nil, errors.New("not found"))
	suite.service.paymentSystem = paymentSystem

	err := suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethod_ErrorInvalidId() {
	req := &billing.PaymentMethod{
		Id:              bson.NewObjectId().Hex(),
		PaymentSystemId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangePaymentMethodResponse{}
	paymentSystem := &mocks.PaymentSystemServiceInterface{}

	paymentSystem.On("GetById", req.PaymentSystemId).Return(&billing.PaymentSystem{}, nil)
	suite.service.paymentSystem = paymentSystem

	err := suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionPaymentMethod), rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethod_ErrorActivate() {
	req := &billing.PaymentMethod{
		PaymentSystemId: bson.NewObjectId().Hex(),
		IsActive:        true,
	}
	rsp := &grpc.ChangePaymentMethodResponse{}
	paymentSystem := &mocks.PaymentSystemServiceInterface{}

	paymentSystem.On("GetById", req.PaymentSystemId).Return(&billing.PaymentSystem{}, nil)
	suite.service.paymentSystem = paymentSystem

	err := suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.ExternalId = "externalId"
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.Type = "type"
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.Group = "group"
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.Name = "name"
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.TestSettings = map[string]*billing.PaymentMethodParams{}
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)

	req.ExternalId = ""
	req.ProductionSettings = map[string]*billing.PaymentMethodParams{"RUB": {Currency: "RUB"}}
	err = suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorPaymentSystem, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethod_OkWithActivate() {
	req := &billing.PaymentMethod{
		PaymentSystemId:    bson.NewObjectId().Hex(),
		IsActive:           true,
		ExternalId:         "externalId",
		Type:               "type",
		Group:              "group",
		Name:               "name",
		TestSettings:       map[string]*billing.PaymentMethodParams{},
		ProductionSettings: map[string]*billing.PaymentMethodParams{"RUB": {Currency: "RUB"}},
	}
	rsp := &grpc.ChangePaymentMethodResponse{}
	paymentSystem := &mocks.PaymentSystemServiceInterface{}

	paymentSystem.On("GetById", req.PaymentSystemId).Return(&billing.PaymentSystem{}, nil)
	suite.service.paymentSystem = paymentSystem

	err := suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethod_OkWithoutActivate() {
	req := &billing.PaymentMethod{
		PaymentSystemId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangePaymentMethodResponse{}
	paymentSystem := &mocks.PaymentSystemServiceInterface{}

	paymentSystem.On("GetById", req.PaymentSystemId).Return(&billing.PaymentSystem{}, nil)
	suite.service.paymentSystem = paymentSystem

	err := suite.service.CreateOrUpdatePaymentMethod(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodProductionSettings_ErrorPaymentMethod() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params:          &billing.PaymentMethodParams{},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}
	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorUnknownMethod, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodProductionSettings_ErrorUpdate() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params: &billing.PaymentMethodParams{
			Currency:       "RUB",
			TerminalId:     "ID",
			Secret:         "unit_test",
			SecretCallback: "unit_test_callback",
		},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{}, nil)
	method.On("Update", mock2.Anything).Return(errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), "not found", rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodProductionSettings_Ok() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params: &billing.PaymentMethodParams{
			Currency:       "RUB",
			TerminalId:     "ID",
			Secret:         "unit_test",
			SecretCallback: "unit_test_callback",
		},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{}, nil)
	method.On("Update", mock2.Anything).Return(nil)
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodProductionSettings_EmptyByPaymentMethod() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetPaymentMethodSettingsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.GetPaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), rsp.Params, 0)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodProductionSettings_Ok() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetPaymentMethodSettingsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"EUR": {Secret: "secret", SecretCallback: "secret_callback", TerminalId: "terminal_id"},
		},
	}, nil)
	suite.service.paymentMethod = method

	err := suite.service.GetPaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), rsp.Params, 1)
	assert.Equal(suite.T(), "EUR", rsp.Params[0].Currency)
	assert.Equal(suite.T(), "secret", rsp.Params[0].Secret)
	assert.Equal(suite.T(), "secret_callback", rsp.Params[0].SecretCallback)
	assert.Equal(suite.T(), "terminal_id", rsp.Params[0].TerminalId)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodProductionSettings_ErrorByPaymentMethod() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorUnknownMethod, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodProductionSettings_ErrorNoSettings() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "EUR",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {Currency: "RUB", Secret: "unit_test"},
		},
	}, nil)
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorNotFoundProductionSettings, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodProductionSettings_ErrorUpdate() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "RUB",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {Currency: "RUB", Secret: "unit_test"},
		},
	}, nil)
	method.On("Update", mock2.Anything).Return(errors.New("service unavailable"))
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), "service unavailable", rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodProductionSettings_Ok() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "RUB",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {Currency: "RUB", Secret: "unit_test"},
		},
	}, nil)
	method.On("Update", mock2.Anything).Return(nil)
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodTestSettings_ErrorPaymentMethod() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params:          &billing.PaymentMethodParams{},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorUnknownMethod, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodTestSettings_ErrorUpdate() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params: &billing.PaymentMethodParams{
			Currency:       "RUB",
			TerminalId:     "ID",
			Secret:         "unit_test",
			SecretCallback: "unit_test_callback",
		},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{}, nil)
	method.On("Update", mock2.Anything).Return(errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), "not found", rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_CreateOrUpdatePaymentMethodTestSettings_Ok() {
	req := &grpc.ChangePaymentMethodParamsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		Params: &billing.PaymentMethodParams{
			Currency:       "RUB",
			TerminalId:     "ID",
			Secret:         "unit_test",
			SecretCallback: "unit_test_callback",
		},
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{}, nil)
	method.On("Update", mock2.Anything).Return(nil)
	suite.service.paymentMethod = method

	err := suite.service.CreateOrUpdatePaymentMethodProductionSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodTestSettings_EmptyByPaymentMethod() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetPaymentMethodSettingsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.GetPaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), rsp.Params, 0)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentMethodTestSettings_Ok() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.GetPaymentMethodSettingsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		TestSettings: map[string]*billing.PaymentMethodParams{
			"EUR": {
				Secret:             "secret",
				SecretCallback:     "secret_callback",
				TerminalId:         "terminal_id",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
	}, nil)
	suite.service.paymentMethod = method

	err := suite.service.GetPaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), rsp.Params, 1)
	assert.Equal(suite.T(), "EUR", rsp.Params[0].Currency)
	assert.Equal(suite.T(), "secret", rsp.Params[0].Secret)
	assert.Equal(suite.T(), "secret_callback", rsp.Params[0].SecretCallback)
	assert.Equal(suite.T(), "terminal_id", rsp.Params[0].TerminalId)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodTestSettings_ErrorByPaymentMethod() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(nil, errors.New("not found"))
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorUnknownMethod, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodTestSettings_ErrorNoSettings() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "EUR",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Currency:           "RUB",
				Secret:             "unit_test",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
	}, nil)
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), paymentMethodErrorNotFoundProductionSettings, rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodTestSettings_ErrorUpdate() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "RUB",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Currency:           "RUB",
				Secret:             "unit_test",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
	}, nil)
	method.On("Update", mock2.Anything).Return(errors.New("service unavailable"))
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), "service unavailable", rsp.Message)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_DeletePaymentMethodTestSettings_Ok() {
	req := &grpc.GetPaymentMethodSettingsRequest{
		PaymentMethodId: bson.NewObjectId().Hex(),
		CurrencyA3:      "RUB",
	}
	rsp := &grpc.ChangePaymentMethodParamsResponse{}
	method := &mocks.PaymentMethodInterface{}

	method.On("GetById", req.PaymentMethodId).Return(&billing.PaymentMethod{
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Currency:           "RUB",
				Secret:             "unit_test",
				MccCode:            pkg.MccCodeLowRisk,
				OperatingCompanyId: suite.operatingCompany.Id,
			},
		},
	}, nil)
	method.On("Update", mock2.Anything).Return(nil)
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}
