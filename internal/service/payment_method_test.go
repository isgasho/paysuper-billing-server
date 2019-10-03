package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
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
	service *Service
	log     *zap.Logger
	cache   CacheInterface
	pmQiwi  *billing.PaymentMethod
}

func Test_PaymentMethod(t *testing.T) {
	suite.Run(t, new(PaymentMethodTestSuite))
}

func (suite *PaymentMethodTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
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
		Currencies:       []string{"RUB", "USD", "EUR"},
		ExternalId:       "QIWI",
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Currency:   "RUB",
				TerminalId: "15993",
				Secret:     "A1tph4I6BD0f",
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
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), &reportingMocks.ReporterService{}, mocks.NewFormatterOK(), )

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
	pm, err := suite.service.paymentMethod.GetByGroupAndCurrency(suite.pmQiwi.Group, "RUB")

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), pm)
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm.Id)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetByGroupAndCurrency_NotFound() {
	_, err := suite.service.paymentMethod.GetByGroupAndCurrency("unknown", "RUB")
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentMethod))

	_, err = suite.service.paymentMethod.GetByGroupAndCurrency(suite.pmQiwi.Group, "")
	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentMethod))
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_Groups() {
	pm, err := suite.service.paymentMethod.Groups()
	assert.NoError(suite.T(), err)

	assert.NotNil(suite.T(), pm)
	assert.NotNil(suite.T(), pm["QIWI"])
	assert.NotNil(suite.T(), pm["QIWI"]["RUB"])
	assert.Equal(suite.T(), suite.pmQiwi.Id, pm["QIWI"]["RUB"].Id)
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
	merchant := &billing.Merchant{
		Banking: &billing.MerchantBanking{
			Currency: "RUB",
		},
	}
	project := &billing.Project{}
	_, err := suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, paymentMethodErrorSettings)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_OkTestSettings() {
	method := &billing.PaymentMethod{
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				Secret: "A1tph4I6BD0f",
			},
		},
	}
	merchant := &billing.Merchant{
		Banking: &billing.MerchantBanking{
			Currency: "RUB",
		},
	}
	project := &billing.Project{}
	settings, err := suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), method.TestSettings["RUB"].Secret, settings.Secret)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_ErrorMerchantBanking() {
	method := &billing.PaymentMethod{}
	merchant := &billing.Merchant{}
	project := &billing.Project{
		Status: pkg.ProjectStatusInProduction,
	}
	_, err := suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, paymentMethodErrorBankingSettings)

	merchant.Banking = nil
	_, err = suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, paymentMethodErrorBankingSettings)

	merchant.Banking = &billing.MerchantBanking{Currency: ""}
	_, err = suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, paymentMethodErrorBankingSettings)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_ErrorNoPaymentCurrency() {
	method := &billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {Currency: "RUB"},
		},
	}
	merchant := &billing.Merchant{
		Banking: &billing.MerchantBanking{
			Currency: "EUR",
		},
	}
	project := &billing.Project{
		Status: pkg.ProjectStatusInProduction,
	}
	_, err := suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, paymentMethodErrorNotFoundProductionSettings)
}

func (suite *PaymentMethodTestSuite) TestPaymentMethod_GetPaymentSettings_Ok() {
	method := &billing.PaymentMethod{
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"EUR": {Secret: "unit_test"},
		},
	}
	merchant := &billing.Merchant{
		Banking: &billing.MerchantBanking{
			Currency: "EUR",
		},
	}
	project := &billing.Project{
		Status: pkg.ProjectStatusInProduction,
	}
	settings, err := suite.service.paymentMethod.GetPaymentSettings(method, merchant, project)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), method.ProductionSettings["EUR"].Secret, settings.Secret)
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

	req.Currencies = []string{""}
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
		Currencies:         []string{""},
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
			"EUR": {Secret: "secret", SecretCallback: "secret_callback", TerminalId: "terminal_id"},
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
			"RUB": {Currency: "RUB", Secret: "unit_test"},
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
			"RUB": {Currency: "RUB", Secret: "unit_test"},
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
			"RUB": {Currency: "RUB", Secret: "unit_test"},
		},
	}, nil)
	method.On("Update", mock2.Anything).Return(nil)
	suite.service.paymentMethod = method

	err := suite.service.DeletePaymentMethodTestSettings(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
}
