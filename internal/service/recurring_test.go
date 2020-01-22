package service

import (
	"context"
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
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type RecurringTestSuite struct {
	suite.Suite
	service *Service
}

func Test_Recurring(t *testing.T) {
	suite.Run(t, new(RecurringTestSuite))
}

func (suite *RecurringTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	redisdb := mocks.NewTestRedis()
	cache, err := database.NewCacheRedis(redisdb, "cache")
	casbin := &casbinMocks.CasbinService{}

	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		&mocks.TaxServiceOkMock{},
		mocks.NewBrokerMockOk(),
		nil,
		cache,
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
}

func (suite *RecurringTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_Ok() {
	customer := &BrowserCookieCustomer{
		VirtualCustomerId: primitive.NewObjectID().Hex(),
		Ip:                "127.0.0.1",
		AcceptLanguage:    "fr-CA",
		UserAgent:         "windows",
		SessionCount:      0,
	}
	cookie, err := suite.service.generateBrowserCookie(customer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_IncorrectCookie_Error() {
	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: primitive.NewObjectID().Hex(),
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err := suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), recurringErrorIncorrectCookie, rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_DontHaveCustomerId_Error() {
	customer := &BrowserCookieCustomer{
		Ip:             "127.0.0.1",
		AcceptLanguage: "fr-CA",
		UserAgent:      "windows",
		SessionCount:   0,
	}
	cookie, err := suite.service.generateBrowserCookie(customer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), recurringCustomerNotFound, rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_RealCustomer_Ok() {
	project := &billingpb.Project{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	req0 := &billingpb.TokenRequest{
		User: &billingpb.TokenUser{
			Id: primitive.NewObjectID().Hex(),
			Locale: &billingpb.TokenUserLocaleValue{
				Value: "en",
			},
		},
		Settings: &billingpb.TokenSettings{
			ProjectId: project.Id,
			Amount:    100,
			Currency:  "USD",
			Type:      pkg.OrderType_simple,
		},
	}
	customer, err := suite.service.createCustomer(context.TODO(), req0, project)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), customer)

	browserCustomer := &BrowserCookieCustomer{
		CustomerId:     customer.Id,
		Ip:             "127.0.0.1",
		AcceptLanguage: "fr-CA",
		UserAgent:      "windows",
		SessionCount:   0,
	}
	cookie, err := suite.service.generateBrowserCookie(browserCustomer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_RealCustomerNotFound_Error() {
	browserCustomer := &BrowserCookieCustomer{
		CustomerId:     primitive.NewObjectID().Hex(),
		Ip:             "127.0.0.1",
		AcceptLanguage: "fr-CA",
		UserAgent:      "windows",
		SessionCount:   0,
	}
	cookie, err := suite.service.generateBrowserCookie(browserCustomer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), recurringCustomerNotFound, rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_RecurringServiceSystem_Error() {
	browserCustomer := &BrowserCookieCustomer{
		VirtualCustomerId: primitive.NewObjectID().Hex(),
		Ip:                "127.0.0.1",
		AcceptLanguage:    "fr-CA",
		UserAgent:         "windows",
		SessionCount:      0,
	}
	cookie, err := suite.service.generateBrowserCookie(browserCustomer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	suite.service.rep = mocks.NewRepositoryServiceError()

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), recurringErrorUnknown, rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_RecurringServiceResult_Error() {
	browserCustomer := &BrowserCookieCustomer{
		VirtualCustomerId: primitive.NewObjectID().Hex(),
		Ip:                "127.0.0.1",
		AcceptLanguage:    "fr-CA",
		UserAgent:         "windows",
		SessionCount:      0,
	}
	cookie, err := suite.service.generateBrowserCookie(browserCustomer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	suite.service.rep = mocks.NewRepositoryServiceEmpty()

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), recurringSavedCardNotFount, rsp.Message)
}

func (suite *RecurringTestSuite) TestRecurring_DeleteSavedCard_RecurringServiceResultSystemError_Error() {
	browserCustomer := &BrowserCookieCustomer{
		VirtualCustomerId: "ffffffffffffffffffffffff",
		Ip:                "127.0.0.1",
		AcceptLanguage:    "fr-CA",
		UserAgent:         "windows",
		SessionCount:      0,
	}
	cookie, err := suite.service.generateBrowserCookie(browserCustomer)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cookie)

	suite.service.rep = mocks.NewRepositoryServiceEmpty()

	req := &billingpb.DeleteSavedCardRequest{
		Id:     primitive.NewObjectID().Hex(),
		Cookie: cookie,
	}
	rsp := &billingpb.EmptyResponseWithStatus{}
	err = suite.service.DeleteSavedCard(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), recurringErrorUnknown, rsp.Message)
}
