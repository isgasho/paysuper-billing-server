package service

import (
	"errors"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type MerchantTestSuite struct {
	suite.Suite
	service    *Service
	log        *zap.Logger
	cache      database.CacheInterface
	merchant   *billingpb.Merchant
	pmBankCard *billingpb.PaymentMethod
}

func Test_Merchant(t *testing.T) {
	suite.Run(t, new(MerchantTestSuite))
}

func (suite *MerchantTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
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

	suite.pmBankCard = &billingpb.PaymentMethod{
		Id:   primitive.NewObjectID().Hex(),
		Name: "Bank card",
	}
	suite.merchant = &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		PaymentMethods: map[string]*billingpb.MerchantPaymentMethod{
			suite.pmBankCard.Id: {
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   suite.pmBankCard.Id,
					Name: suite.pmBankCard.Name,
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
	}
	if err := suite.service.merchant.Insert(ctx, suite.merchant); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}
}

func (suite *MerchantTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MerchantTestSuite) TestMerchant_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.merchant.Insert(ctx, &billingpb.Merchant{}))
}

func (suite *MerchantTestSuite) TestMerchant_Insert_ErrorCacheUpdate() {
	id := primitive.NewObjectID().Hex()
	ci := &mocks.CacheInterface{}
	ci.On("Set", "merchant:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.merchant.Insert(ctx, &billingpb.Merchant{Id: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MerchantTestSuite) TestMerchant_Update_Ok() {
	assert.NoError(suite.T(), suite.service.merchant.Update(ctx, &billingpb.Merchant{Id: suite.merchant.Id}))
}

func (suite *MerchantTestSuite) TestMerchant_Update_NotFound() {
	err := suite.service.merchant.Update(ctx, &billingpb.Merchant{Id: primitive.NewObjectID().Hex()})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "mongo: no documents in result")
}

func (suite *MerchantTestSuite) TestMerchant_Update_ErrorCacheUpdate() {
	id := primitive.NewObjectID().Hex()
	ci := &mocks.CacheInterface{}
	ci.On("Set", "merchant:id:"+id, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	_ = suite.service.merchant.Insert(ctx, &billingpb.Merchant{Id: id})
	err := suite.service.merchant.Update(ctx, &billingpb.Merchant{Id: id})

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MerchantTestSuite) TestMerchant_GetById_Ok() {
	merchant := &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
	}
	if err := suite.service.merchant.Insert(ctx, merchant); err != nil {
		suite.Assert().NoError(err)
	}
	c, err := suite.service.merchant.GetById(ctx, merchant.Id)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), merchant.Id, c.Id)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_Ok_ByCache() {
	ci := &mocks.CacheInterface{}
	ci.On("Get", "merchant:id:"+suite.merchant.Id, mock2.Anything).
		Return(nil)
	suite.service.cacher = ci
	c, err := suite.service.merchant.GetById(ctx, suite.merchant.Id)

	assert.Nil(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.Merchant{}, c)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_Ok() {
	pm, err := suite.service.merchant.GetPaymentMethod(ctx, suite.merchant.Id, suite.pmBankCard.Id)

	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MerchantPaymentMethod{}, pm)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_ErrorByMerchantNotFound() {
	_, err := suite.service.merchant.GetPaymentMethod(ctx, primitive.NewObjectID().Hex(), "")

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, merchantErrorNotFound)
}
