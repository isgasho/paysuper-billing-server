package service

import (
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
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
	if err := suite.service.merchantRepository.Insert(ctx, suite.merchant); err != nil {
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

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_Ok() {
	pm, err := suite.service.getMerchantPaymentMethod(ctx, suite.merchant.Id, suite.pmBankCard.Id)

	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MerchantPaymentMethod{}, pm)
}

func (suite *MerchantTestSuite) TestMerchant_GetPaymentMethod_ErrorByMerchantNotFound() {
	_, err := suite.service.getMerchantPaymentMethod(ctx, primitive.NewObjectID().Hex(), "")

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, merchantErrorNotFound)
}
