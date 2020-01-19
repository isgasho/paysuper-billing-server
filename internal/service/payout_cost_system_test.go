package service

import (
	"context"
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

type PayoutCostSystemTestSuite struct {
	suite.Suite
	service            *Service
	log                *zap.Logger
	cache              database.CacheInterface
	PayoutCostSystemId string
}

func Test_PayoutCostSystem(t *testing.T) {
	suite.Run(t, new(PayoutCostSystemTestSuite))
}

func (suite *PayoutCostSystemTestSuite) SetupTest() {
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

	suite.PayoutCostSystemId = primitive.NewObjectID().Hex()

	PayoutCostSystem := &billingpb.PayoutCostSystem{
		Id:                    suite.PayoutCostSystemId,
		IntrabankCostAmount:   0,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   10,
		InterbankCostCurrency: "EUR",
	}

	if err := suite.service.payoutCostSystem.Set(context.TODO(), PayoutCostSystem); err != nil {
		suite.FailNow("Insert PayoutCostSystem test data failed", "%v", err)
	}
}

func (suite *PayoutCostSystemTestSuite) TearDownTest() {
	suite.cache.FlushAll()
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Get_Ok() {
	val, err := suite.service.payoutCostSystem.Get(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Id, suite.PayoutCostSystemId)
	assert.Equal(suite.T(), val.IntrabankCostAmount, float64(0))
	assert.Equal(suite.T(), val.IntrabankCostCurrency, "EUR")
	assert.Equal(suite.T(), val.InterbankCostAmount, float64(10))
	assert.Equal(suite.T(), val.InterbankCostCurrency, "EUR")
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Set_Ok() {
	req := &billingpb.PayoutCostSystem{
		IntrabankCostAmount:   2,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   7,
		InterbankCostCurrency: "USD",
	}
	assert.NoError(suite.T(), suite.service.payoutCostSystem.Set(context.TODO(), req))

	val, err := suite.service.payoutCostSystem.Get(context.TODO())
	assert.NoError(suite.T(), err)
	assert.NotEqual(suite.T(), val.Id, suite.PayoutCostSystemId)
	assert.Equal(suite.T(), val.IntrabankCostAmount, float64(2))
	assert.Equal(suite.T(), val.IntrabankCostCurrency, "EUR")
	assert.Equal(suite.T(), val.InterbankCostAmount, float64(7))
	assert.Equal(suite.T(), val.InterbankCostCurrency, "USD")
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	obj := &billingpb.PayoutCostSystem{
		IntrabankCostAmount:   2,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   7,
		InterbankCostCurrency: "USD",
	}
	ci.On("Set", cachePayoutCostSystemKey, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.payoutCostSystem.Set(context.TODO(), obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}
