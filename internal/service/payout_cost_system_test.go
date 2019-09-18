package service

import (
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PayoutCostSystemTestSuite struct {
	suite.Suite
	service            *Service
	log                *zap.Logger
	cache              CacheInterface
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
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.PayoutCostSystemId = bson.NewObjectId().Hex()

	PayoutCostSystem := &billing.PayoutCostSystem{
		Id:                    suite.PayoutCostSystemId,
		IntrabankCostAmount:   0,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   10,
		InterbankCostCurrency: "EUR",
	}

	if err := suite.service.payoutCostSystem.Set(PayoutCostSystem); err != nil {
		suite.FailNow("Insert PayoutCostSystem test data failed", "%v", err)
	}
}

func (suite *PayoutCostSystemTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Get_Ok() {
	val, err := suite.service.payoutCostSystem.Get()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Id, suite.PayoutCostSystemId)
	assert.Equal(suite.T(), val.IntrabankCostAmount, float64(0))
	assert.Equal(suite.T(), val.IntrabankCostCurrency, "EUR")
	assert.Equal(suite.T(), val.InterbankCostAmount, float64(10))
	assert.Equal(suite.T(), val.InterbankCostCurrency, "EUR")
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Set_Ok() {
	req := &billing.PayoutCostSystem{
		IntrabankCostAmount:   2,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   7,
		InterbankCostCurrency: "USD",
	}
	assert.NoError(suite.T(), suite.service.payoutCostSystem.Set(req))

	val, err := suite.service.payoutCostSystem.Get()
	assert.NoError(suite.T(), err)
	assert.NotEqual(suite.T(), val.Id, suite.PayoutCostSystemId)
	assert.Equal(suite.T(), val.IntrabankCostAmount, float64(2))
	assert.Equal(suite.T(), val.IntrabankCostCurrency, "EUR")
	assert.Equal(suite.T(), val.InterbankCostAmount, float64(7))
	assert.Equal(suite.T(), val.InterbankCostCurrency, "USD")
}

func (suite *PayoutCostSystemTestSuite) TestPayoutCostSystem_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	obj := &billing.PayoutCostSystem{
		IntrabankCostAmount:   2,
		IntrabankCostCurrency: "EUR",
		InterbankCostAmount:   7,
		InterbankCostCurrency: "USD",
	}
	ci.On("Set", cachePayoutCostSystemKey, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.payoutCostSystem.Set(obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}
