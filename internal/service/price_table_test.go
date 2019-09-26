package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PriceTableTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
}

func Test_PriceTable(t *testing.T) {
	suite.Run(t, new(PriceTableTestSuite))
}

func (suite *PriceTableTestSuite) SetupTest() {
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
	suite.service = NewBillingService(db, cfg, mocks.NewGeoIpServiceTestOk(), mocks.NewRepositoryServiceOk(), mocks.NewTaxServiceOkMock(), nil, nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), nil, nil)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	priceTable := &billing.PriceTable{
		Id:   bson.NewObjectId().Hex(),
		From: 0,
		To:   3,
		Currencies: map[string]*billing.PriceTableCurrency{
			"USD": {From: float64(0), To: float64(10)},
		},
	}
	if err := suite.service.priceTable.Insert(priceTable); err != nil {
		suite.FailNow("Insert price table test data failed", "%v", err)
	}
}

func (suite *PriceTableTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PriceTableTestSuite) TestPriceTable_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.priceTable.Insert(&billing.PriceTable{Id: bson.NewObjectId().Hex()}))
}

func (suite *PriceTableTestSuite) TestPriceTable_GetByAmount_Ok() {
	price, err := suite.service.priceTable.GetByAmount(2)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), price.From <= 2)
	assert.True(suite.T(), price.To >= 2)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetByAmount_Error_NotFound() {
	_, err := suite.service.priceTable.GetByAmount(1000)
	assert.Error(suite.T(), err)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetLatest_Ok() {
	price, err := suite.service.priceTable.GetLatest()
	assert.NoError(suite.T(), err)

	_, err = suite.service.priceTable.GetByAmount(price.To + 1)
	assert.Error(suite.T(), err)
}

func (suite *PriceTableTestSuite) TestPriceTable_InterpolateByAmount_Ok() {
	price := &billing.PriceTable{
		From: 10,
		To:   20,
		Currencies: map[string]*billing.PriceTableCurrency{
			"RUB": {From: 100, To: 150},
		},
	}
	priceInterpolated := suite.service.priceTable.InterpolateByAmount(price, 25)
	assert.Equal(suite.T(), float64(20), priceInterpolated.From)
	assert.Equal(suite.T(), float64(30), priceInterpolated.To)
	assert.Equal(suite.T(), float64(150), priceInterpolated.Currencies["RUB"].From)
	assert.Equal(suite.T(), float64(200), priceInterpolated.Currencies["RUB"].To)
}
