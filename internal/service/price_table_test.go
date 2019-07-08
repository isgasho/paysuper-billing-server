package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
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

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache)

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

func (suite *PriceTableTestSuite) TestPriceTable_getPriceTableForAmount_Ok_HasRange() {
	price, err := suite.service.priceTable.GetByAmount(2)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), price.From <= 2)
	assert.True(suite.T(), price.To >= 2)
}

func (suite *PriceTableTestSuite) TestPriceTable_getPriceTableForAmount_Ok_Interpolate() {
	price, err := suite.service.priceTable.GetByAmount(10)
	assert.NoError(suite.T(), err)
	assert.True(suite.T(), price.From <= 10)
	assert.True(suite.T(), price.To >= 10)
}

func (suite *PriceTableTestSuite) TestPriceTable_getPriceTableForAmount_Error_NotFound() {
	_ = suite.service.db.Drop()
	_, err := suite.service.priceTable.GetByAmount(10)
	assert.Error(suite.T(), err)
}
