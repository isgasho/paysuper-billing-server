package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PriceGroupTestSuite struct {
	suite.Suite
	service      *Service
	log          *zap.Logger
	cache        CacheInterface
	priceGroup   *billing.PriceGroup
	priceGroupId string
}

func Test_PriceGroup(t *testing.T) {
	suite.Run(t, new(PriceGroupTestSuite))
}

func (suite *PriceGroupTestSuite) SetupTest() {
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
	redisdb := mock.NewTestRedis()
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
		mock.NewCurrencyServiceMockOk(),
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.priceGroupId = bson.NewObjectId().Hex()

	suite.priceGroup = &billing.PriceGroup{
		Id:       suite.priceGroupId,
		Currency: "USD",
		IsSimple: true,
		Region:   "",
	}
	if err := suite.service.priceGroup.Insert(suite.priceGroup); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}
}

func (suite *PriceGroupTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PriceGroupTestSuite) TestPriceGroup_TestPriceGroup() {

	pgId := bson.NewObjectId().Hex()

	pgReq := &billing.GetPriceGroupRequest{
		Id: pgId,
	}
	pg := &billing.PriceGroup{}
	err := suite.service.GetPriceGroup(context.TODO(), pgReq, pg)
	assert.EqualError(suite.T(), err, "price_group not found")

	pgRes := &billing.PriceGroup{}

	pg = &billing.PriceGroup{
		Id:       pgId,
		Currency: "USD",
		IsSimple: true,
		Region:   "",
	}
	err = suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))

	pg.Id = pgRes.Id
	err = suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.NoError(suite.T(), err)

	pgReq = &billing.GetPriceGroupRequest{
		Id: pgRes.Id,
	}
	pg2 := billing.PriceGroup{}
	err = suite.service.GetPriceGroup(context.TODO(), pgReq, &pg2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pg2.Currency, "USD")

	pg = &billing.PriceGroup{
		Id:       pgRes.Id,
		Currency: "EUR",
		IsSimple: false,
		Region:   "AFRICA",
	}
	err = suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.NoError(suite.T(), err)

	err = suite.service.GetPriceGroup(context.TODO(), pgReq, &pg2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pg2.Currency, "EUR")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCodeA2_Ok() {
	c, err := suite.service.priceGroup.GetById(suite.priceGroupId)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), suite.priceGroup.Id, c.Id)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCodeA2_NotFound() {
	_, err := suite.service.priceGroup.GetById(bson.NewObjectId().Hex())

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.priceGroup.Insert(&billing.PriceGroup{Currency: "USD"}))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_ErrorCacheUpdate() {
	ci := &mock.CacheInterface{}
	pg := &billing.PriceGroup{
		Id:       bson.NewObjectId().Hex(),
		Currency: "USD",
		IsSimple: true,
		Region:   "",
	}
	key := fmt.Sprintf(cachePriceGroupId, pg.Id)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.priceGroup.Insert(pg)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}
