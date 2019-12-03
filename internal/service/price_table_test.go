package service

import (
	"context"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
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

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache, err = NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
		mocks.NewNotifierOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *PriceTableTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PriceTableTestSuite) TestPriceTable_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.priceTable.Insert(context.TODO(), &billing.PriceTable{Id: primitive.NewObjectID().Hex()}))
}

func (suite *PriceTableTestSuite) TestPriceTable_GetByRegion_Ok() {
	table := &billing.PriceTable{Id: primitive.NewObjectID().Hex(), Currency: "TST"}
	assert.NoError(suite.T(), suite.service.priceTable.Insert(context.TODO(), table))

	t, err := suite.service.priceTable.GetByRegion(context.TODO(), table.Currency)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), table.Id, t.Id)
	assert.Equal(suite.T(), table.Currency, t.Currency)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetByRegion_Error_NotFound() {
	_, err := suite.service.priceTable.GetByRegion(context.TODO(), "TST")
	assert.Error(suite.T(), err)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetRecommendedPriceTable_Ok() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock.Anything, mock.Anything).
		Return(&billing.PriceTable{Ranges: []*billing.PriceTableRange{{From: 0, To: 0, Position: 0}}}, nil)
	suite.service.priceTable = rep

	res := grpc.RecommendedPriceTableResponse{}
	err := suite.service.GetRecommendedPriceTable(context.TODO(), &grpc.RecommendedPriceTableRequest{}, &res)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res.Ranges, 1)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetRecommendedPriceTable_Ok_Empty() {
	res := grpc.RecommendedPriceTableResponse{}
	err := suite.service.GetRecommendedPriceTable(context.TODO(), &grpc.RecommendedPriceTableRequest{}, &res)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res.Ranges, 0)
}
