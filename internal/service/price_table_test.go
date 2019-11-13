package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PriceTableTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   internalPkg.CacheInterface
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
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, mocks.NewGeoIpServiceTestOk(), mocks.NewRepositoryServiceOk(), mocks.NewTaxServiceOkMock(), nil, nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), &reportingMocks.ReporterService{}, mocks.NewFormatterOK(), mocks.NewBrokerMockOk(), nil, )

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
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

func (suite *PriceTableTestSuite) TestPriceTable_GetByRegion_Ok() {
	table := &billing.PriceTable{Id: bson.NewObjectId().Hex(), Currency: "TST"}
	assert.NoError(suite.T(), suite.service.priceTable.Insert(table))

	t, err := suite.service.priceTable.GetByRegion(table.Currency)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), table.Id, t.Id)
	assert.Equal(suite.T(), table.Currency, t.Currency)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetByRegion_Error_NotFound() {
	_, err := suite.service.priceTable.GetByRegion("TST")
	assert.Error(suite.T(), err)
}

func (suite *PriceTableTestSuite) TestPriceTable_GetRecommendedPriceTable_Ok() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock.Anything).
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
