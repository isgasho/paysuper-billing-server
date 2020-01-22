package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	mongodbMocks "gopkg.in/paysuper/paysuper-database-mongo.v2/mocks"
	"testing"
	"time"
)

type TurnoverTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *turnoverRepository
	log        *zap.Logger
}

func Test_Turnover(t *testing.T) {
	suite.Run(t, new(TurnoverTestSuite))
}

func (suite *TurnoverTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &turnoverRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *TurnoverTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *TurnoverTestSuite) TestTurnover_NewTurnoverRepository_Ok() {
	repository := NewTurnoverRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &turnoverRepository{}, repository)
}

func (suite *TurnoverTestSuite) TestTurnover_Upsert_Ok_Insert() {
	turnover := suite.getTurnoverTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	turnover2, err := suite.repository.Get(context.TODO(), turnover.OperatingCompanyId, turnover.Country, int(turnover.Year))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), turnover.OperatingCompanyId, turnover2.OperatingCompanyId)
	assert.Equal(suite.T(), turnover.Country, turnover2.Country)
	assert.Equal(suite.T(), turnover.Year, turnover2.Year)
	assert.Equal(suite.T(), turnover.Currency, turnover2.Currency)
	assert.Equal(suite.T(), turnover.Amount, turnover2.Amount)
}

func (suite *TurnoverTestSuite) TestTurnover_Upsert_Ok_Update() {
	turnover := suite.getTurnoverTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	turnover.Amount = float64(10)
	err = suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	turnover2, err := suite.repository.Get(context.TODO(), turnover.OperatingCompanyId, turnover.Country, int(turnover.Year))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), turnover.OperatingCompanyId, turnover2.OperatingCompanyId)
	assert.Equal(suite.T(), turnover.Country, turnover2.Country)
	assert.Equal(suite.T(), turnover.Year, turnover2.Year)
	assert.Equal(suite.T(), turnover.Currency, turnover2.Currency)
	assert.Equal(suite.T(), turnover.Amount, turnover2.Amount)
}

func (suite *TurnoverTestSuite) TestTurnover_Insert_ErrorDb() {
	turnover := suite.getTurnoverTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.Error(suite.T(), err)
}

func (suite *TurnoverTestSuite) TestTurnover_Insert_SkipErrorCache() {
	turnover := suite.getTurnoverTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)
	cache.On("Set", key, turnover, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)
}

func (suite *TurnoverTestSuite) TestTurnover_Get_NotFound() {
	turnover := suite.getTurnoverTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheTurnoverKey, turnover.Country, turnover.OperatingCompanyId, turnover.Year), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	zipCode2, err := suite.repository.Get(context.TODO(), turnover.Country, turnover.OperatingCompanyId, int(turnover.Year))
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), zipCode2)
}

func (suite *TurnoverTestSuite) TestTurnover_Get_SkipGetByCacheError() {
	turnover := suite.getTurnoverTemplate()
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)

	cache := &mocks.CacheInterface{}
	cache.On("Set", key, turnover, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	turnover2, err := suite.repository.Get(context.TODO(), turnover.OperatingCompanyId, turnover.Country, int(turnover.Year))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), turnover.OperatingCompanyId, turnover2.OperatingCompanyId)
	assert.Equal(suite.T(), turnover.Country, turnover2.Country)
	assert.Equal(suite.T(), turnover.Year, turnover2.Year)
	assert.Equal(suite.T(), turnover.Amount, turnover2.Amount)
	assert.Equal(suite.T(), turnover.Currency, turnover2.Currency)
}

func (suite *TurnoverTestSuite) TestTurnover_Get_ReturnByCache() {
	turnover := suite.getTurnoverTemplate()
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)

	cache := &mocks.CacheInterface{}
	cache.On("Get", key, &billingpb.AnnualTurnover{}).Return(nil)
	suite.repository.cache = cache

	turnover2, err := suite.repository.Get(context.TODO(), turnover.OperatingCompanyId, turnover.Country, int(turnover.Year))
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.AnnualTurnover{}, turnover2)
}

func (suite *TurnoverTestSuite) TestTurnover_Get_SkipSetToCacheError() {
	turnover := suite.getTurnoverTemplate()
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)

	cache := &mocks.CacheInterface{}
	cache.On("Set", key, turnover, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, &billingpb.AnnualTurnover{}).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	turnover2, err := suite.repository.Get(context.TODO(), turnover.OperatingCompanyId, turnover.Country, int(turnover.Year))
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), turnover.OperatingCompanyId, turnover2.OperatingCompanyId)
	assert.Equal(suite.T(), turnover.Country, turnover2.Country)
	assert.Equal(suite.T(), turnover.Year, turnover2.Year)
	assert.Equal(suite.T(), turnover.Amount, turnover2.Amount)
	assert.Equal(suite.T(), turnover.Currency, turnover2.Currency)
}

func (suite *TurnoverTestSuite) TestTurnover_CountAll_ReturnExists() {
	turnover := suite.getTurnoverTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), turnover)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.CountAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), cnt)
}

func (suite *TurnoverTestSuite) TestTurnover_CountAll_ReturnEmpty() {
	cnt, err := suite.repository.CountAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *TurnoverTestSuite) TestTurnover_CountAll_DbError() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cnt, err := suite.repository.CountAll(context.TODO())
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *TurnoverTestSuite) getTurnoverTemplate() *billingpb.AnnualTurnover {
	return &billingpb.AnnualTurnover{
		Currency:           "USD",
		Country:            "US",
		OperatingCompanyId: primitive.NewObjectID().Hex(),
		Amount:             float64(1),
		Year:               2020,
	}
}
