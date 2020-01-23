package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
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

type MerchantBalanceTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *merchantBalanceRepository
	log        *zap.Logger
}

func Test_MerchantBalance(t *testing.T) {
	suite.Run(t, new(MerchantBalanceTestSuite))
}

func (suite *MerchantBalanceTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &merchantBalanceRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *MerchantBalanceTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MerchantBalanceTestSuite) TestTurnover_NewMerchantBalanceRepository_Ok() {
	repository := NewMerchantBalanceRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &merchantBalanceRepository{}, repository)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_Insert_Ok() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), balance.Id, balance2.Id)
	assert.Equal(suite.T(), balance.Currency, balance2.Currency)
	assert.Equal(suite.T(), balance.MerchantId, balance2.MerchantId)
	assert.Equal(suite.T(), balance.Total, balance2.Total)
	assert.Equal(suite.T(), balance.RollingReserve, balance2.RollingReserve)
	assert.Equal(suite.T(), balance.Debit, balance2.Debit)
	assert.Equal(suite.T(), balance.Credit, balance2.Credit)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_Insert_ErrorCacheUpdate() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.Error(suite.T(), err)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_Insert_ErrorDb() {
	balance := suite.getMerchantBalanceTemplate()
	balance.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), balance)
	assert.Error(suite.T(), err)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_Ok() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), balance.Id, balance2.Id)
	assert.Equal(suite.T(), balance.Currency, balance2.Currency)
	assert.Equal(suite.T(), balance.MerchantId, balance2.MerchantId)
	assert.Equal(suite.T(), balance.Total, balance2.Total)
	assert.Equal(suite.T(), balance.RollingReserve, balance2.RollingReserve)
	assert.Equal(suite.T(), balance.Debit, balance2.Debit)
	assert.Equal(suite.T(), balance.Credit, balance2.Credit)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_OkByCache() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, billingpb.MerchantBalance{}).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MerchantBalance{}, balance2)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_Ok_SkipCacheError() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), balance.Id, balance2.Id)
	assert.Equal(suite.T(), balance.Currency, balance2.Currency)
	assert.Equal(suite.T(), balance.MerchantId, balance2.MerchantId)
	assert.Equal(suite.T(), balance.Total, balance2.Total)
	assert.Equal(suite.T(), balance.RollingReserve, balance2.RollingReserve)
	assert.Equal(suite.T(), balance.Debit, balance2.Debit)
	assert.Equal(suite.T(), balance.Credit, balance2.Credit)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_ErrorInvalidId() {
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, "id", "currency")
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	balance, err := suite.repository.GetByIdAndCurrency(context.TODO(), "id", "currency")
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), balance)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_NotFoundByMerchantId() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	id := primitive.NewObjectID().Hex()
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	key2 := fmt.Sprintf(cacheKeyMerchantBalances, id, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(nil)
	cache.On("Get", key2, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), id, balance.Currency)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), balance2)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_GetByIdAndCurrency_NotFoundByCurrency() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	key2 := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, "currency")
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(nil)
	cache.On("Get", key2, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	balance2, err := suite.repository.GetByIdAndCurrency(context.TODO(), balance.MerchantId, "currency")
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), balance2)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_CountByIdAndCurrency_Ok() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.CountByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 1, count)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_CountByIdAndCurrency_ErrorInvalidId() {
	count, err := suite.repository.CountByIdAndCurrency(context.TODO(), "id", "currency")
	assert.Error(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_CountByIdAndCurrency_ErrorDb() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	count, err := suite.repository.CountByIdAndCurrency(context.TODO(), balance.MerchantId, balance.Currency)
	assert.Error(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_CountByIdAndCurrency_NotFoundByMerchantId() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.CountByIdAndCurrency(context.TODO(), primitive.NewObjectID().Hex(), balance.Currency)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *MerchantBalanceTestSuite) TestMerchantBalance_CountByIdAndCurrency_NotFoundByCurrency() {
	balance := suite.getMerchantBalanceTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheKeyMerchantBalances, balance.MerchantId, balance.Currency)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), balance)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.CountByIdAndCurrency(context.TODO(), balance.MerchantId, "currency")
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *MerchantBalanceTestSuite) getMerchantBalanceTemplate() *billingpb.MerchantBalance {
	return &billingpb.MerchantBalance{
		Id:             primitive.NewObjectID().Hex(),
		Currency:       "RUB",
		Credit:         1,
		Debit:          2,
		MerchantId:     primitive.NewObjectID().Hex(),
		RollingReserve: 0,
		Total:          5,
	}
}
