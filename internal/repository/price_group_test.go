package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
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

type PriceGroupTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *priceGroupRepository
	log        *zap.Logger
}

func Test_PriceGroup(t *testing.T) {
	suite.Run(t, new(PriceGroupTestSuite))
}

func (suite *PriceGroupTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &priceGroupRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *PriceGroupTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PriceGroupTestSuite) TestTurnover_NewPriceGroupRepository_Ok() {
	repository := NewPriceGroupRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &priceGroupRepository{}, repository)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_Ok() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetById(context.TODO(), group.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), group.Id, group2.Id)
	assert.Equal(suite.T(), group.IsActive, group2.IsActive)
	assert.Equal(suite.T(), group.Currency, group2.Currency)
	assert.Equal(suite.T(), group.Fraction, group2.Fraction)
	assert.Equal(suite.T(), group.Region, group2.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_ErrorCacheUpdate() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_ErrorDb() {
	group := suite.getPriceGroupTemplate()
	group.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), group)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_MultipleInsert_Ok() {
	groups := []*billing.PriceGroup{suite.getPriceGroupTemplate()}

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, groups[0].Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), groups)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetById(context.TODO(), groups[0].Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), groups[0].Id, group2.Id)
	assert.Equal(suite.T(), groups[0].IsActive, group2.IsActive)
	assert.Equal(suite.T(), groups[0].Currency, group2.Currency)
	assert.Equal(suite.T(), groups[0].Fraction, group2.Fraction)
	assert.Equal(suite.T(), groups[0].Region, group2.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_MultipleInsert_ErrorDb() {
	groups := []*billing.PriceGroup{suite.getPriceGroupTemplate()}
	groups[0].CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.MultipleInsert(context.TODO(), groups)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Update_Ok() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group.Fraction = 0.95
	err = suite.repository.Update(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetById(context.TODO(), group.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), group.Id, group2.Id)
	assert.Equal(suite.T(), group.IsActive, group2.IsActive)
	assert.Equal(suite.T(), group.Currency, group2.Currency)
	assert.Equal(suite.T(), group.Fraction, group2.Fraction)
	assert.Equal(suite.T(), group.Region, group2.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Update_ErrorCacheUpdate() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Update(context.TODO(), group)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Update_ErrorDb() {
	group := suite.getPriceGroupTemplate()
	group.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Update(context.TODO(), group)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Update_ErrorId() {
	group := suite.getPriceGroupTemplate()
	group.Id = "test"
	err := suite.repository.Update(context.TODO(), group)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_NotFound() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), group.Id)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_InvalidId() {
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, "id")
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_ErrorDb() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	singleResultMock := &mongodbMocks.SingleResultInterface{}
	singleResultMock.On("Decode", mock.Anything).Return(errors.New("single result error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("FindOne", mock.Anything, mock.Anything).Return(singleResultMock)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), group.Id)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_OkWithoutCache() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetById(context.TODO(), group.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), group.Id, group2.Id)
	assert.Equal(suite.T(), group.IsActive, group2.IsActive)
	assert.Equal(suite.T(), group.Currency, group2.Currency)
	assert.Equal(suite.T(), group.Fraction, group2.Fraction)
	assert.Equal(suite.T(), group.Region, group2.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_SkipSetCacheError() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), group.Id)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_OkByCache() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, billing.PriceGroup{}).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetById(context.TODO(), group.Id)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.PriceGroup{}, group2)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_SkipInactive() {
	group := suite.getPriceGroupTemplate()
	group.IsActive = false

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupId, group.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), group.Id)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_NotFound() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cachePriceGroupRegion, group.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetByRegion(context.TODO(), group.Id)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_ErrorDb() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	singleResultMock := &mongodbMocks.SingleResultInterface{}
	singleResultMock.On("Decode", mock.Anything).Return(errors.New("single result error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("FindOne", mock.Anything, mock.Anything).Return(singleResultMock)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetByRegion(context.TODO(), group.Region)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_OkWithoutCache() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetByRegion(context.TODO(), group.Region)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), group.Id, group2.Id)
	assert.Equal(suite.T(), group.IsActive, group2.IsActive)
	assert.Equal(suite.T(), group.Currency, group2.Currency)
	assert.Equal(suite.T(), group.Fraction, group2.Fraction)
	assert.Equal(suite.T(), group.Region, group2.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_SkipSetCacheError() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetByRegion(context.TODO(), group.Region)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_OkByCache() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", fmt.Sprintf(cachePriceGroupRegion, group.Region), billing.PriceGroup{}).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	group2, err := suite.repository.GetByRegion(context.TODO(), group.Region)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.PriceGroup{}, group2)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_SkipInactive() {
	group := suite.getPriceGroupTemplate()
	group.IsActive = false

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetByRegion(context.TODO(), group.Region)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cachePriceGroupAll, []*billing.PriceGroup{}).Return(nil)
	suite.repository.cache = cache

	_, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_OkWithoutCache() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", cachePriceGroupAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cachePriceGroupAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), group.Id, list[0].Id)
	assert.Equal(suite.T(), group.IsActive, list[0].IsActive)
	assert.Equal(suite.T(), group.Currency, list[0].Currency)
	assert.Equal(suite.T(), group.Fraction, list[0].Fraction)
	assert.Equal(suite.T(), group.Region, list[0].Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_SkipSetChaneError() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", cachePriceGroupAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cachePriceGroupAll, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_SkipInactive() {
	group := suite.getPriceGroupTemplate()
	group.IsActive = false

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", cachePriceGroupAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cachePriceGroupAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 0)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_DbError() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", cachePriceGroupAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cachePriceGroupAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetAll_DbCursorError() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	cache.On("Get", cachePriceGroupAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cachePriceGroupAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), group)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_updateCache_ErrorById() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	suite.repository.cache = cache

	assert.Error(suite.T(), suite.repository.updateCache(group))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_updateCache_ErrorByRegion() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(errors.New("error"))
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	suite.repository.cache = cache

	assert.Error(suite.T(), suite.repository.updateCache(group))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_updateCache_ErrorByAll() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(errors.New("error"))
	suite.repository.cache = cache

	assert.Error(suite.T(), suite.repository.updateCache(group))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_updateCache_Ok() {
	group := suite.getPriceGroupTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cachePriceGroupId, group.Id), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", fmt.Sprintf(cachePriceGroupRegion, group.Region), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cachePriceGroupAll).Return(nil)
	suite.repository.cache = cache

	assert.NoError(suite.T(), suite.repository.updateCache(group))
}

func (suite *PriceGroupTestSuite) getPriceGroupTemplate() *billing.PriceGroup {
	return &billing.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
		IsActive: true,
		Fraction: 0.05,
	}
}
