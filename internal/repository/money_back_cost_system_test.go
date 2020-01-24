package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
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

type MoneyBackCostSystemTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *moneyBackCostSystemRepository
	log        *zap.Logger
}

func Test_MoneyBackCostSystem(t *testing.T) {
	suite.Run(t, new(MoneyBackCostSystemTestSuite))
}

func (suite *MoneyBackCostSystemTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &moneyBackCostSystemRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *MoneyBackCostSystemTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_NewMoneyBackCostSystemRepository_Ok() {
	repository := NewMoneyBackCostSystemRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &moneyBackCostSystemRepository{}, repository)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Insert_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cost.Id, cost2.Id)
	assert.Equal(suite.T(), cost.OperatingCompanyId, cost2.OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Insert_SkipErrorCacheUpdate() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Insert_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost := suite.getMoneyBackCostSystemTemplate()
	err := suite.repository.Insert(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_MultipleInsert_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()
	cost.Id = ""

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostSystem{cost})
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cost.Id, cost2.Id)
	assert.Equal(suite.T(), cost.OperatingCompanyId, cost2.OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_MultipleInsert_SkipErrorCacheUpdate() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostSystem{cost})
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_MultipleInsert_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertMany", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost := suite.getMoneyBackCostSystemTemplate()
	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostSystem{cost})
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Update_InvalidId() {
	cost := suite.getMoneyBackCostSystemTemplate()
	cost.Id = "id"

	err := suite.repository.Update(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Update_ErrorDb() {
	cost := suite.getMoneyBackCostSystemTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Update(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Update_ErrorUpdateCache() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Update(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Update_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost.Name = "TestName"
	err = suite.repository.Update(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cost.Id, cost2.Id)
	assert.Equal(suite.T(), cost.OperatingCompanyId, cost2.OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cost.Id, cost2.Id)
	assert.Equal(suite.T(), cost.OperatingCompanyId, cost2.OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_NotFound() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_InvalidId() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_ErrorDb() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	singleResultMock := &mongodbMocks.SingleResultInterface{}
	singleResultMock.On("Decode", mock.Anything).Return(errors.New("single result error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("FindOne", mock.Anything, mock.Anything).Return(singleResultMock)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_SkipErrorSetCache() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetById_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, &billingpb.MoneyBackCostSystem{}).Return(nil)
	suite.repository.cache = cache

	obj, err := suite.repository.GetById(context.TODO(), "id")
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MoneyBackCostSystem{}, obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key3, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key3, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.NotEmpty(suite.T(), list.Items)
	assert.Len(suite.T(), list.Items, 1)
	assert.Equal(suite.T(), cost.OperatingCompanyId, list.Items[0].OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, list.Items[0].IsActive)
	assert.Equal(suite.T(), cost.MccCode, list.Items[0].MccCode)
	assert.Equal(suite.T(), cost.Region, list.Items[0].Region)
	assert.Equal(suite.T(), cost.Country, list.Items[0].Country)
	assert.Equal(suite.T(), cost.Name, list.Items[0].Name)
	assert.Equal(suite.T(), cost.DaysFrom, list.Items[0].DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, list.Items[0].FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, list.Items[0].FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, list.Items[0].PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, list.Items[0].PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, list.Items[0].Percent)
	assert.Equal(suite.T(), cost.UndoReason, list.Items[0].UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_NotFound() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	cost2, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_ErrorDb() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost2, err := suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_ErrorDbCursor() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost2, err := suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_SkipErrorSetCache() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.OperatingCompanyId)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAll_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, &billingpb.MoneyBackCostSystemList{}).Return(nil)
	suite.repository.cache = cache

	obj, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MoneyBackCostSystemList{}, obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Delete_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cost2)

	err = suite.repository.Delete(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	cost2, err = suite.repository.GetById(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Delete_InvalidId() {
	cost := suite.getMoneyBackCostSystemTemplate()
	cost.Id = "id"

	err := suite.repository.Delete(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Delete_ErrorDb() {
	cost := suite.getMoneyBackCostSystemTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Delete(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Delete_ErrorCacheUpdate() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Times(1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", key1).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	err = suite.repository.Delete(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_updateCaches_ErrorByDeleteGroupKeys() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	cache.On("Delete", key).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_updateCaches_ErrorByDeleteMerchantKey() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Times(1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_updateCaches_ErrorBSetMerchantKey() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Times(1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Find_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(nil)
	suite.repository.cache = cache

	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	operatingCompanyId := primitive.NewObjectID().Hex()
	obj, err := suite.repository.Find(context.TODO(), name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId, paymentStage)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), []*internalPkg.MoneyBackCostSystemSet{}, obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Find_ErrorDb() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Aggregate", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	operatingCompanyId := primitive.NewObjectID().Hex()
	obj, err := suite.repository.Find(context.TODO(), name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId, paymentStage)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Find_ErrorDbCursor() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Aggregate", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	operatingCompanyId := primitive.NewObjectID().Hex()
	obj, err := suite.repository.Find(context.TODO(), name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId, paymentStage)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Find_SkipErrorSetCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	operatingCompanyId := primitive.NewObjectID().Hex()
	obj, err := suite.repository.Find(context.TODO(), name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId, paymentStage)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Find_Ok() {
	cost := suite.getMoneyBackCostSystemTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key2 := fmt.Sprintf(cacheMoneyBackCostSystemKey, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode, cost.OperatingCompanyId)
	key3 := cacheMoneyBackCostSystemAll
	key4 := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	name := cost.Name
	payoutCurrency := cost.PayoutCurrency
	undoReason := cost.UndoReason
	region := cost.Region
	country := cost.Country
	mccCode := cost.MccCode
	paymentStage := cost.PaymentStage
	operatingCompanyId := cost.OperatingCompanyId
	cost2, err := suite.repository.Find(context.TODO(), name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId, paymentStage)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cost2)
	assert.NotEmpty(suite.T(), cost2[0])
	assert.NotEmpty(suite.T(), cost2[0].Set)
	assert.NotEmpty(suite.T(), cost2[0].Set[0])
	assert.Equal(suite.T(), cost.Id, cost2[0].Set[0].Id)
	assert.Equal(suite.T(), cost.OperatingCompanyId, cost2[0].Set[0].OperatingCompanyId)
	assert.Equal(suite.T(), cost.IsActive, cost2[0].Set[0].IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2[0].Set[0].MccCode)
	assert.Equal(suite.T(), cost.Region, cost2[0].Set[0].Region)
	assert.Equal(suite.T(), cost.Country, cost2[0].Set[0].Country)
	assert.Equal(suite.T(), cost.Name, cost2[0].Set[0].Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2[0].Set[0].DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2[0].Set[0].FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2[0].Set[0].FixAmountCurrency)
	assert.Equal(suite.T(), cost.PaymentStage, cost2[0].Set[0].PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2[0].Set[0].PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2[0].Set[0].Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2[0].Set[0].UndoReason)
}

func (suite *MoneyBackCostSystemTestSuite) getMoneyBackCostSystemTemplate() *billingpb.MoneyBackCostSystem {
	return &billingpb.MoneyBackCostSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "VISA",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "AZ",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            3,
		FixAmount:          5,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: primitive.NewObjectID().Hex(),
	}
}
