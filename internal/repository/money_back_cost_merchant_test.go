package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
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

type MoneyBackCostMerchantTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *moneyBackCostMerchantRepository
	log        *zap.Logger
}

func Test_MoneyBackCostMerchant(t *testing.T) {
	suite.Run(t, new(MoneyBackCostMerchantTestSuite))
}

func (suite *MoneyBackCostMerchantTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &moneyBackCostMerchantRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *MoneyBackCostMerchantTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_NewMoneyBackCostMerchantRepository_Ok() {
	repository := NewMoneyBackCostMerchantRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &moneyBackCostMerchantRepository{}, repository)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Insert_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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
	assert.Equal(suite.T(), cost.MerchantId, cost2.MerchantId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, cost2.IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Insert_SkipErrorCacheUpdate() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Insert_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()
	cost.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_MultipleInsert_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()
	cost.Id = ""

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", mock.Anything).Return(nil)
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostMerchant{cost})
	assert.NoError(suite.T(), err)

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), cost.Id, cost2.Id)
	assert.Equal(suite.T(), cost.MerchantId, cost2.MerchantId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, cost2.IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_MultipleInsert_SkipErrorCacheUpdate() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
	cache.On("Delete", key1).Return(errors.New("error"))
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key4, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostMerchant{cost})
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_MultipleInsert_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()
	cost.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostMerchant{cost})
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Update_InvalidId() {
	cost := suite.getMoneyBackCostMerchantTemplate()
	cost.Id = "id"

	err := suite.repository.Update(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Update_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Update(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Update_ErrorUpdateCache() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Update_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

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
	assert.Equal(suite.T(), cost.MerchantId, cost2.MerchantId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, cost2.IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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
	assert.Equal(suite.T(), cost.MerchantId, cost2.MerchantId)
	assert.Equal(suite.T(), cost.IsActive, cost2.IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2.MccCode)
	assert.Equal(suite.T(), cost.Region, cost2.Region)
	assert.Equal(suite.T(), cost.Country, cost2.Country)
	assert.Equal(suite.T(), cost.Name, cost2.Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2.DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2.FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2.FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, cost2.IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, cost2.PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2.PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2.Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2.UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_NotFound() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	cost2, err := suite.repository.GetById(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_InvalidId() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()

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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_SkipErrorSetCache() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetById_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, &billingpb.MoneyBackCostMerchant{}).Return(nil)
	suite.repository.cache = cache

	obj, err := suite.repository.GetById(context.TODO(), "id")
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MoneyBackCostMerchant{}, obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

	list, err := suite.repository.GetAllForMerchant(context.TODO(), cost.MerchantId)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.NotEmpty(suite.T(), list.Items)
	assert.Len(suite.T(), list.Items, 1)
	assert.Equal(suite.T(), cost.MerchantId, list.Items[0].MerchantId)
	assert.Equal(suite.T(), cost.IsActive, list.Items[0].IsActive)
	assert.Equal(suite.T(), cost.MccCode, list.Items[0].MccCode)
	assert.Equal(suite.T(), cost.Region, list.Items[0].Region)
	assert.Equal(suite.T(), cost.Country, list.Items[0].Country)
	assert.Equal(suite.T(), cost.Name, list.Items[0].Name)
	assert.Equal(suite.T(), cost.DaysFrom, list.Items[0].DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, list.Items[0].FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, list.Items[0].FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, list.Items[0].IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, list.Items[0].PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, list.Items[0].PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, list.Items[0].Percent)
	assert.Equal(suite.T(), cost.UndoReason, list.Items[0].UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_NotFound() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	cost2, err := suite.repository.GetAllForMerchant(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_InvalidId() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetAllForMerchant(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cost2, err := suite.repository.GetAllForMerchant(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_ErrorDbCursor() {
	cost := suite.getMoneyBackCostMerchantTemplate()

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

	cost2, err := suite.repository.GetAllForMerchant(context.TODO(), cost.Id)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), cost2)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_SkipErrorSetCache() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.MerchantId)
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

	_, err = suite.repository.GetAllForMerchant(context.TODO(), cost.Id)
	assert.NoError(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_GetAllForMerchant_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, &billingpb.MoneyBackCostMerchantList{}).Return(nil)
	suite.repository.cache = cache

	obj, err := suite.repository.GetAllForMerchant(context.TODO(), "id")
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MoneyBackCostMerchantList{}, obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Delete_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Delete_InvalidId() {
	cost := suite.getMoneyBackCostMerchantTemplate()
	cost.Id = "id"

	err := suite.repository.Delete(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Delete_ErrorDb() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Delete(context.TODO(), cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Delete_ErrorCacheUpdate() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
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

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_updateCaches_ErrorByDeleteGroupKeys() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	cache.On("Delete", key).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_updateCaches_ErrorByDeleteMerchantKey() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
	cache.On("Delete", key1).Times(1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_updateCaches_ErrorBSetMerchantKey() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
	cache.On("Delete", key1).Times(1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", key4, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCaches(cost)
	assert.Error(suite.T(), err)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(nil)
	suite.repository.cache = cache

	merchantId := "id"
	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	obj, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), []*internalPkg.MoneyBackCostMerchantSet{}, obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_InvalidId() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	merchantId := "id"
	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	obj, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_ErrorDb() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Aggregate", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	merchantId := primitive.NewObjectID().Hex()
	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	obj, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_ErrorDbCursor() {
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

	merchantId := primitive.NewObjectID().Hex()
	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	obj, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.Error(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_SkipErrorSetCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	merchantId := primitive.NewObjectID().Hex()
	name := "name"
	payoutCurrency := "payoutCurrency"
	undoReason := "undoReason"
	region := "region"
	country := "country"
	mccCode := "mccCode"
	paymentStage := int32(1)
	obj, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), obj)
}

func (suite *MoneyBackCostMerchantTestSuite) TestMoneyBackCostMerchant_Find_Ok() {
	cost := suite.getMoneyBackCostMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key1 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, cost.Country, cost.PaymentStage, cost.MccCode)
	key2 := fmt.Sprintf(cacheMoneyBackCostMerchantKey, cost.MerchantId, cost.Name, cost.PayoutCurrency, cost.UndoReason, cost.Region, "", cost.PaymentStage, cost.MccCode)
	key3 := fmt.Sprintf(cacheMoneyBackCostMerchantAll, cost.MerchantId)
	key4 := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, cost.Id)
	cache.On("Delete", key1).Return(nil)
	cache.On("Delete", key2).Return(nil)
	cache.On("Delete", key3).Return(nil)
	cache.On("Delete", key4).Return(nil)
	cache.On("Set", mock.Anything, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), cost)
	assert.NoError(suite.T(), err)

	merchantId := cost.MerchantId
	name := cost.Name
	payoutCurrency := cost.PayoutCurrency
	undoReason := cost.UndoReason
	region := cost.Region
	country := cost.Country
	mccCode := cost.MccCode
	paymentStage := cost.PaymentStage
	cost2, err := suite.repository.Find(context.TODO(), merchantId, name, payoutCurrency, undoReason, region, country, mccCode, paymentStage)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), cost2)
	assert.NotEmpty(suite.T(), cost2[0])
	assert.NotEmpty(suite.T(), cost2[0].Set)
	assert.NotEmpty(suite.T(), cost2[0].Set[0])
	assert.Equal(suite.T(), cost.Id, cost2[0].Set[0].Id)
	assert.Equal(suite.T(), cost.MerchantId, cost2[0].Set[0].MerchantId)
	assert.Equal(suite.T(), cost.IsActive, cost2[0].Set[0].IsActive)
	assert.Equal(suite.T(), cost.MccCode, cost2[0].Set[0].MccCode)
	assert.Equal(suite.T(), cost.Region, cost2[0].Set[0].Region)
	assert.Equal(suite.T(), cost.Country, cost2[0].Set[0].Country)
	assert.Equal(suite.T(), cost.Name, cost2[0].Set[0].Name)
	assert.Equal(suite.T(), cost.DaysFrom, cost2[0].Set[0].DaysFrom)
	assert.Equal(suite.T(), cost.FixAmount, cost2[0].Set[0].FixAmount)
	assert.Equal(suite.T(), cost.FixAmountCurrency, cost2[0].Set[0].FixAmountCurrency)
	assert.Equal(suite.T(), cost.IsPaidByMerchant, cost2[0].Set[0].IsPaidByMerchant)
	assert.Equal(suite.T(), cost.PaymentStage, cost2[0].Set[0].PaymentStage)
	assert.Equal(suite.T(), cost.PayoutCurrency, cost2[0].Set[0].PayoutCurrency)
	assert.Equal(suite.T(), cost.Percent, cost2[0].Set[0].Percent)
	assert.Equal(suite.T(), cost.UndoReason, cost2[0].Set[0].UndoReason)
}

func (suite *MoneyBackCostMerchantTestSuite) getMoneyBackCostMerchantTemplate() *billingpb.MoneyBackCostMerchant {
	return &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        primitive.NewObjectID().Hex(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
		IsActive:          true,
		MccCode:           billingpb.MccCodeLowRisk,
	}
}
