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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	mongodbMocks "gopkg.in/paysuper/paysuper-database-mongo.v2/mocks"
	"testing"
	"time"
)

type MerchantTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *merchantRepository
	log        *zap.Logger
}

func Test_Merchant(t *testing.T) {
	suite.Run(t, new(MerchantTestSuite))
}

func (suite *MerchantTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &merchantRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *MerchantTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *MerchantTestSuite) TestMerchantBalance_NewPMerchantRepository_Ok() {
	repository := NewMerchantRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &merchantRepository{}, repository)
}

func (suite *MerchantTestSuite) TestMerchant_Insert_Ok() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Insert_ErrorCacheUpdate() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Insert_ErrorDb() {
	merchant := suite.getMerchantTemplate()
	merchant.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_MultipleInsert_Ok() {
	merchants := []*billingpb.Merchant{suite.getMerchantTemplate()}

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchants[0].Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), merchants)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchants[0].Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchants[0].Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchants[0].User)
	assert.Equal(suite.T(), merchants[0].User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchants[0].User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchants[0].User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchants[0].User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchants[0].User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchants[0].Company)
	assert.Equal(suite.T(), merchants[0].Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_MultipleInsert_ErrorDb() {
	merchants := []*billingpb.Merchant{suite.getMerchantTemplate()}
	merchants[0].CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.MultipleInsert(context.TODO(), merchants)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Update_Ok() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant.User.Email = "new@test.com"
	err = suite.repository.Update(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Update_ErrorCacheUpdate() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Update(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Update_ErrorDb() {
	merchant := suite.getMerchantTemplate()
	merchant.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Update(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Update_ErrorId() {
	merchant := suite.getMerchantTemplate()
	merchant.Id = "test"
	err := suite.repository.Update(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_UpdateTariffs_ErrorId() {
	err := suite.repository.UpdateTariffs(context.TODO(), "test", &billingpb.PaymentChannelCostMerchant{})
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_UpdateTariffs_ErrorDb() {
	id := primitive.NewObjectID().Hex()
	tariff := &billingpb.PaymentChannelCostMerchant{
		Name:                    "name",
		Region:                  "region",
		MccCode:                 "mcc",
		MethodFixAmountCurrency: "fix_currency",
		PsFixedFeeCurrency:      "ps_fix_currency",
		MinAmount:               1,
		MethodPercent:           2,
		MethodFixAmount:         3,
		PsFixedFee:              4,
		PsPercent:               5,
		IsActive:                true,
	}

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("UpdateOne", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.UpdateTariffs(context.TODO(), id, tariff)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_UpdateTariffs_ErrorDropCache() {
	id := primitive.NewObjectID().Hex()
	tariff := &billingpb.PaymentChannelCostMerchant{
		Name:                    "name",
		Region:                  "region",
		MccCode:                 "mcc",
		MethodFixAmountCurrency: "fix_currency",
		PsFixedFeeCurrency:      "ps_fix_currency",
		MinAmount:               1,
		MethodPercent:           2,
		MethodFixAmount:         3,
		PsFixedFee:              4,
		PsPercent:               5,
		IsActive:                true,
	}

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("UpdateOne", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, id)
	cache.On("Delete", key).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.UpdateTariffs(context.TODO(), id, tariff)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_UpdateTariffs_UnexistsTariffs() {
	merchant := suite.getMerchantTemplate()
	tariff := &billingpb.PaymentChannelCostMerchant{
		Name:                    "name",
		Region:                  "region",
		MccCode:                 "mcc",
		MethodFixAmountCurrency: "fix_currency",
		PsFixedFeeCurrency:      "ps_fix_currency",
		MinAmount:               1,
		MethodPercent:           2,
		MethodFixAmount:         3,
		PsFixedFee:              4,
		PsPercent:               5,
		IsActive:                true,
	}

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Delete", key).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	err = suite.repository.UpdateTariffs(context.TODO(), merchant.Id, tariff)
	assert.Error(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), merchant2.Tariff)
}

func (suite *MerchantTestSuite) TestMerchant_UpdateTariffs_Ok() {
	merchant := suite.getMerchantTemplate()
	tariff := &billingpb.PaymentChannelCostMerchant{
		Name:                    "name",
		Region:                  "region",
		MccCode:                 "mcc",
		MethodFixAmountCurrency: "fix_currency",
		PsFixedFeeCurrency:      "ps_fix_currency",
		MinAmount:               1,
		MethodPercent:           2,
		MethodFixAmount:         3,
		PsFixedFee:              4,
		PsPercent:               5,
		IsActive:                true,
	}
	merchant.Tariff = &billingpb.MerchantTariff{
		Payment: []*billingpb.MerchantTariffRatesPayment{{
			MethodName:             tariff.Name,
			PayerRegion:            tariff.Region,
			MccCode:                tariff.MccCode,
			MethodFixedFeeCurrency: tariff.MethodFixAmountCurrency,
			PsFixedFeeCurrency:     tariff.PsFixedFeeCurrency,
			MinAmount:              tariff.MinAmount,
			MethodPercentFee:       tariff.MethodPercent,
			MethodFixedFee:         tariff.MethodFixAmount,
			PsFixedFee:             tariff.PsFixedFee,
			PsPercentFee:           tariff.PsPercent,
			IsActive:               false,
		}},
	}

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Delete", key).Return(nil)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	err = suite.repository.UpdateTariffs(context.TODO(), merchant.Id, tariff)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), merchant2.Tariff)
	assert.NotEmpty(suite.T(), merchant2.Tariff.Payment)
	assert.EqualValues(suite.T(), tariff.IsActive, merchant2.Tariff.Payment[0].IsActive)
}

func (suite *MerchantTestSuite) TestMerchant_Upsert_InvalidId() {
	merchant := suite.getMerchantTemplate()
	merchant.Id = "id"

	err := suite.repository.Upsert(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Upsert_Ok_Insert() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Upsert_Ok_Update() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant.Company.Name = "newname"
	err = suite.repository.Upsert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Upsert_DbError() {
	merchant := suite.getMerchantTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Upsert(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Upsert_ErrorSetCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Upsert(context.TODO(), merchant)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_NotFound() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_InvalidId() {
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, "id")
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_ErrorDb() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
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

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), merchant.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_OkWithoutCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_SkipSetCacheError() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetById_OkByCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, billingpb.Merchant{}).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.Merchant{}, merchant2)
}

func (suite *MerchantTestSuite) TestMerchant_GetByUserId_NotFound() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetByUserId(context.TODO(), merchant.User.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetByUserId_ErrorDb() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
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

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetByUserId(context.TODO(), merchant.User.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetByUserId_OkWithoutCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetByUserId(context.TODO(), merchant.User.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.NotEmpty(suite.T(), merchant.User)
	assert.Equal(suite.T(), merchant.User.Id, merchant2.User.Id)
	assert.Equal(suite.T(), merchant.User.Email, merchant2.User.Email)
	assert.Equal(suite.T(), merchant.User.ProfileId, merchant2.User.ProfileId)
	assert.Equal(suite.T(), merchant.User.FirstName, merchant2.User.FirstName)
	assert.Equal(suite.T(), merchant.User.LastName, merchant2.User.LastName)
	assert.NotEmpty(suite.T(), merchant.Company)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_GetByUserId_SkipSetCacheError() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetByUserId(context.TODO(), merchant.User.Id)
	assert.NoError(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetByUserId_OkByCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, billingpb.Merchant{}).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetByUserId(context.TODO(), merchant.User.Id)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.Merchant{}, merchant2)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_NotFound() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantCommonId, merchant.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetCommonById(context.TODO(), merchant.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_InvalidId() {
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantCommonId, "id")
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetCommonById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_ErrorDb() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	key2 := fmt.Sprintf(cacheMerchantCommonId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key2, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	singleResultMock := &mongodbMocks.SingleResultInterface{}
	singleResultMock.On("Decode", mock.Anything).Return(errors.New("single result error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("FindOne", mock.Anything, mock.Anything).Return(singleResultMock)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetCommonById(context.TODO(), merchant.Id)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_OkWithoutCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	key2 := fmt.Sprintf(cacheMerchantCommonId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key2, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetCommonById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), merchant.Id, merchant2.Id)
	assert.Equal(suite.T(), merchant.Company.Name, merchant2.Name)
	assert.Equal(suite.T(), merchant.Banking.Currency, merchant2.Currency)
	assert.Equal(suite.T(), merchant.HasProjects, merchant2.HasProjects)
	assert.Equal(suite.T(), merchant.Status, merchant2.Status)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_SkipSetCacheError() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	key2 := fmt.Sprintf(cacheMerchantCommonId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key2, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetCommonById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetCommonById_OkByCache() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	key2 := fmt.Sprintf(cacheMerchantCommonId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key2, billingpb.MerchantCommon{}).Return(nil)
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchant2, err := suite.repository.GetCommonById(context.TODO(), merchant.Id)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.MerchantCommon{}, merchant2)
}

func (suite *MerchantTestSuite) TestMerchant_GetMerchantsWithAutoPayouts_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.GetMerchantsWithAutoPayouts(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetMerchantsWithAutoPayouts_ErrorDbCursor() {
	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.GetMerchantsWithAutoPayouts(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetMerchantsWithAutoPayouts_Empty() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.GetMerchantsWithAutoPayouts(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 0)
}

func (suite *MerchantTestSuite) TestMerchant_GetMerchantsWithAutoPayouts_NotEmpty() {
	merchant := suite.getMerchantTemplate()
	merchant.ManualPayoutsEnabled = false

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.GetMerchantsWithAutoPayouts(context.TODO())
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 1)
}

func (suite *MerchantTestSuite) TestMerchant_GetAll_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetAll_ErrorDbCursor() {
	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.GetAll(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_GetAll_Empty() {
	merchants, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 0)
}

func (suite *MerchantTestSuite) TestMerchant_GetAll_NotEmpty() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 1)
}

func (suite *MerchantTestSuite) TestMerchant_Find_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.Find(context.TODO(), bson.M{}, []string{}, 0, 0)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Find_ErrorDbCursor() {
	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.Find(context.TODO(), bson.M{}, []string{}, 0, 0)
	assert.Error(suite.T(), err)
}

func (suite *MerchantTestSuite) TestMerchant_Find_NotEmpty() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.Find(context.TODO(), bson.M{"company.name": merchant.Company.Name}, []string{}, 0, 1)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 1)
	assert.Equal(suite.T(), merchants[0].Id, merchant.Id)
	assert.NotEmpty(suite.T(), merchants[0].User)
	assert.Equal(suite.T(), merchants[0].User.Id, merchant.User.Id)
	assert.Equal(suite.T(), merchants[0].User.Email, merchant.User.Email)
	assert.Equal(suite.T(), merchants[0].User.ProfileId, merchant.User.ProfileId)
	assert.Equal(suite.T(), merchants[0].User.FirstName, merchant.User.FirstName)
	assert.Equal(suite.T(), merchants[0].User.LastName, merchant.User.LastName)
	assert.NotEmpty(suite.T(), merchants[0].Company)
	assert.Equal(suite.T(), merchants[0].Company.Name, merchant.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Find_OkSortSkipLimit() {
	merchant1 := suite.getMerchantTemplate()
	merchant1.User.LastName = "LastName1"
	merchant2 := suite.getMerchantTemplate()
	merchant2.User.LastName = "LastName2"

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.Merchant{merchant1, merchant2})
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.Find(context.TODO(), bson.M{"company.name": merchant1.Company.Name}, []string{"-user.last_name"}, 1, 1)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 1)
	assert.Equal(suite.T(), merchants[0].Id, merchant1.Id)
	assert.NotEmpty(suite.T(), merchants[0].User)
	assert.Equal(suite.T(), merchants[0].User.Id, merchant1.User.Id)
	assert.Equal(suite.T(), merchants[0].User.Email, merchant1.User.Email)
	assert.Equal(suite.T(), merchants[0].User.ProfileId, merchant1.User.ProfileId)
	assert.Equal(suite.T(), merchants[0].User.FirstName, merchant1.User.FirstName)
	assert.Equal(suite.T(), merchants[0].User.LastName, merchant1.User.LastName)
	assert.NotEmpty(suite.T(), merchants[0].Company)
	assert.Equal(suite.T(), merchants[0].Company.Name, merchant1.Company.Name)
}

func (suite *MerchantTestSuite) TestMerchant_Find_Empty() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	merchants, err := suite.repository.Find(context.TODO(), bson.M{"company.name": "unknown"}, []string{}, 0, 1)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), merchants)
	assert.Len(suite.T(), merchants, 0)
}

func (suite *MerchantTestSuite) TestMerchant_FindCount_DbError() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	cnt, err := suite.repository.FindCount(context.TODO(), bson.M{})
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *MerchantTestSuite) TestMerchant_FindCount_Empty() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.FindCount(context.TODO(), bson.M{"company.name": "unknown"})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *MerchantTestSuite) TestMerchant_FindCount_Ok() {
	merchant := suite.getMerchantTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), merchant)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.FindCount(context.TODO(), bson.M{"company.name": merchant.Company.Name})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), cnt)
}

func (suite *MerchantTestSuite) getMerchantTemplate() *billingpb.Merchant {
	return &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		User: &billingpb.MerchantUser{
			Id:        primitive.NewObjectID().Hex(),
			Email:     "email@test.com",
			FirstName: "firstname",
			LastName:  "lastname",
			ProfileId: primitive.NewObjectID().Hex(),
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name: "company",
		},
		Banking: &billingpb.MerchantBanking{
			Currency: "RUB",
		},
		ManualPayoutsEnabled: true,
		PaymentMethods: map[string]*billingpb.MerchantPaymentMethod{
			primitive.NewObjectID().Hex(): {
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   primitive.NewObjectID().Hex(),
					Name: "name",
				},
				IsActive: true,
			},
		},
	}
}
