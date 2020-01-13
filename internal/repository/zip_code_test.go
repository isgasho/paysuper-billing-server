package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	mongodbMocks "gopkg.in/paysuper/paysuper-database-mongo.v2/mocks"
	"testing"
	"time"
)

type ZipCodeTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *zipCodeRepository
	log        *zap.Logger
}

func Test_ZipCode(t *testing.T) {
	suite.Run(t, new(ZipCodeTestSuite))
}

func (suite *ZipCodeTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &zipCodeRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *ZipCodeTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *ZipCodeTestSuite) TestZipCode_NewZipCodeRepository_Ok() {
	repository := NewZipCodeRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &zipCodeRepository{}, repository)
}

func (suite *ZipCodeTestSuite) TestZipCode_Insert_Ok() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	zipCode2, err := suite.repository.GetByZipAndCountry(context.TODO(), zipCode.Zip, zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), zipCode.Zip, zipCode2.Zip)
	assert.Equal(suite.T(), zipCode.Country, zipCode2.Country)
}

func (suite *ZipCodeTestSuite) TestZipCode_Insert_ErrorDb() {
	zipCode := suite.getZipCodeTemplate()
	zipCode.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.Error(suite.T(), err)
}

func (suite *ZipCodeTestSuite) TestZipCode_Insert_SkipErrorCache() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)
}

func (suite *ZipCodeTestSuite) TestZipCode_GetByZipAndCountry_NotFound() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Country, zipCode.Zip), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	zipCode2, err := suite.repository.GetByZipAndCountry(context.TODO(), zipCode.Country, zipCode.Zip)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), zipCode2)
}

func (suite *ZipCodeTestSuite) TestZipCode_GetByZipAndCountry_SkipGetByCacheError() {
	zipCode := suite.getZipCodeTemplate()
	key := fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country)

	cache := &mocks.CacheInterface{}
	cache.On("Set", key, zipCode, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	zipCode2, err := suite.repository.GetByZipAndCountry(context.TODO(), zipCode.Zip, zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), zipCode.Zip, zipCode2.Zip)
	assert.Equal(suite.T(), zipCode.Country, zipCode2.Country)
}

func (suite *ZipCodeTestSuite) TestZipCode_GetByZipAndCountry_ReturnByCache() {
	zipCode := suite.getZipCodeTemplate()
	key := fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country)

	cache := &mocks.CacheInterface{}
	cache.On("Get", key, &billing.ZipCode{}).Return(nil)
	suite.repository.cache = cache

	zipCode2, err := suite.repository.GetByZipAndCountry(context.TODO(), zipCode.Zip, zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.ZipCode{}, zipCode2)
}

func (suite *ZipCodeTestSuite) TestZipCode_GetByZipAndCountry_SkipSetToCacheError() {
	zipCode := suite.getZipCodeTemplate()
	key := fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country)

	cache := &mocks.CacheInterface{}
	cache.On("Set", key, zipCode, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, &billing.ZipCode{}).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	zipCode2, err := suite.repository.GetByZipAndCountry(context.TODO(), zipCode.Zip, zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), zipCode.Zip, zipCode2.Zip)
	assert.Equal(suite.T(), zipCode.Country, zipCode2.Country)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_Ok() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), "98", zipCode.Country, 0, 10)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), zipCode.Zip, list[0].Zip)
	assert.Equal(suite.T(), zipCode.Country, list[0].Country)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_EmptyByOffset() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), zipCode.Zip, zipCode.Country, 1, 10)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 0)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_EmptyByZip() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), "123", zipCode.Country, 0, 10)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 0)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_EmptyByCountry() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), zipCode.Zip, "UA", 0, 10)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 0)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_ErrorDb() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), zipCode.Zip, "UA", 0, 10)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), list)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipAndCountry_ErrorDbCursor() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(cursorMock, nil)

	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByZipAndCountry(context.TODO(), zipCode.Zip, "UA", 0, 10)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), list)
}

func (suite *ZipCodeTestSuite) TestZipCode_CountByZip_Ok() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.CountByZip(context.TODO(), "98", zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), cnt)
}

func (suite *ZipCodeTestSuite) TestZipCode_CountByZip_EmptyByZip() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.CountByZip(context.TODO(), "123", zipCode.Country)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *ZipCodeTestSuite) TestZipCode_CountByZip_EmptyByCountry() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.CountByZip(context.TODO(), zipCode.Zip, "UA")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *ZipCodeTestSuite) TestZipCode_CountByZip_ErrorDb() {
	zipCode := suite.getZipCodeTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country), zipCode, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("InsertOne", mock.Anything, mock.Anything).Return(nil, nil)
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Insert(context.TODO(), zipCode)
	assert.NoError(suite.T(), err)

	cnt, err := suite.repository.CountByZip(context.TODO(), zipCode.Zip, "UA")
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), int64(0), cnt)
}

func (suite *ZipCodeTestSuite) getZipCodeTemplate() *billing.ZipCode {
	return &billing.ZipCode{
		Zip:     "98001",
		Country: "US",
		City:    "Washington",
		State: &billing.ZipCodeState{
			Code: "NJ",
			Name: "New Jersey",
		},
		CreatedAt: ptypes.TimestampNow(),
	}
}
