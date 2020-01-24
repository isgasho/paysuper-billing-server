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

type ProjectTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *projectRepository
	log        *zap.Logger
}

func Test_Project(t *testing.T) {
	suite.Run(t, new(ProjectTestSuite))
}

func (suite *ProjectTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &projectRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *ProjectTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *ProjectTestSuite) TestProject_NewProjectRepository_Ok() {
	repository := NewProjectRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &projectRepository{}, repository)
}

func (suite *ProjectTestSuite) TestProject_Insert_Ok() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project2, err := suite.repository.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, project2.Id)
	assert.Equal(suite.T(), project.MerchantId, project2.MerchantId)
	assert.Equal(suite.T(), project.Status, project2.Status)
	assert.Equal(suite.T(), project.Name, project2.Name)
}

func (suite *ProjectTestSuite) TestProject_Insert_SkipErrorCacheUpdate() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Insert_ErrorDb() {
	project := suite.getProjectTemplate()
	project.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), project)
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_MultipleInsert_Ok() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.Project{project})
	assert.NoError(suite.T(), err)

	project2, err := suite.repository.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, project2.Id)
	assert.Equal(suite.T(), project.MerchantId, project2.MerchantId)
	assert.Equal(suite.T(), project.Status, project2.Status)
	assert.Equal(suite.T(), project.Name, project2.Name)
}

func (suite *ProjectTestSuite) TestProject_MultipleInsert_ErrorDb() {
	project := suite.getProjectTemplate()
	project.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.MultipleInsert(context.TODO(), []*billingpb.Project{project})
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Update_Ok() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project.Status = 2
	err = suite.repository.Update(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project2, err := suite.repository.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, project2.Id)
	assert.Equal(suite.T(), project.MerchantId, project2.MerchantId)
	assert.Equal(suite.T(), project.Status, project2.Status)
	assert.Equal(suite.T(), project.Name, project2.Name)
}

func (suite *ProjectTestSuite) TestProject_Update_InvalidId() {
	project := suite.getProjectTemplate()
	project.Id = "id"

	err := suite.repository.Update(context.TODO(), project)
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Update_ErrorDb() {
	project := suite.getProjectTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("ReplaceOne", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	err := suite.repository.Update(context.TODO(), project)
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Update_ErrorCacheUpdate() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	err = suite.repository.Update(context.TODO(), project)
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_GetById_Ok() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project2, err := suite.repository.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, project2.Id)
	assert.Equal(suite.T(), project.MerchantId, project2.MerchantId)
	assert.Equal(suite.T(), project.Status, project2.Status)
	assert.Equal(suite.T(), project.Name, project2.Name)
}

func (suite *ProjectTestSuite) TestProject_GetById_OkByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", mock.Anything, mock.Anything).Return(nil)
	suite.repository.cache = cache

	obj, err := suite.repository.GetById(context.TODO(), "id")
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billingpb.Project{}, obj)
}

func (suite *ProjectTestSuite) TestProject_GetById_InvalidId() {
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, "id")
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_GetById_NotFound() {
	id := primitive.NewObjectID().Hex()
	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, id)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetById(context.TODO(), id)
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_GetById_SkipErrorUpdateCache() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Get", key, mock.Anything).Return(errors.New("error"))
	cache.On("Set", key, mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project2, err := suite.repository.GetById(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), project.Id, project2.Id)
	assert.Equal(suite.T(), project.MerchantId, project2.MerchantId)
	assert.Equal(suite.T(), project.Status, project2.Status)
	assert.Equal(suite.T(), project.Name, project2.Name)
}

func (suite *ProjectTestSuite) TestProject_CountByMerchantId_Ok() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.CountByMerchantId(context.TODO(), project.MerchantId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), count)
}

func (suite *ProjectTestSuite) TestProject_CountByMerchantId_NotFound() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.CountByMerchantId(context.TODO(), project.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), count)
}

func (suite *ProjectTestSuite) TestProject_CountByMerchantId_InvalidId() {
	count, err := suite.repository.CountByMerchantId(context.TODO(), "id")
	assert.Error(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *ProjectTestSuite) TestProject_CountByMerchantId_ErrorDb() {
	project := suite.getProjectTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	count, err := suite.repository.CountByMerchantId(context.TODO(), project.MerchantId)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), int64(0), count)
}

func (suite *ProjectTestSuite) TestProject_Find_OkByMerchantId() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.Find(context.TODO(), project.MerchantId, "", []int32{}, 0, 1, []string{})
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), project.Id, list[0].Id)
	assert.Equal(suite.T(), project.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), project.Status, list[0].Status)
	assert.Equal(suite.T(), project.Name, list[0].Name)
}

func (suite *ProjectTestSuite) TestProject_Find_ErrorMerchantId() {
	_, err := suite.repository.Find(context.TODO(), "id", "", []int32{}, 0, 1, []string{})
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Find_OkByQuickSearch() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.Find(context.TODO(), "", project.Name["en"], []int32{}, 0, 1, []string{})
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), project.Id, list[0].Id)
	assert.Equal(suite.T(), project.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), project.Status, list[0].Status)
	assert.Equal(suite.T(), project.Name, list[0].Name)
}

func (suite *ProjectTestSuite) TestProject_Find_OkByStatuses() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.Find(context.TODO(), "", "", []int32{project.Status}, 0, 1, []string{})
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), project.Id, list[0].Id)
	assert.Equal(suite.T(), project.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), project.Status, list[0].Status)
	assert.Equal(suite.T(), project.Name, list[0].Name)
}

func (suite *ProjectTestSuite) TestProject_Find_Sort() {
	project := suite.getProjectTemplate()
	project2 := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	key2 := fmt.Sprintf(cacheProjectId, project2.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Set", key2, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	project2.MerchantId = project.MerchantId
	project2.Status = 3
	err = suite.repository.Insert(context.TODO(), project2)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.Find(context.TODO(), project.MerchantId, "", []int32{}, 0, 10, []string{"-status"})
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), list)
	assert.Len(suite.T(), list, 2)
	assert.Equal(suite.T(), project2.Id, list[0].Id)
	assert.Equal(suite.T(), project2.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), project2.Status, list[0].Status)
	assert.Equal(suite.T(), project2.Name, list[0].Name)
	assert.Equal(suite.T(), project.Id, list[1].Id)
	assert.Equal(suite.T(), project.MerchantId, list[1].MerchantId)
	assert.Equal(suite.T(), project.Status, list[1].Status)
	assert.Equal(suite.T(), project.Name, list[1].Name)
}

func (suite *ProjectTestSuite) TestProject_Find_ErrorDb() {
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Aggregate", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.Find(context.TODO(), "", "", []int32{}, 0, 1, []string{})
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_Find_ErrorDbCursor() {
	cursorMock := &mongodbMocks.CursorInterface{}
	cursorMock.On("All", mock.Anything, mock.Anything).Return(errors.New("cursor error"))
	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("Aggregate", mock.Anything, mock.Anything).Return(cursorMock, nil)
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	_, err := suite.repository.Find(context.TODO(), "", "", []int32{}, 0, 1, []string{})
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_FindCount_OkByMerchantId() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.FindCount(context.TODO(), project.MerchantId, "", []int32{})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), int64(1), count)
}

func (suite *ProjectTestSuite) TestProject_FindCount_ErrorMerchantId() {
	_, err := suite.repository.FindCount(context.TODO(), "id", "", []int32{})
	assert.Error(suite.T(), err)
}

func (suite *ProjectTestSuite) TestProject_FindCount_OkByQuickSearch() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.FindCount(context.TODO(), "", project.Name["en"], []int32{})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 1, count)
}

func (suite *ProjectTestSuite) TestProject_FindCount_OkByStatuses() {
	project := suite.getProjectTemplate()

	cache := &mocks.CacheInterface{}
	key := fmt.Sprintf(cacheProjectId, project.Id)
	cache.On("Set", key, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), project)
	assert.NoError(suite.T(), err)

	count, err := suite.repository.FindCount(context.TODO(), "", "", []int32{project.Status})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), 1, count)
}

func (suite *ProjectTestSuite) TestProject_FindCount_ErrorDb() {
	project := suite.getProjectTemplate()

	collectionMock := &mongodbMocks.CollectionInterface{}
	collectionMock.On("CountDocuments", mock.Anything, mock.Anything).Return(int64(0), errors.New("error"))
	dbMock := &mongodbMocks.SourceInterface{}
	dbMock.On("Collection", mock.Anything).Return(collectionMock, nil)
	suite.repository.db = dbMock

	count, err := suite.repository.FindCount(context.TODO(), "", "", []int32{project.Status})
	assert.Error(suite.T(), err)
	assert.EqualValues(suite.T(), 0, count)
}

func (suite *ProjectTestSuite) getProjectTemplate() *billingpb.Project {
	return &billingpb.Project{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Status:     1,
		Name:       map[string]string{"en": "Name"},
	}
}
