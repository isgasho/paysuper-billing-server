package service

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type ReportFileTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
}

func Test_ReportFile(t *testing.T) {
	suite.Run(t, new(ReportFileTestSuite))
}

func (suite *ReportFileTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *ReportFileTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *ReportFileTestSuite) TestReportFile_Insert_Ok() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file))
}

func (suite *ReportFileTestSuite) TestReportFile_Update_Error_NotFound() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	err := suite.service.reportFileRepository.Update(file)
	assert.Error(suite.T(), err)
}

func (suite *ReportFileTestSuite) TestReportFile_Update_Ok() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file))

	file.FilePath = "path"
	err := suite.service.reportFileRepository.Update(file)
	assert.NoError(suite.T(), err)

	f, err := suite.service.reportFileRepository.GetById(file.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), file.Id, f.Id)
	assert.Equal(suite.T(), file.FilePath, f.FilePath)
}

func (suite *ReportFileTestSuite) TestReportFile_GetById_Ok() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file))

	f, err := suite.service.reportFileRepository.GetById(file.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), file.Id, f.Id)
}

func (suite *ReportFileTestSuite) TestReportFile_GetById_Error_NotFound() {
	_, err := suite.service.reportFileRepository.GetById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *ReportFileTestSuite) TestReportFile_Delete_Ok() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file))

	err := suite.service.reportFileRepository.Delete(file)
	assert.NoError(suite.T(), err)

	_, err = suite.service.reportFileRepository.GetById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *ReportFileTestSuite) TestReportFile_Delete_Error_NotFound() {
	file := &billing.ReportFile{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	err := suite.service.reportFileRepository.Delete(file)
	assert.Error(suite.T(), err)
}

func (suite *ReportFileTestSuite) TestReportFile_DeleteOldestByDays_Ok() {
	t1, _ := ptypes.TimestampProto(time.Now().AddDate(0, 0, -3))
	file1 := &billing.ReportFile{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		CreatedAt:  t1,
	}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file1))

	t2, _ := ptypes.TimestampProto(time.Now().AddDate(0, 0, -4))
	file2 := &billing.ReportFile{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		CreatedAt:  t2,
	}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file2))

	count, err := suite.service.reportFileRepository.DeleteOldestByDays(3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 2, count)
}

func (suite *ReportFileTestSuite) TestReportFile_DeleteOldestByDays_Error_NotFound() {
	t, _ := ptypes.TimestampProto(time.Now().AddDate(0, 0, -3))
	file := &billing.ReportFile{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		CreatedAt:  t,
	}
	assert.NoError(suite.T(), suite.service.reportFileRepository.Insert(file))

	count, err := suite.service.reportFileRepository.DeleteOldestByDays(4)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, count)
}

func (suite *ReportFileTestSuite) TestReportFile_CreateReportFile_Error_InvalidReportType() {
	req := &grpc.CreateReportFileRequest{
		ReportType: "type",
	}
	res := grpc.CreateReportFileResponse{}
	err := suite.service.CreateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, res.Status)
	assert.Equal(suite.T(), errorReportFileTemplateNotFound, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_CreateReportFile_Error_InvalidFileType() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("Insert", mock2.Anything).Return(errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.CreateReportFileRequest{
		ReportType: pkg.ReportTypeTax,
		FileType:   "type",
	}
	res := grpc.CreateReportFileResponse{}
	err := suite.service.CreateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, res.Status)
	assert.Equal(suite.T(), errorReportFileType, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_CreateReportFile_Error_Insert() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("Insert", mock2.Anything).Return(errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.CreateReportFileRequest{
		ReportType: pkg.ReportTypeTax,
		FileType:   pkg.ReportFileTypePdf,
		PeriodFrom: time.Now().Unix(),
		PeriodTo:   time.Now().Unix(),
	}
	res := grpc.CreateReportFileResponse{}
	err := suite.service.CreateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errorReportFileUnableToCreate, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_CreateReportFile_Error_MessageBroker() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("Insert", mock2.Anything).Return(nil)
	suite.service.reportFileRepository = rfr

	mb := &mock.MessageBrokerInterface{}
	mb.On("Publish", mock2.Anything, mock2.Anything).Return(errors.New("error"))
	suite.service.messageBroker = mb

	req := &grpc.CreateReportFileRequest{
		ReportType: pkg.ReportTypeTax,
		FileType:   pkg.ReportFileTypePdf,
		PeriodFrom: time.Now().Unix(),
		PeriodTo:   time.Now().Unix(),
	}
	res := grpc.CreateReportFileResponse{}
	err := suite.service.CreateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errorReportFileMessageBrokerFailed, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_CreateReportFile_Ok() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("Insert", mock2.Anything).Return(nil)
	suite.service.reportFileRepository = rfr

	mb := &mock.MessageBrokerInterface{}
	mb.On("Publish", mock2.Anything, mock2.Anything).Return(nil)
	suite.service.messageBroker = mb

	req := &grpc.CreateReportFileRequest{
		ReportType: pkg.ReportTypeTax,
		FileType:   pkg.ReportFileTypePdf,
		PeriodFrom: time.Now().Unix(),
		PeriodTo:   time.Now().Unix(),
	}
	res := grpc.CreateReportFileResponse{}
	err := suite.service.CreateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, res.Status)
	assert.NotEmpty(suite.T(), res.FileId)
}

func (suite *ReportFileTestSuite) TestReportFile_UpdateReportFile_Error_NotFound() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(nil, errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.UpdateReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.ResponseError{}
	err := suite.service.UpdateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, res.Status)
	assert.Equal(suite.T(), errorReportFileNotFound, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_UpdateReportFile_Error_Update() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(&billing.ReportFile{}, nil)
	rfr.On("Update", mock2.Anything).Return(errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.UpdateReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.ResponseError{}
	err := suite.service.UpdateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errorReportFileUnableToUpdate, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_UpdateReportFile_Error_Centrifugo() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(&billing.ReportFile{}, nil)
	rfr.On("Update", mock2.Anything).Return(nil)
	suite.service.reportFileRepository = rfr

	ci := &mock.CentrifugoInterface{}
	ci.On("Publish", mock2.Anything, mock2.Anything).Return(errors.New("error"))
	suite.service.centrifugo = ci

	req := &grpc.UpdateReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.ResponseError{}
	err := suite.service.UpdateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errorReportFileCentrifugoNotificationFailed, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_UpdateReportFile_Ok() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(&billing.ReportFile{}, nil)
	rfr.On("Update", mock2.Anything).Return(nil)
	suite.service.reportFileRepository = rfr

	ci := &mock.CentrifugoInterface{}
	ci.On("Publish", mock2.Anything, mock2.Anything).Return(nil)
	suite.service.centrifugo = ci

	req := &grpc.UpdateReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.ResponseError{}
	err := suite.service.UpdateReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, res.Status)
}

func (suite *ReportFileTestSuite) TestReportFile_GetReportFile_Error_NotFound() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(nil, errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.GetReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.GetReportFileResponse{}
	err := suite.service.GetReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, res.Status)
	assert.Equal(suite.T(), errorReportFileNotFound, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_GetReportFile_Ok() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("GetById", mock2.Anything).Return(&billing.ReportFile{}, nil)
	suite.service.reportFileRepository = rfr

	req := &grpc.GetReportFileRequest{
		Id: bson.NewObjectId().Hex(),
	}
	res := grpc.GetReportFileResponse{}
	err := suite.service.GetReportFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, res.Status)
}

func (suite *ReportFileTestSuite) TestReportFile_DeleteOldestReportFiles_Error() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("DeleteOldestByDays", mock2.Anything).Return(0, errors.New("error"))
	suite.service.reportFileRepository = rfr

	req := &grpc.DeleteOldestReportFilesRequest{Days: 1}
	res := grpc.ResponseError{}
	err := suite.service.DeleteOldestReportFiles(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errorReportFileDeleteOldest, res.Message)
}

func (suite *ReportFileTestSuite) TestReportFile_DeleteOldestReportFiles_Ok() {
	rfr := &mock.ReportFileRepositoryInterface{}
	rfr.On("DeleteOldestByDays", mock2.Anything).Return(1, nil)
	suite.service.reportFileRepository = rfr

	req := &grpc.DeleteOldestReportFilesRequest{Days: 1}
	res := grpc.ResponseError{}
	err := suite.service.DeleteOldestReportFiles(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, res.Status)
}
