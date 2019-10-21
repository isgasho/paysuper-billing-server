package service

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	casbinProto "github.com/paysuper/casbin-server/pkg/generated/api/proto/casbinpb"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type UsersTestSuite struct {
	suite.Suite
	service *Service
	cache   CacheInterface

	merchant  *billing.Merchant
	user      *billing.UserRoleProfile
	adminUser *billing.UserRoleProfile
}

func Test_Users(t *testing.T) {
	suite.Run(t, new(UsersTestSuite))
}

func (suite *UsersTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		mocks.NewBrokerMockOk(),
		mocks.NewTestRedis(),
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		nil,
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
	)

	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *UsersTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *UsersTestSuite) TestGetUsers_Error() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForMerchant", mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: bson.NewObjectId().Hex()}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestGetUsers_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForMerchant", mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: bson.NewObjectId().Hex()}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) TestGetAdminUsers_Error() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForAdmin", mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetAdminUsersResponse{}
	err := suite.service.GetAdminUsers(context.TODO(), &grpc.EmptyRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestGetAdminUsers_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForAdmin", mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetAdminUsersResponse{}
	err := suite.service.GetAdminUsers(context.TODO(), &grpc.EmptyRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Error_GetMerchantsForUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Error_GetById() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything).Return([]*billing.UserRole{{Id: bson.NewObjectId().Hex()}}, nil)
	suite.service.userRoleRepository = repository

	repositoryM := &mocks.MerchantRepositoryInterface{}
	repositoryM.
		On("GetById", mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.merchant = repositoryM

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Ok_Empty() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
	shouldBe.Empty(res.Merchants)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Ok_NotEmpty() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything).Return([]*billing.UserRole{{Id: bson.NewObjectId().Hex()}}, nil)
	suite.service.userRoleRepository = repository

	repositoryM := &mocks.MerchantRepositoryInterface{}
	repositoryM.
		On("GetById", mock.Anything).
		Return(&billing.Merchant{Id: bson.NewObjectId().Hex(), Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = repositoryM

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
	shouldBe.NotEmpty(res.Merchants)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Error_GetUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetAdminUserByUserId", mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Error_ExistRole() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetAdminUserByUserId", mock.Anything).Return(&billing.UserRole{Role: "test_role"}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Error_Update() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetAdminUserByUserId", mock.Anything).Return(&billing.UserRole{Role: pkg.RoleSystemAdmin}, nil)
	repository.On("GetAdminUserById", mock.Anything).Return(&billing.UserRole{Role: "test"}, nil)
	repository.On("UpdateAdminUser", mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserByUserId", mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateAdminUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Error_AddRoleForUserCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserByUserId", mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateAdminUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserByUserId", mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateAdminUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Error_GetUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantUserByUserId", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Error_ExistRole() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantUserByUserId", mock.Anything, mock.Anything).Return(&billing.UserRole{Role: "test_role"}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Error_Update() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantUserByUserId", mock.Anything, mock.Anything).Return(&billing.UserRole{Role: pkg.RoleMerchantOwner}, nil)
	repository.On("GetMerchantUserById", mock.Anything).Return(&billing.UserRole{Role: "test"}, nil)
	repository.On("UpdateMerchantUser", mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserByUserId", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateMerchantUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Error_AddRoleForUserCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserByUserId", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateMerchantUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserByUserId", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", User: &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}}, nil)
	repository.On("UpdateMerchantUser", mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		UserId: bson.NewObjectId().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}
