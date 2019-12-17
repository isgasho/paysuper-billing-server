package service

import (
	"context"
	"errors"
	casbinProto "github.com/paysuper/casbin-server/pkg/generated/api/proto/casbinpb"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
)

type UsersTestSuite struct {
	suite.Suite
	service *Service
	cache   CacheInterface
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
	suite.cache, err = NewCacheRedis(redisdb, "cache")
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
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *UsersTestSuite) Test_GetUsers_Error() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForMerchant", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: primitive.NewObjectID().Hex()}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_GetUsers_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForMerchant", mock.Anything, mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: primitive.NewObjectID().Hex()}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_GetAdminUsers_Error() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForAdmin", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetAdminUsersResponse{}
	err := suite.service.GetAdminUsers(context.TODO(), &grpc.EmptyRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_GetAdminUsers_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetUsersForAdmin", mock.Anything, mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetAdminUsersResponse{}
	err := suite.service.GetAdminUsers(context.TODO(), &grpc.EmptyRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_GetMerchantsForUser_Error_GetMerchantsForUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_GetMerchantsForUser_Error_GetById() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything, mock.Anything).Return([]*billing.UserRole{{Id: primitive.NewObjectID().Hex()}}, nil)
	suite.service.userRoleRepository = repository

	repositoryM := &mocks.MerchantRepositoryInterface{}
	repositoryM.
		On("GetById", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.merchant = repositoryM

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_GetMerchantsForUser_Ok_Empty() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything, mock.Anything).Return([]*billing.UserRole{}, nil)
	suite.service.userRoleRepository = repository

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
	shouldBe.Empty(res.Merchants)
}

func (suite *UsersTestSuite) Test_GetMerchantsForUser_Ok_NotEmpty() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantsForUser", mock.Anything, mock.Anything).Return([]*billing.UserRole{{Id: primitive.NewObjectID().Hex()}}, nil)
	suite.service.userRoleRepository = repository

	repositoryM := &mocks.MerchantRepositoryInterface{}
	repositoryM.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Id: primitive.NewObjectID().Hex(), Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = repositoryM

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
	shouldBe.NotEmpty(res.Merchants)
}

func (suite *UsersTestSuite) Test_ChangeAdminUserRole_Error_GetUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetAdminUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeAdminUserRole_Error_Update() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetAdminUserByUserId", mock.Anything, mock.Anything).Return(&billing.UserRole{Role: pkg.RoleSystemAdmin}, nil)
	repository.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{Role: "test"}, nil)
	repository.On("UpdateAdminUser", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeAdminUserRole_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeAdminUserRole_Error_AddRoleForUserCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeAdminUserRole_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Error_SetRoleOwner() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   pkg.RoleMerchantOwner,
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnsupportedRoleType, res.Message)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Error_GetUser() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Error_Update() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.On("GetMerchantUserByUserId", mock.Anything, mock.Anything, mock.Anything).Return(&billing.UserRole{Role: pkg.RoleMerchantOwner}, nil)
	repository.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{Role: "test"}, nil)
	repository.On("UpdateMerchantUser", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = repository

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Error_AddRoleForUserCasbin() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusSystemError, res.Status)
}

func (suite *UsersTestSuite) Test_ChangeRoleForMerchantUser_Ok() {
	shouldBe := require.New(suite.T())

	repository := &mocks.UserRoleServiceInterface{}
	repository.
		On("GetMerchantUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Role: "test", UserId: primitive.NewObjectID().Hex()}, nil)
	repository.On("UpdateMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = repository

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything, mock.Anything).Return(&casbinProto.Empty{}, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		RoleId: primitive.NewObjectID().Hex(),
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) TestInviteUserMerchant_Error_SetRoleOwner() {
	shouldBe := require.New(suite.T())

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       pkg.RoleMerchantOwner,
		Email:      "test@test.com",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnsupportedRoleType, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_GetMerchant() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.merchant = merchRep

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserMerchantNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_MerchantCompanyIsEmpty() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{}}, nil)
	suite.service.merchant = merchRep

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserEmptyCompanyName, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_OwnerNotFound() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_UserAlreadyExists() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserAlreadyExist, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_AddMerchantUser() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddMerchantUser", mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToAdd, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Error_SendEmail() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddMerchantUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToSendInvite, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserMerchant_Ok() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddMerchantUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.InviteUserMerchantResponse{}
	err := suite.service.InviteUserMerchant(context.TODO(), &grpc.InviteUserMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_InviteUserAdmin_Error_GetAdmin() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserAdminResponse{}
	err := suite.service.InviteUserAdmin(context.TODO(), &grpc.InviteUserAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserAdmin_Error_UserExists() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserAdminResponse{}
	err := suite.service.InviteUserAdmin(context.TODO(), &grpc.InviteUserAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserAlreadyExist, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserAdmin_Error_AddAdminUser() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddAdminUser", mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.InviteUserAdminResponse{}
	err := suite.service.InviteUserAdmin(context.TODO(), &grpc.InviteUserAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToAdd, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserAdmin_Error_SendEmail() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.InviteUserAdminResponse{}
	err := suite.service.InviteUserAdmin(context.TODO(), &grpc.InviteUserAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToSendInvite, res.Message)
}

func (suite *UsersTestSuite) Test_InviteUserAdmin_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(nil, nil)
	userRoleRep.
		On("AddAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.InviteUserAdminResponse{}
	err := suite.service.InviteUserAdmin(context.TODO(), &grpc.InviteUserAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Error_GetMerchant() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.merchant = merchRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserMerchantNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Error_OwnerNotFound() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Error_UserNotFound() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Error_UnableToResend() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusAccepted}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableResendInvite, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Error_SendEmail() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToSendInvite, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteMerchant_Ok() {
	shouldBe := require.New(suite.T())

	merchRep := &mocks.MerchantRepositoryInterface{}
	merchRep.
		On("GetById", mock.Anything, mock.Anything).
		Return(&billing.Merchant{Company: &billing.MerchantCompanyInfo{Name: "name"}}, nil)
	suite.service.merchant = merchRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetMerchantOwner", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetMerchantUserByEmail", mock.Anything, mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteMerchant(context.TODO(), &grpc.ResendInviteMerchantRequest{
		MerchantId: primitive.NewObjectID().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_ResendInviteAdmin_Error_GetAdmin() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteAdmin(context.TODO(), &grpc.ResendInviteAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteAdmin_Error_GetAdminUserByEmail() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteAdmin(context.TODO(), &grpc.ResendInviteAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteAdmin_Error_SendEmail() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteAdmin(context.TODO(), &grpc.ResendInviteAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToSendInvite, res.Message)
}

func (suite *UsersTestSuite) Test_ResendInviteAdmin_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetSystemAdmin", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Email: "test@test.com", FirstName: "firstName", LastName: "lastName"}, nil)
	userRoleRep.
		On("GetAdminUserByEmail", mock.Anything, mock.Anything).
		Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	postmanBroker := &mocks.BrokerInterface{}
	postmanBroker.
		On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	suite.service.postmarkBroker = postmanBroker

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ResendInviteAdmin(context.TODO(), &grpc.ResendInviteAdminRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_ParseToken() {
	shouldBe := require.New(suite.T())

	res := &grpc.AcceptInviteResponse{}
	err := suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserInvalidToken, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_InvalidEmail() {
	shouldBe := require.New(suite.T())

	token, err := suite.service.createInviteToken(&billing.UserRole{Email: "aaa@aaa.aaa"})
	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: "bbb@bbb.bbb", Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserInvalidInviteEmail, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_GetByUserId() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userProfileRepository = userProfileRep

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserProfileNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_NoPersonalData() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(&grpc.UserProfile{Personal: nil}, nil)
	suite.service.userProfileRepository = userProfileRep

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserEmptyNames, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_GetAdminUserById() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(&grpc.UserProfile{Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"}}, nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_AlreadyAccept() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(&grpc.UserProfile{Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"}}, nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusAccepted}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserInviteAlreadyAccepted, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_UpdateAdminUser() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(&grpc.UserProfile{Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"}}, nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	userRoleRep.
		On("UpdateAdminUser", mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToAdd, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_AddToCasbin() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(&grpc.UserProfile{Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"}}, nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	userRoleRep.
		On("UpdateAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToAddToCasbin, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_ConfirmEmail() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	profile := &grpc.UserProfile{
		Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"},
		Email:    &grpc.UserProfileEmail{},
	}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(profile, nil)
	userProfileRep.
		On("Update", mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	userRoleRep.
		On("UpdateAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Error_CentrifugoPublish() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	profile := &grpc.UserProfile{
		Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"},
		Email:    &grpc.UserProfileEmail{},
	}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(profile, nil)
	userProfileRep.
		On("Update", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	userRoleRep.
		On("UpdateAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.centrifugo = centrifugo

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_AcceptInvite_Ok() {
	shouldBe := require.New(suite.T())

	role := &billing.UserRole{Email: "aaa@aaa.aaa"}
	token, err := suite.service.createInviteToken(role)

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	profile := &grpc.UserProfile{
		Personal: &grpc.UserProfilePersonal{FirstName: "firstName", LastName: "lastName"},
		Email:    &grpc.UserProfileEmail{},
	}
	userProfileRep.
		On("GetByUserId", mock.Anything, mock.Anything).
		Return(profile, nil)
	userProfileRep.
		On("Update", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userProfileRepository = userProfileRep

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.
		On("GetAdminUserById", mock.Anything, mock.Anything).
		Return(&billing.UserRole{Status: pkg.UserRoleStatusInvited}, nil)
	userRoleRep.
		On("UpdateAdminUser", mock.Anything, mock.Anything).
		Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("AddRoleForUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.service.centrifugo = centrifugo

	res := &grpc.AcceptInviteResponse{}
	err = suite.service.AcceptInvite(context.TODO(), &grpc.AcceptInviteRequest{Email: role.Email, Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_CheckInviteToken_Error_ParseToken() {
	shouldBe := require.New(suite.T())

	res := &grpc.CheckInviteTokenResponse{}
	err := suite.service.CheckInviteToken(context.TODO(), &grpc.CheckInviteTokenRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserInvalidToken, res.Message)
}

func (suite *UsersTestSuite) Test_CheckInviteToken_Error_InvalidEmail() {
	shouldBe := require.New(suite.T())

	token, err := suite.service.createInviteToken(&billing.UserRole{Email: "aaa@aaa.aaa"})
	res := &grpc.CheckInviteTokenResponse{}
	err = suite.service.CheckInviteToken(context.TODO(), &grpc.CheckInviteTokenRequest{Email: "bbb@bbb.bbb", Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserInvalidInviteEmail, res.Message)
}

func (suite *UsersTestSuite) Test_CheckInviteToken_Ok() {
	shouldBe := require.New(suite.T())

	token, err := suite.service.createInviteToken(&billing.UserRole{Email: "aaa@aaa.aaa"})
	res := &grpc.CheckInviteTokenResponse{}
	err = suite.service.CheckInviteToken(context.TODO(), &grpc.CheckInviteTokenRequest{Email: "aaa@aaa.aaa", Token: token}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_GetRoleList_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetRoleListResponse{}
	err := suite.service.GetRoleList(context.TODO(), &grpc.GetRoleListRequest{Type: pkg.RoleTypeSystem}, res)
	shouldBe.NoError(err)
	shouldBe.Len(res.Items, 5)
}

func (suite *UsersTestSuite) Test_GetRoleList_Ok_UnknownType() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetRoleListResponse{}
	err := suite.service.GetRoleList(context.TODO(), &grpc.GetRoleListRequest{Type: "unknown"}, res)
	shouldBe.NoError(err)
	shouldBe.Len(res.Items, 0)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_GetMerchantUserById() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_AnotherMerchantUser() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{MerchantId: "1"}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{MerchantId: "2"}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_DeleteUser() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteMerchantUser", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToDelete, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{UserId: "1"}, nil)
	userRoleRep.On("DeleteMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToDeleteFromCasbin, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_TruncateEmailConfirmation() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Error_CentrifugoPublish() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(nil)
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.centrifugo = centrifugo

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteMerchantUser_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteMerchantUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(nil)
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.service.centrifugo = centrifugo

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteMerchantUser(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Error_GetMerchantUserById() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Error_DeleteUser() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteAdminUser", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToDelete, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Error_DeleteFromCasbin() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{UserId: "1"}, nil)
	userRoleRep.On("DeleteAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserUnableToDeleteFromCasbin, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Error_TruncateEmailConfirmation() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Error_CentrifugoPublish() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(nil)
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
	suite.service.centrifugo = centrifugo

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserConfirmEmail, res.Message)
}

func (suite *UsersTestSuite) Test_DeleteAdminUser_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	userRoleRep.On("DeleteAdminUser", mock.Anything, mock.Anything).Return(nil)
	suite.service.userRoleRepository = userRoleRep

	userProfileRep := &mocks.UserProfileRepositoryInterface{}
	userProfileRep.On("GetByUserId", mock.Anything, mock.Anything).Return(&grpc.UserProfile{Email: &grpc.UserProfileEmail{}}, nil)
	userProfileRep.On("Update", mock.Anything, mock.Anything).Return(nil)
	suite.service.userProfileRepository = userProfileRep

	casbin := &casbinMocks.CasbinService{}
	casbin.On("DeleteUser", mock.Anything, mock.Anything).Return(nil, nil)
	suite.service.casbinService = casbin

	centrifugo := &mocks.CentrifugoInterface{}
	centrifugo.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.service.centrifugo = centrifugo

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.DeleteAdminUser(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_GetMerchantUserRole_Error_UserNotFound() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.UserRoleResponse{}
	err := suite.service.GetMerchantUserRole(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_GetMerchantUserRole_Error_AnotherMerchant() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{MerchantId: "1"}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.UserRoleResponse{}
	err := suite.service.GetMerchantUserRole(context.TODO(), &grpc.MerchantRoleRequest{MerchantId: "2"}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_GetMerchantUserRole_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetMerchantUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.UserRoleResponse{}
	err := suite.service.GetMerchantUserRole(context.TODO(), &grpc.MerchantRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_GetAdminUserRole_Error_UserNotFound() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.UserRoleResponse{}
	err := suite.service.GetAdminUserRole(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusBadData, res.Status)
	shouldBe.EqualValues(errorUserNotFound, res.Message)
}

func (suite *UsersTestSuite) Test_GetAdminUserRole_Ok() {
	shouldBe := require.New(suite.T())

	userRoleRep := &mocks.UserRoleServiceInterface{}
	userRoleRep.On("GetAdminUserById", mock.Anything, mock.Anything).Return(&billing.UserRole{}, nil)
	suite.service.userRoleRepository = userRoleRep

	res := &grpc.UserRoleResponse{}
	err := suite.service.GetAdminUserRole(context.TODO(), &grpc.AdminRoleRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(pkg.ResponseStatusOk, res.Status)
}

func (suite *UsersTestSuite) Test_getUserPermissions_Error_GetImplicitPermissionsForUser() {
	shouldBe := require.New(suite.T())

	casbin := &casbinMocks.CasbinService{}
	casbin.On("GetImplicitPermissionsForUser", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
	suite.service.casbinService = casbin

	_, err := suite.service.getUserPermissions(context.TODO(), "1", "2")
	shouldBe.Error(err)
	shouldBe.EqualValues(errorUserGetImplicitPermissions, err)
}

func (suite *UsersTestSuite) Test_getUserPermissions_Error_EmptyPermissions() {
	shouldBe := require.New(suite.T())

	casbin := &casbinMocks.CasbinService{}
	casbin.
		On("GetImplicitPermissionsForUser", mock.Anything, mock.Anything).
		Return(&casbinProto.Array2DReply{D2: nil}, nil)
	suite.service.casbinService = casbin

	_, err := suite.service.getUserPermissions(context.TODO(), "1", "2")
	shouldBe.Error(err)
	shouldBe.EqualValues(errorUserDontHaveRole, err)
}

func (suite *UsersTestSuite) Test_getUserPermissions_Ok() {
	shouldBe := require.New(suite.T())

	casbin := &casbinMocks.CasbinService{}
	casbin.
		On("GetImplicitPermissionsForUser", mock.Anything, mock.Anything).
		Return(&casbinProto.Array2DReply{D2: []*casbinProto.Array2DReplyD{{D1: []string{"a", "b"}}}}, nil)
	suite.service.casbinService = casbin

	perm, err := suite.service.getUserPermissions(context.TODO(), "1", "2")
	shouldBe.NoError(err)
	shouldBe.Len(perm, 1)
}
