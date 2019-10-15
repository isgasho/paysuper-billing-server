package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
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
	suite.service = NewBillingService(db, cfg, mocks.NewGeoIpServiceTestOk(), mocks.NewRepositoryServiceOk(), mocks.NewTaxServiceOkMock(), mocks.NewBrokerMockOk(), mocks.NewTestRedis(), suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), nil, mocks.NewFormatterOK())

	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant = &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
	}
	err = db.Collection(collectionMerchant).Insert(suite.merchant)
	if err != nil {
		suite.FailNow("Insert merchant failed", "%v", err)
	}

	repository := newUserRoleRepository(suite.service)

	user := &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}
	err = repository.AddMerchantUser(&billing.UserRole{
		MerchantId: suite.merchant.Id, Id: bson.NewObjectId().Hex(), User: user, ProjectRole: []*billing.UserRoleProject{
			{Role: pkg.UserRoleDeveloper},
		},
	})

	if err != nil {
		suite.FailNow("Users failed to insert", "%v", err)
	}

	suite.user = user
	suite.adminUser = &billing.UserRoleProfile{UserId: bson.NewObjectId().Hex()}
	err = repository.AddAdminUser(&billing.UserRole{
		Id: bson.NewObjectId().Hex(),
		User: suite.adminUser, Role: "some_role",
	})

	if err != nil {
		suite.FailNow("Users failed to insert", "%v", err)
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

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: bson.NewObjectId().Hex()}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, res.Status)
}

func (suite *UsersTestSuite) TestGetUsers_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetMerchantUsersResponse{}
	err := suite.service.GetMerchantUsers(context.TODO(), &grpc.GetMerchantUsersRequest{MerchantId: suite.merchant.Id}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
	shouldBe.NotEmpty(res.Users)
}

func (suite *UsersTestSuite) TestGetAdminUsers_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetAdminUsersResponse{}
	err := suite.service.GetAdminUsers(context.TODO(), &grpc.EmptyRequest{}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
	shouldBe.NotEmpty(res.Users)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Empty_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
	shouldBe.Empty(res.Merchants)
}

func (suite *UsersTestSuite) TestGetMerchantsForUser_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.GetMerchantsForUserResponse{}
	err := suite.service.GetMerchantsForUser(context.TODO(), &grpc.GetMerchantsForUserRequest{
		UserId: suite.user.UserId,
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
	shouldBe.NotEmpty(res.Merchants)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_RoleError() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: suite.adminUser.UserId,
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)

	res = &grpc.EmptyResponseWithStatus{}
	err = suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: suite.adminUser.UserId,
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, res.Status)
	shouldBe.NotEmpty(res.Message)
}

func (suite *UsersTestSuite) TestChangeAdminUserRole_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForAdminUser(context.TODO(), &grpc.ChangeRoleForAdminUserRequest{
		UserId: suite.adminUser.UserId,
		Role:   "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_NotFoundError() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		Role: "test_role",
		MerchantId: bson.NewObjectId().Hex(),
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, res.Status)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_RoleError() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		MerchantId: suite.merchant.Id,
		UserId:     suite.user.UserId,
		Role:       "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)

	res = &grpc.EmptyResponseWithStatus{}
	err = suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		MerchantId: suite.merchant.Id,
		UserId:     suite.user.UserId,
		Role:       "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, res.Status)
	shouldBe.NotEmpty(res.Message)
}

func (suite *UsersTestSuite) TestChangeMerchantUserRole_Ok() {
	shouldBe := require.New(suite.T())

	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.ChangeRoleForMerchantUser(context.TODO(), &grpc.ChangeRoleForMerchantUserRequest{
		MerchantId: suite.merchant.Id,
		UserId:     suite.user.UserId,
		Role:       "test_role",
	}, res)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, res.Status)
}
