package service

import (
	"context"
	"errors"
	"fmt"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"testing"
)

type UserRoleTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
}

func Test_UserRole(t *testing.T) {
	suite.Run(t, new(UserRoleTestSuite))
}

func (suite *UserRoleTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache, err = NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		mocks.NewGeoIpServiceTestOk(),
		mocks.NewRepositoryServiceOk(),
		mocks.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *UserRoleTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *UserRoleTestSuite) TestUserRole_AddMerchantUser_Ok() {
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Ok() {
	role := &billing.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	role.Role = "test"
	assert.NoError(suite.T(), suite.service.userRoleRepository.UpdateMerchantUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.UpdateMerchantUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Error_CacheDelete() {
	role := &billing.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	ci := &mocks.CacheInterface{}
	ci.On("Delete", mock.Anything).
		Return(errors.New("error"))
	suite.service.cacher = ci

	role.Role = "test"
	assert.Error(suite.T(), suite.service.userRoleRepository.UpdateMerchantUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_Ok() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Email:      "test",
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserByEmail(context.TODO(), role.MerchantId, role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserByEmail(context.TODO(), primitive.NewObjectID().Hex(), "test")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_Ok() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserByUserId(context.TODO(), role.MerchantId, role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.UserId, role2.UserId)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserByUserId(context.TODO(), primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_Ok() {
	role := &billing.UserRole{
		Id: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserById(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Ok() {
	role := &billing.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))
	assert.NoError(suite.T(), suite.service.userRoleRepository.DeleteMerchantUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.DeleteMerchantUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Error_CacheDelete() {
	role := &billing.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	ci := &mocks.CacheInterface{}
	ci.On("Delete", mock.Anything).
		Return(errors.New("error"))
	suite.service.cacher = ci

	assert.Error(suite.T(), suite.service.userRoleRepository.DeleteMerchantUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_Ok() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex(), MerchantId: primitive.NewObjectID().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	users, err := suite.service.userRoleRepository.GetUsersForMerchant(context.TODO(), role.MerchantId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_Error_NotFound() {
	users, err := suite.service.userRoleRepository.GetUsersForMerchant(context.TODO(), primitive.NewObjectID().Hex())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 0)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Ok() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), merchants, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Error_NotFound() {
	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(context.TODO(), primitive.NewObjectID().Hex())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), merchants, 0)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Ok_ByCache() {
	userId := primitive.NewObjectID().Hex()

	ci := &mocks.CacheInterface{}
	ci.On("Get", fmt.Sprintf(cacheUserMerchants, userId), mock.Anything).
		Return(nil)
	suite.service.cacher = ci

	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(context.TODO(), userId)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), []*billing.UserRole{}, merchants)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Ok_SetCacheError() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	ci := &mocks.CacheInterface{}
	ci.On("Get", mock.Anything, mock.Anything).
		Return(errors.New("error"))
	ci.On("Set", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("error"))
	suite.service.cacher = ci

	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), merchants, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_AddAdminUser_Ok() {
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_Ok() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	role.Role = "test"
	assert.NoError(suite.T(), suite.service.userRoleRepository.UpdateAdminUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.UpdateAdminUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_Ok() {
	role := &billing.UserRole{
		Id:    primitive.NewObjectID().Hex(),
		Email: "test",
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetAdminUserByEmail(context.TODO(), role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserByEmail(context.TODO(), "test")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_Ok() {
	role := &billing.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetAdminUserByUserId(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.UserId, role2.UserId)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserByUserId(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_Ok() {
	role := &billing.UserRole{
		Id: primitive.NewObjectID().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	role2, err := suite.service.userRoleRepository.GetAdminUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserById(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_Ok() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))
	assert.NoError(suite.T(), suite.service.userRoleRepository.DeleteAdminUser(context.TODO(), role))
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.DeleteAdminUser(context.TODO(), &billing.UserRole{Id: primitive.NewObjectID().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForAdmin_Ok() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	users, err := suite.service.userRoleRepository.GetUsersForAdmin(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetSystemAdmin_Ok() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex(), Role: pkg.RoleSystemAdmin}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	_, err := suite.service.userRoleRepository.GetSystemAdmin(context.TODO())
	assert.NoError(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetSystemAdmin_Error_NotFound() {
	role := &billing.UserRole{Id: primitive.NewObjectID().Hex(), Role: pkg.RoleSystemFinancial}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(context.TODO(), role))

	_, err := suite.service.userRoleRepository.GetSystemAdmin(context.TODO())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_Ok() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       pkg.RoleMerchantOwner,
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	_, err := suite.service.userRoleRepository.GetMerchantOwner(context.TODO(), role.MerchantId)
	assert.NoError(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_Error_NotFound() {
	role := &billing.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       pkg.RoleMerchantOwner,
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(context.TODO(), role))

	_, err := suite.service.userRoleRepository.GetMerchantOwner(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}
