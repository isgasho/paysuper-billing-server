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
	"testing"
	"time"
)

type UserRoleTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *userRoleRepository
	log        *zap.Logger
}

func Test_UserRole(t *testing.T) {
	suite.Run(t, new(UserRoleTestSuite))
}

func (suite *UserRoleTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &userRoleRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *UserRoleTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *UserRoleTestSuite) TestCountry_NewUserRoleRepository_Ok() {
	repository := NewUserRoleRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &userRoleRepository{}, repository)
}

func (suite *UserRoleTestSuite) TestUserRole_AddMerchantUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_AddMerchantUser_ErrorDb() {
	role := &billingpb.UserRole{
		Id:        primitive.NewObjectID().Hex(),
		CreatedAt: &timestamp.Timestamp{Seconds: -100000000000000},
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_AddAdminUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_AddAdminUser_ErrorDb() {
	role := &billingpb.UserRole{
		Id:        primitive.NewObjectID().Hex(),
		CreatedAt: &timestamp.Timestamp{Seconds: -100000000000000},
	}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}

	cache := &mocks.CacheInterface{}
	cache.On("Delete", fmt.Sprintf(cacheUserMerchants, role.UserId)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role.Email = "test@paysuper.com"
	err = suite.repository.UpdateMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_ErrorInvalidId() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role.Id = "test"
	err = suite.repository.UpdateMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}
	err := suite.repository.UpdateMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_ErrorDropCache() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}

	cache := &mocks.CacheInterface{}
	cache.On("Delete", fmt.Sprintf(cacheUserMerchants, role.UserId)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role.Email = "test@paysuper.com"
	err = suite.repository.UpdateMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role.Email = "test@paysuper.com"
	err = suite.repository.UpdateAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_ErrorInvalidId() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role.Id = "test"
	err = suite.repository.UpdateAdminUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: "id"}
	err := suite.repository.UpdateAdminUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_ErrorInvalidId() {
	role, err := suite.repository.GetAdminUserById(context.TODO(), "id")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_ErrorNotFound() {
	role, err := suite.repository.GetAdminUserById(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserById(context.TODO(), role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_ErrorInvalidId() {
	role, err := suite.repository.GetMerchantUserById(context.TODO(), "id")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_ErrorNotFound() {
	role, err := suite.repository.GetMerchantUserById(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	err = suite.repository.DeleteAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserById(context.TODO(), role.Id)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role2)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_ErrorInvalidId() {
	role := &billingpb.UserRole{Id: "id"}
	err := suite.repository.DeleteAdminUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.DeleteAdminUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}

	cache := &mocks.CacheInterface{}
	cache.On("Delete", fmt.Sprintf(cacheUserMerchants, role.UserId)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	err = suite.repository.DeleteMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserById(context.TODO(), role.Id)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role2)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_ErrorInvalidId() {
	role := &billingpb.UserRole{Id: "id"}
	err := suite.repository.DeleteMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}
	err := suite.repository.DeleteMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_ErrorDropCache() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex()}

	cache := &mocks.CacheInterface{}
	cache.On("Delete", fmt.Sprintf(cacheUserMerchants, role.UserId)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	err = suite.repository.DeleteMerchantUser(context.TODO(), role)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetSystemAdmin_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), Role: billingpb.RoleSystemAdmin}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetSystemAdmin(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Role, role2.Role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetSystemAdmin_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), Role: billingpb.RoleSystemFinancial}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetSystemAdmin(context.TODO())
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role2)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_Ok() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       billingpb.RoleMerchantOwner,
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantOwner(context.TODO(), role.MerchantId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Role, role2.Role)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_ErrorNotFoundByRole() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       billingpb.RoleMerchantAccounting,
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantOwner(context.TODO(), role.MerchantId)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role2)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_ErrorNotFoundByMerchant() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Role:       billingpb.RoleMerchantOwner,
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantOwner(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role2)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantOwner_ErrorInvalidId() {
	role, err := suite.repository.GetMerchantOwner(context.TODO(), "id")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Ok() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}

	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), role.Id, list[0].Id)
	assert.Equal(suite.T(), role.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), role.UserId, list[0].UserId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_OkByCache() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}

	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything).Return(nil)
	suite.repository.cache = cache

	list, err := suite.repository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 0)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_OkWithFailedToSetCache() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
	}

	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), role.Id, list[0].Id)
	assert.Equal(suite.T(), role.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), role.UserId, list[0].UserId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_ErrorInvalidId() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		UserId:     "id",
	}

	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheUserMerchants, role.UserId), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	_, err := suite.repository.GetMerchantsForUser(context.TODO(), role.UserId)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForAdmin_Ok() {
	role := &billingpb.UserRole{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
	}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetUsersForAdmin(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), role.Id, list[0].Id)
	assert.Equal(suite.T(), role.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), role.UserId, list[0].UserId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_Ok() {
	role1 := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role1)
	assert.NoError(suite.T(), err)

	role2 := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	err = suite.repository.AddMerchantUser(context.TODO(), role2)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetUsersForMerchant(context.TODO(), role2.MerchantId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list, 1)
	assert.Equal(suite.T(), role2.Id, list[0].Id)
	assert.Equal(suite.T(), role2.MerchantId, list[0].MerchantId)
	assert.Equal(suite.T(), role2.UserId, list[0].UserId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_ErrorInvalidId() {
	_, err := suite.repository.GetUsersForMerchant(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), Email: "email"}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserByEmail(context.TODO(), role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_ErrorNotFound() {
	role, err := suite.repository.GetAdminUserByEmail(context.TODO(), "email")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_Ok() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
		Email:      "email",
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserByEmail(context.TODO(), role.MerchantId, role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_ErrorInvalidId() {
	role, err := suite.repository.GetMerchantUserByEmail(context.TODO(), "id", "email")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_ErrorNotFound() {
	role, err := suite.repository.GetMerchantUserByEmail(context.TODO(), primitive.NewObjectID().Hex(), "email")
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), role)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_Ok() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: primitive.NewObjectID().Hex()}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetAdminUserByUserId(context.TODO(), role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_ErrorInvalidId() {
	_, err := suite.repository.GetAdminUserByUserId(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_ErrorNotFound() {
	role := &billingpb.UserRole{Id: primitive.NewObjectID().Hex(), UserId: primitive.NewObjectID().Hex()}
	err := suite.repository.AddAdminUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetAdminUserByUserId(context.TODO(), primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_Ok() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	role2, err := suite.repository.GetMerchantUserByUserId(context.TODO(), role.MerchantId, role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_ErrorNotFoundByMerchantId() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetMerchantUserByUserId(context.TODO(), primitive.NewObjectID().Hex(), role.UserId)
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_ErrorNotFoundByUserId() {
	role := &billingpb.UserRole{
		Id:         primitive.NewObjectID().Hex(),
		UserId:     primitive.NewObjectID().Hex(),
		MerchantId: primitive.NewObjectID().Hex(),
	}
	err := suite.repository.AddMerchantUser(context.TODO(), role)
	assert.NoError(suite.T(), err)

	_, err = suite.repository.GetMerchantUserByUserId(context.TODO(), role.MerchantId, primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_ErrorInvalidMerchantId() {
	_, err := suite.repository.GetMerchantUserByUserId(context.TODO(), "id", primitive.NewObjectID().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_ErrorInvalidUserId() {
	_, err := suite.repository.GetMerchantUserByUserId(context.TODO(), primitive.NewObjectID().Hex(), "id")
	assert.Error(suite.T(), err)
}
