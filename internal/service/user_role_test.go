package service

import (
	"github.com/globalsign/mgo/bson"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
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
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
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
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *UserRoleTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *UserRoleTestSuite) TestUserRole_AddMerchantUser_Ok() {
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Ok() {
	role := &billing.UserRole{
		Id:     bson.NewObjectId().Hex(),
		UserId: bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	role.Role = "test"
	assert.NoError(suite.T(), suite.service.userRoleRepository.UpdateMerchantUser(role))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateMerchantUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.UpdateMerchantUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_Ok() {
	role := &billing.UserRole{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		Email:      "test",
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserByEmail(role.MerchantId, role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByEmail_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserByEmail(bson.NewObjectId().Hex(), "test")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_Ok() {
	role := &billing.UserRole{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		UserId:     bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserByUserId(role.MerchantId, role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.UserId, role2.UserId)
	assert.Equal(suite.T(), role.Id, role2.Id)
	assert.Equal(suite.T(), role.MerchantId, role2.MerchantId)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserByUserId_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserByUserId(bson.NewObjectId().Hex(), bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_Ok() {
	role := &billing.UserRole{
		Id: bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	role2, err := suite.service.userRoleRepository.GetMerchantUserById(role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantUserById_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetMerchantUserById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Ok() {
	role := &billing.UserRole{
		Id:     bson.NewObjectId().Hex(),
		UserId: bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))
	assert.NoError(suite.T(), suite.service.userRoleRepository.DeleteMerchantUser(role))
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteMerchantUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.DeleteMerchantUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_Ok() {
	role := &billing.UserRole{Id: bson.NewObjectId().Hex(), MerchantId: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	users, err := suite.service.userRoleRepository.GetUsersForMerchant(role.MerchantId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForMerchant_Error_NotFound() {
	users, err := suite.service.userRoleRepository.GetUsersForMerchant(bson.NewObjectId().Hex())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 0)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Ok() {
	role := &billing.UserRole{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: bson.NewObjectId().Hex(),
		UserId:     bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddMerchantUser(role))

	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(role.UserId)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), merchants, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetMerchantsForUser_Error_NotFound() {
	merchants, err := suite.service.userRoleRepository.GetMerchantsForUser(bson.NewObjectId().Hex())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), merchants, 0)
}

func (suite *UserRoleTestSuite) TestUserRole_AddAdminUser_Ok() {
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_Ok() {
	role := &billing.UserRole{Id: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))

	role.Role = "test"
	assert.NoError(suite.T(), suite.service.userRoleRepository.UpdateAdminUser(role))
}

func (suite *UserRoleTestSuite) TestUserRole_UpdateAdminUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.UpdateAdminUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_Ok() {
	role := &billing.UserRole{
		Id:    bson.NewObjectId().Hex(),
		Email: "test",
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))

	role2, err := suite.service.userRoleRepository.GetAdminUserByEmail(role.Email)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Email, role2.Email)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByEmail_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserByEmail("test")
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_Ok() {
	role := &billing.UserRole{
		Id:     bson.NewObjectId().Hex(),
		UserId: bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))

	role2, err := suite.service.userRoleRepository.GetAdminUserByUserId(role.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.UserId, role2.UserId)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserByUserId_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserByUserId(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_Ok() {
	role := &billing.UserRole{
		Id: bson.NewObjectId().Hex(),
	}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))

	role2, err := suite.service.userRoleRepository.GetAdminUserById(role.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), role.Id, role2.Id)
}

func (suite *UserRoleTestSuite) TestUserRole_GetAdminUserById_Error_NotFound() {
	_, err := suite.service.userRoleRepository.GetAdminUserById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_Ok() {
	role := &billing.UserRole{Id: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))
	assert.NoError(suite.T(), suite.service.userRoleRepository.DeleteAdminUser(role))
}

func (suite *UserRoleTestSuite) TestUserRole_DeleteAdminUser_Error_NotFound() {
	assert.Error(suite.T(), suite.service.userRoleRepository.DeleteAdminUser(&billing.UserRole{Id: bson.NewObjectId().Hex()}))
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForAdmin_Ok() {
	role := &billing.UserRole{Id: bson.NewObjectId().Hex()}
	assert.NoError(suite.T(), suite.service.userRoleRepository.AddAdminUser(role))

	users, err := suite.service.userRoleRepository.GetUsersForAdmin()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 1)
}

func (suite *UserRoleTestSuite) TestUserRole_GetUsersForAdmin_Error_NotFound() {
	users, err := suite.service.userRoleRepository.GetUsersForAdmin()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), users, 0)
}
