package repository

import (
	"context"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type UserProfileTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *userProfileRepository
	log        *zap.Logger
}

func Test_UserProfile(t *testing.T) {
	suite.Run(t, new(UserProfileTestSuite))
}

func (suite *UserProfileTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &userProfileRepository{db: suite.db}
}

func (suite *UserProfileTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *UserProfileTestSuite) TestUserProfile_NewUserProfileRepository_Ok() {
	repository := NewUserProfileRepository(suite.db)
	assert.IsType(suite.T(), &userProfileRepository{}, repository)
}

func (suite *UserProfileTestSuite) TestUserProfile_Add_Ok() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Add(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) TestUserProfile_Add_ErrorDb() {
	profile := suite.getUserProfileTemplate()
	profile.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Add(context.TODO(), profile)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_Update_Ok() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Add(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile.Email.Email = "unit@test.com"
	err = suite.repository.Update(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) TestUserProfile_Update_ErrorDb() {
	profile := suite.getUserProfileTemplate()
	profile.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Update(context.TODO(), profile)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_Update_ErrorId() {
	profile := suite.getUserProfileTemplate()
	profile.Id = "test"
	err := suite.repository.Update(context.TODO(), profile)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_Upsert_Ok_Insert() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Upsert(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) TestUserProfile_Upsert_Ok_Update() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Upsert(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile.Email.Email = "test@unit.com"
	err = suite.repository.Upsert(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) TestUserProfile_Upsert_ErrorDb() {
	profile := suite.getUserProfileTemplate()
	profile.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Upsert(context.TODO(), profile)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_Upsert_ErrorId() {
	profile := suite.getUserProfileTemplate()
	profile.Id = "test"
	err := suite.repository.Upsert(context.TODO(), profile)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetById_NotFound() {
	profile := suite.getUserProfileTemplate()
	_, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetById_InvalidId() {
	_, err := suite.repository.GetById(context.TODO(), "id")
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetById_Ok() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Add(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetById(context.TODO(), profile.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetByUserId_NotFound() {
	profile := suite.getUserProfileTemplate()
	_, err := suite.repository.GetByUserId(context.TODO(), profile.UserId)
	assert.Error(suite.T(), err)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetByUserId_Ok() {
	profile := suite.getUserProfileTemplate()

	err := suite.repository.Add(context.TODO(), profile)
	assert.NoError(suite.T(), err)

	profile2, err := suite.repository.GetByUserId(context.TODO(), profile.UserId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), profile.Id, profile2.Id)
	assert.Equal(suite.T(), profile.UserId, profile2.UserId)
	assert.Equal(suite.T(), profile.Email.Email, profile2.Email.Email)
}

func (suite *UserProfileTestSuite) getUserProfileTemplate() *grpc.UserProfile {
	return &grpc.UserProfile{
		Id:     primitive.NewObjectID().Hex(),
		UserId: primitive.NewObjectID().Hex(),
		Email: &grpc.UserProfileEmail{
			Email: "test@unit.test",
		},
		Personal: &grpc.UserProfilePersonal{
			FirstName: "Unit test",
			LastName:  "Unit Test",
			Position:  "test",
		},
		Help: &grpc.UserProfileHelp{
			ProductPromotionAndDevelopment: false,
			ReleasedGamePromotion:          true,
			InternationalSales:             true,
			Other:                          false,
		},
		LastStep: "step2",
	}
}
