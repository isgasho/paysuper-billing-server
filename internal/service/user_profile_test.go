package service

import (
	"context"
	"errors"
	"github.com/centrifugal/gocent"
	"github.com/elliotchance/redismock"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"net/url"
	"testing"
)

type UserProfileTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	merchant          *billing.Merchant
	merchantAgreement *billing.Merchant
	merchant1         *billing.Merchant

	project *billing.Project

	pmBankCard *billing.PaymentMethod
	pmQiwi     *billing.PaymentMethod
}

func Test_UserProfile(t *testing.T) {
	suite.Run(t, new(UserProfileTestSuite))
}

func (suite *UserProfileTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	db, err := mongodb.NewDatabase()

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	err = InitTestCurrency(db, []interface{}{rub})

	if err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, mock.NewBrokerMockOk(), mock.NewTestRedis(), suite.cache)

	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.service.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       cfg.CentrifugoURL,
			Key:        cfg.CentrifugoSecret,
			HTTPClient: mock.NewClientStatusOk(),
		},
	)
}

func (suite *UserProfileTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *UserProfileTestSuite) TestUserProfile_CreateOrUpdateUserProfile_NewProfile_Ok() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	profile := suite.service.getOnboardingProfileByUser(req.UserId)
	assert.Nil(suite.T(), profile)

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.IsType(suite.T(), &grpc.UserProfile{}, rsp.Item)

	profile = suite.service.getOnboardingProfileByUser(req.UserId)
	assert.NotNil(suite.T(), rsp.Item)
	assert.IsType(suite.T(), &grpc.UserProfile{}, rsp.Item)

	assert.Equal(suite.T(), profile.UserId, rsp.Item.UserId)
	assert.Equal(suite.T(), profile.LastStep, rsp.Item.LastStep)
	assert.Equal(suite.T(), profile.Personal.LastName, rsp.Item.Personal.LastName)
	assert.Equal(suite.T(), profile.Personal.FirstName, rsp.Item.Personal.FirstName)
	assert.Equal(suite.T(), profile.Personal.Position, rsp.Item.Personal.Position)
	assert.Equal(suite.T(), profile.Help.Other, rsp.Item.Help.Other)
	assert.Equal(suite.T(), profile.Help.InternationalSales, rsp.Item.Help.InternationalSales)
	assert.Equal(suite.T(), profile.Help.ReleasedGamePromotion, rsp.Item.Help.ReleasedGamePromotion)
	assert.Equal(suite.T(), profile.Help.ProductPromotionAndDevelopment, rsp.Item.Help.ProductPromotionAndDevelopment)
	assert.NotEmpty(suite.T(), rsp.Item.CentrifugoToken)

	b, ok := suite.service.broker.(*mock.BrokerMockOk)
	assert.True(suite.T(), ok)
	assert.True(suite.T(), b.IsSent)
}

func (suite *UserProfileTestSuite) TestUserProfile_CreateOrUpdateOnboardingProfile_ExistProfile_Ok() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
		Email: &grpc.UserProfileEmail{
			Email: "test@unit.test",
		},
		LastStep: "step1",
	}
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.IsType(suite.T(), &grpc.UserProfile{}, rsp.Item)
	assert.NotEmpty(suite.T(), rsp.Item.CentrifugoToken)

	b, ok := suite.service.broker.(*mock.BrokerMockOk)
	assert.True(suite.T(), ok)
	assert.True(suite.T(), b.IsSent)

	b.IsSent = false

	req1 := &grpc.UserProfile{
		UserId: req.UserId,
		Personal: &grpc.UserProfilePersonal{
			FirstName: "test",
			LastName:  "test",
			Position:  "unit",
		},
		Help: &grpc.UserProfileHelp{
			ProductPromotionAndDevelopment: true,
			ReleasedGamePromotion:          true,
			InternationalSales:             true,
			Other:                          true,
		},
		Company: &grpc.UserProfileCompany{
			CompanyName:       "company name",
			Website:           "http://127.0.0.1",
			AnnualIncome:      &grpc.RangeInt{From: 10, To: 100000},
			NumberOfEmployees: &grpc.RangeInt{From: 10, To: 50},
			KindOfActivity:    "test",
			Monetization: &grpc.UserProfileCompanyMonetization{
				PaidSubscription:  true,
				InGameAdvertising: true,
				InGamePurchases:   true,
				PremiumAccess:     true,
				Other:             true,
			},
			Platforms: &grpc.UserProfileCompanyPlatforms{
				PcMac:        true,
				GameConsole:  true,
				MobileDevice: true,
				WebBrowser:   true,
				Other:        true,
			},
		},
	}

	rsp1 := &grpc.GetUserProfileResponse{}
	err = suite.service.CreateOrUpdateUserProfile(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.NotEmpty(suite.T(), rsp1.Item.CentrifugoToken)

	assert.Equal(suite.T(), rsp.Item.UserId, rsp1.Item.UserId)
	assert.NotEqual(suite.T(), rsp.Item.Personal, rsp1.Item.Personal)
	assert.NotEqual(suite.T(), rsp.Item.Help, rsp1.Item.Help)
	assert.NotEqual(suite.T(), rsp.Item.Company, rsp1.Item.Company)

	profile := suite.service.getOnboardingProfileByUser(req.UserId)
	assert.NotNil(suite.T(), profile)

	assert.Equal(suite.T(), profile.UserId, rsp1.Item.UserId)
	assert.Equal(suite.T(), profile.LastStep, rsp1.Item.LastStep)
	assert.Equal(suite.T(), profile.Personal, rsp1.Item.Personal)
	assert.Equal(suite.T(), profile.Help, rsp1.Item.Help)
	assert.Equal(suite.T(), profile.Company, rsp1.Item.Company)

	assert.False(suite.T(), b.IsSent)
}

func (suite *UserProfileTestSuite) TestUserProfile_CreateOrUpdateUserProfile_NewProfile_SetUserEmailConfirmationToken_Error() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	profile := suite.service.getOnboardingProfileByUser(req.UserId)
	assert.Nil(suite.T(), profile)

	redisCl, ok := suite.service.redis.(*redismock.ClientMock)
	assert.True(suite.T(), ok)

	core, recorded := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	redisCl.On("Set").
		Return(redis.NewStatusResult("", errors.New("server not available")))

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), userProfileErrorUnknown, rsp.Message)

	messages := recorded.All()
	assert.Equal(suite.T(), "Save confirm email token to Redis failed", messages[0].Message)
}

func (suite *UserProfileTestSuite) TestUserProfile_CreateOrUpdateUserProfile_NewProfile_SendUserEmailConfirmationToken_Error() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	profile := suite.service.getOnboardingProfileByUser(req.UserId)
	assert.Nil(suite.T(), profile)

	suite.service.broker = mock.NewBrokerMockError()

	core, recorded := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp.Status)
	assert.Equal(suite.T(), userProfileErrorUnknown, rsp.Message)

	messages := recorded.All()
	assert.Equal(suite.T(), "Publication message to user email confirmation to queue failed", messages[0].Message)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetOnboardingProfile_Ok() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)
	assert.IsType(suite.T(), &grpc.UserProfile{}, rsp.Item)
	assert.NotEmpty(suite.T(), rsp.Item.CentrifugoToken)

	req1 := &grpc.GetUserProfileRequest{UserId: req.UserId}
	rsp1 := &grpc.GetUserProfileResponse{}
	err = suite.service.GetUserProfile(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp1.Status)
	assert.Empty(suite.T(), rsp1.Message)
	assert.NotNil(suite.T(), rsp1.Item)
	assert.NotEmpty(suite.T(), rsp1.Item.CentrifugoToken)

	assert.Equal(suite.T(), rsp.Item.Id, rsp1.Item.Id)
	assert.Equal(suite.T(), rsp.Item.UserId, rsp1.Item.UserId)
	assert.Equal(suite.T(), rsp.Item.Personal.LastName, rsp1.Item.Personal.LastName)
	assert.Equal(suite.T(), rsp.Item.Personal.FirstName, rsp1.Item.Personal.FirstName)
	assert.Equal(suite.T(), rsp.Item.Personal.Position, rsp1.Item.Personal.Position)
	assert.Equal(suite.T(), rsp.Item.Help.Other, rsp1.Item.Help.Other)
	assert.Equal(suite.T(), rsp.Item.Help.InternationalSales, rsp1.Item.Help.InternationalSales)
	assert.Equal(suite.T(), rsp.Item.Help.ReleasedGamePromotion, rsp1.Item.Help.ReleasedGamePromotion)
	assert.Equal(suite.T(), rsp.Item.Help.ProductPromotionAndDevelopment, rsp1.Item.Help.ProductPromotionAndDevelopment)
	assert.Equal(suite.T(), rsp.Item.Company, rsp1.Item.Company)
	assert.Equal(suite.T(), rsp.Item.LastStep, rsp1.Item.LastStep)
}

func (suite *UserProfileTestSuite) TestUserProfile_GetOnboardingProfile_NotFound_Error() {
	req := &grpc.GetUserProfileRequest{UserId: bson.NewObjectId().Hex()}
	rsp := &grpc.GetUserProfileResponse{}
	err := suite.service.GetUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), userProfileErrorNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)
}

func (suite *UserProfileTestSuite) TestUserProfile_ConfirmUserEmail_Ok() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	u, err := url.ParseRequestURI(rsp.Item.Email.ConfirmationUrl)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), u)
	assert.NotEmpty(suite.T(), u.RawQuery)

	p, err := url.ParseQuery(u.RawQuery)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), p, 1)
	assert.Contains(suite.T(), p, "token")

	req2 := &grpc.ConfirmUserEmailRequest{Token: p["token"][0]}
	rsp2 := &grpc.CheckProjectRequestSignatureResponse{}
	err = suite.service.ConfirmUserEmail(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	profile := suite.service.getOnboardingProfileByUser(req.UserId)
	assert.NotNil(suite.T(), profile)
	assert.True(suite.T(), profile.Email.Confirmed)
	assert.NotNil(suite.T(), profile.Email.ConfirmedAt)
}

func (suite *UserProfileTestSuite) TestUserProfile_ConfirmUserEmail_TokenNotFound_Error() {
	req := &grpc.ConfirmUserEmailRequest{Token: bson.NewObjectId().Hex()}
	rsp := &grpc.CheckProjectRequestSignatureResponse{}
	err := suite.service.ConfirmUserEmail(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), userProfileEmailConfirmationTokenNotFound, rsp.Message)
}

func (suite *UserProfileTestSuite) TestUserProfile_ConfirmUserEmail_UserNotFound_Error() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	u, err := url.ParseRequestURI(rsp.Item.Email.ConfirmationUrl)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), u)
	assert.NotEmpty(suite.T(), u.RawQuery)

	p, err := url.ParseQuery(u.RawQuery)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), p, 1)
	assert.Contains(suite.T(), p, "token")

	token := p["token"][0]
	err = suite.service.redis.Set(
		suite.service.getConfirmEmailStorageKey(token),
		bson.NewObjectId().Hex(),
		suite.service.cfg.GetEmailConfirmTokenLifetime(),
	).Err()
	assert.NoError(suite.T(), err)

	req2 := &grpc.ConfirmUserEmailRequest{Token: token}
	rsp2 := &grpc.CheckProjectRequestSignatureResponse{}
	err = suite.service.ConfirmUserEmail(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp2.Status)
	assert.Equal(suite.T(), userProfileErrorUnknown, rsp2.Message)
}

func (suite *UserProfileTestSuite) TestUserProfile_ConfirmUserEmail_EmailConfirmedSuccessfully_Error() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	u, err := url.ParseRequestURI(rsp.Item.Email.ConfirmationUrl)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), u)
	assert.NotEmpty(suite.T(), u.RawQuery)

	p, err := url.ParseQuery(u.RawQuery)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), p, 1)
	assert.Contains(suite.T(), p, "token")

	suite.service.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       suite.service.cfg.CentrifugoURL,
			Key:        suite.service.cfg.CentrifugoSecret,
			HTTPClient: mock.NewClientStatusError(),
		},
	)

	req2 := &grpc.ConfirmUserEmailRequest{Token: p["token"][0]}
	rsp2 := &grpc.CheckProjectRequestSignatureResponse{}
	err = suite.service.ConfirmUserEmail(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, rsp2.Status)
	assert.Equal(suite.T(), userProfileErrorUnknown, rsp2.Message)
}

func (suite *UserProfileTestSuite) TestUserProfile_CreatePageReview_Ok() {
	req := &grpc.UserProfile{
		UserId: bson.NewObjectId().Hex(),
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
	rsp := &grpc.GetUserProfileResponse{}

	err := suite.service.CreateOrUpdateUserProfile(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	req1 := &grpc.CreatePageReviewRequest{
		UserId: req.UserId,
		Review: "review 1",
		PageId: "primary_onboarding",
	}
	rsp1 := &grpc.CheckProjectRequestSignatureResponse{}
	err = suite.service.CreatePageReview(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	req1.Review = "review 2"
	err = suite.service.CreatePageReview(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	req1.Review = "review 3"
	err = suite.service.CreatePageReview(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)

	var reviews []*grpc.PageReview
	err = suite.service.db.Collection(collectionOPageReview).Find(bson.M{}).All(&reviews)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reviews, 3)

	for _, v := range reviews {
		assert.NotEmpty(suite.T(), v.UserId)
		assert.NotEmpty(suite.T(), v.Review)
		assert.NotEmpty(suite.T(), v.PageId)
	}
}
