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

	merchant *billing.Merchant
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

	err = repository.AddMerchantUser(&billing.UserRoleMerchant{
		MerchantId: suite.merchant.Id, Id: bson.NewObjectId().Hex(), User: &billing.UserRoleProfile{}, ProjectRole: []*billing.UserRoleProject{
			{Role: pkg.MerchantUserRoleDeveloper},
		},
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