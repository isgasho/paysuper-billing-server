package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	casbinMocks "github.com/paysuper/casbin-server/pkg/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ZipCodeTestSuite struct {
	suite.Suite
	service *Service
}

func Test_ZipCode(t *testing.T) {
	suite.Run(t, new(ZipCodeTestSuite))
}

func (suite *ZipCodeTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	zipCode := &billing.ZipCode{
		Zip:     "98001",
		Country: "US",
		City:    "Washington",
		State: &billing.ZipCodeState{
			Code: "NJ",
			Name: "New Jersey",
		},
		CreatedAt: ptypes.TimestampNow(),
	}

	err = db.Collection(collectionZipCode).Insert(zipCode)

	if err != nil {
		suite.FailNow("Insert zip codes test data failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	cache, err := NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
	)
	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *ZipCodeTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *ZipCodeTestSuite) TestZipCode_GetExist_Ok() {
	zip := "98001"
	zipCode, err := suite.service.zipCode.getByZipAndCountry(zip, CountryCodeUSA)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), zipCode)
	assert.Equal(suite.T(), zip, zipCode.Zip)
	assert.Equal(suite.T(), CountryCodeUSA, zipCode.Country)

	zipCode, err = suite.service.zipCode.getByZipAndCountry(zip, CountryCodeUSA)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), zipCode)
	assert.Equal(suite.T(), zip, zipCode.Zip)
	assert.Equal(suite.T(), CountryCodeUSA, zipCode.Country)
}

func (suite *ZipCodeTestSuite) TestZipCode_NotFound_Error() {
	zip := "98002"
	zipCode, err := suite.service.zipCode.getByZipAndCountry(zip, CountryCodeUSA)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionZipCode), err.Error())
	assert.Nil(suite.T(), zipCode)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipCode_Ok() {
	req := &grpc.FindByZipCodeRequest{
		Zip:     "98",
		Country: "US",
	}
	rsp := &grpc.FindByZipCodeResponse{}
	err := suite.service.FindByZipCode(context.Background(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(1), rsp.Count)
	assert.Len(suite.T(), rsp.Items, 1)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipCode_NotUSA_Ok() {
	req := &grpc.FindByZipCodeRequest{
		Zip:     "99",
		Country: "RU",
	}
	rsp := &grpc.FindByZipCodeResponse{}
	err := suite.service.FindByZipCode(context.Background(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}

func (suite *ZipCodeTestSuite) TestZipCode_FindByZipCode_USANotFound_Ok() {
	req := &grpc.FindByZipCodeRequest{
		Zip:     "99",
		Country: "US",
	}
	rsp := &grpc.FindByZipCodeResponse{}
	err := suite.service.FindByZipCode(context.Background(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(0), rsp.Count)
	assert.Empty(suite.T(), rsp.Items)
}
