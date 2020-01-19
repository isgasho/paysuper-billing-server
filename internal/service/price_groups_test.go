package service

import (
	"context"
	"errors"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
)

type PriceGroupTestSuite struct {
	suite.Suite
	service      *Service
	log          *zap.Logger
	cache        database.CacheInterface
	priceGroup   *billingpb.PriceGroup
	priceGroupId string
}

func Test_PriceGroup(t *testing.T) {
	suite.Run(t, new(PriceGroupTestSuite))
}

func (suite *PriceGroupTestSuite) SetupTest() {
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
	suite.cache, err = database.NewCacheRedis(redisdb, "cache")
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
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

	suite.priceGroupId = primitive.NewObjectID().Hex()

	suite.priceGroup = &billingpb.PriceGroup{
		Id:       suite.priceGroupId,
		Currency: "USD",
		Region:   "USD",
		IsActive: true,
	}
	if err := suite.service.priceGroupRepository.Insert(context.TODO(), suite.priceGroup); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}
}

func (suite *PriceGroupTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroup_Error_NotFound() {
	pgId := primitive.NewObjectID().Hex()
	pgReq := &billingpb.GetPriceGroupRequest{
		Id: pgId,
	}
	pg := &billingpb.PriceGroup{}
	err := suite.service.GetPriceGroup(context.TODO(), pgReq, pg)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroup_Ok() {
	id := primitive.NewObjectID().Hex()
	err := suite.service.priceGroupRepository.Insert(context.TODO(), &billingpb.PriceGroup{Id: id, Currency: "USD", IsActive: true})

	pgReq := &billingpb.GetPriceGroupRequest{
		Id: id,
	}
	pg2 := billingpb.PriceGroup{}
	err = suite.service.GetPriceGroup(context.TODO(), pgReq, &pg2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "USD", pg2.Currency)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_UpdatePriceGroup_Error_NotFound() {
	pgRes := &billingpb.PriceGroup{}
	pg := &billingpb.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
	}
	err := suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_UpdatePriceGroup_Ok() {
	pgRes := &billingpb.PriceGroup{}
	pg := &billingpb.PriceGroup{
		Id:       primitive.NewObjectID().Hex(),
		Currency: "USD",
		Region:   "",
	}
	err := suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.Error(suite.T(), err)

	pg.Id = pgRes.Id
	err = suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Error_CountryNotFound() {
	req := &billingpb.PriceGroupByCountryRequest{}
	res := billingpb.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "country not found")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Error_PriceGroupNotFound() {
	country := &billingpb.Country{
		Id:        primitive.NewObjectID().Hex(),
		IsoCodeA2: "RU",
	}
	if err := suite.service.country.Insert(context.TODO(), country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetById", mock2.Anything, mock2.Anything).Return(nil, errors.New("price group not found"))
	suite.service.priceGroupRepository = pg

	req := &billingpb.PriceGroupByCountryRequest{
		Country: "RU",
	}
	res := billingpb.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not found")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Ok() {
	country := &billingpb.Country{
		Id:        primitive.NewObjectID().Hex(),
		IsoCodeA2: "RU",
	}
	if err := suite.service.country.Insert(context.TODO(), country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetById", mock2.Anything, mock2.Anything).Return(&billingpb.PriceGroup{}, nil)
	suite.service.priceGroupRepository = pg

	req := &billingpb.PriceGroupByCountryRequest{
		Country: "RU",
	}
	res := billingpb.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencies_Error_NonePriceGroups() {
	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetAll", mock2.Anything).Return(nil, errors.New("price group not exists"))
	suite.service.priceGroupRepository = pg

	req := &billingpb.EmptyRequest{}
	res := billingpb.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencies(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceGroupCurrencies_Ok() {
	req := &billingpb.EmptyRequest{}
	res := billingpb.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencies(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencyByRegion_Error_NonePriceGroup() {
	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetByRegion", mock2.Anything, mock2.Anything).Return(nil, errors.New("price group not exists"))
	suite.service.priceGroupRepository = pg

	req := &billingpb.PriceGroupByRegionRequest{}
	res := billingpb.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencyByRegion(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencyByRegion_Ok() {
	req := &billingpb.PriceGroupByRegionRequest{Region: "USD"}
	res := billingpb.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencyByRegion(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange() {
	rng := suite.service.getPriceTableRange(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 3)

	assert.Equal(suite.T(), float64(2), rng.From)
	assert.Equal(suite.T(), float64(4), rng.To)
	assert.Equal(suite.T(), int32(1), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange_ByTopValue() {
	rng := suite.service.getPriceTableRange(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 4)

	assert.Equal(suite.T(), float64(2), rng.From)
	assert.Equal(suite.T(), float64(4), rng.To)
	assert.Equal(suite.T(), int32(1), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange_ByBottomValue() {
	rng := suite.service.getPriceTableRange(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 2)

	assert.Equal(suite.T(), float64(0), rng.From)
	assert.Equal(suite.T(), float64(2), rng.To)
	assert.Equal(suite.T(), int32(0), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_NotExistRange() {
	rng := suite.service.getPriceTableRange(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 9)

	assert.Equal(suite.T(), float64(8), rng.From)
	assert.Equal(suite.T(), float64(10), rng.To)
	assert.Equal(suite.T(), int32(4), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getRecommendedPriceForRegion_Error_RegionNotFound() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.On("GetByRegion", mock2.Anything, mock2.Anything).Return(nil, errors.New("error"))
	suite.service.priceTable = rep

	_, err := suite.service.getRecommendedPriceForRegion(context.TODO(), &billingpb.PriceGroup{}, &billingpb.PriceTableRange{}, 1)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getRecommendedPriceForRegion_Ok_ExistRange() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock2.Anything, mock2.Anything).
		Return(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
			{From: 6, To: 10, Position: 0},
		}}, nil)
	suite.service.priceTable = rep

	amount := float64(1)
	pg := &billingpb.PriceGroup{Fraction: 0}
	pt := &billingpb.PriceTableRange{From: 0, To: 2, Position: 0}
	price, err := suite.service.getRecommendedPriceForRegion(context.TODO(), pg, pt, amount)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(8), price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getRecommendedPriceForRegion_Ok_NotExistRange() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock2.Anything, mock2.Anything).
		Return(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{
			{From: 6, To: 10, Position: 0},
		}}, nil)
	suite.service.priceTable = rep

	amount := float64(1)
	pg := &billingpb.PriceGroup{Fraction: 0}
	pt := &billingpb.PriceTableRange{From: 0, To: 2, Position: 1}
	price, err := suite.service.getRecommendedPriceForRegion(context.TODO(), pg, pt, amount)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(12), price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Error_NonePriceGroups() {
	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetAll", mock2.Anything).Return(nil, errors.New("price group not exists"))
	suite.service.priceGroupRepository = pg

	req := &billingpb.RecommendedPriceRequest{}
	res := billingpb.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Error_NonePriceTable() {
	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetAll", mock2.Anything).Return([]*billingpb.PriceGroup{}, nil)
	suite.service.priceGroupRepository = pg

	pt := &mocks.PriceTableServiceInterface{}
	pt.On("GetByRegion", mock2.Anything, mock2.Anything).Return(nil, errors.New("price table not exists"))
	suite.service.priceTable = pt

	req := &billingpb.RecommendedPriceRequest{}
	res := billingpb.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price table not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Ok() {
	pg := &mocks.PriceGroupRepositoryInterface{}
	pg.On("GetAll", mock2.Anything).Return([]*billingpb.PriceGroup{{Fraction: 0}}, nil)
	pg.On("CalculatePriceWithFraction", mock2.Anything, mock2.Anything, mock2.Anything).Return(float64(1))
	suite.service.priceGroupRepository = pg

	pt := &mocks.PriceTableServiceInterface{}
	pt.
		On("GetByRegion", mock2.Anything, mock2.Anything).
		Return(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{{From: 0, To: 2, Position: 0}}}, nil)
	pt.
		On("GetByRegion", mock2.Anything, mock2.Anything).
		Return(&billingpb.PriceTable{Ranges: []*billingpb.PriceTableRange{{From: 0, To: 2, Position: 0}}}, nil)
	suite.service.priceTable = pt

	req := &billingpb.RecommendedPriceRequest{Currency: "USD", Amount: 1}
	res := billingpb.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res.RecommendedPrice, 1)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByRegion_Error() {
	shouldBe := require.New(suite.T())

	rsp := &billingpb.GetPriceGroupByRegionResponse{}
	err := suite.service.GetPriceGroupByRegion(context.TODO(), &billingpb.GetPriceGroupByRegionRequest{Region: "TEST_SOME_REGION"}, rsp)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, rsp.Status)
	shouldBe.NotEmpty(rsp.Message)
	shouldBe.Nil(rsp.Group)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByRegion_Ok() {
	shouldBe := require.New(suite.T())

	rsp := &billingpb.GetPriceGroupByRegionResponse{}
	err := suite.service.GetPriceGroupByRegion(context.TODO(), &billingpb.GetPriceGroupByRegionRequest{Region: "USD"}, rsp)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, rsp.Status)
	shouldBe.NotNil(rsp.Group)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_makePriceGroupCurrencyList_Ok() {
	id := primitive.NewObjectID().Hex()
	c := &billingpb.CountriesList{
		Countries: []*billingpb.Country{{
			PriceGroupId: id,
			IsoCodeA2:    "RU",
		}},
	}
	pg := []*billingpb.PriceGroup{{
		Id:       id,
		Currency: "RUB",
		Region:   "RUB",
	}}
	list := suite.service.makePriceGroupCurrencyList(pg, c)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), "RUB", list[0].Currency)
	assert.NotEmpty(suite.T(), list[0].Regions)
	assert.Equal(suite.T(), "RUB", list[0].Regions[0].Region)
	assert.NotEmpty(suite.T(), list[0].Regions[0].Country)
	assert.Equal(suite.T(), "RU", list[0].Regions[0].Country[0])
}

func (suite *PriceGroupTestSuite) TestPriceGroup_makePriceGroupCurrencyList_Ok_WithoutCountries() {
	id := primitive.NewObjectID().Hex()
	c := &billingpb.CountriesList{
		Countries: []*billingpb.Country{{}},
	}
	pg := []*billingpb.PriceGroup{{
		Id:       id,
		Currency: "RUB",
		Region:   "RUB",
	}}
	list := suite.service.makePriceGroupCurrencyList(pg, c)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), "RUB", list[0].Currency)
	assert.NotEmpty(suite.T(), list[0].Regions)
	assert.Equal(suite.T(), "RUB", list[0].Regions[0].Region)
	assert.Empty(suite.T(), list[0].Regions[0].Country)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_5() {
	price := suite.service.calculatePriceWithFraction(0.5, 0.12)
	assert.Equal(suite.T(), 0.5, price)

	price = suite.service.calculatePriceWithFraction(0.5, 0.98)
	assert.Equal(suite.T(), float64(1), price)

	price = suite.service.calculatePriceWithFraction(0.5, 2.5)
	assert.Equal(suite.T(), 2.5, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_05() {
	price := suite.service.calculatePriceWithFraction(0.05, 1.01)
	assert.Equal(suite.T(), 1.05, price)

	price = suite.service.calculatePriceWithFraction(0.05, 2.98)
	assert.Equal(suite.T(), float64(3), price)

	price = suite.service.calculatePriceWithFraction(0.05, 2.05)
	assert.Equal(suite.T(), 2.05, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_95() {
	price := suite.service.calculatePriceWithFraction(0.95, 1.01)
	assert.Equal(suite.T(), 1.95, price)

	price = suite.service.calculatePriceWithFraction(0.95, 2.96)
	assert.Equal(suite.T(), 3.95, price)

	price = suite.service.calculatePriceWithFraction(0.95, 3.95)
	assert.Equal(suite.T(), 3.95, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_09() {
	price := suite.service.calculatePriceWithFraction(0.09, 1.01)
	assert.Equal(suite.T(), 1.09, price)

	price = suite.service.calculatePriceWithFraction(0.09, 2.95)
	assert.Equal(suite.T(), 2.99, price)

	price = suite.service.calculatePriceWithFraction(0.09, 3.99)
	assert.Equal(suite.T(), 3.99, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0() {
	price := suite.service.calculatePriceWithFraction(0, 1.21)
	assert.Equal(suite.T(), float64(2), price)

	price = suite.service.calculatePriceWithFraction(0, 2)
	assert.Equal(suite.T(), float64(2), price)
}
