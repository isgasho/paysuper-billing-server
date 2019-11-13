package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PriceGroupTestSuite struct {
	suite.Suite
	service      *Service
	log          *zap.Logger
	cache        internalPkg.CacheInterface
	priceGroup   *billing.PriceGroup
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
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), &reportingMocks.ReporterService{}, mocks.NewFormatterOK(), mocks.NewBrokerMockOk(), nil, )

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.priceGroupId = bson.NewObjectId().Hex()

	suite.priceGroup = &billing.PriceGroup{
		Id:       suite.priceGroupId,
		Currency: "USD",
		Region:   "USD",
		IsActive: true,
	}
	if err := suite.service.priceGroup.Insert(suite.priceGroup); err != nil {
		suite.FailNow("Insert price group test data failed", "%v", err)
	}
}

func (suite *PriceGroupTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroup_Error_NotFound() {
	pgId := bson.NewObjectId().Hex()
	pgReq := &billing.GetPriceGroupRequest{
		Id: pgId,
	}
	pg := &billing.PriceGroup{}
	err := suite.service.GetPriceGroup(context.TODO(), pgReq, pg)
	assert.EqualError(suite.T(), err, "price_group not found")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroup_Ok() {
	id := bson.NewObjectId().Hex()
	err := suite.service.priceGroup.Insert(&billing.PriceGroup{Id: id, Currency: "USD", IsActive: true})

	pgReq := &billing.GetPriceGroupRequest{
		Id: id,
	}
	pg2 := billing.PriceGroup{}
	err = suite.service.GetPriceGroup(context.TODO(), pgReq, &pg2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "USD", pg2.Currency)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_UpdatePriceGroup_Error_NotFound() {
	pgRes := &billing.PriceGroup{}
	pg := &billing.PriceGroup{
		Id:       bson.NewObjectId().Hex(),
		Currency: "USD",
		Region:   "",
	}
	err := suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_UpdatePriceGroup_Ok() {
	pgRes := &billing.PriceGroup{}
	pg := &billing.PriceGroup{
		Id:       bson.NewObjectId().Hex(),
		Currency: "USD",
		Region:   "",
	}
	err := suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))

	pg.Id = pgRes.Id
	err = suite.service.UpdatePriceGroup(context.TODO(), pg, pgRes)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_Ok() {
	c, err := suite.service.priceGroup.GetById(suite.priceGroup.Id)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), suite.priceGroup.Id, c.Id)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetById_NotFound() {
	_, err := suite.service.priceGroup.GetById(bson.NewObjectId().Hex())

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_Ok() {
	c, err := suite.service.priceGroup.GetByRegion(suite.priceGroup.Region)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), suite.priceGroup.Id, c.Id)
	assert.Equal(suite.T(), suite.priceGroup.Region, c.Region)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetByRegion_NotFound() {
	_, err := suite.service.priceGroup.GetByRegion(bson.NewObjectId().Hex())

	assert.Error(suite.T(), err)
	assert.Errorf(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPriceGroup))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.priceGroup.Insert(&billing.PriceGroup{Currency: "USD"}))
}

func (suite *PriceGroupTestSuite) TestPriceGroup_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	pg := &billing.PriceGroup{
		Id:       bson.NewObjectId().Hex(),
		Currency: "USD",
		Region:   "",
	}
	key := fmt.Sprintf(cachePriceGroupId, pg.Id)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.priceGroup.Insert(pg)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_5() {
	price := suite.service.priceGroup.CalculatePriceWithFraction(0.5, 0.12)
	assert.Equal(suite.T(), 0.5, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.5, 0.98)
	assert.Equal(suite.T(), float64(1), price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.5, 2.5)
	assert.Equal(suite.T(), 2.5, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_05() {
	price := suite.service.priceGroup.CalculatePriceWithFraction(0.05, 1.01)
	assert.Equal(suite.T(), 1.05, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.05, 2.98)
	assert.Equal(suite.T(), float64(3), price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.05, 2.05)
	assert.Equal(suite.T(), 2.05, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_95() {
	price := suite.service.priceGroup.CalculatePriceWithFraction(0.95, 1.01)
	assert.Equal(suite.T(), 1.95, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.95, 2.96)
	assert.Equal(suite.T(), 3.95, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.95, 3.95)
	assert.Equal(suite.T(), 3.95, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0_09() {
	price := suite.service.priceGroup.CalculatePriceWithFraction(0.09, 1.01)
	assert.Equal(suite.T(), 1.09, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.09, 2.95)
	assert.Equal(suite.T(), 2.99, price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0.09, 3.99)
	assert.Equal(suite.T(), 3.99, price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_CalculatePriceWithFraction_Ok_0() {
	price := suite.service.priceGroup.CalculatePriceWithFraction(0, 1.21)
	assert.Equal(suite.T(), float64(2), price)

	price = suite.service.priceGroup.CalculatePriceWithFraction(0, 2)
	assert.Equal(suite.T(), float64(2), price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_MakeCurrencyList_Ok() {
	id := bson.NewObjectId().Hex()
	c := &billing.CountriesList{
		Countries: []*billing.Country{{
			PriceGroupId: id,
			IsoCodeA2:    "RU",
		}},
	}
	pg := []*billing.PriceGroup{{
		Id:       id,
		Currency: "RUB",
		Region:   "RUB",
	}}
	list := suite.service.priceGroup.MakeCurrencyList(pg, c)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), "RUB", list[0].Currency)
	assert.NotEmpty(suite.T(), list[0].Regions)
	assert.Equal(suite.T(), "RUB", list[0].Regions[0].Region)
	assert.NotEmpty(suite.T(), list[0].Regions[0].Country)
	assert.Equal(suite.T(), "RU", list[0].Regions[0].Country[0])
}

func (suite *PriceGroupTestSuite) TestPriceGroup_MakeCurrencyList_Ok_WithoutCountries() {
	id := bson.NewObjectId().Hex()
	c := &billing.CountriesList{
		Countries: []*billing.Country{{}},
	}
	pg := []*billing.PriceGroup{{
		Id:       id,
		Currency: "RUB",
		Region:   "RUB",
	}}
	list := suite.service.priceGroup.MakeCurrencyList(pg, c)
	assert.NotEmpty(suite.T(), list)
	assert.Equal(suite.T(), "RUB", list[0].Currency)
	assert.NotEmpty(suite.T(), list[0].Regions)
	assert.Equal(suite.T(), "RUB", list[0].Regions[0].Region)
	assert.Empty(suite.T(), list[0].Regions[0].Country)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Error_CountryNotFound() {
	req := &grpc.PriceGroupByCountryRequest{}
	res := billing.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "country not found")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Error_PriceGroupNotFound() {
	country := &billing.Country{
		Id:        bson.NewObjectId().Hex(),
		IsoCodeA2: "RU",
	}
	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetById", mock2.Anything).Return(nil, errors.New("price group not found"))
	suite.service.priceGroup = pg

	req := &grpc.PriceGroupByCountryRequest{
		Country: "RU",
	}
	res := billing.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not found")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByCountry_Ok() {
	country := &billing.Country{
		Id:        bson.NewObjectId().Hex(),
		IsoCodeA2: "RU",
	}
	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetById", mock2.Anything).Return(&billing.PriceGroup{}, nil)
	suite.service.priceGroup = pg

	req := &grpc.PriceGroupByCountryRequest{
		Country: "RU",
	}
	res := billing.PriceGroup{}
	err := suite.service.GetPriceGroupByCountry(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencies_Error_NonePriceGroups() {
	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetAll").Return(nil, errors.New("price group not exists"))
	suite.service.priceGroup = pg

	req := &grpc.EmptyRequest{}
	res := grpc.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencies(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceGroupCurrencies_Ok() {
	req := &grpc.EmptyRequest{}
	res := grpc.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencies(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencyByRegion_Error_NonePriceGroup() {
	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetByRegion", mock2.Anything).Return(nil, errors.New("price group not exists"))
	suite.service.priceGroup = pg

	req := &grpc.PriceGroupByRegionRequest{}
	res := grpc.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencyByRegion(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupCurrencyByRegion_Ok() {
	req := &grpc.PriceGroupByRegionRequest{Region: "USD"}
	res := grpc.PriceGroupCurrenciesResponse{}
	err := suite.service.GetPriceGroupCurrencyByRegion(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange() {
	rng := suite.service.getPriceTableRange(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 3)

	assert.Equal(suite.T(), float64(2), rng.From)
	assert.Equal(suite.T(), float64(4), rng.To)
	assert.Equal(suite.T(), int32(1), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange_ByTopValue() {
	rng := suite.service.getPriceTableRange(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 4)

	assert.Equal(suite.T(), float64(2), rng.From)
	assert.Equal(suite.T(), float64(4), rng.To)
	assert.Equal(suite.T(), int32(1), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_ExistRange_ByBottomValue() {
	rng := suite.service.getPriceTableRange(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
		{From: 0, To: 2, Position: 0},
		{From: 2, To: 4, Position: 1},
		{From: 4, To: 6, Position: 2},
	}}, 2)

	assert.Equal(suite.T(), float64(0), rng.From)
	assert.Equal(suite.T(), float64(2), rng.To)
	assert.Equal(suite.T(), int32(0), rng.Position)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getPriceTableRange_NotExistRange() {
	rng := suite.service.getPriceTableRange(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
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
	rep.On("GetByRegion", mock2.Anything).Return(nil, errors.New("error"))
	suite.service.priceTable = rep

	_, err := suite.service.getRecommendedPriceForRegion(&billing.PriceGroup{}, &billing.PriceTableRange{}, 1)
	assert.Error(suite.T(), err)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getRecommendedPriceForRegion_Ok_ExistRange() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock2.Anything).
		Return(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
			{From: 6, To: 10, Position: 0},
		}}, nil)
	suite.service.priceTable = rep

	amount := float64(1)
	pg := &billing.PriceGroup{Fraction: 0}
	pt := &billing.PriceTableRange{From: 0, To: 2, Position: 0}
	price, err := suite.service.getRecommendedPriceForRegion(pg, pt, amount)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(8), price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_getRecommendedPriceForRegion_Ok_NotExistRange() {
	rep := &mocks.PriceTableServiceInterface{}
	rep.
		On("GetByRegion", mock2.Anything).
		Return(&billing.PriceTable{Ranges: []*billing.PriceTableRange{
			{From: 6, To: 10, Position: 0},
		}}, nil)
	suite.service.priceTable = rep

	amount := float64(1)
	pg := &billing.PriceGroup{Fraction: 0}
	pt := &billing.PriceTableRange{From: 0, To: 2, Position: 1}
	price, err := suite.service.getRecommendedPriceForRegion(pg, pt, amount)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), float64(12), price)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Error_NonePriceGroups() {
	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetAll").Return(nil, errors.New("price group not exists"))
	suite.service.priceGroup = pg

	req := &grpc.RecommendedPriceRequest{}
	res := grpc.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price group not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Error_NonePriceTable() {
	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetAll").Return([]*billing.PriceGroup{}, nil)
	suite.service.priceGroup = pg

	pt := &mocks.PriceTableServiceInterface{}
	pt.On("GetByRegion", mock2.Anything).Return(nil, errors.New("price table not exists"))
	suite.service.priceTable = pt

	req := &grpc.RecommendedPriceRequest{}
	res := grpc.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.EqualError(suite.T(), err, "price table not exists")
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetRecommendedPriceByPriceGroup_Ok() {
	pg := &mocks.PriceGroupServiceInterface{}
	pg.On("GetAll").Return([]*billing.PriceGroup{{Fraction: 0}}, nil)
	pg.On("CalculatePriceWithFraction", mock2.Anything, mock2.Anything).Return(float64(1))
	suite.service.priceGroup = pg

	pt := &mocks.PriceTableServiceInterface{}
	pt.
		On("GetByRegion", mock2.Anything).
		Return(&billing.PriceTable{Ranges: []*billing.PriceTableRange{{From: 0, To: 2, Position: 0}}}, nil)
	pt.
		On("GetByRegion", mock2.Anything).
		Return(&billing.PriceTable{Ranges: []*billing.PriceTableRange{{From: 0, To: 2, Position: 0}}}, nil)
	suite.service.priceTable = pt

	req := &grpc.RecommendedPriceRequest{Currency: "USD", Amount: 1}
	res := grpc.RecommendedPriceResponse{}
	err := suite.service.GetRecommendedPriceByPriceGroup(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res.RecommendedPrice, 1)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByRegion_Error() {
	shouldBe := require.New(suite.T())

	rsp := &grpc.GetPriceGroupByRegionResponse{}
	err := suite.service.GetPriceGroupByRegion(context.TODO(), &grpc.GetPriceGroupByRegionRequest{Region: "TEST_SOME_REGION"}, rsp)
	shouldBe.NoError(err)
	shouldBe.EqualValues(400, rsp.Status)
	shouldBe.NotEmpty(rsp.Message)
	shouldBe.Nil(rsp.Group)
}

func (suite *PriceGroupTestSuite) TestPriceGroup_GetPriceGroupByRegion_Ok() {
	shouldBe := require.New(suite.T())

	rsp := &grpc.GetPriceGroupByRegionResponse{}
	err := suite.service.GetPriceGroupByRegion(context.TODO(), &grpc.GetPriceGroupByRegionRequest{Region: "USD"}, rsp)
	shouldBe.NoError(err)
	shouldBe.EqualValues(200, rsp.Status)
	shouldBe.NotNil(rsp.Group)
}
