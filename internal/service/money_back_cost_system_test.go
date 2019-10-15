package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	casbinMocks "github.com/paysuper/casbin-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type MoneyBackCostSystemTestSuite struct {
	suite.Suite
	service               *Service
	log                   *zap.Logger
	cache                 internalPkg.CacheInterface
	moneyBackCostSystemId string
	merchantId            string
}

func Test_MoneyBackCostSystem(t *testing.T) {
	suite.Run(t, new(MoneyBackCostSystemTestSuite))
}

func (suite *MoneyBackCostSystemTestSuite) SetupTest() {
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
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache, mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		&casbinMocks.CasbinService{},
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	countryAz := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "AZ",
		Region:          "CIS",
		Currency:        "AZN",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "AZN",
	}
	countryUs := &billing.Country{
		Id:              bson.NewObjectId().Hex(),
		IsoCodeA2:       "US",
		Region:          "US",
		Currency:        "USD",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "USD",
	}
	countries := []*billing.Country{countryAz, countryUs}
	if err := suite.service.country.MultipleInsert(countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.moneyBackCostSystemId = bson.NewObjectId().Hex()
	suite.merchantId = bson.NewObjectId().Hex()

	moneyBackCostSystem := &billing.MoneyBackCostSystem{
		Id:             suite.moneyBackCostSystemId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3,
		FixAmount:      5,
	}

	moneyBackCostSystem2 := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       30,
		PaymentStage:   1,
		Percent:        10,
		FixAmount:      15,
	}

	anotherMoneyBackCostSystem := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        2,
		FixAmount:      3,
	}
	pucs := []*billing.MoneyBackCostSystem{moneyBackCostSystem, moneyBackCostSystem2, anotherMoneyBackCostSystem}
	if err := suite.service.moneyBackCostSystem.MultipleInsert(pucs); err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}
}

func (suite *MoneyBackCostSystemTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GrpcGet_Ok() {
	req := &billing.MoneyBackCostSystemRequest{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		Days:           10,
		PaymentStage:   1,
	}

	res := &grpc.MoneyBackCostSystemResponse{}

	err := suite.service.GetMoneyBackCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(5))
	assert.Equal(suite.T(), res.Item.Percent, float64(3))

	req.Country = ""
	err = suite.service.GetMoneyBackCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(3))
	assert.Equal(suite.T(), res.Item.Percent, float64(2))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GrpcSet_Ok() {
	req := &billing.MoneyBackCostSystem{
		Id:             suite.moneyBackCostSystemId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3.33,
		FixAmount:      7.5,
	}

	res := grpc.MoneyBackCostSystemResponse{}

	err := suite.service.SetMoneyBackCostSystem(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Country, "AZ")
	assert.Equal(suite.T(), res.Item.FixAmount, float64(7.5))
	assert.Equal(suite.T(), res.Item.Id, suite.moneyBackCostSystemId)
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Insert_Ok() {
	req := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "US",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3.33,
		FixAmount:      7.5,
	}

	assert.NoError(suite.T(), suite.service.moneyBackCostSystem.Insert(req))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Insert_ErrorCacheUpdate() {
	ci := &mocks.CacheInterface{}
	obj := &billing.MoneyBackCostSystem{
		Name:           "MASTERCARD",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "US",
		Country:        "",
		DaysFrom:       0,
		PaymentStage:   1,
		Percent:        3.33,
		FixAmount:      7.5,
	}
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.moneyBackCostSystem.Insert(obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_UpdateOk() {
	obj := &billing.MoneyBackCostSystem{
		Id:             suite.moneyBackCostSystemId,
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		DaysFrom:       0,
		PaymentStage:   2,
		Percent:        4,
		FixAmount:      7,
	}

	assert.NoError(suite.T(), suite.service.moneyBackCostSystem.Update(obj))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Get_Ok() {
	val, err := suite.service.moneyBackCostSystem.Get("VISA", "USD", "chargeback", "CIS", "AZ", 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 2)
	assert.Equal(suite.T(), val.Items[0].Country, "AZ")
	assert.Equal(suite.T(), val.Items[0].FixAmount, float64(5))

	val, err = suite.service.moneyBackCostSystem.Get("VISA", "USD", "chargeback", "CIS", "", 1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(val.Items), 1)
	assert.Equal(suite.T(), val.Items[0].Country, "")
	assert.Equal(suite.T(), val.Items[0].FixAmount, float64(3))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_getMoneyBackCostSystem() {
	req := &billing.MoneyBackCostSystemRequest{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "CIS",
		Country:        "AZ",
		Days:           5,
		PaymentStage:   1,
	}

	val, err := suite.service.getMoneyBackCostSystem(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(0))
	assert.Equal(suite.T(), val.Percent, float64(3))
	assert.Equal(suite.T(), val.FixAmount, float64(5))

	req.Days = 45
	val, err = suite.service.getMoneyBackCostSystem(req)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.DaysFrom, int32(30))
	assert.Equal(suite.T(), val.Percent, float64(10))
	assert.Equal(suite.T(), val.FixAmount, float64(15))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_Delete_Ok() {
	req := &billing.PaymentCostDeleteRequest{
		Id: suite.moneyBackCostSystemId,
	}

	res := &grpc.ResponseError{}
	err := suite.service.DeleteMoneyBackCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	_, err = suite.service.moneyBackCostSystem.GetById(suite.moneyBackCostSystemId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionMoneyBackCostSystem))
}

func (suite *MoneyBackCostSystemTestSuite) TestMoneyBackCostSystem_GetAllMoneyBackCostSystem_Ok() {
	res := &grpc.MoneyBackCostSystemListResponse{}
	err := suite.service.GetAllMoneyBackCostSystem(context.TODO(), &grpc.EmptyRequest{}, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), len(res.Item.Items), 3)
}
