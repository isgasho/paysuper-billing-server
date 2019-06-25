package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type PaymentChannelCostSystemTestSuite struct {
	suite.Suite
	service                    *Service
	log                        *zap.Logger
	cache                      CacheInterface
	paymentChannelCostSystemId string
}

func Test_PaymentChannelCostSystem(t *testing.T) {
	suite.Run(t, new(PaymentChannelCostSystemTestSuite))
}

func (suite *PaymentChannelCostSystemTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

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

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, nil)

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

	suite.paymentChannelCostSystemId = bson.NewObjectId().Hex()

	paymentChannelCostSystem := &billing.PaymentChannelCostSystem{
		Id:        suite.paymentChannelCostSystemId,
		Name:      "VISA",
		Region:    "CIS",
		Country:   "AZ",
		Percent:   1.5,
		FixAmount: 5,
	}

	anotherPaymentChannelCostSystem := &billing.PaymentChannelCostSystem{
		Name:      "VISA",
		Region:    "CIS",
		Country:   "",
		Percent:   2.2,
		FixAmount: 0,
	}
	pccs := []*billing.PaymentChannelCostSystem{paymentChannelCostSystem, anotherPaymentChannelCostSystem}
	if err := suite.service.paymentChannelCostSystem.MultipleInsert(pccs); err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}
}

func (suite *PaymentChannelCostSystemTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GrpcGet_Ok() {
	req := &billing.PaymentChannelCostSystemRequest{
		Name:    "VISA",
		Region:  "CIS",
		Country: "AZ",
	}

	res := &billing.PaymentChannelCostSystem{}

	err := suite.service.GetPaymentChannelCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "AZ")
	assert.Equal(suite.T(), res.FixAmount, float64(5))

	req.Country = ""
	err = suite.service.GetPaymentChannelCostSystem(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "")
	assert.Equal(suite.T(), res.FixAmount, float64(0))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GrpcSet_Ok() {
	req := &billing.PaymentChannelCostSystem{
		Name:      "VISA",
		Region:    "CIS",
		Country:   "AZ",
		Percent:   1.7,
		FixAmount: 4,
	}

	res := billing.PaymentChannelCostSystem{}

	err := suite.service.SetPaymentChannelCostSystem(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Country, "AZ")
	assert.Equal(suite.T(), res.FixAmount, float64(4))
	assert.Equal(suite.T(), res.Id, suite.paymentChannelCostSystemId)

	req2 := &billing.PaymentChannelCostSystem{
		Name:      "MASTERCARD",
		Region:    "US",
		Country:   "",
		Percent:   2.2,
		FixAmount: 1,
	}

	res2 := billing.PaymentChannelCostSystem{}
	err = suite.service.SetPaymentChannelCostSystem(context.TODO(), req2, &res2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res2.Country, "")
	assert.Equal(suite.T(), res2.Region, "US")
	assert.Equal(suite.T(), res2.FixAmount, float64(1))
	assert.NotEqual(suite.T(), res2.Id, suite.paymentChannelCostSystemId)
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Insert_Ok() {
	req := &billing.PaymentChannelCostSystem{
		Name:      "MASTERCARD",
		Region:    "US",
		Country:   "",
		Percent:   2.2,
		FixAmount: 0,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostSystem.Insert(req))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Insert_ErrorCacheUpdate() {
	ci := &mock.CacheInterface{}
	obj := &billing.PaymentChannelCostSystem{
		Name:      "Mastercard",
		Region:    "US",
		Country:   "",
		Percent:   2.1,
		FixAmount: 0,
	}
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, obj.Country)
	ci.On("Set", key, mock2.Anything, mock2.Anything).
		Return(errors.New("service unavailable"))
	suite.service.cacher = ci
	err := suite.service.paymentChannelCostSystem.Insert(obj)

	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "service unavailable")
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_UpdateOk() {
	obj := &billing.PaymentChannelCostSystem{
		Id:        suite.paymentChannelCostSystemId,
		Name:      "Mastercard",
		Region:    "US",
		Country:   "",
		Percent:   2.1,
		FixAmount: 0,
	}

	assert.NoError(suite.T(), suite.service.paymentChannelCostSystem.Update(obj))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Get_Ok() {
	val, err := suite.service.paymentChannelCostSystem.Get("VISA", "CIS", "AZ")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "AZ")
	assert.Equal(suite.T(), val.FixAmount, float64(5))

	val, err = suite.service.paymentChannelCostSystem.Get("VISA", "CIS", "")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), val.Country, "")
	assert.Equal(suite.T(), val.FixAmount, float64(0))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_Delete_Ok() {
	req := &billing.PaymentCostDeleteRequest{
		Id: suite.paymentChannelCostSystemId,
	}
	assert.NoError(suite.T(), suite.service.DeletePaymentChannelCostSystem(context.TODO(), req, &grpc.EmptyResponse{}))

	_, err := suite.service.paymentChannelCostSystem.GetById(suite.paymentChannelCostSystemId)
	assert.EqualError(suite.T(), err, fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem))
}

func (suite *PaymentChannelCostSystemTestSuite) TestPaymentChannelCostSystem_GetAllPaymentChannelCostSystem_Ok() {
	res := &billing.PaymentChannelCostSystemList{}
	err := suite.service.GetAllPaymentChannelCostSystem(context.TODO(), &grpc.EmptyRequest{}, res)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(res.Items), 2)
}
