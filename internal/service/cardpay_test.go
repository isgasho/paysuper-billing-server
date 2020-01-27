package service

import (
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"testing"
)

var (
	bankCardRequisites = map[string]string{
		billingpb.PaymentCreateFieldPan:    "4000000000000002",
		billingpb.PaymentCreateFieldMonth:  "12",
		billingpb.PaymentCreateFieldYear:   "2019",
		billingpb.PaymentCreateFieldHolder: "Mr. Card Holder",
		billingpb.PaymentCreateFieldCvv:    "000",
	}

	orderSimpleBankCard = &billingpb.Order{
		Id: primitive.NewObjectID().Hex(),
		Project: &billingpb.ProjectOrder{
			Id:         primitive.NewObjectID().Hex(),
			Name:       map[string]string{"en": "Project Name"},
			UrlSuccess: "http://localhost/success",
			UrlFail:    "http://localhost/false",
		},
		Description:        fmt.Sprintf(orderDefaultDescription, primitive.NewObjectID().Hex()),
		PrivateStatus:      recurringpb.OrderStatusNew,
		CreatedAt:          ptypes.TimestampNow(),
		IsJsonRequest:      false,
		Items:              []*billingpb.OrderItem{},
		TotalPaymentAmount: 10.2,
		Currency:           "RUB",
		User: &billingpb.OrderUser{
			Id:     primitive.NewObjectID().Hex(),
			Object: "user",
			Email:  "test@unit.test",
			Ip:     "127.0.0.1",
			Locale: "ru",
			Address: &billingpb.OrderBillingAddress{
				Country:    "RU",
				City:       "St.Petersburg",
				PostalCode: "190000",
				State:      "SPE",
			},
			TechEmail: fmt.Sprintf("%s@paysuper.com", primitive.NewObjectID().Hex()),
		},
		PaymentMethod: &billingpb.PaymentMethodOrder{
			Id:         primitive.NewObjectID().Hex(),
			Name:       "Bank card",
			Handler:    "cardpay",
			ExternalId: "BANKCARD",
			Params: &billingpb.PaymentMethodParams{
				Currency:       "USD",
				TerminalId:     "123456",
				Secret:         "secret_key",
				SecretCallback: "callback_secret_key",
				ApiUrl:         "https://sandbox.cardpay.com",
			},
			PaymentSystemId: primitive.NewObjectID().Hex(),
			Group:           "BANKCARD",
			Saved:           false,
		},
	}
)

type CardPayTestSuite struct {
	suite.Suite

	cfg          *config.Config
	handler      Gate
	typedHandler *cardPay
	logObserver  *zap.Logger
	zapRecorder  *observer.ObservedLogs
}

func Test_CardPay(t *testing.T) {
	suite.Run(t, new(CardPayTestSuite))
}

func (suite *CardPayTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}

	suite.cfg = cfg

	var core zapcore.Core

	lvl := zap.NewAtomicLevel()
	core, suite.zapRecorder = observer.New(lvl)
	suite.logObserver = zap.New(core)
	zap.ReplaceGlobals(suite.logObserver)

	suite.handler = newCardPayHandler()
	handler, ok := suite.handler.(*cardPay)
	assert.True(suite.T(), ok)
	suite.typedHandler = handler
}

func (suite *CardPayTestSuite) TearDownTest() {}

func (suite *CardPayTestSuite) TestCardPay_GetCardPayOrder_Ok() {
	res, err := suite.typedHandler.getCardPayOrder(
		orderSimpleBankCard,
		suite.cfg.GetRedirectUrlSuccess(nil),
		suite.cfg.GetRedirectUrlFail(nil),
		bankCardRequisites,
	)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), res.CardAccount)
	assert.NotNil(suite.T(), res.ReturnUrls)
	assert.Equal(suite.T(), res.ReturnUrls.SuccessUrl, suite.cfg.GetRedirectUrlSuccess(nil))
	assert.Equal(suite.T(), res.ReturnUrls.CancelUrl, suite.cfg.GetRedirectUrlFail(nil))
	assert.Equal(suite.T(), res.ReturnUrls.DeclineUrl, suite.cfg.GetRedirectUrlFail(nil))
}

func (suite *CardPayTestSuite) TestCardPay_CreatePayment_Mock_Ok() {
	suite.typedHandler.httpClient = mocks.NewCardPayHttpClientStatusOk()
	url, err := suite.handler.CreatePayment(
		orderSimpleBankCard,
		suite.cfg.GetRedirectUrlSuccess(nil),
		suite.cfg.GetRedirectUrlFail(nil),
		bankCardRequisites,
	)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), url)
}
