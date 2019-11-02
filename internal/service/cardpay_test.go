package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"testing"
)

var (
	bankCardRequisites = map[string]string{
		pkg.PaymentCreateFieldPan:    "4000000000000002",
		pkg.PaymentCreateFieldMonth:  "12",
		pkg.PaymentCreateFieldYear:   "2019",
		pkg.PaymentCreateFieldHolder: "Mr. Card Holder",
		pkg.PaymentCreateFieldCvv:    "000",
	}

	orderSimpleBankCard = &billing.Order{
		Id: bson.NewObjectId().Hex(),
		Project: &billing.ProjectOrder{
			Id:         bson.NewObjectId().Hex(),
			Name:       map[string]string{"en": "Project Name"},
			UrlSuccess: "http://localhost/success",
			UrlFail:    "http://localhost/false",
		},
		Description:        fmt.Sprintf(orderDefaultDescription, bson.NewObjectId().Hex()),
		PrivateStatus:      constant.OrderStatusNew,
		CreatedAt:          ptypes.TimestampNow(),
		IsJsonRequest:      false,
		Items:              []*billing.OrderItem{},
		TotalPaymentAmount: 10.2,
		Currency:           "RUB",
		User: &billing.OrderUser{
			Id:     bson.NewObjectId().Hex(),
			Object: "user",
			Email:  "test@unit.test",
			Ip:     "127.0.0.1",
			Locale: "ru",
			Address: &billing.OrderBillingAddress{
				Country:    "RU",
				City:       "St.Petersburg",
				PostalCode: "190000",
				State:      "SPE",
			},
			TechEmail: fmt.Sprintf("%s@paysuper.com", bson.NewObjectId().Hex()),
		},
		PaymentMethod: &billing.PaymentMethodOrder{
			Id:         bson.NewObjectId().Hex(),
			Name:       "Bank card",
			Handler:    "cardpay",
			ExternalId: "BANKCARD",
			Params: &billing.PaymentMethodParams{
				Currency:       "USD",
				TerminalId:     "123456",
				Secret:         "secret_key",
				SecretCallback: "callback_secret_key",
				ApiUrl:         "https://sandbox.cardpay.com",
			},
			PaymentSystemId: bson.NewObjectId().Hex(),
			Group:           "BANKCARD",
			Saved:           false,
		},
	}
)

type CardPayTestSuite struct {
	suite.Suite

	cfg         *config.Config
	handler     *cardPay
	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs
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

	suite.handler = &cardPay{}
}

func (suite *CardPayTestSuite) TearDownTest() {}

func (suite *CardPayTestSuite) TestCardPay_GetCardPayOrder_Ok() {
	res, err := suite.handler.getCardPayOrder(
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
	suite.handler.httpClient = mocks.NewCardPayHttpClientStatusOk()
	url, err := suite.handler.CreatePayment(
		orderSimpleBankCard,
		suite.cfg.GetRedirectUrlSuccess(nil),
		suite.cfg.GetRedirectUrlFail(nil),
		bankCardRequisites,
	)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), url)
}
