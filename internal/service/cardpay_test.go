package service

import (
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type CardpayTestSuite struct {
	suite.Suite
	cfg     *config.Config
	service *Service
	log     *zap.Logger
}

func Test_Cardpay(t *testing.T) {
	suite.Run(t, new(CardpayTestSuite))
}

func (suite *CardpayTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"
	suite.cfg = cfg

	settings := database.Connection{
		Host:     cfg.MongoHost,
		Database: cfg.MongoDatabase,
		User:     cfg.MongoUser,
		Password: cfg.MongoPassword,
	}

	db, err := database.NewDatabase(settings)

	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	currency := []interface{}{rub}

	err = db.Collection(pkg.CollectionCurrency).Insert(currency...)

	if err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil)
	err = suite.service.Init()

	if err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}
}

func (suite *CardpayTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *CardpayTestSuite) TestCardpay_fillPaymentDataCrypto() {
	var (
		name    = "Bitcoin"
		address = "1ByR2GSfDMuFGVoUzh4a5pzgrVuoTdr8wU"
	)

	order := &billing.Order{
		PaymentMethod: &billing.PaymentMethodOrder{
			Name: name,
		},
	}
	processor := &paymentProcessor{cfg: suite.cfg.PaymentSystemConfig, order: order, service: suite.service}
	h := &cardPay{processor: processor}

	assert.Empty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.Empty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.Nil(suite.T(), h.processor.order.PaymentMethod.CryptoCurrency)

	req := &billing.CardPayPaymentCallback{
		PaymentData: &billing.CallbackCardPayPaymentData{
			Status: pkg.CardPayPaymentResponseStatusInProgress,
		},
		CryptocurrencyAccount: &billing.CallbackCardPayCryptoCurrencyAccount{
			CryptoAddress:       address,
			CryptoTransactionId: "7d8c131c-092c-4a5b-83ed-5137ecb9b083",
			PrcAmount:           "0.0001",
			PrcCurrency:         "BTC",
		},
	}
	err := h.fillPaymentDataCrypto(req)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.NotNil(suite.T(), h.processor.order.PaymentMethod.CryptoCurrency)

	assert.Equal(suite.T(), h.processor.order.PaymentMethodPayerAccount, address)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.CryptoCurrency.Brand, name)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.CryptoCurrency.Address, address)
}

func (suite *CardpayTestSuite) TestCardpay_fillPaymentDataEwallet() {
	var (
		name    = "yamoney"
		account = "41001811131268"
	)

	order := &billing.Order{
		PaymentMethod: &billing.PaymentMethodOrder{
			Name: name,
		},
	}
	processor := &paymentProcessor{cfg: suite.cfg.PaymentSystemConfig, order: order, service: suite.service}
	h := &cardPay{processor: processor}

	assert.Empty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.Empty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.Nil(suite.T(), h.processor.order.PaymentMethod.Wallet)

	req := &billing.CardPayPaymentCallback{
		PaymentData: &billing.CallbackCardPayPaymentData{
			Status: pkg.CardPayPaymentResponseStatusInProgress,
		},
		EwalletAccount: &billing.CardPayEWalletAccount{
			Id: account,
		},
	}
	err := h.fillPaymentDataEwallet(req)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.NotNil(suite.T(), h.processor.order.PaymentMethod.Wallet)

	assert.Equal(suite.T(), h.processor.order.PaymentMethodPayerAccount, account)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Wallet.Brand, name)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Wallet.Account, account)
}

func (suite *CardpayTestSuite) TestCardpay_fillPaymentDataCard() {
	var (
		name      = "bank_card"
		maskedPan = "444444******4448"
		expMonth  = "10"
		expYear   = "2021"
		cardBrand = "VISA"
	)

	order := &billing.Order{
		PaymentMethod: &billing.PaymentMethodOrder{
			Name: name,
		},
		PaymentRequisites: make(map[string]string),
	}
	order.PaymentRequisites["card_brand"] = cardBrand
	order.PaymentRequisites["pan"] = maskedPan
	order.PaymentRequisites["month"] = expMonth
	order.PaymentRequisites["year"] = expYear

	processor := &paymentProcessor{cfg: suite.cfg.PaymentSystemConfig, order: order, service: suite.service}
	h := &cardPay{processor: processor}

	assert.Empty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.Empty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.Nil(suite.T(), h.processor.order.PaymentMethod.Card)

	req := &billing.CardPayPaymentCallback{
		PaymentData: &billing.CallbackCardPayPaymentData{
			Status: pkg.CardPayPaymentResponseStatusInProgress,
			Rrn:    "12312324312",
			Is_3D:  true,
		},
		CardAccount: &billing.CallbackCardPayBankCardAccount{
			MaskedPan:          maskedPan,
			Holder:             "IVAN IVANOV",
			IssuingCountryCode: "RU",
			Token:              "9b888a3ffbb44bbcb8edf7ae71887e43",
		},
	}
	err := h.fillPaymentDataCard(req)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodPayerAccount)
	assert.NotEmpty(suite.T(), h.processor.order.PaymentMethodTxnParams)
	assert.NotNil(suite.T(), h.processor.order.PaymentMethod.Card)

	assert.Equal(suite.T(), h.processor.order.PaymentMethodPayerAccount, maskedPan)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.Brand, cardBrand)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.Masked, maskedPan)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.First6, "444444")
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.Last4, "4448")
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.ExpiryMonth, expMonth)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.ExpiryYear, expYear)
	assert.Equal(suite.T(), h.processor.order.PaymentMethod.Card.Secure3D, true)
}
