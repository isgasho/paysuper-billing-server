package service

import (
	"context"
	"fmt"
	rabbitmq "github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type AccountingEntryTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger

	project    *billing.Project
	pmBankCard *billing.PaymentMethod
}

func Test_AccountingEntry(t *testing.T) {
	suite.Run(t, new(AccountingEntryTestSuite))
}

func (suite *AccountingEntryTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	rub := &billing.Currency{
		CodeInt:  643,
		CodeA3:   "RUB",
		Name:     &billing.Name{Ru: "Российский рубль", En: "Russian ruble"},
		IsActive: true,
	}

	rate := &billing.CurrencyRate{
		CurrencyFrom: 643,
		CurrencyTo:   643,
		Rate:         1,
		Date:         ptypes.TimestampNow(),
		IsActive:     true,
	}

	country := &billing.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
	}

	paymentSystem := &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: rub,
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "mock_ok",
	}
	pmBankCard := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 100,
		MaxPaymentAmount: 15000,
		Currencies:       []int32{643, 840, 980},
		ExternalId:       "BANKCARD",
		TestSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			},
		},
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId:     "15985",
				Secret:         "A1tph4I6BD0f",
				SecretCallback: "0V1rJ7t4jCRv",
			}},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: paymentSystem.Id,
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	merchant := &billing.Merchant{
		Id:      bson.NewObjectId().Hex(),
		Name:    "Unit test",
		Country: country.IsoCodeA2,
		Zip:     "190000",
		City:    "St.Petersburg",
		Contacts: &billing.MerchantContact{
			Authorized: &billing.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billing.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billing.MerchantBanking{
			Currency: rub,
			Name:     "Bank name",
		},
		IsVatEnabled:              false,
		IsCommissionToUserEnabled: false,
		Status:                    pkg.MerchantStatusDraft,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned: true,
		PaymentMethods: map[string]*billing.MerchantPaymentMethod{
			pmBankCard.Id: {
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   pmBankCard.Id,
					Name: pmBankCard.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: 2.5,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      30,
						Currency: rub.CodeA3,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{
					TerminalId:       "1234567890",
					TerminalPassword: "0987654321",
					Integrated:       true,
				},
				IsActive: true,
			},
		},
	}

	project := &billing.Project{
		Id:                       bson.NewObjectId().Hex(),
		CallbackCurrency:         rub.CodeA3,
		CallbackProtocol:         "default",
		LimitsCurrency:           rub.CodeA3,
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   pkg.ProjectStatusInProduction,
		MerchantId:               merchant.Id,
	}

	broker, err := rabbitmq.NewBroker(cfg.BrokerAddress)
	assert.NoError(suite.T(), err, "Creating RabbitMQ publisher failed")

	if err := InitTestCurrency(db, []interface{}{rub}); err != nil {
		suite.FailNow("Insert currency test data failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	cache := NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		broker,
		nil,
		cache,
		mock.NewCurrencyServiceMockOk(),
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billing.PaymentMethod{pmBankCard}
	if err := suite.service.paymentMethod.MultipleInsert(pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	merchants := []*billing.Merchant{merchant}
	if err := suite.service.merchant.MultipleInsert(merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	if err = suite.service.currencyRate.Insert(rate); err != nil {
		suite.FailNow("Insert rates test data failed", "%v", err)
	}

	if err := suite.service.paymentSystem.MultipleInsert([]*billing.PaymentSystem{paymentSystem}); err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	sysCost := &billing.MoneyBackCostSystem{
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

	sysCost1 := &billing.MoneyBackCostSystem{
		Name:           "VISA",
		PayoutCurrency: "USD",
		UndoReason:     "chargeback",
		Region:         "Russia",
		Country:        "RU",
		DaysFrom:       30,
		PaymentStage:   1,
		Percent:        10,
		FixAmount:      15,
	}

	sysCost2 := &billing.MoneyBackCostSystem{
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

	err = suite.service.moneyBackCostSystem.MultipleInsert([]*billing.MoneyBackCostSystem{sysCost, sysCost1, sysCost2})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	merCost := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}

	merCost1 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "RUB",
		IsPaidByMerchant:  true,
	}

	merCost2 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            "CIS",
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	merCost3 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "CIS",
		Country:           "AZ",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           3,
		FixAmount:         5,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}
	merCost4 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "Russia",
		Country:           "RU",
		DaysFrom:          30,
		PaymentStage:      1,
		Percent:           10,
		FixAmount:         15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
	}
	merCost5 := &billing.MoneyBackCostMerchant{
		Id:                bson.NewObjectId().Hex(),
		MerchantId:        project.GetMerchantId(),
		Name:              "VISA",
		PayoutCurrency:    "USD",
		UndoReason:        "refund",
		Region:            "CIS",
		Country:           "",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           2,
		FixAmount:         3,
		FixAmountCurrency: "USD",
		IsPaidByMerchant:  true,
	}

	err = suite.service.moneyBackCostMerchant.MultipleInsert([]*billing.MoneyBackCostMerchant{merCost, merCost1, merCost2, merCost3, merCost4, merCost5})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	paymentSysCost := &billing.PaymentChannelCostSystem{
		Id:        bson.NewObjectId().Hex(),
		Name:      "VISA",
		Region:    "CIS",
		Country:   "AZ",
		Percent:   1.5,
		FixAmount: 5,
	}
	paymentSysCost1 := &billing.PaymentChannelCostSystem{
		Name:      "VISA",
		Region:    "CIS",
		Country:   "",
		Percent:   2.2,
		FixAmount: 0,
	}

	err = suite.service.paymentChannelCostSystem.MultipleInsert([]*billing.PaymentChannelCostSystem{paymentSysCost, paymentSysCost1})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	paymentMerCost := &billing.PaymentChannelCostMerchant{
		Id:                 bson.NewObjectId().Hex(),
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          0.75,
		Region:             "CIS",
		Country:            "AZ",
		MethodPercent:      1.5,
		MethodFixAmount:    0.01,
		PsPercent:          3,
		PsFixedFee:         0.01,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost1 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost2 := &billing.PaymentChannelCostMerchant{
		MerchantId:         project.GetMerchantId(),
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          0,
		Region:             "CIS",
		Country:            "",
		MethodPercent:      2.2,
		MethodFixAmount:    0,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}
	paymentMerCost3 := &billing.PaymentChannelCostMerchant{
		MerchantId:         mock.MerchantIdMock,
		Name:               "VISA",
		PayoutCurrency:     "USD",
		MinAmount:          5,
		Region:             "Russia",
		Country:            "RU",
		MethodPercent:      2.5,
		MethodFixAmount:    2,
		PsPercent:          5,
		PsFixedFee:         0.05,
		PsFixedFeeCurrency: "EUR",
	}

	err = suite.service.paymentChannelCostMerchant.MultipleInsert([]*billing.PaymentChannelCostMerchant{paymentMerCost, paymentMerCost1, paymentMerCost2, paymentMerCost3})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}

	bin := &BinData{
		Id:                 bson.NewObjectId(),
		CardBin:            400000,
		CardBrand:          "VISA",
		CardType:           "DEBIT",
		CardCategory:       "WORLD",
		BankName:           "ALFA BANK",
		BankCountryName:    "UKRAINE",
		BankCountryIsoCode: "US",
	}

	err = db.Collection(collectionBinData).Insert(bin)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
	}

	suite.project = project
	suite.pmBankCard = pmBankCard
}

func (suite *AccountingEntryTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Ok() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	req := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypePayoutFee,
		OrderId:    order.Id,
		RefundId:   refund.Id,
		MerchantId: order.GetMerchantId(),
		Amount:     10,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).FindId(bson.ObjectIdHex(rsp.Item.Id)).One(&accountingEntry)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), accountingEntry)

	assert.Equal(suite.T(), req.Type, accountingEntry.Type)
	assert.Equal(suite.T(), req.MerchantId, accountingEntry.Source.Id)
	assert.Equal(suite.T(), collectionMerchant, accountingEntry.Source.Type)
	assert.Equal(suite.T(), req.Amount, accountingEntry.Amount)
	assert.Equal(suite.T(), req.Currency, accountingEntry.Currency)
	assert.Equal(suite.T(), req.Status, accountingEntry.Status)
	assert.Equal(suite.T(), req.Reason, accountingEntry.Reason)

	t, err := ptypes.Timestamp(accountingEntry.CreatedAt)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), req.Date, t.Unix())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_MerchantNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:       pkg.AccountingEntryTypePayoutFee,
		MerchantId: bson.NewObjectId().Hex(),
		Amount:     10,
		Currency:   "RUB",
		Status:     pkg.BalanceTransactionStatusAvailable,
		Date:       time.Now().Unix(),
		Reason:     "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorMerchantNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": req.MerchantId, "source.type": collectionMerchant}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_OrderNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypePayoutFee,
		OrderId:  bson.NewObjectId().Hex(),
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_RefundNotFound_Error() {
	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypePayoutFee,
		RefundId: bson.NewObjectId().Hex(),
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_Refund_OrderNotFound_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	refund.Order.Id = bson.NewObjectId().Hex()
	err := suite.service.db.Collection(collectionRefund).UpdateId(bson.ObjectIdHex(refund.Id), refund)
	assert.NoError(suite.T(), err)

	req := &grpc.CreateAccountingEntryRequest{
		Type:     pkg.AccountingEntryTypePayoutFee,
		RefundId: refund.Id,
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err = suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": req.RefundId, "source.type": collectionRefund}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateAccountingEntry_EntryNotExist_Error() {
	order := suite.createOrder()

	req := &grpc.CreateAccountingEntryRequest{
		Type:     "not_exist_accounting_entry_name",
		OrderId:  order.Id,
		Amount:   10,
		Currency: "RUB",
		Status:   pkg.BalanceTransactionStatusAvailable,
		Date:     time.Now().Unix(),
		Reason:   "unit test",
	}
	rsp := &grpc.CreateAccountingEntryResponse{}
	err := suite.service.CreateAccountingEntry(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusBadData, rsp.Status)
	assert.Equal(suite.T(), accountingEntryErrorUnknownEntry, rsp.Message)
	assert.Nil(suite.T(), rsp.Item)

	var accountingEntry *billing.AccountingEntry
	err = suite.service.db.Collection(collectionAccountingEntry).
		Find(bson.M{"source.id": req.OrderId, "source.type": collectionOrder}).One(&accountingEntry)
	assert.Error(suite.T(), mgo.ErrNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Payment_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.payment()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Payment_ExchangeCurrencyCurrentForMerchantRequest_Error() {
	suite.service.curService = mock.NewCurrencyServiceMockError()
	handler := &accountingEntry{Service: suite.service, order: suite.createOrder(), ctx: context.TODO()}
	err := handler.payment()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupPaymentFx_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupPaymentFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupPaymentFx_GetRateCurrentForMerchant_Error() {
	suite.service.curService = mock.NewCurrencyServiceMockError()
	handler := &accountingEntry{Service: suite.service, order: suite.createOrder(), ctx: context.TODO()}
	err := handler.psMarkupPaymentFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupPaymentFx_GetRateCurrentCommonRequest_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = mock.MerchantIdMock

	suite.service.curService = mock.NewCurrencyServiceMockError()
	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupPaymentFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_MethodFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.methodFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_MethodFee_CommissionNotFound_Error() {
	order := suite.createOrder()
	order.Project.Id = bson.NewObjectId().Hex()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.methodFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupMethodFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupMethodFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupMethodFee_GetPaymentChannelCostMerchant_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = bson.NewObjectId().Hex()
	order.RoyaltyData = &billing.RoyaltyData{}

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupMethodFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_MethodFixedFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.methodFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_MethodFixedFee_GetByProjectIdAndMethod_Error() {
	order := suite.createOrder()
	order.Project.Id = bson.NewObjectId().Hex()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.methodFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_MethodFixedFee_ExchangeCurrencyCurrentForMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{}
	suite.service.curService = mock.NewCurrencyServiceMockError()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.methodFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupMethodFixedFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupMethodFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupMethodFixedFee_GetPaymentChannelCostMerchant_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = bson.NewObjectId().Hex()
	order.RoyaltyData = &billing.RoyaltyData{}

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupMethodFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsFee_GetPaymentChannelCostMerchant_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = bson.NewObjectId().Hex()
	order.RoyaltyData = &billing.RoyaltyData{}

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsFixedFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsFixedFee_GetPaymentChannelCostMerchant_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = bson.NewObjectId().Hex()
	order.RoyaltyData = &billing.RoyaltyData{}

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsFixedFee_ExchangeCurrencyCurrentForMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	suite.service.curService = mock.NewCurrencyServiceMockError()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupFixedFeeFx_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupFixedFeeFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupFixedFeeFx_GetPaymentChannelCostMerchant_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = bson.NewObjectId().Hex()
	order.RoyaltyData = &billing.RoyaltyData{}

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupFixedFeeFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupFixedFeeFx_GetRateCurrentForMerchantRequest_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	suite.service.curService = mock.NewCurrencyServiceMockError()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupFixedFeeFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupFixedFeeFx_GetRateCurrentCommon_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.Project.MerchantId = mock.MerchantIdMock
	suite.service.curService = mock.NewCurrencyServiceMockError()

	handler := &accountingEntry{Service: suite.service, order: order, ctx: context.TODO()}
	err := handler.psMarkupFixedFeeFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_TaxFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.taxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_TaxFee_ExchangeCurrencyCurrentForMerchantRequest_Error() {
	suite.service.curService = mock.NewCurrencyServiceMockError()
	handler := &accountingEntry{Service: suite.service, order: suite.createOrder(), ctx: context.TODO()}
	err := handler.taxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsTaxFxFee_OrderNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psTaxFxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorOrderNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsTaxFxFee_Ok() {
	order := suite.createOrder()

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		req: &grpc.CreateAccountingEntryRequest{
			Type:     pkg.AccountingEntryTypePsTaxFxFee,
			Amount:   order.TotalPaymentAmount,
			OrderId:  order.Id,
			Currency: order.Currency,
			Status:   pkg.BalanceTransactionStatusAvailable,
		},
	}
	err := handler.psTaxFxFee()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), handler.accountingEntries, 1)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.Equal(suite.T(), handler.req.Type, entry.Type)
	assert.Equal(suite.T(), handler.req.Amount, entry.Amount)
	assert.Equal(suite.T(), handler.req.Currency, entry.Currency)
	assert.Equal(suite.T(), handler.req.Status, entry.Status)
	assert.Equal(suite.T(), handler.req.OrderId, entry.Source.Id)
	assert.Equal(suite.T(), collectionOrder, entry.Source.Type)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundEntry_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.refundEntry()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundEntry_ExchangeCurrencyCurrentForMerchant_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}
	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.refundEntry()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.refundFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.Project.MerchantId = mock.MerchantIdMock

	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}
	err := handler.refundFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFixedFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.refundFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFixedFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.Project.MerchantId = mock.MerchantIdMock

	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}
	err := handler.refundFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFixedFee_ExchangeCurrencyCurrentForMerchantRequest_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	refund := suite.createRefund(order)

	suite.service.curService = mock.NewCurrencyServiceMockError()

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}
	err := handler.refundFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupRefundFx_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupRefundFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupRefundFx_GetRateCurrentForMerchant_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.psMarkupRefundFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupRefundFx_GetRateCurrentCommon_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.psMarkupRefundFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundBody_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.refundBody()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundBody_ExchangeCurrencyCurrentForMerchantRequest_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.refundBody()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundBody_ExchangeCurrencyCurrentForMerchantRequest2_Error() {
	order := suite.createOrder()
	order.PaymentMethod.Params.Currency = "RUB"
	order.Tax.Currency = "USD"

	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.refundBody()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ReverseTaxFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.reverseTaxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ReverseTaxFee_ExchangeCurrencyCurrentForMerchantRequest_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.reverseTaxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupReverseTaxFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupReverseTaxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupReverseTaxFee_GetRateCurrentForMerchantRequest_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.psMarkupReverseTaxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupReverseTaxFee_GetRateCurrentCommon_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		order:   order,
		refund:  refund,
		ctx:     context.TODO(),
	}

	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.psMarkupReverseTaxFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ReverseTaxFeeDelta_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.reverseTaxFeeDelta()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ReverseTaxFeeDelta_AmountLowerZero_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		PaymentTaxAmountInRoyaltyCurrency: 9,
		RefundTaxAmountInRoyaltyCurrency:  10,
	}

	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.reverseTaxFeeDelta()
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), handler.accountingEntries)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsReverseTaxFeeDelta_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psReverseTaxFeeDelta()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsReverseTaxFeeDelta_AmountGreaterZero_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		PaymentTaxAmountInRoyaltyCurrency: 10,
		RefundTaxAmountInRoyaltyCurrency:  9,
	}

	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.psReverseTaxFeeDelta()
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), handler.accountingEntries)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFailure_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.refundFailure()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFailure_Ok() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
		req: &grpc.CreateAccountingEntryRequest{
			Type:     pkg.AccountingEntryTypeRefundFailure,
			Amount:   order.TotalPaymentAmount,
			OrderId:  order.Id,
			RefundId: refund.Id,
			Currency: order.Currency,
			Status:   pkg.BalanceTransactionStatusAvailable,
		},
	}
	err := handler.refundFailure()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), handler.accountingEntries, 1)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.Equal(suite.T(), handler.req.Type, entry.Type)
	assert.Equal(suite.T(), handler.req.Amount, entry.Amount)
	assert.Equal(suite.T(), handler.req.Currency, entry.Currency)
	assert.Equal(suite.T(), handler.req.Status, entry.Status)
	assert.Equal(suite.T(), handler.req.RefundId, entry.Source.Id)
	assert.Equal(suite.T(), collectionRefund, entry.Source.Type)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFailure_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.chargebackFailure()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFailure_Ok() {
	order := suite.createOrder()
	refund := suite.createRefund(order)

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
		req: &grpc.CreateAccountingEntryRequest{
			Type:     pkg.AccountingEntryTypeChargebackFailure,
			Amount:   order.TotalPaymentAmount,
			OrderId:  order.Id,
			RefundId: refund.Id,
			Currency: order.Currency,
			Status:   pkg.BalanceTransactionStatusAvailable,
		},
	}
	err := handler.chargebackFailure()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), handler.accountingEntries, 1)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.Equal(suite.T(), handler.req.Type, entry.Type)
	assert.Equal(suite.T(), handler.req.Amount, entry.Amount)
	assert.Equal(suite.T(), handler.req.Currency, entry.Currency)
	assert.Equal(suite.T(), handler.req.Status, entry.Status)
	assert.Equal(suite.T(), handler.req.RefundId, entry.Source.Id)
	assert.Equal(suite.T(), collectionRefund, entry.Source.Type)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.chargeback()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_Chargeback_ExchangeCurrencyCurrentForMerchant_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	suite.service.curService = mock.NewCurrencyServiceMockError()

	err := handler.chargeback()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFx_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupChargebackFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFx_GetRateCurrentForMerchant_Error() {
	order := suite.createOrder()
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	suite.service.curService = mock.NewCurrencyServiceMockError()

	err := handler.psMarkupChargebackFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFx_GetRateCurrentCommon_Error() {
	order := suite.createOrder()
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	suite.service.curService = mock.NewCurrencyServiceMockError()

	err := handler.psMarkupChargebackFx()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorGetExchangeRateFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.chargebackFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.chargebackFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupChargebackFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "KZT"
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.psMarkupChargebackFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFixedFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.chargebackFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFixedFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "KZT"
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.chargebackFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFixedFee_ExchangeCurrencyCurrentForMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "USD"
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	suite.service.curService = mock.NewCurrencyServiceMockError()
	err := handler.chargebackFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorExchangeFailed, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFixedFee_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.psMarkupChargebackFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorRefundNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_PsMarkupChargebackFixedFee_GetMoneyBackCostMerchant_Error() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		AmountInRoyaltyCurrency: 5,
	}
	order.PaymentMethod.Params.Currency = "KZT"
	order.Project.MerchantId = mock.MerchantIdMock
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}
	err := handler.psMarkupChargebackFixedFee()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorCommissionNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_CreateEntry_RefundNotSet_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	err := handler.createEntry(pkg.AccountingEntryTypeChargebackFailure)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorMerchantNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetPaymentChannelCostMerchant_GetCostPaymentMethodName_Error() {
	order := suite.createOrder()
	delete(order.PaymentRequisites, pkg.PaymentCreateBankCardFieldBrand)

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getPaymentChannelCostMerchant()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "brand for bank card not found", err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetPaymentChannelCostMerchant_GetByIsoCodeA2_Error() {
	order := suite.createOrder()
	order.User.Address.Country = "KZ"

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getPaymentChannelCostMerchant()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionCountry), err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetMoneyBackCostMerchant_GetCostPaymentMethodName_Error() {
	order := suite.createOrder()
	delete(order.PaymentRequisites, pkg.PaymentCreateBankCardFieldBrand)

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getMoneyBackCostMerchant(pkg.AccountingEntryTypeChargeback)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "brand for bank card not found", err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetMoneyBackCostMerchant_GetByIsoCodeA2_Error() {
	order := suite.createOrder()
	order.User.Address.Country = "KZ"

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getMoneyBackCostMerchant(pkg.AccountingEntryTypeChargeback)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionCountry), err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetMoneyBackCostSystem_GetCostPaymentMethodName_Error() {
	order := suite.createOrder()
	delete(order.PaymentRequisites, pkg.PaymentCreateBankCardFieldBrand)

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getMoneyBackCostSystem(pkg.AccountingEntryTypeChargeback)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "brand for bank card not found", err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_GetMoneyBackCostSystem_GetByIsoCodeA2_Error() {
	order := suite.createOrder()
	order.User.Address.Country = "KZ"

	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}
	_, err := handler.getMoneyBackCostSystem(pkg.AccountingEntryTypeChargeback)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionCountry), err.Error())
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_AllEntriesWithRequest_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{}
	refund := suite.createRefund(order)

	merchant, err := suite.service.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(order.GetMerchantId())})
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), merchant)

	handler := &accountingEntry{
		Service:  suite.service,
		ctx:      context.TODO(),
		order:    order,
		refund:   refund,
		merchant: merchant,
		req: &grpc.CreateAccountingEntryRequest{
			Type:       pkg.AccountingEntryTypeRefundFailure,
			Amount:     order.TotalPaymentAmount,
			OrderId:    order.Id,
			RefundId:   refund.Id,
			MerchantId: order.GetMerchantId(),
			Currency:   order.Currency,
			Status:     pkg.BalanceTransactionStatusAvailable,
		},
	}

	counter := 0

	for _, fn := range availableAccountingEntry {
		counter++

		err := fn(handler)
		assert.NoError(suite.T(), err)
		assert.Len(suite.T(), handler.accountingEntries, counter)
	}
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ProcessEvent_UnknownEntry_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	entries := []string{"some_unknown_entry"}

	err := suite.service.processEvent(handler, entries)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorUnknownEntry, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ProcessEvent_CreateEntry_Error() {
	handler := &accountingEntry{Service: suite.service, ctx: context.TODO()}
	entries := []string{pkg.AccountingEntryTypeAdjustment}

	err := suite.service.processEvent(handler, entries)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), accountingEntryErrorMerchantNotFound, err)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_TaxFee_ConvertAmount_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{}
	order.Tax.Currency = "KZT"
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
	}

	err := handler.taxFee()
	assert.NoError(suite.T(), err)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.NotEqual(suite.T(), order.Tax.Amount, entry.Amount)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_RefundFixedFee_ConvertAmount_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{}
	order.Tax.Currency = "KZT"
	order.PaymentMethod.Params.Currency = "USD"
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}

	err := handler.refundFixedFee()
	assert.NoError(suite.T(), err)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.NotEqual(suite.T(), order.Tax.Amount, entry.Amount)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ReverseTaxFeeDelta_ConvertAmount_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{
		PaymentTaxAmountInRoyaltyCurrency: 10,
		RefundTaxAmountInRoyaltyCurrency:  9,
	}
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}

	err := handler.reverseTaxFeeDelta()
	assert.NoError(suite.T(), err)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.Equal(suite.T(), order.RoyaltyData.PaymentTaxAmountInRoyaltyCurrency-order.RoyaltyData.RefundTaxAmountInRoyaltyCurrency, entry.Amount)
}

func (suite *AccountingEntryTestSuite) TestAccountingEntry_ChargebackFixedFee_ConvertAmount_Ok() {
	order := suite.createOrder()
	order.RoyaltyData = &billing.RoyaltyData{}
	order.Tax.Currency = "KZT"
	order.PaymentMethod.Params.Currency = "USD"
	refund := suite.createRefund(order)
	handler := &accountingEntry{
		Service: suite.service,
		ctx:     context.TODO(),
		order:   order,
		refund:  refund,
	}

	err := handler.chargebackFixedFee()
	assert.NoError(suite.T(), err)
	cost, err := handler.getMoneyBackCostMerchant(pkg.AccountingEntryTypeChargeback)
	assert.NoError(suite.T(), err)

	entry := handler.accountingEntries[0].(*billing.AccountingEntry)
	assert.NotEqual(suite.T(), cost.FixAmount, entry.Amount)
}

func (suite *AccountingEntryTestSuite) createOrder() *billing.Order {
	req := &billing.OrderCreateRequest{
		ProjectId:   suite.project.Id,
		Currency:    "RUB",
		Amount:      100,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     bson.NewObjectId().Hex(),
		User: &billing.OrderUser{
			Email: "some_email@unit.com",
			Ip:    "127.0.0.1",
			Phone: "123456789",
		},
	}

	rsp0 := &grpc.OrderCreateProcessResponse{}
	err := suite.service.OrderCreateProcess(context.TODO(), req, rsp0)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp0.Status, pkg.ResponseStatusOk)
	rsp := rsp0.Item

	expireYear := time.Now().AddDate(1, 0, 0)

	createPaymentRequest := &grpc.PaymentCreateRequest{
		Data: map[string]string{
			pkg.PaymentCreateFieldOrderId:         rsp.Uuid,
			pkg.PaymentCreateFieldPaymentMethodId: suite.pmBankCard.Id,
			pkg.PaymentCreateFieldEmail:           "test@unit.unit",
			pkg.PaymentCreateFieldPan:             "4000000000000002",
			pkg.PaymentCreateFieldCvv:             "123",
			pkg.PaymentCreateFieldMonth:           "02",
			pkg.PaymentCreateFieldYear:            expireYear.Format("2006"),
			pkg.PaymentCreateFieldHolder:          "Mr. Card Holder",
		},
	}

	rsp1 := &grpc.PaymentCreateResponse{}
	err = suite.service.PaymentCreateProcess(context.TODO(), createPaymentRequest, rsp1)
	assert.NoError(suite.T(), err)

	var order *billing.Order
	err = suite.service.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(rsp.Id)).One(&order)
	assert.NotNil(suite.T(), order)

	order.PrivateStatus = constant.OrderStatusPaymentSystemComplete
	order.Tax = &billing.OrderTax{
		Type:     taxTypeVat,
		Rate:     20,
		Amount:   10,
		Currency: "RUB",
	}
	err = suite.service.updateOrder(order)
	assert.NoError(suite.T(), err)

	return order
}

func (suite *AccountingEntryTestSuite) createRefund(order *billing.Order) *billing.Refund {
	req := &grpc.CreateRefundRequest{
		OrderId:   order.Uuid,
		Amount:    10,
		CreatorId: bson.NewObjectId().Hex(),
		Reason:    "unit test",
	}
	rsp := &grpc.CreateRefundResponse{}
	err := suite.service.CreateRefund(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, rsp.Status)
	assert.Empty(suite.T(), rsp.Message)
	assert.NotNil(suite.T(), rsp.Item)

	return rsp.Item
}
