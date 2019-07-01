package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type FinanceTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface

	project       *billing.Project
	paymentMethod *billing.PaymentMethod
}

func Test_Finance(t *testing.T) {
	suite.Run(t, new(FinanceTestSuite))
}

func (suite *FinanceTestSuite) SetupTest() {
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

	ps1 := &billing.PaymentSystem{
		Id:                 bson.NewObjectId().Hex(),
		Name:               "CardPay",
		AccountingCurrency: rub,
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            "cardpay",
	}
	pmBankCard := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		Currencies:       []int32{643, 840, 980},
		ExternalId:       "BANKCARD",
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId: "15985",
			}},
		Type:            "bank_card",
		IsActive:        true,
		PaymentSystemId: ps1.Id,
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
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
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
		Id:                 bson.NewObjectId().Hex(),
		CallbackCurrency:   rub.CodeA3,
		CallbackProtocol:   "default",
		LimitsCurrency:     rub.CodeA3,
		MaxPaymentAmount:   15000,
		MinPaymentAmount:   0,
		Name:               map[string]string{"en": "test project 1"},
		IsProductsCheckout: true,
		SecretKey:          "test project 1 secret key",
		Status:             pkg.ProjectStatusInProduction,
		MerchantId:         merchant.Id,
	}

	pmQiwi := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Qiwi",
		Group:            "QIWI",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		Currencies:       []int32{643, 840, 980},
		ExternalId:       "QIWI",
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId: "15993",
			}},
		Type:            "ewallet",
		IsActive:        true,
		PaymentSystemId: ps1.Id,
	}
	pmBitcoin := &billing.PaymentMethod{
		Id:               bson.NewObjectId().Hex(),
		Name:             "Bitcoin",
		Group:            "BITCOIN",
		MinPaymentAmount: 0,
		MaxPaymentAmount: 0,
		Currencies:       []int32{643, 840, 980},
		ExternalId:       "BITCOIN",
		ProductionSettings: map[string]*billing.PaymentMethodParams{
			"RUB": {
				TerminalId: "16007",
			}},
		Type:            "crypto",
		IsActive:        true,
		PaymentSystemId: ps1.Id,
	}

	commissionStartDate, err := ptypes.TimestampProto(time.Now().Add(time.Minute * -10))

	if err != nil {
		suite.FailNow("Commission start date conversion failed", "%v", err)
	}

	commissions := []interface{}{
		&billing.Commission{
			PaymentMethodId:         pmBankCard.Id,
			ProjectId:               project.Id,
			PaymentMethodCommission: 1,
			PspCommission:           2,
			TotalCommissionToUser:   1,
			StartDate:               commissionStartDate,
		},
		&billing.Commission{
			PaymentMethodId:         pmQiwi.Id,
			ProjectId:               project.Id,
			PaymentMethodCommission: 1,
			PspCommission:           2,
			TotalCommissionToUser:   2,
			StartDate:               commissionStartDate,
		},
		&billing.Commission{
			PaymentMethodId:         pmBitcoin.Id,
			ProjectId:               project.Id,
			PaymentMethodCommission: 1,
			PspCommission:           2,
			TotalCommissionToUser:   3,
			StartDate:               commissionStartDate,
		},
	}

	err = db.Collection(collectionCommission).Insert(commissions...)

	if err != nil {
		suite.FailNow("Insert commission test data failed", "%v", err)
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
	suite.service = NewBillingService(db, cfg, nil, nil, nil, nil, nil, suite.cache, nil, nil)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	pms := []*billing.PaymentMethod{pmBankCard, pmQiwi, pmBitcoin}
	if err := suite.service.paymentMethod.MultipleInsert(pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	if err := suite.service.merchant.Insert(merchant); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.project.Insert(project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	if err := suite.service.paymentSystem.Insert(ps1); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	suite.project = project
	suite.paymentMethod = pmBankCard
}

func (suite *FinanceTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *FinanceTestSuite) TestFinance_GetCurrencyByCodeA3Ok() {
	c, err := suite.service.currency.GetByCodeA3("RUB")

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), c)
	assert.Equal(suite.T(), int32(643), c.CodeInt)
}

func (suite *FinanceTestSuite) TestFinance_GetCurrencyByCodeA3Error() {
	c, err := suite.service.currency.GetByCodeA3("AUD")

	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), c)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionCurrency), err.Error())
}

func (suite *FinanceTestSuite) TestFinance_CalculateCommissionOk() {
	amount := float64(100)

	commission, err := suite.service.commission.CalculatePmCommission(suite.project.Id, suite.paymentMethod.Id, amount)

	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), commission)
	assert.True(suite.T(), commission > 0)
	assert.Equal(suite.T(), float64(2.5), commission)
}

func (suite *FinanceTestSuite) TestFinance_CalculateCommissionProjectError() {
	commission, err := suite.service.commission.CalculatePmCommission(bson.NewObjectId().Hex(), suite.paymentMethod.Id, float64(100))

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), float64(0), commission)
	assert.Equal(suite.T(), fmt.Sprintf(errorNotFound, collectionCommission), err.Error())
}

func (suite *FinanceTestSuite) TestFinance_CalculateCommissionPaymentMethod_DefaultPaymentCommission() {
	commission, err := suite.service.commission.CalculatePmCommission(suite.project.Id, bson.NewObjectId().Hex(), float64(100))
	dCommission := suite.service.getDefaultPaymentMethodCommissions()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), commission)
	assert.Equal(suite.T(), tools.FormatAmount(float64(100)*(dCommission.Fee/100)), commission)
}
