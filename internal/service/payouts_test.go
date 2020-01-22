package service

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinMocks "github.com/paysuper/paysuper-proto/go/casbinpb/mocks"
	reportingMocks "github.com/paysuper/paysuper-proto/go/reporterpb/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

var (
	alreadyPaidRoyalty = float64(10432)
)

type PayoutsTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   database.CacheInterface

	merchant         *billingpb.Merchant
	operatingCompany *billingpb.OperatingCompany

	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs

	report1 *billingpb.RoyaltyReport
	report2 *billingpb.RoyaltyReport
	report3 *billingpb.RoyaltyReport
	report4 *billingpb.RoyaltyReport
	report5 *billingpb.RoyaltyReport
	report6 *billingpb.RoyaltyReport
	report7 *billingpb.RoyaltyReport

	payout1 *billingpb.PayoutDocument
	payout2 *billingpb.PayoutDocument
	payout3 *billingpb.PayoutDocument
	payout4 *billingpb.PayoutDocument
	payout5 *billingpb.PayoutDocument
	payout6 *billingpb.PayoutDocument
	payout7 *billingpb.PayoutDocument

	dateFrom1 *timestamp.Timestamp
	dateFrom2 *timestamp.Timestamp
	dateTo1   *timestamp.Timestamp
	dateTo2   *timestamp.Timestamp
}

func Test_Payouts(t *testing.T) {
	suite.Run(t, new(PayoutsTestSuite))
}

func (suite *PayoutsTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	assert.NoError(suite.T(), err, "Config load failed")

	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	db, err := mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.operatingCompany = &billingpb.OperatingCompany{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "Legal name",
		Country:            "RU",
		RegistrationNumber: "some number",
		VatNumber:          "some vat number",
		Address:            "Home, home 0",
		VatAddress:         "Address for VAT purposes",
		SignatoryName:      "Vassiliy Poupkine",
		SignatoryPosition:  "CEO",
		BankingDetails:     "bank details including bank, bank address, account number, swift/ bic, intermediary bank",
		PaymentCountries:   []string{},
	}

	_, err = db.Collection(collectionOperatingCompanies).InsertOne(context.TODO(), suite.operatingCompany)
	if err != nil {
		suite.FailNow("Insert operatingCompany test data failed", "%v", err)
	}

	country := &billingpb.Country{
		IsoCodeA2:       "RU",
		Region:          "Russia",
		Currency:        "RUB",
		PaymentsAllowed: true,
		ChangeAllowed:   true,
		VatEnabled:      true,
		PriceGroupId:    "",
		VatCurrency:     "RUB",
	}

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	suite.merchant = &billingpb.Merchant{
		Id: primitive.NewObjectID().Hex(),
		User: &billingpb.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test@unit.test",
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:    "Unit test",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		MinPayoutAmount:           13000,
		IsCommissionToUserEnabled: true,
		Status:                    billingpb.MerchantStatusDraft,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned:             true,
		PaymentMethods:       map[string]*billingpb.MerchantPaymentMethod{},
		ManualPayoutsEnabled: true,
		MccCode:              billingpb.MccCodeLowRisk,
		OperatingCompanyId:   suite.operatingCompany.Id,
	}

	layout := "2006-01-02T15:04:05.000Z"

	from1, err := time.Parse(layout, "2019-07-01T00:00:00.000Z")
	assert.NoError(suite.T(), err, "Generate date failed")
	suite.dateFrom1, err = ptypes.TimestampProto(from1)
	assert.NoError(suite.T(), err, "Generate date failed")

	to1, err := time.Parse(layout, "2019-07-31T00:00:00.000Z")
	assert.NoError(suite.T(), err, "Generate date failed")
	suite.dateTo1, err = ptypes.TimestampProto(to1)
	assert.NoError(suite.T(), err, "Generate date failed")

	from2, err := time.Parse(layout, "2019-08-01T00:00:00.000Z")
	assert.NoError(suite.T(), err, "Generate date failed")
	suite.dateFrom2, err = ptypes.TimestampProto(from2)
	assert.NoError(suite.T(), err, "Generate date failed")

	to2, err := time.Parse(layout, "2019-08-31T00:00:00.000Z")
	assert.NoError(suite.T(), err, "Generate date failed")
	suite.dateTo2, err = ptypes.TimestampProto(to2)
	assert.NoError(suite.T(), err, "Generate date failed")

	suite.report1 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount:    100,
			PayoutAmount:         12345,
			VatAmount:            100,
			FeeAmount:            50,
			RollingReserveAmount: 0,
			CorrectionAmount:     0,
		},
		Status:             billingpb.RoyaltyReportStatusAccepted,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         suite.dateFrom1,
		PeriodTo:           suite.dateTo1,
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report2 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:             billingpb.RoyaltyReportStatusAccepted,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         suite.dateFrom2,
		PeriodTo:           suite.dateTo2,
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report3 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:             billingpb.RoyaltyReportStatusPending,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         ptypes.TimestampNow(),
		PeriodTo:           ptypes.TimestampNow(),
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report4 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 0,
			PayoutAmount:      0,
			VatAmount:         0,
			FeeAmount:         0,
		},
		Status:             billingpb.RoyaltyReportStatusAccepted,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         ptypes.TimestampNow(),
		PeriodTo:           ptypes.TimestampNow(),
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report5 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      30,
			VatAmount:         40,
			FeeAmount:         50,
		},
		Status:             billingpb.RoyaltyReportStatusAccepted,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         ptypes.TimestampNow(),
		PeriodTo:           ptypes.TimestampNow(),
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           "USD",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report6 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 100,
			PayoutAmount:      alreadyPaidRoyalty,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:             billingpb.RoyaltyReportStatusAccepted,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         ptypes.TimestampNow(),
		PeriodTo:           ptypes.TimestampNow(),
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.report7 = &billingpb.RoyaltyReport{
		Id:         primitive.NewObjectID().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billingpb.RoyaltyReportTotals{
			TransactionsCount: 100,
			PayoutAmount:      90,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:             billingpb.RoyaltyReportStatusDispute,
		CreatedAt:          ptypes.TimestampNow(),
		PeriodFrom:         ptypes.TimestampNow(),
		PeriodTo:           ptypes.TimestampNow(),
		AcceptExpireAt:     ptypes.TimestampNow(),
		Currency:           suite.merchant.GetPayoutCurrency(),
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout1 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{suite.report1.Id, suite.report2.Id},
		TotalFees:          765000,
		Balance:            765000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          ptypes.TimestampNow(),
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout2 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{suite.report6.Id},
		TotalFees:          alreadyPaidRoyalty,
		Balance:            alreadyPaidRoyalty,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          date,
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "124123",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout3 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	assert.NoError(suite.T(), err, "Generate payout url expire date failed")

	suite.payout4 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex()},
		TotalFees:          765000,
		Balance:            765000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          ptypes.TimestampNow(),
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout5 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex()},
		TotalFees:          765000,
		Balance:            765000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          ptypes.TimestampNow(),
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout6 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex(), primitive.NewObjectID().Hex()},
		TotalFees:          765000,
		Balance:            765000,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPending,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          ptypes.TimestampNow(),
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.payout7 = &billingpb.PayoutDocument{
		Id:                 primitive.NewObjectID().Hex(),
		MerchantId:         suite.merchant.Id,
		SourceId:           []string{suite.report6.Id},
		TotalFees:          alreadyPaidRoyalty,
		Balance:            alreadyPaidRoyalty,
		Currency:           "RUB",
		Status:             pkg.PayoutDocumentStatusPaid,
		Description:        "test payout document",
		Destination:        suite.merchant.Banking,
		CreatedAt:          date,
		UpdatedAt:          ptypes.TimestampNow(),
		ArrivalDate:        ptypes.TimestampNow(),
		Transaction:        "124123",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
		OperatingCompanyId: suite.operatingCompany.Id,
	}

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

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

	merchants := []*billingpb.Merchant{suite.merchant}
	if err := suite.service.merchant.MultipleInsert(context.TODO(), merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(context.TODO(), country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	var core zapcore.Core

	lvl := zap.NewAtomicLevel()
	core, suite.zapRecorder = observer.New(lvl)
	suite.logObserver = zap.New(core)
}

func (suite *PayoutsTestSuite) TearDownTest() {
	err := suite.service.db.Drop()

	if err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	err = suite.service.db.Close()

	if err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *PayoutsTestSuite) helperInsertRoyaltyReports(data []*billingpb.RoyaltyReport) {
	for _, r := range data {
		if _, err := suite.service.db.Collection(collectionRoyaltyReport).InsertOne(context.TODO(), r); err != nil {
			suite.FailNow("Insert royalty report test data failed", "%v", err)
		}
	}
}

func (suite *PayoutsTestSuite) helperInsertPayoutDocuments(data []*billingpb.PayoutDocument) {
	for _, p := range data {
		if err := suite.service.payoutDocument.Insert(context.TODO(), p, "127.0.0.1", payoutChangeSourceAdmin); err != nil {
			suite.FailNow("Insert payout test data failed", "%v", err)
		}
	}
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Ok_NoPayoutsYet() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report6})

	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), suite.merchant)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 2)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Ok_FilteringByCurrency() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report5, suite.report6})

	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), suite.merchant)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 2)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_NotFound() {
	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesNotFound.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_MerchantNotFound() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report6})
	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), &billingpb.Merchant{Id: primitive.NewObjectID().Hex()})
	assert.EqualError(suite.T(), err, errorPayoutSourcesNotFound.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_HasPendingReports() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report3})

	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesPending.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_HasDisputingReports() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report7})

	reports, err := suite.service.getPayoutDocumentSources(context.TODO(), suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesDispute.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_Pending() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	controlAmount := suite.report1.Totals.PayoutAmount + suite.report2.Totals.PayoutAmount
	assert.Equal(suite.T(), res.Items[0].Balance, controlAmount)
	assert.EqualValues(suite.T(), res.Items[0].Balance, 13579.5)
	assert.True(suite.T(), suite.merchant.MinPayoutAmount < controlAmount)
	assert.Equal(suite.T(), res.Items[0].Status, pkg.PayoutDocumentStatusPending)
	assert.Len(suite.T(), res.Items[0].SourceId, 2)
	assert.Equal(suite.T(), res.Items[0].PeriodFrom, suite.dateFrom1)
	assert.Equal(suite.T(), res.Items[0].PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_SkipByAmount() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	controlAmount := suite.report2.Totals.PayoutAmount
	assert.Equal(suite.T(), res.Items[0].Balance, controlAmount)
	assert.EqualValues(suite.T(), res.Items[0].Balance, 1234.5)
	assert.True(suite.T(), suite.merchant.MinPayoutAmount > controlAmount)
	assert.Equal(suite.T(), res.Items[0].Status, pkg.PayoutDocumentStatusSkip)
	assert.Len(suite.T(), res.Items[0].SourceId, 1)
	assert.Equal(suite.T(), res.Items[0].PeriodFrom, suite.dateFrom2)
	assert.Equal(suite.T(), res.Items[0].PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_SkipByRollingReserve() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.report1.Totals.RollingReserveAmount = 600

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req1 := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res1 := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req1, res1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res1.Status, billingpb.ResponseStatusOk)
	controlAmount := (suite.report1.Totals.PayoutAmount - suite.report1.Totals.CorrectionAmount - suite.report1.Totals.RollingReserveAmount) +
		(suite.report2.Totals.PayoutAmount - suite.report2.Totals.CorrectionAmount - suite.report2.Totals.RollingReserveAmount)
	assert.Equal(suite.T(), res1.Items[0].Balance, controlAmount)
	assert.EqualValues(suite.T(), res1.Items[0].Balance, 12979.5)
	assert.True(suite.T(), suite.merchant.MinPayoutAmount > controlAmount)
	assert.Equal(suite.T(), res1.Items[0].Status, pkg.PayoutDocumentStatusSkip)
	assert.Len(suite.T(), res1.Items[0].SourceId, 2)
	assert.Equal(suite.T(), res1.Items[0].PeriodFrom, suite.dateFrom1)
	assert.Equal(suite.T(), res1.Items[0].PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NoSources() {

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutSourcesNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_MerchantNotFound() {

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  primitive.NewObjectID().Hex(),
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, merchantErrorNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_ZeroAmount() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report4})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutAmountInvalid)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NoBalance() {
	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1})

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusSystemError)
	assert.Equal(suite.T(), res.Message, errorPayoutBalanceError)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NotEnoughBalance() {

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1})
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.GetMerchantBalanceRequest{
		MerchantId: suite.merchant.Id,
	}
	res := &billingpb.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), res.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), res.Item.Debit, float64(12345))
	assert.Equal(suite.T(), res.Item.Credit, float64(10432))
	assert.Equal(suite.T(), res.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), res.Item.Total, float64(1913))

	req1 := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res1 := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req1, res1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res1.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res1.Message, errorPayoutNotEnoughBalance)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_InsertError() {

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	pds.On("GetBalanceAmount", mock2.Anything, mock2.Anything, mock2.Anything).Return(float64(0), nil)
	pds.On("GetLast", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.payoutDocument = pds

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_InsertErrorWithResponse() {

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything, mock2.Anything).Return(newBillingServerErrorMsg("0", "test"))
	pds.On("GetBalanceAmount", mock2.Anything, mock2.Anything, mock2.Anything).Return(float64(0), nil)
	pds.On("GetLast", mock2.Anything, mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.payoutDocument = pds

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(context.TODO(), suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &billingpb.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &billingpb.CreatePayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusSystemError)
	assert.EqualError(suite.T(), res.Message, "test")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok() {

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report6})
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout2})

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout2.Id,
		Status:             pkg.PayoutDocumentStatusPaid,
		Transaction:        "transaction123",
		FailureTransaction: "failure456",
		FailureMessage:     "bla-bla-bla",
		FailureCode:        "999",
		Ip:                 "192.168.1.1",
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout2.Id)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusPaid)
	assert.Equal(suite.T(), res.Item.Transaction, "transaction123")
	assert.Equal(suite.T(), res.Item.FailureTransaction, "failure456")
	assert.Equal(suite.T(), res.Item.FailureMessage, "bla-bla-bla")
	assert.Equal(suite.T(), res.Item.FailureCode, "999")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok_PaidOk() {

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report6})
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout2})

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout2.Id,
		Status:           pkg.PayoutDocumentStatusPaid,
		Transaction:      "transaction123",
		Ip:               "192.168.1.1",
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout2.Id)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusPaid)
	assert.Equal(suite.T(), res.Item.Transaction, "transaction123")

	rr, err := suite.service.royaltyReport.GetById(context.TODO(), suite.report6.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rr.Status, billingpb.RoyaltyReportStatusPaid)
	assert.Equal(suite.T(), rr.PayoutDocumentId, suite.payout2.Id)
	assert.Greater(suite.T(), rr.PayoutDate.Seconds, int64(-62135596800))
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_StatusForbidden() {

	suite.helperInsertRoyaltyReports([]*billingpb.RoyaltyReport{suite.report6})
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout7})

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout7.Id,
		Status:           pkg.PayoutDocumentStatusFailed,
		Transaction:      "transaction123",
		Ip:               "192.168.1.1",
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutStatusChangeIsForbidden)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok_NotModified() {

	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout1})

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout1.Id,
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotModified)
	assert.Equal(suite.T(), res.Item.Id, suite.payout1.Id)
	assert.Equal(suite.T(), res.Item.Status, "pending")
	assert.Equal(suite.T(), res.Item.Transaction, "")
	assert.Equal(suite.T(), res.Item.FailureTransaction, "")
	assert.Equal(suite.T(), res.Item.FailureMessage, "")
	assert.Equal(suite.T(), res.Item.FailureCode, "")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_NotFound() {

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPayoutNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_UpdateError() {

	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout1})

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Update", mock2.Anything, mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	pds.On("GetById", mock2.Anything, mock2.Anything).Return(suite.payout2, nil)
	suite.service.payoutDocument = pds

	req := &billingpb.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout2.Id,
		Status:             pkg.PayoutDocumentStatusPaid,
		Transaction:        "transaction123",
		FailureTransaction: "failure456",
		FailureMessage:     "bla-bla-bla",
		FailureCode:        "999",
		Ip:                 "192.168.1.1",
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocument_ById_Ok() {
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout7})

	req := &billingpb.GetPayoutDocumentRequest{
		MerchantId:       suite.payout7.MerchantId,
		PayoutDocumentId: suite.payout7.Id,
	}

	res := &billingpb.PayoutDocumentResponse{}

	err := suite.service.GetPayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout7.Id)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_ByQuery_Ok() {

	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout1, suite.payout2, suite.payout3, suite.payout4, suite.payout7})

	req := &billingpb.GetPayoutDocumentsRequest{
		Status:     []string{"paid"},
		MerchantId: suite.merchant.Id,
		Limit:      10,
		Offset:     0,
	}

	res := &billingpb.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(1))
	assert.Len(suite.T(), res.Data.Items, 1)
	assert.Equal(suite.T(), res.Data.Items[0].Id, suite.payout7.Id)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_AllWithPaging_Ok() {
	suite.helperInsertPayoutDocuments([]*billingpb.PayoutDocument{suite.payout1, suite.payout2, suite.payout3, suite.payout4, suite.payout5})
	req := &billingpb.GetPayoutDocumentsRequest{
		MerchantId: suite.payout1.MerchantId,
		Limit:      1,
		Offset:     0,
	}

	res := &billingpb.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(5))
	assert.Len(suite.T(), res.Data.Items, 1)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_Ok_NotFound() {
	req := &billingpb.GetPayoutDocumentsRequest{
		MerchantId:       primitive.NewObjectID().Hex(),
		PayoutDocumentId: primitive.NewObjectID().Hex(),
	}

	res := &billingpb.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, billingpb.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(0))
	assert.Nil(suite.T(), res.Data.Items)
}
