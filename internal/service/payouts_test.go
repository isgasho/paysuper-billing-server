package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
	cache   CacheInterface

	merchant *billing.Merchant

	logObserver *zap.Logger
	zapRecorder *observer.ObservedLogs

	report1 *billing.RoyaltyReport
	report2 *billing.RoyaltyReport
	report3 *billing.RoyaltyReport
	report4 *billing.RoyaltyReport
	report5 *billing.RoyaltyReport
	report6 *billing.RoyaltyReport
	report7 *billing.RoyaltyReport

	payout1 *billing.PayoutDocument
	payout2 *billing.PayoutDocument
	payout3 *billing.PayoutDocument
	payout4 *billing.PayoutDocument
	payout5 *billing.PayoutDocument
	payout6 *billing.PayoutDocument
	payout7 *billing.PayoutDocument

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

	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -480))
	assert.NoError(suite.T(), err, "Generate merchant date failed")

	suite.merchant = &billing.Merchant{
		Id: bson.NewObjectId().Hex(),
		User: &billing.MerchantUser{
			Id:    uuid.New().String(),
			Email: "test@unit.test",
		},
		Company: &billing.MerchantCompanyInfo{
			Name:    "Unit test",
			Country: country.IsoCodeA2,
			Zip:     "190000",
			City:    "St.Petersburg",
		},
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
			Currency: "RUB",
			Name:     "Bank name",
		},
		IsVatEnabled:              true,
		MinPayoutAmount:           13000,
		IsCommissionToUserEnabled: true,
		Status:                    pkg.MerchantStatusDraft,
		LastPayout: &billing.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned:       true,
		PaymentMethods: map[string]*billing.MerchantPaymentMethod{},
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

	suite.report1 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount:    100,
			PayoutAmount:         12345,
			VatAmount:            100,
			FeeAmount:            50,
			RollingReserveAmount: 0,
			CorrectionAmount:     0,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     suite.dateFrom1,
		PeriodTo:       suite.dateTo1,
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.report2 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     suite.dateFrom2,
		PeriodTo:       suite.dateTo2,
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.report3 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusPending,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.report4 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 0,
			PayoutAmount:      0,
			VatAmount:         0,
			FeeAmount:         0,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.report5 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 10,
			PayoutAmount:      30,
			VatAmount:         40,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       "USD",
	}

	suite.report6 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 100,
			PayoutAmount:      alreadyPaidRoyalty,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.report7 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount: 100,
			PayoutAmount:      90,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusDispute,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
		Currency:       suite.merchant.GetPayoutCurrency(),
	}

	suite.payout1 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           suite.merchant.Id,
		SourceId:             []string{suite.report1.Id, suite.report2.Id},
		TotalFees:            765000,
		Balance:              765000,
		Currency:             "RUB",
		Status:               pkg.PayoutDocumentStatusPending,
		Description:          "test payout document",
		Destination:          suite.merchant.Banking,
		CreatedAt:            ptypes.TimestampNow(),
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          ptypes.TimestampNow(),
		HasMerchantSignature: false,
		HasPspSignature:      false,
		SignatureData:        &billing.PayoutDocumentSignatureData{},
		Transaction:          "",
		FailureTransaction:   "",
		FailureMessage:       "",
		FailureCode:          "",
	}

	suite.payout2 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           suite.merchant.Id,
		SourceId:             []string{suite.report6.Id},
		TotalFees:            alreadyPaidRoyalty,
		Balance:              alreadyPaidRoyalty,
		Currency:             "RUB",
		Status:               pkg.PayoutDocumentStatusPending,
		Description:          "test payout document",
		Destination:          suite.merchant.Banking,
		CreatedAt:            date,
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          ptypes.TimestampNow(),
		HasMerchantSignature: true,
		HasPspSignature:      true,
		SignatureData:        &billing.PayoutDocumentSignatureData{},
		Transaction:          "124123",
		FailureTransaction:   "",
		FailureMessage:       "",
		FailureCode:          "",
	}

	suite.payout3 = &billing.PayoutDocument{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: suite.merchant.Id,
	}

	expires, err := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))
	assert.NoError(suite.T(), err, "Generate payout url expire date failed")

	suite.payout4 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           suite.merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		TotalFees:            765000,
		Balance:              765000,
		Currency:             "RUB",
		Status:               pkg.PayoutDocumentStatusPending,
		Description:          "test payout document",
		Destination:          suite.merchant.Banking,
		CreatedAt:            ptypes.TimestampNow(),
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          ptypes.TimestampNow(),
		HasMerchantSignature: false,
		HasPspSignature:      false,
		SignatureData: &billing.PayoutDocumentSignatureData{
			DetailsUrl:          "http://127.0.0.1/details",
			FilesUrl:            "http://127.0.0.1/files",
			SignatureRequestId:  bson.NewObjectId().Hex(),
			MerchantSignatureId: bson.NewObjectId().Hex(),
			PsSignatureId:       bson.NewObjectId().Hex(),
			MerchantSignUrl: &billing.PayoutDocumentSignatureDataSignUrl{
				SignUrl:   "http://127.0.0.1/merchant",
				ExpiresAt: expires,
			},
			PsSignUrl: &billing.PayoutDocumentSignatureDataSignUrl{
				SignUrl:   "http://127.0.0.1/ps",
				ExpiresAt: expires,
			},
		},
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
	}

	suite.payout5 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           suite.merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		TotalFees:            765000,
		Balance:              765000,
		Currency:             "RUB",
		Status:               pkg.PayoutDocumentStatusPending,
		Description:          "test payout document",
		Destination:          suite.merchant.Banking,
		CreatedAt:            ptypes.TimestampNow(),
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          ptypes.TimestampNow(),
		HasMerchantSignature: false,
		HasPspSignature:      false,
		SignatureData: &billing.PayoutDocumentSignatureData{
			DetailsUrl:          "http://127.0.0.1/details",
			FilesUrl:            "http://127.0.0.1/files",
			SignatureRequestId:  bson.NewObjectId().Hex(),
			MerchantSignatureId: bson.NewObjectId().Hex(),
			PsSignatureId:       bson.NewObjectId().Hex(),
			MerchantSignUrl: &billing.PayoutDocumentSignatureDataSignUrl{
				SignUrl:   "http://127.0.0.1/merchant",
				ExpiresAt: date,
			},
			PsSignUrl: &billing.PayoutDocumentSignatureDataSignUrl{
				SignUrl:   "http://127.0.0.1/ps",
				ExpiresAt: date,
			},
		},
		Transaction:        "",
		FailureTransaction: "",
		FailureMessage:     "",
		FailureCode:        "",
	}

	suite.payout6 = &billing.PayoutDocument{
		Id:                      bson.NewObjectId().Hex(),
		MerchantId:              suite.merchant.Id,
		SourceId:                []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		TotalFees:               765000,
		Balance:                 765000,
		Currency:                "RUB",
		Status:                  pkg.PayoutDocumentStatusPending,
		Description:             "test payout document",
		Destination:             suite.merchant.Banking,
		CreatedAt:               ptypes.TimestampNow(),
		UpdatedAt:               ptypes.TimestampNow(),
		ArrivalDate:             ptypes.TimestampNow(),
		RenderedDocumentFileUrl: "http://localhost.rendered.pdf",
		HasMerchantSignature:    false,
		HasPspSignature:         false,
		Transaction:             "",
		FailureTransaction:      "",
		FailureMessage:          "",
		FailureCode:             "",
	}

	suite.payout7 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           suite.merchant.Id,
		SourceId:             []string{suite.report6.Id},
		TotalFees:            alreadyPaidRoyalty,
		Balance:              alreadyPaidRoyalty,
		Currency:             "RUB",
		Status:               pkg.PayoutDocumentStatusPaid,
		Description:          "test payout document",
		Destination:          suite.merchant.Banking,
		CreatedAt:            date,
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          ptypes.TimestampNow(),
		HasMerchantSignature: true,
		HasPspSignature:      true,
		SignatureData:        &billing.PayoutDocumentSignatureData{},
		Transaction:          "124123",
		FailureTransaction:   "",
		FailureMessage:       "",
		FailureCode:          "",
	}

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

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
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	merchants := []*billing.Merchant{suite.merchant}
	if err := suite.service.merchant.MultipleInsert(merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	var core zapcore.Core

	lvl := zap.NewAtomicLevel()
	core, suite.zapRecorder = observer.New(lvl)
	suite.logObserver = zap.New(core)
}

func (suite *PayoutsTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *PayoutsTestSuite) helperInsertRoyaltyReports(data []*billing.RoyaltyReport) {
	for _, r := range data {
		if err := suite.service.db.Collection(collectionRoyaltyReport).Insert(r); err != nil {
			suite.FailNow("Insert royalty report test data failed", "%v", err)
		}
	}
}

func (suite *PayoutsTestSuite) helperInsertPayoutDocuments(data []*billing.PayoutDocument) {
	for _, p := range data {
		if err := suite.service.payoutDocument.Insert(p, "127.0.0.1", payoutChangeSourceAdmin); err != nil {
			suite.FailNow("Insert payout test data failed", "%v", err)
		}
	}
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Ok_NoPayoutsYet() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report6})

	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 2)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Ok_AlreadyHasPayouts() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report6})
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1, suite.payout2})

	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 1)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Ok_FilteringByCurrency() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report5, suite.report6})

	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 2)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_NotFound() {
	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesNotFound.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_MerchantNotFound() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report6})
	reports, err := suite.service.getPayoutDocumentSources(&billing.Merchant{Id: bson.NewObjectId().Hex()})
	assert.EqualError(suite.T(), err, errorPayoutSourcesNotFound.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_HasPendingReports() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report3})

	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesPending.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_getPayoutDocumentSources_Fail_HasDisputingReports() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report7})

	reports, err := suite.service.getPayoutDocumentSources(suite.merchant)
	assert.EqualError(suite.T(), err, errorPayoutSourcesDispute.Error())
	assert.Len(suite.T(), reports, 0)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_Pending() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	controlAmount := suite.report1.Totals.PayoutAmount + suite.report2.Totals.PayoutAmount
	assert.Equal(suite.T(), res.Item.Balance, controlAmount)
	assert.Equal(suite.T(), res.Item.Balance, float64(13579.5))
	assert.True(suite.T(), suite.merchant.MinPayoutAmount < controlAmount)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusPending)
	assert.Len(suite.T(), res.Item.SourceId, 2)
	assert.Equal(suite.T(), res.Item.PeriodFrom, suite.dateFrom1)
	assert.Equal(suite.T(), res.Item.PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_SkipByAmount() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	controlAmount := suite.report2.Totals.PayoutAmount
	assert.Equal(suite.T(), res.Item.Balance, controlAmount)
	assert.Equal(suite.T(), res.Item.Balance, float64(1234.5))
	assert.True(suite.T(), suite.merchant.MinPayoutAmount > controlAmount)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusSkip)
	assert.Len(suite.T(), res.Item.SourceId, 1)
	assert.Equal(suite.T(), res.Item.PeriodFrom, suite.dateFrom2)
	assert.Equal(suite.T(), res.Item.PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Ok_SkipByRollingReserve() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.report1.Totals.RollingReserveAmount = 600

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req1 := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res1 := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req1, res1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res1.Status, pkg.ResponseStatusOk)
	controlAmount := (suite.report1.Totals.PayoutAmount - suite.report1.Totals.CorrectionAmount - suite.report1.Totals.RollingReserveAmount) +
		(suite.report2.Totals.PayoutAmount - suite.report2.Totals.CorrectionAmount - suite.report2.Totals.RollingReserveAmount)
	assert.Equal(suite.T(), res1.Item.Balance, controlAmount)
	assert.Equal(suite.T(), res1.Item.Balance, float64(12979.5))
	assert.True(suite.T(), suite.merchant.MinPayoutAmount > controlAmount)
	assert.Equal(suite.T(), res1.Item.Status, pkg.PayoutDocumentStatusSkip)
	assert.Len(suite.T(), res1.Item.SourceId, 2)
	assert.Equal(suite.T(), res1.Item.PeriodFrom, suite.dateFrom1)
	assert.Equal(suite.T(), res1.Item.PeriodTo, suite.dateTo2)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NoSources() {

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutSourcesNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_MerchantNotFound() {

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  bson.NewObjectId().Hex(),
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, fmt.Errorf(errorNotFound, collectionMerchant).Error())
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_ZeroAmount() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report4})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutAmountInvalid)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NoBalance() {
	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1})

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.Equal(suite.T(), res.Message, errorPayoutBalanceError)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_NotEnoughBalance() {

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1})
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.GetMerchantBalanceRequest{
		MerchantId: suite.merchant.Id,
	}
	res := &grpc.GetMerchantBalanceResponse{}
	err = suite.service.GetMerchantBalance(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.MerchantId, suite.merchant.Id)
	assert.Equal(suite.T(), res.Item.Currency, suite.merchant.GetPayoutCurrency())
	assert.Equal(suite.T(), res.Item.Debit, float64(12345))
	assert.Equal(suite.T(), res.Item.Credit, float64(10432))
	assert.Equal(suite.T(), res.Item.RollingReserve, float64(0))
	assert.Equal(suite.T(), res.Item.Total, float64(1913))

	req1 := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res1 := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req1, res1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res1.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res1.Message, errorPayoutNotEnoughBalance)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_InsertError() {

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	pds.On("GetAllSourcesIdHex", mock2.Anything, mock2.Anything).Return([]string{}, nil)
	pds.On("GetBalanceAmount", mock2.Anything, mock2.Anything).Return(float64(0), nil)
	pds.On("GetLast", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.payoutDocument = pds

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_InsertErrorWithResponse() {

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything).Return(newBillingServerErrorMsg("0", "test"))
	pds.On("GetAllSourcesIdHex", mock2.Anything, mock2.Anything).Return([]string{}, nil)
	pds.On("GetBalanceAmount", mock2.Anything, mock2.Anything).Return(float64(0), nil)
	pds.On("GetLast", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.payoutDocument = pds

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report2})

	_, err := suite.service.updateMerchantBalance(suite.merchant.Id)
	assert.NoError(suite.T(), err)

	req := &grpc.CreatePayoutDocumentRequest{
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err = suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.EqualError(suite.T(), res.Message, "test")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout2})

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout2.Id,
		Status:             pkg.PayoutDocumentStatusInProgress,
		Transaction:        "transaction123",
		FailureTransaction: "failure456",
		FailureMessage:     "bla-bla-bla",
		FailureCode:        "999",
		Ip:                 "192.168.1.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout2.Id)
	assert.True(suite.T(), res.Item.HasMerchantSignature)
	assert.True(suite.T(), res.Item.HasPspSignature)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusInProgress)
	assert.Equal(suite.T(), res.Item.Transaction, "transaction123")
	assert.Equal(suite.T(), res.Item.FailureTransaction, "failure456")
	assert.Equal(suite.T(), res.Item.FailureMessage, "bla-bla-bla")
	assert.Equal(suite.T(), res.Item.FailureCode, "999")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_StatusRequiresFullSign() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1})

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout1.Id,
		Status:             pkg.PayoutDocumentStatusInProgress,
		Transaction:        "transaction123",
		FailureTransaction: "failure456",
		FailureMessage:     "bla-bla-bla",
		FailureCode:        "999",
		Ip:                 "192.168.1.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutStatusRequiresFullSign)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok_PaidOk() {

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report6})
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout2})

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout2.Id,
		Status:           pkg.PayoutDocumentStatusPaid,
		Transaction:      "transaction123",
		Ip:               "192.168.1.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout2.Id)
	assert.True(suite.T(), res.Item.HasMerchantSignature)
	assert.True(suite.T(), res.Item.HasPspSignature)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusPaid)
	assert.Equal(suite.T(), res.Item.Transaction, "transaction123")

	rr, err := suite.service.royaltyReport.GetById(suite.report6.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rr.Status, pkg.RoyaltyReportStatusPaid)
	assert.Equal(suite.T(), rr.PayoutDocumentId, suite.payout2.Id)
	assert.Greater(suite.T(), rr.PayoutDate.Seconds, int64(-62135596800))
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_StatusForbidden() {

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report6})
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout7})

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout7.Id,
		Status:           pkg.PayoutDocumentStatusFailed,
		Transaction:      "transaction123",
		Ip:               "192.168.1.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutStatusChangeIsForbidden)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Ok_NotModified() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1})

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: suite.payout1.Id,
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotModified)
	assert.Equal(suite.T(), res.Item.Id, suite.payout1.Id)
	assert.False(suite.T(), res.Item.HasMerchantSignature)
	assert.False(suite.T(), res.Item.HasPspSignature)
	assert.Equal(suite.T(), res.Item.Status, "pending")
	assert.Equal(suite.T(), res.Item.Transaction, "")
	assert.Equal(suite.T(), res.Item.FailureTransaction, "")
	assert.Equal(suite.T(), res.Item.FailureMessage, "")
	assert.Equal(suite.T(), res.Item.FailureCode, "")
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_NotFound() {

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: bson.NewObjectId().Hex(),
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPayoutNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocument_Failed_UpdateError() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1})

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Update", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	pds.On("GetById", mock2.Anything).Return(suite.payout2, nil)
	suite.service.payoutDocument = pds

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout2.Id,
		Status:             pkg.PayoutDocumentStatusInProgress,
		Transaction:        "transaction123",
		FailureTransaction: "failure456",
		FailureMessage:     "bla-bla-bla",
		FailureCode:        "999",
		Ip:                 "192.168.1.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_ById_Ok() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout7})

	req := &grpc.GetPayoutDocumentsRequest{
		PayoutDocumentId: suite.payout7.Id,
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(1))
	assert.Len(suite.T(), res.Data.Items, 1)
	assert.Equal(suite.T(), res.Data.Items[0].Id, suite.payout7.Id)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_ByQuery_Ok() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1, suite.payout2, suite.payout3, suite.payout4, suite.payout7})

	req := &grpc.GetPayoutDocumentsRequest{
		Signed:     true,
		Status:     []string{"paid"},
		MerchantId: suite.merchant.Id,
		Limit:      10,
		Offset:     0,
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(1))
	assert.Len(suite.T(), res.Data.Items, 1)
	assert.Equal(suite.T(), res.Data.Items[0].Id, suite.payout7.Id)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_AllWithPaging_Ok() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1, suite.payout2, suite.payout3, suite.payout4, suite.payout5})
	req := &grpc.GetPayoutDocumentsRequest{
		Limit:  1,
		Offset: 0,
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(5))
	assert.Len(suite.T(), res.Data.Items, 1)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocuments_Ok_NotFound() {
	req := &grpc.GetPayoutDocumentsRequest{
		PayoutDocumentId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(0))
	assert.Nil(suite.T(), res.Data.Items)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Ok_Merchant() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout4})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout4.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.SignUrl, "http://127.0.0.1/merchant")
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Ok_Merchant_UrlRefreshed() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout5})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout5.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.SignUrl, "http://127.0.0.1")
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Ok_Ps() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout4})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout4.Id,
		SignerType:       pkg.SignerTypePs,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.SignUrl, "http://127.0.0.1/ps")
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Ok_PsUrlRefreshed() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout5})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout5.Id,
		SignerType:       pkg.SignerTypePs,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.SignUrl, "http://127.0.0.1")
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Ok_SignetureCreated() {
	reporting := &reportingMocks.ReporterService{}
	reporting.On("CreateFile", mock2.Anything, mock2.Anything).Return(nil, nil)
	suite.service.reporterService = reporting

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout6})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout6.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.SignUrl, "http://127.0.0.1")
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_CreateSignatureGrpcError() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout6})

	ds := &mocks.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(nil, errors.New(mocks.SomeError))
	suite.service.documentSigner = ds

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout6.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.Equal(suite.T(), res.Message, errorPayoutCreateSignature)
}

func (suite *PayoutsTestSuite) TestPayouts_CreatePayoutDocument_Failed_CreateSignatureResponseError() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout6})

	ds := &mocks.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(&proto.CreateSignatureResponse{
			Status: pkg.ResponseStatusBadData,
			Message: &proto.ResponseErrorMessage{
				Message: "some error message",
			},
		}, nil)
	suite.service.documentSigner = ds

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout6.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.Equal(suite.T(), res.Message.Message, "some error message")
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Failed_NotFound() {
	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: bson.NewObjectId().Hex(),
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPayoutNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Failed_AlreadySignedByMerchant() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout2})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout2.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutAlreadySigned)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Failed_AlreadySignedByPs() {
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout2})

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout2.Id,
		SignerType:       pkg.SignerTypePs,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutAlreadySigned)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Failed_RefreshSignatureUrl_AsError() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout5})

	ds := &mocks.DocumentSignerService{}
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(nil, errors.New(mocks.SomeError))
	suite.service.documentSigner = ds

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout5.Id,
		SignerType:       pkg.SignerTypePs,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestPayouts_GetPayoutDocumentSignUrl_Failed_RefreshSignatureUrl_AsResponse() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout5})

	ds := &mocks.DocumentSignerService{}
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).
		Return(&proto.GetSignatureUrlResponse{
			Status:  pkg.ResponseStatusSystemError,
			Message: &proto.ResponseErrorMessage{},
		}, nil)
	suite.service.documentSigner = ds

	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout5.Id,
		SignerType:       pkg.SignerTypePs,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocumentSignatures_Ok() {

	suite.helperInsertRoyaltyReports([]*billing.RoyaltyReport{suite.report1, suite.report2})
	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1})

	req := &grpc.UpdatePayoutDocumentSignaturesRequest{
		PayoutDocumentId:      suite.payout1.Id,
		HasPspSignature:       true,
		HasMerchantSignature:  true,
		SignedDocumentFileUrl: "http://localhost/123.pdf",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocumentSignatures(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Item.Id, suite.payout1.Id)
	assert.True(suite.T(), res.Item.HasMerchantSignature)
	assert.True(suite.T(), res.Item.HasPspSignature)
	assert.Equal(suite.T(), res.Item.SignedDocumentFileUrl, "http://localhost/123.pdf")

	rr, err := suite.service.royaltyReport.GetById(suite.report1.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rr.Status, pkg.RoyaltyReportStatusWaitForPayment)
	assert.Equal(suite.T(), rr.PayoutDocumentId, suite.payout1.Id)
	assert.EqualValues(suite.T(), -62135596800, rr.PayoutDate.Seconds)

	rr, err = suite.service.royaltyReport.GetById(suite.report2.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rr.Status, pkg.RoyaltyReportStatusWaitForPayment)
	assert.Equal(suite.T(), rr.PayoutDocumentId, suite.payout1.Id)
	assert.EqualValues(suite.T(), -62135596800, rr.PayoutDate.Seconds)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocumentSignatures_Failed_NotFound() {

	req := &grpc.UpdatePayoutDocumentSignaturesRequest{
		PayoutDocumentId:      bson.NewObjectId().Hex(),
		HasPspSignature:       true,
		HasMerchantSignature:  true,
		SignedDocumentFileUrl: "http://localhost/123.pdf",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocumentSignatures(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPayoutNotFound)
}

func (suite *PayoutsTestSuite) TestPayouts_UpdatePayoutDocumentSignatures_Failed_UpdateError() {

	suite.helperInsertPayoutDocuments([]*billing.PayoutDocument{suite.payout1})

	pds := &mocks.PayoutDocumentServiceInterface{}
	pds.On("Update", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mocks.SomeError))
	pds.On("GetById", mock2.Anything).Return(suite.payout1, nil)
	suite.service.payoutDocument = pds

	req := &grpc.UpdatePayoutDocumentSignaturesRequest{
		PayoutDocumentId:      suite.payout1.Id,
		HasPspSignature:       true,
		HasMerchantSignature:  true,
		SignedDocumentFileUrl: "http://localhost/123.pdf",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocumentSignatures(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}
