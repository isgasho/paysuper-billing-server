package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"testing"
	"time"
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

	payout1 *billing.PayoutDocument
	payout2 *billing.PayoutDocument
	payout3 *billing.PayoutDocument
	payout4 *billing.PayoutDocument
	payout5 *billing.PayoutDocument
}

func Test_Payouts(t *testing.T) {
	suite.Run(t, new(PayoutsTestSuite))
}

func (suite *PayoutsTestSuite) SetupTest() {
	cfg, err := config.NewConfig()

	assert.NoError(suite.T(), err, "Config load failed")

	cfg.AccountingCurrency = "RUB"
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

	merchant := &billing.Merchant{
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

	suite.report1 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchant.Id,
		Amounts: &billing.RoyaltyReportDetails{
			Currency:          merchant.GetPayoutCurrency(),
			TransactionsCount: 100,
			GrossAmount:       100500,
			PayoutAmount:      12345,
			VatAmount:         100,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
	}

	suite.report2 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchant.Id,
		Amounts: &billing.RoyaltyReportDetails{
			Currency:          merchant.GetPayoutCurrency(),
			TransactionsCount: 10,
			GrossAmount:       10050,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
	}

	suite.report3 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchant.Id,
		Amounts: &billing.RoyaltyReportDetails{
			Currency:          merchant.GetPayoutCurrency(),
			TransactionsCount: 10,
			GrossAmount:       10050,
			PayoutAmount:      1234.5,
			VatAmount:         10,
			FeeAmount:         5,
		},
		Status:         pkg.RoyaltyReportStatusPending,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
	}

	suite.report4 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchant.Id,
		Amounts: &billing.RoyaltyReportDetails{
			Currency:          merchant.GetPayoutCurrency(),
			TransactionsCount: 0,
			GrossAmount:       0,
			PayoutAmount:      0,
			VatAmount:         0,
			FeeAmount:         0,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
	}

	suite.report5 = &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchant.Id,
		Amounts: &billing.RoyaltyReportDetails{
			Currency:          "USD",
			TransactionsCount: 10,
			GrossAmount:       20,
			PayoutAmount:      30,
			VatAmount:         40,
			FeeAmount:         50,
		},
		Status:         pkg.RoyaltyReportStatusAccepted,
		CreatedAt:      ptypes.TimestampNow(),
		PeriodFrom:     ptypes.TimestampNow(),
		PeriodTo:       ptypes.TimestampNow(),
		AcceptExpireAt: ptypes.TimestampNow(),
	}

	suite.payout1 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		Amount:               765000,
		Currency:             "RUB",
		Status:               "pending",
		Description:          "test payout document",
		Destination:          merchant.Banking,
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
		MerchantId:           merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		Amount:               765000,
		Currency:             "RUB",
		Status:               "paid",
		Description:          "test payout document",
		Destination:          merchant.Banking,
		CreatedAt:            ptypes.TimestampNow(),
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
		MerchantId: merchant.Id,
	}

	expires, err := ptypes.TimestampProto(time.Now().Add(1 * time.Hour))
	assert.NoError(suite.T(), err, "Generate payout url expire date failed")

	suite.payout4 = &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		MerchantId:           merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		Amount:               765000,
		Currency:             "RUB",
		Status:               "pending",
		Description:          "test payout document",
		Destination:          merchant.Banking,
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
		MerchantId:           merchant.Id,
		SourceId:             []string{bson.NewObjectId().Hex(), bson.NewObjectId().Hex(), bson.NewObjectId().Hex()},
		Amount:               765000,
		Currency:             "RUB",
		Status:               "pending",
		Description:          "test payout document",
		Destination:          merchant.Banking,
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

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	redisdb := mock.NewTestRedis()
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
		mock.NewCurrencyServiceMockOk(),
		mock.NewDocumentSignerMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	merchants := []*billing.Merchant{merchant}
	if err := suite.service.merchant.MultipleInsert(merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	reports := []*billing.RoyaltyReport{suite.report1, suite.report2, suite.report3, suite.report4, suite.report5}
	for _, r := range reports {
		if err := suite.service.db.Collection(collectionRoyaltyReport).Insert(r); err != nil {
			suite.FailNow("Insert royalty report test data failed", "%v", err)
		}
	}

	payouts := []*billing.PayoutDocument{suite.payout1, suite.payout2, suite.payout3, suite.payout4, suite.payout5}
	for _, p := range payouts {
		if err := suite.service.payoutDocument.Insert(p, "127.0.0.1", payoutChangeSourceAdmin); err != nil {
			suite.FailNow("Insert payout test data failed", "%v", err)
		}
	}

	if err := suite.service.country.Insert(country); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.merchant = merchant

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

func (suite *PayoutsTestSuite) TestRoyaltyReport_getPayoutDocumentSources_Ok() {
	sources := []string{suite.report1.Id, suite.report2.Id, suite.report3.Id}
	reports, err := suite.service.getPayoutDocumentSources(sources, suite.merchant.Id)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), reports, 2)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_getPayoutDocumentSources_NotFound() {
	sources := []string{suite.report1.Id, suite.report2.Id, suite.report3.Id}
	_, err := suite.service.getPayoutDocumentSources(sources, bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, errorPayoutSourcesNotFound.Error())
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Ok_Pending() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	controlAmount := suite.report1.Amounts.PayoutAmount + suite.report2.Amounts.PayoutAmount
	assert.Equal(suite.T(), res.Item.Amount, controlAmount)
	assert.Equal(suite.T(), res.Item.Amount, float64(13579.5))
	assert.True(suite.T(), suite.merchant.MinPayoutAmount < controlAmount)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusPending)
	assert.Len(suite.T(), res.Item.SourceId, 2)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Ok_Skip() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	controlAmount := suite.report2.Amounts.PayoutAmount
	assert.Equal(suite.T(), res.Item.Amount, controlAmount)
	assert.Equal(suite.T(), res.Item.Amount, float64(1234.5))
	assert.True(suite.T(), suite.merchant.MinPayoutAmount > controlAmount)
	assert.Equal(suite.T(), res.Item.Status, pkg.PayoutDocumentStatusSkip)
	assert.Len(suite.T(), res.Item.SourceId, 1)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_NoSources() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutNoSources)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_MerchantNotFound() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report2.Id},
		MerchantId:  bson.NewObjectId().Hex(),
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, fmt.Errorf(errorNotFound, collectionMerchant).Error())
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_SourcesNotFound() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report3.Id},
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_SourcesInconsistent() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report5.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutSourcesInconsistent)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_ZeroAmount() {

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report4.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutAmountInvalid)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_CreateSignatureGrpcError() {

	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(nil, errors.New(mock.SomeError))
	suite.service.documentSigner = ds

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, errorCreateSignature.Error())
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_CreateSignatureResponseError() {

	ds := &mock.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(&proto.CreateSignatureResponse{
			Status: pkg.ResponseStatusBadData,
			Message: &proto.ResponseErrorMessage{
				Message: "some error message",
			},
		}, nil)
	suite.service.documentSigner = ds

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, "some error message")
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_InsertError() {

	pds := &mock.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mock.SomeError))
	suite.service.payoutDocument = pds

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.Error(suite.T(), err)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_CreatePayoutDocument_Failed_InsertErrorWithResponse() {

	pds := &mock.PayoutDocumentServiceInterface{}
	pds.On("Insert", mock2.Anything, mock2.Anything, mock2.Anything).Return(newBillingServerErrorMsg("0", "test"))
	suite.service.payoutDocument = pds

	req := &grpc.CreatePayoutDocumentRequest{
		SourceId:    []string{suite.report1.Id, suite.report2.Id},
		MerchantId:  suite.merchant.Id,
		Description: "test payout",
		Ip:          "127.0.0.1",
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.CreatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusSystemError)
	assert.EqualError(suite.T(), res.Message, "test")
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocumentSignatures_Ok() {

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
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocumentSignatures_Failed_NotFound() {

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

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocumentSignatures_Failed_UpdateError() {

	pds := &mock.PayoutDocumentServiceInterface{}
	pds.On("Update", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mock.SomeError))
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocument_Ok() {

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout1.Id,
		Status:             "in_progress",
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
	assert.Equal(suite.T(), res.Item.Id, suite.payout1.Id)
	assert.False(suite.T(), res.Item.HasMerchantSignature)
	assert.False(suite.T(), res.Item.HasPspSignature)
	assert.Equal(suite.T(), res.Item.Status, "in_progress")
	assert.Equal(suite.T(), res.Item.Transaction, "transaction123")
	assert.Equal(suite.T(), res.Item.FailureTransaction, "failure456")
	assert.Equal(suite.T(), res.Item.FailureMessage, "bla-bla-bla")
	assert.Equal(suite.T(), res.Item.FailureCode, "999")
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocument_Ok_NotModified() {

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

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocument_Failed_NotFound() {

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId: bson.NewObjectId().Hex(),
	}

	res := &grpc.PayoutDocumentResponse{}

	err := suite.service.UpdatePayoutDocument(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
	assert.Equal(suite.T(), res.Message, errorPayoutNotFound)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_UpdatePayoutDocument_Failed_UpdateError() {

	pds := &mock.PayoutDocumentServiceInterface{}
	pds.On("Update", mock2.Anything, mock2.Anything, mock2.Anything).Return(errors.New(mock.SomeError))
	suite.service.payoutDocument = pds

	req := &grpc.UpdatePayoutDocumentRequest{
		PayoutDocumentId:   suite.payout1.Id,
		Status:             "in_progress",
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocuments_ById_Ok() {
	req := &grpc.GetPayoutDocumentsRequest{
		PayoutDocumentId: suite.payout1.Id,
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Equal(suite.T(), res.Data.Count, int32(1))
	assert.Len(suite.T(), res.Data.Items, 1)
	assert.Equal(suite.T(), res.Data.Items[0].Id, suite.payout1.Id)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocuments_ByQuery_Ok() {
	req := &grpc.GetPayoutDocumentsRequest{
		Signed:     true,
		Status:     "paid",
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
	assert.Equal(suite.T(), res.Data.Items[0].Id, suite.payout2.Id)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocuments_AllWithPaging_Ok() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocuments_Failed_NotFound() {
	req := &grpc.GetPayoutDocumentsRequest{
		PayoutDocumentId: bson.NewObjectId().Hex(),
	}

	res := &grpc.GetPayoutDocumentsResponse{}

	err := suite.service.GetPayoutDocuments(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusNotFound)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Ok_Merchant() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Ok_Merchant_UrlRefreshed() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Ok_Ps() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Ok_PsUrlRefreshed() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_NotFound() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_NoSignatureData() {
	req := &grpc.GetPayoutDocumentSignUrlRequest{
		PayoutDocumentId: suite.payout3.Id,
		SignerType:       pkg.SignerTypeMerchant,
		Ip:               "127.0.0.1",
	}

	res := &grpc.GetPayoutDocumentSignUrlResponse{}

	err := suite.service.GetPayoutDocumentSignUrl(context.TODO(), req, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorPayoutInvalid)
}

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_AlreadySignedByMerchant() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_AlreadySignedByPs() {
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_RefreshSignatureUrl_AsError() {
	ds := &mock.DocumentSignerService{}
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(nil, errors.New(mock.SomeError))
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

func (suite *PayoutsTestSuite) TestRoyaltyReport_GetPayoutDocumentSignUrl_Failed_RefreshSignatureUrl_AsResponse() {
	ds := &mock.DocumentSignerService{}
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
