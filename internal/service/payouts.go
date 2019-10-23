package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	documentSignerConst "github.com/paysuper/document-signer/pkg/constant"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	reporterConst "github.com/paysuper/paysuper-reporter/pkg"
	reporterProto "github.com/paysuper/paysuper-reporter/pkg/proto"
	"go.uber.org/zap"
	"sort"
	"time"
)

const (
	collectionPayoutDocuments       = "payout_documents"
	collectionPayoutDocumentChanges = "payout_documents_changes"

	cacheKeyPayoutDocument = "payout_document:id:%s"

	payoutChangeSourceMerchant  = "merchant"
	payoutChangeSourceAdmin     = "admin"
	payoutChangeSourceReporter  = "reporter"
	payoutChangeSourceHellosign = "hellosign"

	payoutArrivalInDays = 5
)

var (
	errorPayoutSourcesNotFound         = newBillingServerErrorMsg("po000001", "no source documents found for payout")
	errorPayoutSourcesPending          = newBillingServerErrorMsg("po000002", "you have at least one royalty report waiting for acceptance")
	errorPayoutSourcesDispute          = newBillingServerErrorMsg("po000003", "you have at least one unclosed dispute in your royalty reports")
	errorPayoutNotFound                = newBillingServerErrorMsg("po000004", "payout document not found")
	errorPayoutAmountInvalid           = newBillingServerErrorMsg("po000005", "payout amount is invalid")
	errorPayoutAlreadySigned           = newBillingServerErrorMsg("po000006", "payout already signed for this signer type")
	errorPayoutCreateSignature         = newBillingServerErrorMsg("po000007", "create signature failed")
	errorPayoutUpdateBalance           = newBillingServerErrorMsg("po000008", "balance update failed")
	errorPayoutBalanceError            = newBillingServerErrorMsg("po000009", "getting balance failed")
	errorPayoutNotEnoughBalance        = newBillingServerErrorMsg("po000010", "not enough balance for payout")
	errorPayoutNotRendered             = newBillingServerErrorMsg("po000011", "payout document not rendered yet")
	errorPayoutUpdateRoyaltyReports    = newBillingServerErrorMsg("po000012", "royalty reports update failed")
	errorPayoutStatusRequiresFullSign  = newBillingServerErrorMsg("po000013", "requested status requires fully signed payout document")
	errorPayoutStatusChangeIsForbidden = newBillingServerErrorMsg("po000014", "status change is forbidden")

	statusForUpdateBalance = map[string]bool{
		pkg.PayoutDocumentStatusPending:    true,
		pkg.PayoutDocumentStatusInProgress: true,
		pkg.PayoutDocumentStatusPaid:       true,
	}

	statusForBecomePaid = map[string]bool{
		pkg.PayoutDocumentStatusPaid: true,
	}

	statusForBecomeFailed = map[string]bool{
		pkg.PayoutDocumentStatusFailed:   true,
		pkg.PayoutDocumentStatusCanceled: true,
	}

	statusRequiresFullySigned = map[string]bool{
		pkg.PayoutDocumentStatusInProgress: true,
		pkg.PayoutDocumentStatusPaid:       true,
	}

	payoutDocumentStatusActive = []string{
		pkg.PayoutDocumentStatusPending,
		pkg.PayoutDocumentStatusInProgress,
		pkg.PayoutDocumentStatusPaid,
	}
)

type PayoutDocumentServiceInterface interface {
	Insert(document *billing.PayoutDocument, ip, source string) error
	Update(document *billing.PayoutDocument, ip, source string) error
	GetById(id string) (*billing.PayoutDocument, error)
	GetAllSourcesIdHex(merchantId, currency string) ([]string, error)
	CountByQuery(query bson.M) (int, error)
	FindByQuery(query bson.M, sorts []string, limit, offset int) ([]*billing.PayoutDocument, error)
	GetBalanceAmount(merchantId, currency string) (float64, error)
	GetLast(merchantId, currency string) (*billing.PayoutDocument, error)
}

type sources struct {
	Items []string `bson:"sources"`
}

func newPayoutService(svc *Service) PayoutDocumentServiceInterface {
	s := &PayoutDocument{svc: svc}
	return s
}

func (s *Service) CreatePayoutDocument(
	ctx context.Context,
	req *grpc.CreatePayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) error {

	arrivalDate, err := ptypes.TimestampProto(now.EndOfDay().Add(time.Hour * 24 * payoutArrivalInDays))
	if err != nil {
		return err
	}

	pd := &billing.PayoutDocument{
		Id:                   bson.NewObjectId().Hex(),
		Status:               pkg.PayoutDocumentStatusPending,
		SourceId:             []string{},
		Description:          req.Description,
		CreatedAt:            ptypes.TimestampNow(),
		UpdatedAt:            ptypes.TimestampNow(),
		ArrivalDate:          arrivalDate,
		HasMerchantSignature: false,
		HasPspSignature:      false,
	}

	merchant, err := s.merchant.GetById(req.MerchantId)
	if err != nil {
		return err
	}

	pd.MerchantId = merchant.Id
	pd.Destination = merchant.Banking
	pd.Company = merchant.Company
	pd.MerchantAgreementNumber = merchant.AgreementNumber

	reports, err := s.getPayoutDocumentSources(merchant)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	pd.Currency = reports[0].Currency

	var times []time.Time

	for _, r := range reports {
		pd.TotalFees += r.Totals.PayoutAmount - r.Totals.CorrectionAmount
		pd.Balance += r.Totals.PayoutAmount - r.Totals.CorrectionAmount - r.Totals.RollingReserveAmount
		pd.TotalTransactions += r.Totals.TransactionsCount
		pd.SourceId = append(pd.SourceId, r.Id)

		from, err := ptypes.Timestamp(r.PeriodFrom)

		if err != nil {
			zap.L().Error(
				"Time conversion error",
				zap.Error(err),
			)
			return err
		}

		to, err := ptypes.Timestamp(r.PeriodTo)
		if err != nil {
			zap.L().Error(
				"Payout source time conversion error",
				zap.Error(err),
			)
			return err
		}
		times = append(times, from, to)
	}

	if pd.Balance <= 0 {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutAmountInvalid
		return nil
	}

	balance, err := s.getMerchantBalance(merchant.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPayoutBalanceError
		return nil
	}

	if pd.Balance > (balance.Debit - balance.Credit) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutNotEnoughBalance
		return nil
	}

	if pd.Balance < merchant.MinPayoutAmount {
		pd.Status = pkg.PayoutDocumentStatusSkip
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	from := times[0]
	to := times[len(times)-1]

	pd.PeriodFrom, err = ptypes.TimestampProto(from)
	if err != nil {
		zap.L().Error(
			"Payout PeriodFrom time conversion error",
			zap.Error(err),
		)
		return err
	}
	pd.PeriodTo, err = ptypes.TimestampProto(to)
	if err != nil {
		zap.L().Error(
			"Payout PeriodTo time conversion error",
			zap.Error(err),
		)
		return err
	}

	err = s.payoutDocument.Insert(pd, req.Ip, payoutChangeSourceMerchant)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	err = s.renderPayoutDocument(ctx, pd, merchant)
	if err != nil {
		return err
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = pd
	return nil
}

func (s *Service) renderPayoutDocument(
	ctx context.Context,
	pd *billing.PayoutDocument,
	merchant *billing.Merchant,
) error {
	params, err := json.Marshal(map[string]interface{}{reporterConst.ParamsFieldId: pd.Id})
	if err != nil {
		zap.L().Error(
			"Unable to marshal the params of payout for the reporting service.",
			zap.Error(err),
		)
		return err
	}

	fileReq := &reporterProto.ReportFile{
		UserId:           merchant.User.Id,
		MerchantId:       merchant.Id,
		ReportType:       reporterConst.ReportTypePayout,
		FileType:         reporterConst.OutputExtensionPdf,
		Params:           params,
		SendNotification: false,
	}

	if _, err = s.reporterService.CreateFile(ctx, fileReq); err != nil {
		zap.L().Error(
			"Unable to create file in the reporting service for payout.",
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (s *Service) UpdatePayoutDocumentSignatures(
	ctx context.Context,
	req *grpc.UpdatePayoutDocumentSignaturesRequest,
	res *grpc.PayoutDocumentResponse,
) error {
	pd, err := s.payoutDocument.GetById(req.PayoutDocumentId)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	pd.HasMerchantSignature = req.HasMerchantSignature
	pd.HasPspSignature = req.HasPspSignature

	if req.SignedDocumentFileUrl != "" {
		pd.SignedDocumentFileUrl = req.SignedDocumentFileUrl
	}

	err = s.payoutDocument.Update(pd, "", payoutChangeSourceHellosign)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	if pd.IsFullySigned() {
		_, err = s.updateMerchantBalance(pd.MerchantId)
		if err != nil {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = errorPayoutUpdateBalance

			return nil
		}
		err = s.royaltyReport.SetPayoutDocumentId(pd.SourceId, pd.Id, "", pkg.RoyaltyReportChangeSourceHellosign)
		if err != nil {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = errorPayoutUpdateRoyaltyReports

			return nil
		}
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = pd

	return nil
}

func (s *Service) UpdatePayoutDocument(
	ctx context.Context,
	req *grpc.UpdatePayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) error {
	pd, err := s.payoutDocument.GetById(req.PayoutDocumentId)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	isChanged := false
	needBalanceUpdate := false

	_, isReqStatusRequiresFullySigned := statusRequiresFullySigned[req.Status]
	if isReqStatusRequiresFullySigned && pd.Status != req.Status && !pd.IsFullySigned() {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutStatusRequiresFullSign

		return nil
	}

	_, isReqStatusForBecomePaid := statusForBecomePaid[req.Status]
	becomePaid := pd.IsFullySigned() && isReqStatusForBecomePaid && pd.Status != req.Status

	_, isReqStatusForBecomeFailed := statusForBecomeFailed[req.Status]
	_, isPayoutStatusForBecomeFailed := statusForBecomeFailed[pd.Status]
	becomeFailed := pd.IsFullySigned() && isReqStatusForBecomeFailed && !isPayoutStatusForBecomeFailed

	if req.Status != "" && pd.Status != req.Status {
		if pd.Status == pkg.PayoutDocumentStatusPaid || pd.Status == pkg.PayoutDocumentStatusFailed {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPayoutStatusChangeIsForbidden

			return nil
		}

		if req.Status == pkg.PayoutDocumentStatusPaid {
			pd.PaidAt = ptypes.TimestampNow()
		}

		isChanged = true
		pd.Status = req.Status
		if _, ok := statusForUpdateBalance[pd.Status]; ok {
			needBalanceUpdate = true
		}
	}

	if req.Transaction != "" && pd.Transaction != req.Transaction {
		isChanged = true
		pd.Transaction = req.Transaction
	}

	if req.FailureCode != "" && pd.FailureCode != req.FailureCode {
		isChanged = true
		pd.FailureCode = req.FailureCode
	}

	if req.FailureMessage != "" && pd.FailureMessage != req.FailureMessage {
		isChanged = true
		pd.FailureMessage = req.FailureMessage
	}

	if req.FailureTransaction != "" && pd.FailureTransaction != req.FailureTransaction {
		isChanged = true
		pd.FailureTransaction = req.FailureTransaction
	}

	if isChanged {
		err = s.payoutDocument.Update(pd, req.Ip, payoutChangeSourceAdmin)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = e
				return nil
			}
			return err
		}

		if becomePaid == true {
			err = s.royaltyReport.SetPaid(pd.SourceId, pd.Id, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
			if err != nil {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = errorPayoutUpdateRoyaltyReports

				return nil
			}

		} else {
			if becomeFailed == true {
				err = s.royaltyReport.UnsetPaid(pd.SourceId, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
				if err != nil {
					res.Status = pkg.ResponseStatusSystemError
					res.Message = errorPayoutUpdateRoyaltyReports

					return nil
				}
			}
		}

		res.Status = pkg.ResponseStatusOk
	} else {
		res.Status = pkg.ResponseStatusNotModified
	}

	if needBalanceUpdate == true {
		_, err = s.updateMerchantBalance(pd.MerchantId)
		if err != nil {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = errorPayoutUpdateBalance

			return nil
		}
	}

	res.Item = pd
	return nil
}

func (s *Service) GetPayoutDocuments(
	ctx context.Context,
	req *grpc.GetPayoutDocumentsRequest,
	res *grpc.GetPayoutDocumentsResponse,
) error {

	res.Status = pkg.ResponseStatusOk

	query := bson.M{}

	if req.PayoutDocumentId != "" {
		query["_id"] = bson.ObjectIdHex(req.PayoutDocumentId)
	} else {
		if len(req.Status) > 0 {
			query["status"] = bson.M{"$in": req.Status}
		}
		if req.MerchantId != "" {
			query["merchant_id"] = bson.ObjectIdHex(req.MerchantId)
		}
		if req.Signed == true {
			query["has_merchant_signature"] = true
			query["has_psp_signature"] = true
		}
	}

	count, err := s.payoutDocument.CountByQuery(query)

	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	if count == 0 {
		res.Status = pkg.ResponseStatusOk
		res.Data = &grpc.PayoutDocumentsPaginate{
			Count: 0,
			Items: nil,
		}
		return nil
	}

	sorts := []string{"_id"}

	pds, err := s.payoutDocument.FindByQuery(query, sorts, int(req.Limit), int(req.Offset))
	if err != nil {
		return err
	}

	res.Status = pkg.ResponseStatusOk
	res.Data = &grpc.PayoutDocumentsPaginate{
		Count: int32(count),
		Items: pds,
	}
	return nil
}

func (s *Service) GetPayoutDocumentSignUrl(
	ctx context.Context,
	req *grpc.GetPayoutDocumentSignUrlRequest,
	res *grpc.GetPayoutDocumentSignUrlResponse,
) error {

	res.Status = pkg.ResponseStatusOk

	pd, err := s.payoutDocument.GetById(req.PayoutDocumentId)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	if pd.SignatureData == nil {
		pd.SignatureData, err = s.getPayoutSignature(ctx, pd)
		if err != nil {
			zap.L().Error(
				"Getting signature data failed",
				zap.Error(err),
			)
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = e
				return nil
			}
			return err
		}
	}

	if req.SignerType == pkg.SignerTypeMerchant && pd.HasMerchantSignature {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutAlreadySigned

		return nil
	}

	if req.SignerType != pkg.SignerTypeMerchant && pd.HasPspSignature {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutAlreadySigned

		return nil
	}

	res.Item, err = s.changePayoutDocumentSignUrl(ctx, req.SignerType, pd, req.Ip)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	return nil
}

func (s *Service) PayoutDocumentPdfUploaded(
	ctx context.Context,
	req *grpc.PayoutDocumentPdfUploadedRequest,
	res *grpc.PayoutDocumentPdfUploadedResponse,
) error {
	res.Status = pkg.ResponseStatusOk

	pd, err := s.payoutDocument.GetById(req.PayoutId)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	if req.Filename != "" {
		pd.RenderedDocumentFileUrl = req.Filename
	}

	// update first to ensure save rendered document file url to db
	err = s.payoutDocument.Update(pd, "", payoutChangeSourceReporter)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	pd.SignatureData, err = s.getPayoutSignature(ctx, pd)
	if err != nil {
		zap.L().Error(
			"Getting signature data failed",
			zap.Error(err),
		)
		return err
	}

	// updating once again, to save signature data
	err = s.payoutDocument.Update(pd, "", payoutChangeSourceReporter)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	return nil
}

func (s *Service) getPayoutDocumentSources(merchant *billing.Merchant) ([]*billing.RoyaltyReport, error) {
	excludeIdsString, err := s.payoutDocument.GetAllSourcesIdHex(merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	result, err := s.royaltyReport.GetNonPayoutReports(merchant.Id, merchant.GetPayoutCurrency(), excludeIdsString)

	if err != nil && err != mgo.ErrNotFound {
		return nil, err
	}

	if err == mgo.ErrNotFound || result == nil || len(result) == 0 {
		return nil, errorPayoutSourcesNotFound
	}

	for _, v := range result {
		if v.Status == pkg.RoyaltyReportStatusPending {
			return nil, errorPayoutSourcesPending
		}
		if v.Status == pkg.RoyaltyReportStatusDispute {
			return nil, errorPayoutSourcesDispute
		}
	}

	return result, nil
}

func (s *Service) getPayoutSignature(
	ctx context.Context,
	pd *billing.PayoutDocument,
) (*billing.PayoutDocumentSignatureData, error) {

	merchant, err := s.merchant.GetById(pd.MerchantId)
	if err != nil {
		return nil, err
	}

	if pd.RenderedDocumentFileUrl == "" {
		err = s.renderPayoutDocument(ctx, pd, merchant)
		if err != nil {
			return nil, err
		}
		return nil, errorPayoutNotRendered
	}

	req := &proto.CreateSignatureRequest{
		RequestType: documentSignerConst.RequestTypeCreateEmbedded,
		ClientId:    s.cfg.HelloSignPayoutsClientId,
		Title:       s.cfg.HelloSignPayoutsTitle,
		Subject:     s.cfg.HelloSignPayoutsSubject,
		Message:     s.cfg.HelloSignPayoutsMessage,
		Signers: []*proto.CreateSignatureRequestSigner{
			{
				Email:    merchant.GetAuthorizedEmail(),
				Name:     merchant.GetAuthorizedName(),
				RoleName: documentSignerConst.SignerRoleNameMerchant,
			},
			{
				Email:    s.cfg.PaysuperDocumentSignerEmail,
				Name:     s.cfg.PaysuperDocumentSignerName,
				RoleName: documentSignerConst.SignerRoleNamePaysuper,
			},
		},
		Metadata: map[string]string{
			documentSignerConst.MetadataFieldPayoutDocumentId: pd.Id,
		},
		FileUrl: []*proto.CreateSignatureRequestFileUrl{
			{
				Name:    pd.RenderedDocumentFileUrl,
				Storage: documentSignerConst.StorageTypeReport,
			},
		},
	}

	rsp, err := s.documentSigner.CreateSignature(ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "DocumentSignerService"),
			zap.String(errorFieldMethod, "CreateSignature"),
			zap.Any(errorFieldRequest, req),
		)

		return nil, errorPayoutCreateSignature
	}

	if rsp.Status != pkg.ResponseStatusOk {
		err = &grpc.ResponseErrorMessage{
			Code:    rsp.Message.Code,
			Message: rsp.Message.Message,
			Details: rsp.Message.Details,
		}

		return nil, err
	}

	data := &billing.PayoutDocumentSignatureData{
		DetailsUrl:          rsp.Item.DetailsUrl,
		FilesUrl:            rsp.Item.FilesUrl,
		SignatureRequestId:  rsp.Item.SignatureRequestId,
		MerchantSignatureId: rsp.Item.MerchantSignatureId,
		PsSignatureId:       rsp.Item.PsSignatureId,
	}

	return data, nil
}

func (s *Service) changePayoutDocumentSignUrl(
	ctx context.Context,
	signerType int32,
	pd *billing.PayoutDocument,
	ip string,
) (*billing.PayoutDocumentSignatureDataSignUrl, error) {
	var (
		signUrl     *billing.PayoutDocumentSignatureDataSignUrl
		signatureId string
	)

	if signerType == pkg.SignerTypeMerchant {
		signUrl = pd.SignatureData.MerchantSignUrl
		signatureId = pd.SignatureData.MerchantSignatureId
	} else {
		signUrl = pd.SignatureData.PsSignUrl
		signatureId = pd.SignatureData.PsSignatureId
	}

	if signUrl != nil {
		t, err := ptypes.Timestamp(signUrl.ExpiresAt)

		if err != nil {
			zap.L().Error(
				`Merchant sign url contain broken value in "expires_at"" field`,
				zap.Error(err),
				zap.Any("data", pd),
			)

			return nil, err
		}

		if t.After(time.Now()) {
			return signUrl, nil
		}
	}

	req := &proto.GetSignatureUrlRequest{SignatureId: signatureId}
	rsp, err := s.documentSigner.GetSignatureUrl(ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "DocumentSignerService"),
			zap.String(errorFieldMethod, "GetSignatureUrl"),
			zap.Any(errorFieldRequest, req),
		)

		return nil, err
	}

	if rsp.Status != pkg.ResponseStatusOk {
		err = &grpc.ResponseErrorMessage{
			Code:    rsp.Message.Code,
			Message: rsp.Message.Message,
			Details: rsp.Message.Details,
		}

		return nil, err
	}

	signUrl = &billing.PayoutDocumentSignatureDataSignUrl{
		SignUrl:   rsp.Item.SignUrl,
		ExpiresAt: rsp.Item.ExpiresAt,
	}

	var source string

	if signerType == pkg.SignerTypeMerchant {
		pd.SignatureData.MerchantSignUrl = signUrl
		source = payoutChangeSourceMerchant
	} else {
		pd.SignatureData.PsSignUrl = signUrl
		source = payoutChangeSourceAdmin
	}

	err = s.payoutDocument.Update(pd, ip, source)
	if err != nil {
		return nil, err
	}

	return signUrl, nil
}

func (h *PayoutDocument) Insert(pd *billing.PayoutDocument, ip, source string) (err error) {
	err = h.svc.db.Collection(collectionPayoutDocuments).Insert(pd)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, pd),
		)
		return
	}

	err = h.onPayoutDocumentChange(pd, ip, source)
	if err != nil {
		return
	}

	key := fmt.Sprintf(cacheKeyPayoutDocument, pd.Id)
	err = h.svc.cacher.Set(key, pd, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, pd),
		)
	}
	return
}

func (h *PayoutDocument) Update(pd *billing.PayoutDocument, ip, source string) error {
	err := h.svc.db.Collection(collectionPayoutDocuments).UpdateId(bson.ObjectIdHex(pd.Id), pd)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldDocument, pd),
		)

		return err
	}

	err = h.onPayoutDocumentChange(pd, ip, source)
	if err != nil {
		return err
	}

	key := fmt.Sprintf(cacheKeyPayoutDocument, pd.Id)
	err = h.svc.cacher.Set(fmt.Sprintf(cacheKeyPayoutDocument, pd.Id), pd, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, pd),
		)
	}

	return nil
}

func (h *PayoutDocument) onPayoutDocumentChange(document *billing.PayoutDocument, ip, source string) (err error) {
	change := &billing.PayoutDocumentChanges{
		Id:               bson.NewObjectId().Hex(),
		PayoutDocumentId: document.Id,
		Source:           source,
		Ip:               ip,
		CreatedAt:        ptypes.TimestampNow(),
	}

	b, err := json.Marshal(document)
	if err != nil {
		zap.L().Error(
			pkg.ErrorJsonMarshallingFailed,
			zap.Error(err),
			zap.Any("document", document),
		)
		return
	}
	hash := md5.New()
	hash.Write(b)
	change.Hash = hex.EncodeToString(hash.Sum(nil))

	err = h.svc.db.Collection(collectionPayoutDocumentChanges).Insert(change)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, change),
		)
		return
	}

	return
}

func (h *PayoutDocument) GetById(id string) (pd *billing.PayoutDocument, err error) {

	var c billing.PayoutDocument
	key := fmt.Sprintf(cacheKeyPayoutDocument, id)
	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err = h.svc.db.Collection(collectionPayoutDocuments).FindId(bson.ObjectIdHex(id)).One(&pd)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.String(pkg.ErrorDatabaseFieldDocumentId, id),
		)
		return
	}

	err = h.svc.cacher.Set(key, pd, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, pd),
		)
		// suppress error returning here
		err = nil
	}
	return
}

func (h *PayoutDocument) GetAllSourcesIdHex(merchantId, currency string) ([]string, error) {
	ids := []string{}

	pdSourcesQuery := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":            bson.ObjectIdHex(merchantId),
				"currency":               currency,
				"has_merchant_signature": true,
				"has_psp_signature":      true,
				"status":                 bson.M{"$in": payoutDocumentStatusActive},
			},
		},
		{
			"$project": bson.M{
				"_id":       0,
				"source_id": 1,
			},
		},
		{
			"$unwind": "$source_id",
		},
		{
			"$group": bson.M{
				"_id":     nil,
				"sources": bson.M{"$push": "$source_id"},
			},
		},
	}

	res := &sources{}
	err := h.svc.db.Collection(collectionPayoutDocuments).Pipe(pdSourcesQuery).One(res)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPayoutDocuments),
			zap.Any("query", pdSourcesQuery),
		)
		return ids, err
	}

	if res.Items == nil {
		return ids, nil
	}

	return res.Items, nil
}

func (h *PayoutDocument) CountByQuery(query bson.M) (count int, err error) {
	count, err = h.svc.db.Collection(collectionPayoutDocuments).Find(query).Count()
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
	}
	return
}

func (h *PayoutDocument) FindByQuery(query bson.M, sorts []string, limit, offset int) (pds []*billing.PayoutDocument, err error) {
	err = h.svc.db.Collection(collectionPayoutDocuments).
		Find(query).
		Sort(sorts...).
		Limit(limit).
		Skip(offset).
		All(&pds)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
	}
	return
}

func (h *PayoutDocument) GetBalanceAmount(merchantId, currency string) (float64, error) {
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":            bson.ObjectIdHex(merchantId),
				"currency":               currency,
				"has_merchant_signature": true,
				"has_psp_signature":      true,
				"status":                 bson.M{"$in": payoutDocumentStatusActive},
			},
		},
		{
			"$group": bson.M{
				"_id":    "$currency",
				"amount": bson.M{"$sum": "$total_fees"},
			},
		},
	}

	res := &balanceQueryResItem{}

	err := h.svc.db.Collection(collectionPayoutDocuments).Pipe(query).One(&res)
	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPayoutDocuments),
			zap.Any("query", query),
		)
		return 0, err
	}

	return res.Amount, nil
}

func (h *PayoutDocument) GetLast(merchantId, currency string) (pd *billing.PayoutDocument, err error) {
	query := bson.M{
		"merchant_id":            bson.ObjectIdHex(merchantId),
		"currency":               currency,
		"has_merchant_signature": true,
		"has_psp_signature":      true,
		"status":                 bson.M{"$in": payoutDocumentStatusActive},
	}

	sorts := "-created_at"

	err = h.svc.db.Collection(collectionPayoutDocuments).Find(query).Sort(sorts).One(&pd)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
	}

	return
}
