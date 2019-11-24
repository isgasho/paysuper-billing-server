package service

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	reporterConst "github.com/paysuper/paysuper-reporter/pkg"
	reporterProto "github.com/paysuper/paysuper-reporter/pkg/proto"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"mime"
	"path/filepath"
	"sort"
	"time"
)

const (
	collectionPayoutDocuments       = "payout_documents"
	collectionPayoutDocumentChanges = "payout_documents_changes"

	cacheKeyPayoutDocument         = "payout_document:id:%s"
	cacheKeyPayoutDocumentMerchant = "payout_document:id:%s:merchant:id:%s"

	payoutChangeSourceMerchant = "merchant"
	payoutChangeSourceAdmin    = "admin"

	payoutArrivalInDays = 5
)

var (
	errorPayoutSourcesNotFound         = newBillingServerErrorMsg("po000001", "no source documents found for payout")
	errorPayoutSourcesPending          = newBillingServerErrorMsg("po000002", "you have at least one royalty report waiting for acceptance")
	errorPayoutSourcesDispute          = newBillingServerErrorMsg("po000003", "you have at least one unclosed dispute in your royalty reports")
	errorPayoutNotFound                = newBillingServerErrorMsg("po000004", "payout document not found")
	errorPayoutAmountInvalid           = newBillingServerErrorMsg("po000005", "payout amount is invalid")
	errorPayoutUpdateBalance           = newBillingServerErrorMsg("po000008", "balance update failed")
	errorPayoutBalanceError            = newBillingServerErrorMsg("po000009", "getting balance failed")
	errorPayoutNotEnoughBalance        = newBillingServerErrorMsg("po000010", "not enough balance for payout")
	errorPayoutUpdateRoyaltyReports    = newBillingServerErrorMsg("po000012", "royalty reports update failed")
	errorPayoutStatusChangeIsForbidden = newBillingServerErrorMsg("po000014", "status change is forbidden")
	errorPayoutManualPayoutsDisabled   = newBillingServerErrorMsg("po000015", "manual payouts disabled")
	errorPayoutAutoPayoutsDisabled     = newBillingServerErrorMsg("po000016", "auto payouts disabled")

	statusForUpdateBalance = map[string]bool{
		pkg.PayoutDocumentStatusPending: true,
		pkg.PayoutDocumentStatusPaid:    true,
	}

	statusForBecomePaid = map[string]bool{
		pkg.PayoutDocumentStatusPaid: true,
	}

	statusForBecomeFailed = map[string]bool{
		pkg.PayoutDocumentStatusFailed:   true,
		pkg.PayoutDocumentStatusCanceled: true,
	}

	payoutDocumentStatusActive = []string{
		pkg.PayoutDocumentStatusPending,
		pkg.PayoutDocumentStatusPaid,
	}
)

type PayoutDocumentServiceInterface interface {
	Insert(ctx context.Context, document *billing.PayoutDocument, ip, source string) error
	Update(ctx context.Context, document *billing.PayoutDocument, ip, source string) error
	GetById(ctx context.Context, id string) (*billing.PayoutDocument, error)
	GetByIdAndMerchant(ctx context.Context, id, merchantId string) (*billing.PayoutDocument, error)
	CountByQuery(ctx context.Context, query bson.M) (int64, error)
	FindByQuery(ctx context.Context, query bson.M, sorts []string, limit, offset int64) ([]*billing.PayoutDocument, error)
	GetBalanceAmount(ctx context.Context, merchantId, currency string) (float64, error)
	GetLast(ctx context.Context, merchantId, currency string) (*billing.PayoutDocument, error)
}

func newPayoutService(svc *Service) PayoutDocumentServiceInterface {
	s := &PayoutDocument{svc: svc}
	return s
}

func (s *Service) CreatePayoutDocument(
	ctx context.Context,
	req *grpc.CreatePayoutDocumentRequest,
	res *grpc.CreatePayoutDocumentResponse,
) error {

	merchant, err := s.merchant.GetById(ctx, req.MerchantId)
	if err != nil {
		return err
	}

	if merchant.ManualPayoutsEnabled == req.IsAutoGeneration {
		res.Status = pkg.ResponseStatusBadData
		if req.IsAutoGeneration {
			res.Message = errorPayoutAutoPayoutsDisabled
		} else {
			res.Message = errorPayoutManualPayoutsDisabled
		}
		return nil
	}

	return s.createPayoutDocument(ctx, merchant, req, res)
}

func (s *Service) createPayoutDocument(
	ctx context.Context,
	merchant *billing.Merchant,
	req *grpc.CreatePayoutDocumentRequest,
	res *grpc.CreatePayoutDocumentResponse,
) error {
	operatingCompaniesIds, err := s.royaltyReport.GetNonPayoutReportsOperatingCompaniesIds(
		ctx,
		merchant.Id,
		merchant.GetPayoutCurrency(),
	)

	if err != nil {
		return err
	}

	if len(operatingCompaniesIds) == 0 {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutSourcesNotFound
		return nil
	}

	arrivalDate, err := ptypes.TimestampProto(now.EndOfDay().Add(time.Hour * 24 * payoutArrivalInDays))
	if err != nil {
		return err
	}

	for _, operatingCompanyId := range operatingCompaniesIds {

		pd := &billing.PayoutDocument{
			Id:                 primitive.NewObjectID().Hex(),
			Status:             pkg.PayoutDocumentStatusPending,
			SourceId:           []string{},
			Description:        req.Description,
			CreatedAt:          ptypes.TimestampNow(),
			UpdatedAt:          ptypes.TimestampNow(),
			ArrivalDate:        arrivalDate,
			OperatingCompanyId: operatingCompanyId,
		}

		pd.MerchantId = merchant.Id
		pd.Destination = merchant.Banking
		pd.Company = merchant.Company
		pd.MerchantAgreementNumber = merchant.AgreementNumber

		reports, err := s.getPayoutDocumentSources(ctx, merchant, operatingCompanyId)

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

		balance, err := s.getMerchantBalance(ctx, merchant.Id)
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

		err = s.payoutDocument.Insert(ctx, pd, req.Ip, payoutChangeSourceMerchant)

		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = e
				return nil
			}
			return err
		}

		err = s.royaltyReport.SetPayoutDocumentId(ctx, pd.SourceId, pd.Id, req.Ip, req.Initiator)

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

		res.Items = append(res.Items, pd)
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetPayoutDocument(
	ctx context.Context,
	req *grpc.GetPayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) (err error) {
	res.Item, err = s.payoutDocument.GetByIdAndMerchant(ctx, req.PayoutDocumentId, req.MerchantId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetPayoutDocumentRoyaltyReports(
	ctx context.Context,
	req *grpc.GetPayoutDocumentRequest,
	res *grpc.ListRoyaltyReportsResponse,
) error {
	pd, err := s.payoutDocument.GetByIdAndMerchant(ctx, req.PayoutDocumentId, req.MerchantId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Data.Items, err = s.royaltyReport.GetByPayoutId(ctx, pd.Id)
	res.Data.Count = int64(len(res.Data.Items))
	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) AutoCreatePayoutDocuments(
	ctx context.Context,
	req *grpc.EmptyRequest,
	rsp *grpc.EmptyResponse,
) error {
	zap.L().Info("start auto-creation of payout documents")

	merchants, err := s.merchant.GetMerchantsWithAutoPayouts(ctx)
	if err != nil {
		zap.L().Error("GetMerchantsWithAutoPayouts failed", zap.Error(err))
		return err
	}

	req1 := &grpc.CreatePayoutDocumentRequest{
		Ip:               "0.0.0.0",
		Initiator:        pkg.RoyaltyReportChangeSourceAuto,
		IsAutoGeneration: true,
	}
	res := &grpc.CreatePayoutDocumentResponse{}

	for _, m := range merchants {
		req1.MerchantId = m.Id
		err = s.createPayoutDocument(ctx, m, req1, res)

		if err != nil {
			if err == errorPayoutSourcesNotFound {
				continue
			}
			zap.L().Error(
				"auto createPayoutDocument failed with error",
				zap.Error(err),
				zap.String("merchantId", m.Id),
			)
			return err
		}
		if res.Status != pkg.ResponseStatusOk {
			if res.Message == errorPayoutAmountInvalid {
				continue
			}
			zap.L().Error(
				"auto createPayoutDocument failed in response",
				zap.Int32("code", res.Status),
				zap.Any("message", res.Message),
				zap.String("merchantId", m.Id),
			)
			return err
		}
	}

	zap.L().Info("auto-creation of payout documents finished")

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
		SendNotification: merchant.ManualPayoutsEnabled,
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

func (s *Service) UpdatePayoutDocument(
	ctx context.Context,
	req *grpc.UpdatePayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) error {
	pd, err := s.payoutDocument.GetById(ctx, req.PayoutDocumentId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	isChanged := false
	needBalanceUpdate := false

	_, isReqStatusForBecomePaid := statusForBecomePaid[req.Status]
	becomePaid := isReqStatusForBecomePaid && pd.Status != req.Status

	_, isReqStatusForBecomeFailed := statusForBecomeFailed[req.Status]
	_, isPayoutStatusForBecomeFailed := statusForBecomeFailed[pd.Status]
	becomeFailed := isReqStatusForBecomeFailed && !isPayoutStatusForBecomeFailed

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
		err = s.payoutDocument.Update(ctx, pd, req.Ip, payoutChangeSourceAdmin)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = e
				return nil
			}
			return err
		}

		if becomePaid == true {
			err = s.royaltyReport.SetPaid(ctx, pd.SourceId, pd.Id, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
			if err != nil {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = errorPayoutUpdateRoyaltyReports

				return nil
			}

		} else {
			if becomeFailed == true {
				err = s.royaltyReport.UnsetPaid(ctx, pd.SourceId, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
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
		_, err = s.updateMerchantBalance(ctx, pd.MerchantId)
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

	merchantOid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	query := bson.M{"merchant_id": merchantOid}

	if len(req.Status) > 0 {
		query["status"] = bson.M{"$in": req.Status}
	}

	if req.PeriodFrom > 0 || req.PeriodTo > 0 {
		date := bson.M{}
		if req.PeriodFrom > 0 {
			date["$gte"] = time.Unix(req.PeriodFrom, 0)
		}
		if req.PeriodTo > 0 {
			date["$lte"] = time.Unix(req.PeriodTo, 0)
		}
		query["created_at"] = date
	}

	count, err := s.payoutDocument.CountByQuery(ctx, query)

	if err != nil && err != mongo.ErrNoDocuments {
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

	sorts := []string{"-_id"}
	pds, err := s.payoutDocument.FindByQuery(ctx, query, sorts, req.Limit, req.Offset)

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

func (s *Service) PayoutDocumentPdfUploaded(
	ctx context.Context,
	req *grpc.PayoutDocumentPdfUploadedRequest,
	res *grpc.PayoutDocumentPdfUploadedResponse,
) error {
	pd, err := s.payoutDocument.GetById(ctx, req.PayoutId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}

	merchant, err := s.merchant.GetById(ctx, pd.MerchantId)

	if err != nil {

		return err
	}

	if merchant.HasAuthorizedEmail() == false {
		zap.L().Warn("Merchant has no authorized email", zap.String("merchant_id", merchant.Id))
		res.Status = pkg.ResponseStatusOk
		return nil
	}

	content := base64.StdEncoding.EncodeToString(req.Content)
	contentType := mime.TypeByExtension(filepath.Ext(req.Filename))

	periodFrom, err := ptypes.Timestamp(pd.PeriodFrom)
	if err != nil {
		zap.L().Error(
			pkg.ErrorTimeConversion,
			zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
			zap.Any(pkg.ErrorTimeConversionValue, pd.PeriodFrom),
			zap.Error(err),
		)
		return err
	}
	periodTo, err := ptypes.Timestamp(pd.PeriodTo)
	if err != nil {
		zap.L().Error(
			pkg.ErrorTimeConversion,
			zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
			zap.Any(pkg.ErrorTimeConversionValue, pd.PeriodTo),
			zap.Error(err),
		)
		return err
	}

	operatingCompany, err := s.operatingCompany.GetById(ctx, pd.OperatingCompanyId)

	if err != nil {
		zap.L().Error("Operating company not found", zap.Error(err), zap.String("operating_company_id", pd.OperatingCompanyId))
		return err
	}

	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: s.cfg.EmailNewRoyaltyReportTemplate,
		TemplateModel: map[string]string{
			"merchant_id":            merchant.Id,
			"payout_id":              pd.Id,
			"period_from":            periodFrom.Format("2006-01-02"),
			"period_to":              periodTo.Format("2006-01-02"),
			"license_agreement":      merchant.AgreementNumber,
			"status":                 pd.Status,
			"merchant_greeting":      merchant.GetAuthorizedName(),
			"payouts_url":            s.cfg.PayoutsUrl,
			"operating_company_name": operatingCompany.Name,
		},
		To: merchant.GetAuthorizedEmail(),
		Attachments: []*postmarkSdrPkg.PayloadAttachment{
			{
				Name:        req.Filename,
				Content:     content,
				ContentType: contentType,
			},
		},
	}

	err = s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

	if err != nil {
		zap.L().Error(
			"Publication message about merchant new payout document to queue failed",
			zap.Error(err),
			zap.Any("payout document", pd),
		)
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) getPayoutDocumentSources(
	ctx context.Context,
	merchant *billing.Merchant,
	operatingCompanyId string,
) ([]*billing.RoyaltyReport, error) {
	result, err := s.royaltyReport.GetNonPayoutReports(ctx, merchant.Id, operatingCompanyId, merchant.GetPayoutCurrency())

	if err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}

	if err == mongo.ErrNoDocuments || result == nil || len(result) == 0 {
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

func (h *PayoutDocument) Insert(ctx context.Context, pd *billing.PayoutDocument, ip, source string) (err error) {
	_, err = h.svc.db.Collection(collectionPayoutDocuments).InsertOne(ctx, pd)

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

	err = h.onPayoutDocumentChange(ctx, pd, ip, source)
	if err != nil {
		return
	}

	return h.updateCaches(pd)
}

func (h *PayoutDocument) Update(ctx context.Context, pd *billing.PayoutDocument, ip, source string) error {
	oid, _ := primitive.ObjectIDFromHex(pd.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPayoutDocuments).ReplaceOne(ctx, filter, pd)

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

	err = h.onPayoutDocumentChange(ctx, pd, ip, source)
	if err != nil {
		return err
	}

	return h.updateCaches(pd)
}

func (h *PayoutDocument) onPayoutDocumentChange(
	ctx context.Context,
	document *billing.PayoutDocument,
	ip, source string,
) (err error) {
	change := &billing.PayoutDocumentChanges{
		Id:               primitive.NewObjectID().Hex(),
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

	_, err = h.svc.db.Collection(collectionPayoutDocumentChanges).InsertOne(ctx, change)

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

func (h *PayoutDocument) GetById(ctx context.Context, id string) (pd *billing.PayoutDocument, err error) {
	var c billing.PayoutDocument
	key := fmt.Sprintf(cacheKeyPayoutDocument, id)
	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err = h.svc.db.Collection(collectionPayoutDocuments).FindOne(ctx, filter).Decode(&pd)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.String(pkg.ErrorDatabaseFieldDocumentId, id),
		)
		return
	}

	return pd, h.updateCaches(pd)
}

func (h *PayoutDocument) GetByIdAndMerchant(
	ctx context.Context,
	id, merchantId string,
) (pd *billing.PayoutDocument, err error) {
	var c billing.PayoutDocument
	key := fmt.Sprintf(cacheKeyPayoutDocumentMerchant, id, merchantId)
	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"_id": oid, "merchant_id": merchantOid}

	err = h.svc.db.Collection(collectionPayoutDocuments).FindOne(ctx, query).Decode(&pd)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	return pd, h.updateCaches(pd)
}

func (h *PayoutDocument) CountByQuery(ctx context.Context, query bson.M) (count int64, err error) {
	count, err = h.svc.db.Collection(collectionPayoutDocuments).CountDocuments(ctx, query)
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

func (h *PayoutDocument) FindByQuery(
	ctx context.Context,
	query bson.M,
	sorts []string,
	limit, offset int64,
) ([]*billing.PayoutDocument, error) {
	opts := options.Find().
		SetSort(mongodb.ToSortOption(sorts)).
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := h.svc.db.Collection(collectionPayoutDocuments).Find(ctx, query, opts)

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
		return nil, err
	}

	var pds []*billing.PayoutDocument
	err = cursor.All(ctx, &pds)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return nil, err
	}

	return pds, nil
}

func (h *PayoutDocument) GetBalanceAmount(ctx context.Context, merchantId, currency string) (float64, error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id": oid,
				"currency":    currency,
				"status":      bson.M{"$in": payoutDocumentStatusActive},
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
	cursor, err := h.svc.db.Collection(collectionPayoutDocuments).Aggregate(ctx, query)

	if err != nil {
		if err != mongo.ErrNoDocuments {
			zap.L().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
		}
		return 0, err
	}

	for cursor.Next(ctx) {
		err = cursor.Decode(&res)
		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
			return 0, err
		}
	}
	_ = cursor.Close(ctx)

	return res.Amount, nil
}

func (h *PayoutDocument) GetLast(
	ctx context.Context,
	merchantId, currency string,
) (pd *billing.PayoutDocument, err error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id": oid,
		"currency":    currency,
		"status":      bson.M{"$in": payoutDocumentStatusActive},
	}

	sorts := bson.M{"created_at": -1}
	opts := options.FindOne().SetSort(sorts)
	err = h.svc.db.Collection(collectionPayoutDocuments).FindOne(ctx, query, opts).Decode(&pd)

	if err != nil && err != mongo.ErrNoDocuments {
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

func (h *PayoutDocument) updateCaches(pd *billing.PayoutDocument) (err error) {
	key1 := fmt.Sprintf(cacheKeyPayoutDocument, pd.Id)
	key2 := fmt.Sprintf(cacheKeyPayoutDocumentMerchant, pd.Id, pd.MerchantId)

	err = h.svc.cacher.Set(key1, pd, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key1),
			zap.Any(pkg.ErrorCacheFieldData, pd),
		)
		return
	}

	err = h.svc.cacher.Set(key2, pd, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key2),
			zap.Any(pkg.ErrorCacheFieldData, pd),
		)
	}
	return
}
