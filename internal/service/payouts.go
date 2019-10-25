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
	Insert(document *billing.PayoutDocument, ip, source string) error
	Update(document *billing.PayoutDocument, ip, source string) error
	GetById(id string) (*billing.PayoutDocument, error)
	GetByIdAndMerchant(id, merchantId string) (*billing.PayoutDocument, error)
	CountByQuery(query bson.M) (int, error)
	FindByQuery(query bson.M, sorts []string, limit, offset int) ([]*billing.PayoutDocument, error)
	GetBalanceAmount(merchantId, currency string) (float64, error)
	GetLast(merchantId, currency string) (*billing.PayoutDocument, error)
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

	merchant, err := s.merchant.GetById(req.MerchantId)
	if err != nil {
		return err
	}

	if merchant.ManualPayoutsEnabled == req.IsAutoGeneration == true {
		res.Status = pkg.ResponseStatusBadData
		if req.IsAutoGeneration {
			res.Message = errorPayoutAutoPayoutsDisabled
		} else {
			res.Message = errorPayoutManualPayoutsDisabled
		}
	}

	return s.createPayoutDocument(ctx, merchant, req, res)
}

func (s *Service) createPayoutDocument(
	ctx context.Context,
	merchant *billing.Merchant,
	req *grpc.CreatePayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) error {
	arrivalDate, err := ptypes.TimestampProto(now.EndOfDay().Add(time.Hour * 24 * payoutArrivalInDays))
	if err != nil {
		return err
	}

	pd := &billing.PayoutDocument{
		Id:          bson.NewObjectId().Hex(),
		Status:      pkg.PayoutDocumentStatusPending,
		SourceId:    []string{},
		Description: req.Description,
		CreatedAt:   ptypes.TimestampNow(),
		UpdatedAt:   ptypes.TimestampNow(),
		ArrivalDate: arrivalDate,
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

	err = s.royaltyReport.SetPayoutDocumentId(pd.SourceId, pd.Id, req.Ip, req.Initiator)
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

func (s *Service) GetPayoutDocument(
	ctx context.Context,
	req *grpc.GetPayoutDocumentRequest,
	res *grpc.PayoutDocumentResponse,
) (err error) {
	res.Item, err = s.payoutDocument.GetByIdAndMerchant(req.PayoutDocumentId, req.MerchantId)
	if err != nil {
		if err == mgo.ErrNotFound {
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

	pd, err := s.payoutDocument.GetByIdAndMerchant(req.PayoutDocumentId, req.MerchantId)
	if err != nil {
		if err == mgo.ErrNotFound {
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

	res.Data.Items, err = s.royaltyReport.GetByPayoutId(pd.Id)
	res.Data.Count = int32(len(res.Data.Items))
	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) AutoCreatePayoutDocuments(context.Context, *grpc.EmptyRequest, *grpc.EmptyResponse) error {
	zap.L().Info("start auto-creation of payout documents")

	merchants, err := s.merchant.GetMerchantsWithAutoPayouts()
	if err != nil {
		zap.L().Error("GetMerchantsWithAutoPayouts failed", zap.Error(err))
		return err
	}

	req := &grpc.CreatePayoutDocumentRequest{
		Ip:               "0.0.0.0",
		Initiator:        pkg.RoyaltyReportChangeSourceAuto,
		IsAutoGeneration: true,
	}
	res := &grpc.PayoutDocumentResponse{}
	ctx := context.TODO()

	for _, m := range merchants {
		req.MerchantId = m.Id
		err = s.createPayoutDocument(ctx, m, req, res)
		if err != nil {
			zap.L().Error(
				"auto createPayoutDocument failed with error",
				zap.Error(err),
				zap.String("merchantId", m.Id),
			)
			return err
		}
		if res.Status != pkg.ResponseStatusOk {
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

	query := bson.M{
		"merchant_id": bson.ObjectIdHex(req.MerchantId),
	}

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

	sorts := []string{"-_id"}

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

func (s *Service) PayoutDocumentPdfUploaded(
	ctx context.Context,
	req *grpc.PayoutDocumentPdfUploadedRequest,
	res *grpc.PayoutDocumentPdfUploadedResponse,
) error {
	res.Status = pkg.ResponseStatusOk

	/*pd, err := s.payoutDocument.GetById(req.PayoutId)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}
		return err
	}*/

	if req.Filename != "" {
		// todo: send by email
	}

	return nil
}

func (s *Service) getPayoutDocumentSources(merchant *billing.Merchant) ([]*billing.RoyaltyReport, error) {
	result, err := s.royaltyReport.GetNonPayoutReports(merchant.Id, merchant.GetPayoutCurrency())

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

	return h.updateCaches(pd)
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

	return h.updateCaches(pd)
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

	return pd, h.updateCaches(pd)
}

func (h *PayoutDocument) GetByIdAndMerchant(id, merchantId string) (pd *billing.PayoutDocument, err error) {

	var c billing.PayoutDocument
	key := fmt.Sprintf(cacheKeyPayoutDocumentMerchant, id, merchantId)
	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{
		"_id":         bson.ObjectIdHex(id),
		"merchant_id": bson.ObjectIdHex(merchantId),
	}

	err = h.svc.db.Collection(collectionPayoutDocuments).Find(query).One(&pd)
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
				"merchant_id": bson.ObjectIdHex(merchantId),
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
		"merchant_id": bson.ObjectIdHex(merchantId),
		"currency":    currency,
		"status":      bson.M{"$in": payoutDocumentStatusActive},
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
