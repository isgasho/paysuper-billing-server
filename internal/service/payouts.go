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
	documentSignerPkg "github.com/paysuper/document-signer/pkg"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
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
	payoutChangeSourceHellosign = "hellosign"

	payoutArrivalInDays = 5
)

var (
	errorPayoutSourcesNotFound  = newBillingServerErrorMsg("po000001", "no source documents found for payout")
	errorPayoutSourcesPending   = newBillingServerErrorMsg("po000002", "you have at least one royalty report waiting for acceptance")
	errorPayoutSourcesDispute   = newBillingServerErrorMsg("po000003", "you have at least one unclosed dispute in your royalty reports")
	errorPayoutNotFound         = newBillingServerErrorMsg("po000004", "payout document not found")
	errorPayoutAmountInvalid    = newBillingServerErrorMsg("po000005", "payout amount is invalid")
	errorPayoutAlreadySigned    = newBillingServerErrorMsg("po000006", "payout already signed for this signer type")
	errorPayoutCreateSignature  = newBillingServerErrorMsg("po000007", "create signature failed")
	errorPayoutUpdateBalance    = newBillingServerErrorMsg("po000008", "balance update failed")
	errorPayoutBalanceError     = newBillingServerErrorMsg("po000009", "getting balance failed")
	errorPayoutNotEnoughBalance = newBillingServerErrorMsg("po000010", "not enough balance for payout")
	errorPayoutNetRevenueFailed = newBillingServerErrorMsg("po000011", "failed to calc net revenue report for payout")
	errorPayoutOrdersFailed     = newBillingServerErrorMsg("po000012", "failed to calc order stat for payout")

	statusForUpdateBalance = map[string]bool{
		pkg.PayoutDocumentStatusPending:    true,
		pkg.PayoutDocumentStatusInProgress: true,
		pkg.PayoutDocumentStatusPaid:       true,
	}
)

type PayoutDocumentServiceInterface interface {
	Insert(document *billing.PayoutDocument, ip, source string) error
	Update(document *billing.PayoutDocument, ip, source string) error
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
		Summary:              &billing.PayoutDocumentSummary{},
	}

	merchant, err := s.merchant.GetById(req.MerchantId)

	if err != nil {
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", req.MerchantId))
		return err
	}

	pd.MerchantId = merchant.Id
	pd.Destination = merchant.Banking

	reports, err := s.getPayoutDocumentSources(merchant)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	pd.Currency = reports[0].Amounts.Currency

	var times []time.Time

	for _, r := range reports {
		pd.Amount += r.Amounts.PayoutAmount
		pd.SourceId = append(pd.SourceId, r.Id)

		from, err := ptypes.Timestamp(r.PeriodFrom)

		if err != nil {
			zap.S().Error(
				"Time conversion error",
				zap.Error(err),
			)
			return err
		}

		to, err := ptypes.Timestamp(r.PeriodTo)
		if err != nil {
			zap.S().Error(
				"Payout source time conversion error",
				zap.Error(err),
			)
			return err
		}
		times = append(times, from)
		times = append(times, to)
	}

	if pd.Amount <= 0 {
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

	if pd.Amount > (balance.Debit - balance.Credit) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutNotEnoughBalance
		return nil
	}

	// We must substract rolling reserve amount from payout amount.
	// In case of rolling reserve release, balance.RollingReserve will have negative amount
	// and finally will be added to payout amount.
	pd.Amount = pd.Amount - balance.RollingReserve

	if pd.Amount < merchant.MinPayoutAmount {
		pd.Status = pkg.PayoutDocumentStatusSkip
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	from := times[0]
	to := times[len(times)-1]

	pd.PeriodFrom, err = ptypes.TimestampProto(from)
	if err != nil {
		zap.S().Error(
			"Payout PeriodFrom time conversion error",
			zap.Error(err),
		)
		return err
	}
	pd.PeriodTo, err = ptypes.TimestampProto(to)
	if err != nil {
		zap.S().Error(
			"Payout PeriodTo time conversion error",
			zap.Error(err),
		)
		return err
	}

	pd.Summary.NetRevenue, err = s.getPayoutDocumentNetRevenue(merchant.Id, from, to)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	pd.Summary.Orders, err = s.getPayoutDocumentOrders(merchant.Id, from, to)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
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

	// todo: send created document to render service

	res.Status = pkg.ResponseStatusOk
	res.Item = pd
	return nil
}

func (s *Service) UpdatePayoutDocumentSignatures(
	ctx context.Context,
	req *grpc.UpdatePayoutDocumentSignaturesRequest,
	res *grpc.PayoutDocumentResponse,
) error {
	var pd *billing.PayoutDocument
	err := s.db.Collection(collectionPayoutDocuments).FindId(bson.ObjectIdHex(req.PayoutDocumentId)).One(&pd)

	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)
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
	var pd *billing.PayoutDocument
	err := s.db.Collection(collectionPayoutDocuments).FindId(bson.ObjectIdHex(req.PayoutDocumentId)).One(&pd)

	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)
		return err
	}

	isChanged := false
	needBalanceUpdate := false

	if req.Status != "" && pd.Status != req.Status {
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
		if req.Status != "" {
			query["status"] = req.Status
		}
		if req.MerchantId != "" {
			query["merchant_id"] = bson.ObjectIdHex(req.MerchantId)
		}
		if req.Signed == true {
			query["has_merchant_signature"] = true
			query["has_psp_signature"] = true
		}
	}

	count, err := s.db.Collection(collectionPayoutDocuments).Find(query).Count()

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return err
	}

	if count == 0 {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorPayoutNotFound
		return nil
	}

	sorts := []string{"_id"}

	var pds []*billing.PayoutDocument
	err = s.db.Collection(collectionPayoutDocuments).
		Find(query).
		Sort(sorts...).
		Limit(int(req.Limit)).
		Skip(int(req.Offset)).
		All(&pds)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

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

	var pd *billing.PayoutDocument
	err := s.db.Collection(collectionPayoutDocuments).FindId(bson.ObjectIdHex(req.PayoutDocumentId)).One(&pd)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPayoutNotFound
			return nil
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)
		return err
	}

	if pd.SignatureData == nil {
		pd.SignatureData, err = s.getPayoutSignature(ctx, pd)
		if err != nil {
			zap.S().Error(
				"Getting signature data failed",
				zap.Error(err),
			)
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

func (s *Service) getPayoutDocumentSources(merchant *billing.Merchant) ([]*billing.RoyaltyReport, error) {
	var result []*billing.RoyaltyReport

	pdSourcesQuery := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":            bson.ObjectIdHex(merchant.Id),
				"currency":               merchant.GetPayoutCurrency(),
				"has_merchant_signature": true,
				"has_psp_signature":      true,
				"status":                 bson.M{"$in": []string{pkg.PayoutDocumentStatusPending, pkg.PayoutDocumentStatusInProgress, pkg.PayoutDocumentStatusPaid}},
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

	var res []*sources
	err := s.db.Collection(collectionPayoutDocuments).Pipe(pdSourcesQuery).All(&res)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrderView),
			zap.Any("query", pdSourcesQuery),
		)
		return nil, err
	}

	royaltyReportsQuery := bson.M{
		"merchant_id":      bson.ObjectIdHex(merchant.Id),
		"amounts.currency": merchant.GetPayoutCurrency(),
		"status":           bson.M{"$in": []string{pkg.RoyaltyReportStatusPending, pkg.RoyaltyReportStatusAccepted, pkg.RoyaltyReportStatusDispute}},
	}

	if len(res) > 0 {
		excludeIds := []bson.ObjectId{}
		for _, v := range res[0].Items {
			excludeIds = append(excludeIds, bson.ObjectIdHex(v))
		}

		royaltyReportsQuery["_id"] = bson.M{"$nin": excludeIds}
	}

	err = s.db.Collection(collectionRoyaltyReport).Find(royaltyReportsQuery).Sort("period_from").All(&result)

	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errorPayoutSourcesNotFound
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, royaltyReportsQuery),
		)
		return nil, err
	}

	if result == nil || len(result) == 0 {
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
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", pd.MerchantId))
		return nil, err
	}

	// todo: check for rendered document exists
	// return error if not

	req := &proto.CreateSignatureRequest{
		TemplateId: s.cfg.HelloSignPayoutTemplate,
		ClientId:   s.cfg.HelloSignClientId,
		Signers: []*proto.CreateSignatureRequestSigner{
			{
				Email:    merchant.GetAuthorizedEmail(),
				Name:     merchant.GetAuthorizedName(),
				RoleName: documentSignerPkg.SignerRoleNameMerchant,
			},
			{
				Email:    s.cfg.PaysuperDocumentSignerEmail,
				Name:     s.cfg.PaysuperDocumentSignerName,
				RoleName: documentSignerPkg.SignerRoleNamePaysuper,
			},
		},
		// todo: pass rendered document
		Metadata: map[string]string{
			documentSignerPkg.MetadataFieldAction:           documentSignerPkg.MetadataFieldActionValueMerchantPayout,
			documentSignerPkg.MetadataFieldPayoutDocumentId: pd.Id,
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

func (s *Service) getPayoutDocumentNetRevenue(merchantId string, from, to time.Time) (*billing.PayoutDocumentNetRevenue, error) {
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":         bson.ObjectIdHex(merchantId),
				"status":              "processed",
				"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
			},
		},
		{
			"$project": bson.M{
				"country": bson.M{
					"$cond": list{
						bson.M{
							"$eq": []string{
								"$billing_address.country",
								"",
							},
						},
						"$user.address.country",
						"$billing_address.country",
					},
				},
				"amount":   "$net_revenue.amount",
				"currency": "$net_revenue.currency",
			},
		},
		{
			"$facet": bson.M{
				"top": []bson.M{
					{
						"$group": bson.M{
							"_id":      "$country",
							"country":  bson.M{"$first": "$country"},
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
				"total": []bson.M{
					{
						"$group": bson.M{
							"_id":      nil,
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": []interface{}{"$total", 0}},
			},
		},
	}

	var res []*billing.PayoutDocumentNetRevenue
	err := s.db.Collection(collectionOrderView).Pipe(query).All(&res)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrderView),
			zap.Any("query", query),
		)
		return nil, err
	}

	if len(res) != 1 {
		return nil, errorPayoutNetRevenueFailed
	}

	return res[0], nil
}

func (s *Service) getPayoutDocumentOrders(merchantId string, from, to time.Time) (*billing.PayoutDocumentOrders, error) {
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":         bson.ObjectIdHex(merchantId),
				"status":              "processed",
				"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
			},
		},
		{
			"$project": bson.M{
				"names": bson.M{
					"$filter": bson.M{
						"input": "$project.name",
						"as":    "name",
						"cond":  bson.M{"$eq": []interface{}{"$$name.lang", "en"}},
					},
				},
				"items": bson.M{
					"$cond": list{
						bson.M{
							"$ne": []string{
								"$items",
								"[]",
							},
						},
						"$items",
						"[]",
					},
				},
			},
		},
		{
			"$unwind": "$items",
		},
		{
			"$project": bson.M{
				"item": bson.M{
					"$cond": list{
						bson.M{
							"$eq": []string{
								"$items",
								"",
							},
						},
						bson.M{"$arrayElemAt": []interface{}{"$names.value", 0}},
						"$items.name",
					},
				},
			},
		},
		{
			"$facet": bson.M{
				"top": []bson.M{
					{
						"$group": bson.M{
							"_id":   "$item",
							"name":  bson.M{"$first": "$item"},
							"count": bson.M{"$sum": 1},
						},
					},
				},
				"total": []bson.M{
					{
						"$group": bson.M{
							"_id":   nil,
							"count": bson.M{"$sum": 1},
						},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": []interface{}{"$total.count", 0}},
			},
		},
	}

	var res []*billing.PayoutDocumentOrders
	err := s.db.Collection(collectionOrder).Pipe(query).All(&res)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrder),
			zap.Any("query", query),
		)
		return nil, err
	}

	if len(res) != 1 {
		return nil, errorPayoutOrdersFailed
	}

	return res[0], nil
}

func (h *PayoutDocument) Insert(pd *billing.PayoutDocument, ip, source string) error {
	err := h.svc.db.Collection(collectionPayoutDocuments).Insert(pd)
	if err != nil {
		return err
	}

	err = h.onPayoutDocumentChange(pd, ip, source)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheKeyPayoutDocument, pd.Id), pd, 0)
	if err != nil {
		return err
	}

	return nil
}

func (h *PayoutDocument) Update(pd *billing.PayoutDocument, ip, source string) error {
	err := h.svc.db.Collection(collectionPayoutDocuments).UpdateId(bson.ObjectIdHex(pd.Id), pd)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, pd),
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
			zap.Any(pkg.ErrorDatabaseFieldQuery, pd),
		)

		return err
	}

	return nil
}

func (h *PayoutDocument) onPayoutDocumentChange(document *billing.PayoutDocument, ip, source string) error {
	change := &billing.PayoutDocumentChanges{
		Id:               bson.NewObjectId().Hex(),
		PayoutDocumentId: document.Id,
		Source:           source,
		Ip:               ip,
		CreatedAt:        ptypes.TimestampNow(),
	}

	b, err := json.Marshal(document)
	if err != nil {
		zap.S().Error(
			pkg.ErrorJsonMarshallingFailed,
			zap.Error(err),
			zap.Any("document", document),
		)
		return err
	}
	hash := md5.New()
	hash.Write(b)
	change.Hash = hex.EncodeToString(hash.Sum(nil))

	err = h.svc.db.Collection(collectionPayoutDocumentChanges).Insert(change)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionPayoutDocumentChanges),
			zap.Any(errorFieldQuery, "insert"),
		)
		return err
	}

	return nil
}
