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
	errorPayoutNoSources           = newBillingServerErrorMsg("po000001", "no source documents passed for payout")
	errorPayoutSourcesNotFound     = newBillingServerErrorMsg("po000002", "no source documents found for payout")
	errorPayoutNotFound            = newBillingServerErrorMsg("po000003", "payout document not found")
	errorPayoutSourcesInconsistent = newBillingServerErrorMsg("po000004", "sources have inconsistent currencies")
	errorPayoutAmountInvalid       = newBillingServerErrorMsg("po000005", "payout amount is invalid")
	errorPayoutInvalid             = newBillingServerErrorMsg("po000006", "requested payout is invalid")
	errorPayoutAlreadySigned       = newBillingServerErrorMsg("po000007", "payout already signed for this signer type")
	errorCreateSignature           = newBillingServerErrorMsg("po000008", "create signature failed")
)

type PayoutDocumentServiceInterface interface {
	Insert(document *billing.PayoutDocument, ip, source string) error
	Update(document *billing.PayoutDocument, ip, source string) error
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

	if len(req.SourceId) == 0 {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutNoSources
		return nil
	}

	merchant, err := s.merchant.GetById(req.MerchantId)

	if err != nil {
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", req.MerchantId))
		return err
	}

	pd.MerchantId = merchant.Id
	pd.Destination = merchant.Banking

	reports, err := s.getPayoutDocumentSources(req.SourceId, req.MerchantId)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	pd.Currency = reports[0].Amounts.Currency

	for _, r := range reports {
		if pd.Currency != r.Amounts.Currency {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPayoutSourcesInconsistent
			return nil
		}
		pd.Amount += r.Amounts.PayoutAmount
		pd.SourceId = append(pd.SourceId, r.Id)
	}

	if pd.Amount <= 0 {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutAmountInvalid
		return nil
	}

	if pd.Amount < merchant.MinPayoutAmount {
		pd.Status = pkg.PayoutDocumentStatusSkip
	}

	pd.SignatureData, err = s.getPayoutSignature(ctx, merchant, pd)
	if err != nil {
		zap.S().Error(
			"Getting signature data failed",
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

	if req.Status != "" && pd.Status != req.Status {
		isChanged = true
		pd.Status = req.Status
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
			query["merchant_id"] = req.MerchantId
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

	sort := []string{"_id"}

	var pds []*billing.PayoutDocument
	err = s.db.Collection(collectionPayoutDocuments).
		Find(query).
		Sort(sort...).
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

	var pd *billing.PayoutDocument

	err := s.db.Collection(collectionPayoutDocuments).Find(bson.M{"_id": bson.ObjectIdHex(req.PayoutDocumentId)}).One(&pd)

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
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPayoutInvalid

		return nil
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

	data, err := s.changePayoutDocumentSignUrl(ctx, req.SignerType, pd, req.Ip)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = data

	return nil
}

func (s *Service) getPayoutDocumentSources(sources []string, merchant_id string) ([]*billing.RoyaltyReport, error) {
	var result []*billing.RoyaltyReport

	var sourcesIdHex []bson.ObjectId

	for _, v := range sources {
		sourcesIdHex = append(sourcesIdHex, bson.ObjectIdHex(v))
	}

	query := bson.M{
		"_id":         bson.M{"$in": sourcesIdHex},
		"merchant_id": bson.ObjectIdHex(merchant_id),
		"status":      pkg.RoyaltyReportStatusAccepted,
	}

	err := s.db.Collection(collectionRoyaltyReport).Find(query).All(&result)

	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errorPayoutSourcesNotFound
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)
		return nil, err
	}

	if result == nil || len(result) == 0 {
		return nil, errorPayoutSourcesNotFound
	}

	return result, nil
}

func (s *Service) getPayoutSignature(
	ctx context.Context,
	merchant *billing.Merchant,
	pd *billing.PayoutDocument,
) (*billing.PayoutDocumentSignatureData, error) {
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

		return nil, errorCreateSignature
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
