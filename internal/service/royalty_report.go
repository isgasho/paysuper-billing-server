package service

//1) крон для формирования - 1 раз в неделю
//2) крон для проверки не пропущена ли дата - каждый день

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
	"sync"
	"time"
)

const (
	errorFieldCollection = "service"
	errorFieldQuery      = "method"

	collectionRoyaltyReport        = "royalty_report"
	collectionRoyaltyReportChanges = "royalty_report_changes"

	royaltyReportErrorCodeReportNotFound                  = "rr00001"
	royaltyReportErrorCodeReportStatusChangeDenied        = "rr00002"
	royaltyReportErrorCodeReportDisputeCorrectionRequired = "rr00003"
	royaltyReportErrorCodeUnknown                         = "rr00004"

	royaltyReportErrorNoTransactions                      = "no transactions for the period"
	royaltyReportErrorTextReportNotFound                  = "royalty report with specified identifier not found"
	royaltyReportErrorTextReportStatusChangeDenied        = "change royalty report to new status denied"
	royaltyReportErrorTextReportDisputeCorrectionRequired = "for change royalty report status to dispute fields with correction amount and correction reason is required"
	royaltyReportErrorTextUnknown                         = "unknown error. try request later"
)

var (
	grossAmountDebitEntities = map[string]bool{
		pkg.AccountingEntryTypePayment:           true,
		pkg.AccountingEntryTypeRefundFailure:     true,
		pkg.AccountingEntryTypeChargebackFailure: true,
		pkg.AccountingEntryTypePayoutFailure:     true,
		pkg.AccountingEntryTypePayoutCancel:      true,
		pkg.AccountingEntryTypeAdjustment:        true,
	}

	grossAmountCreditEntities = map[string]bool{
		pkg.AccountingEntryTypeRefundBody:         true,
		pkg.AccountingEntryTypeReverseTaxFeeDelta: true,
		pkg.AccountingEntryTypeChargeback:         true,
		pkg.AccountingEntryTypeChargebackFee:      true,
		pkg.AccountingEntryTypeChargebackFixedFee: true,
		pkg.AccountingEntryTypePayout:             true,
		pkg.AccountingEntryTypePayoutFee:          true,
	}

	royaltyReportErrorReportNotFound                  = newBillingServerErrorMsg(royaltyReportErrorCodeReportNotFound, royaltyReportErrorTextReportNotFound)
	royaltyReportErrorReportStatusChangeDenied        = newBillingServerErrorMsg(royaltyReportErrorCodeReportStatusChangeDenied, royaltyReportErrorTextReportStatusChangeDenied)
	royaltyReportErrorReportDisputeCorrectionRequired = newBillingServerErrorMsg(royaltyReportErrorCodeReportDisputeCorrectionRequired, royaltyReportErrorTextReportDisputeCorrectionRequired)
	royaltyReportEntryErrorUnknown                    = newBillingServerErrorMsg(royaltyReportErrorCodeUnknown, royaltyReportErrorTextUnknown)
)

type RoyaltyReportMerchant struct {
	Id bson.ObjectId `bson:"_id"`
}

type royaltyHandler struct {
	*Service
	from time.Time
	to   time.Time
}

func (s *Service) CreateRoyaltyReport(
	ctx context.Context,
	req *grpc.CreateRoyaltyReportRequest,
	rsp *grpc.CreateRoyaltyReportRequest,
) error {
	loc, err := time.LoadLocation(s.cfg.RoyaltyReportTimeZone)

	if err != nil {
		return err
	}

	to := now.Monday().In(loc).Add(time.Duration(18) * time.Hour)
	from := to.Add(-time.Duration(s.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	var merchants []*RoyaltyReportMerchant

	if len(req.Merchants) > 0 {
		for _, v := range req.Merchants {
			if bson.IsObjectIdHex(v) == false {
				continue
			}

			merchants = append(merchants, &RoyaltyReportMerchant{Id: bson.ObjectIdHex(v)})
		}
	} else {
		merchants = s.getRoyaltyReportMerchantsByPeriod(from, to)
	}

	if len(merchants) <= 0 {
		return errors.New(royaltyReportErrorNoTransactions)
	}

	var wg sync.WaitGroup
	wg.Add(len(merchants))

	handler := &royaltyHandler{
		Service: s,
		from:    from,
		to:      to,
	}

	for _, v := range merchants {
		go func(merchantId bson.ObjectId) {
			err := handler.processMerchantRoyaltyReport(merchantId)

			if err != nil {
				rsp.Merchants = append(rsp.Merchants, merchantId.Hex())
			}

			wg.Done()
		}(v.Id)
	}
	wg.Wait()

	return nil
}

func (s *Service) AutoAcceptRoyaltyReports(
	ctx context.Context,
	req *grpc.EmptyRequest,
	rsp *grpc.EmptyResponse,
) error {
	tNow := time.Now()
	query := bson.M{
		"accept_expire_at": bson.M{"$lte": tNow},
		"status":           pkg.RoyaltyReportStatusPending,
		"deleted":          false,
	}
	set := bson.M{
		"$set": bson.M{
			"status":           pkg.RoyaltyReportStatusAccepted,
			"accepted_at":      tNow,
			"is_auto_accepted": true,
			"updated_at":       tNow,
		},
	}
	_, err := s.db.Collection(collectionRoyaltyReport).UpdateAll(query, set)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
			zap.Any("set", set),
		)

		return err
	}

	return nil
}

func (s *Service) ListRoyaltyReports(
	ctx context.Context,
	req *grpc.ListRoyaltyReportsRequest,
	rsp *grpc.ListRoyaltyReportsResponse,
) error {
	query := bson.M{"deleted": false}

	if req.Id != "" {
		query["_id"] = bson.ObjectIdHex(req.Id)
	}

	if req.MerchantId != "" {
		query["merchant_id"] = bson.ObjectIdHex(req.MerchantId)
	}

	if req.PeriodFrom != 0 {
		query["period_from"] = bson.M{"$gte": time.Unix(req.PeriodFrom, 0)}
	}

	if req.PeriodTo != 0 {
		query["period_to"] = bson.M{"$gte": time.Unix(req.PeriodFrom, 0)}
	}

	count, err := s.db.Collection(collectionRoyaltyReport).Find(query).Count()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)

		return nil
	}

	if count <= 0 {
		return nil
	}

	err = s.db.Collection(collectionRoyaltyReport).Find(query).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&rsp.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)

		return nil
	}

	rsp.Count = int32(count)

	return nil
}

func (s *Service) ChangeRoyaltyReport(
	ctx context.Context,
	req *grpc.ChangeRoyaltyReportRequest,
	rsp *grpc.ResponseError,
) error {
	var report *billing.RoyaltyReport
	err := s.db.Collection(collectionRoyaltyReport).FindId(bson.ObjectIdHex(req.ReportId)).One(&report)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.L().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(errorFieldCollection, collectionRoyaltyReport),
			)
		}

		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = royaltyReportErrorReportNotFound

		return nil
	}

	if req.Status != "" && report.ChangesAvailable(req.Status) == false {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportStatusChangeDenied

		return nil
	}

	if req.Status == pkg.RoyaltyReportStatusDispute && (req.Correction == nil || req.Correction.Reason == "" ||
		req.Correction.Amount <= 0) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportDisputeCorrectionRequired

		return nil
	}

	reportOld := &billing.RoyaltyReport{
		Id:             report.Id,
		MerchantId:     report.MerchantId,
		CreatedAt:      report.CreatedAt,
		UpdatedAt:      report.UpdatedAt,
		PayoutId:       report.PayoutId,
		PayoutDate:     report.PayoutDate,
		Status:         report.Status,
		Deleted:        report.Deleted,
		PeriodFrom:     report.PeriodFrom,
		PeriodTo:       report.PeriodTo,
		AcceptExpireAt: report.AcceptExpireAt,
		AcceptedAt:     report.AcceptedAt,
		Amounts:        report.Amounts,
		Correction:     report.Correction,
		IsAutoAccepted: report.IsAutoAccepted,
	}

	report.Status = req.Status
	report.Correction = req.Correction
	report.UpdatedAt = ptypes.TimestampNow()

	if req.Status == pkg.RoyaltyReportStatusAccepted {
		report.AcceptedAt = ptypes.TimestampNow()
	}

	s.onRoyaltyReportChange(reportOld, report, req.Ip, req.Source)
	err = s.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown

		return nil
	}

	if req.Status == pkg.RoyaltyReportStatusPending {
		s.sendRoyaltyReportNotification(report)
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) ListRoyaltyReportOrders(
	ctx context.Context,
	req *grpc.ListRoyaltyReportOrdersRequest,
	rsp *grpc.ListRoyaltyReportOrdersResponse,
) error {
	hexReportId := bson.ObjectIdHex(req.ReportId)
	count, err := s.db.Collection(collectionRoyaltyReport).FindId(hexReportId).Count()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)
	}

	if count != 1 {
		return nil
	}

	query := bson.M{"royalty_report_id": req.ReportId}
	count, err = s.db.Collection(collectionOrder).Find(query).Count()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrder),
			zap.Any(errorFieldQuery, query),
		)
	}

	if count <= 0 {
		return nil
	}

	err = s.db.Collection(collectionOrder).Find(query).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&rsp.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrder),
			zap.Any(errorFieldQuery, query),
		)

		return nil
	}

	rsp.Count = int32(count)

	return nil
}

func (s *Service) getRoyaltyReportMerchantsByPeriod(from, to time.Time) []*RoyaltyReportMerchant {
	var merchants []*RoyaltyReportMerchant

	query := []bson.M{
		{
			"$match": bson.M{
				"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
				"status":              constant.OrderPublicStatusProcessed,
			},
		},
		{"$project": bson.M{"project.merchant_id": true}},
		{"$group": bson.M{"_id": "$project.merchant_id"}},
	}

	err := s.db.Collection(collectionOrder).Pipe(query).All(&merchants)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrder),
			zap.Any(errorFieldQuery, query),
		)
	}

	return merchants
}

func (s *Service) onRoyaltyReportChange(reportOld, reportNew *billing.RoyaltyReport, ip, source string) {
	change := &billing.RoyaltyReportChanges{
		Id:              bson.NewObjectId().Hex(),
		RoyaltyReportId: reportNew.Id,
		Source:          source,
		Before:          reportOld,
		After:           reportNew,
		Ip:              ip,
	}

	b, _ := json.Marshal(reportNew)
	hash := md5.New()
	hash.Write(b)

	change.Hash = hex.EncodeToString(hash.Sum(nil))

	err := s.db.Collection(collectionRoyaltyReportChanges).Insert(change)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReportChanges),
			zap.Any(errorFieldQuery, "insert"),
		)
	}

	return
}

func (h *royaltyHandler) processMerchantRoyaltyReport(merchantId bson.ObjectId) error {
	//If the report isn't generated the first time, then mark the previous report as deleted
	query := bson.M{
		"merchant_id": merchantId,
		"period_from": bson.M{"$gte": h.from},
		"period_to":   bson.M{"$lte": h.to},
	}
	update := bson.M{"$set": bson.M{"deleted": true}}
	_, err := h.db.Collection(collectionRoyaltyReport).UpdateAll(query, update)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
			zap.Any("update", update),
		)

		return err
	}

	report, err := h.createMerchantRoyaltyReport(merchantId)

	if err != nil {
		return err
	}

	query = bson.M{
		"project.merchant_id": merchantId,
		"pm_order_close_date": bson.M{"$gte": h.from, "$lte": h.to},
		"status":              constant.OrderPublicStatusProcessed,
	}
	update = bson.M{"$set": bson.M{"royalty_report_id": report.Id}}
	_, err = h.db.Collection(collectionOrder).UpdateAll(query, update)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrder),
			zap.Any(errorFieldQuery, query),
			zap.Any("update", update),
		)

		return err
	}

	return nil
}

func (h *royaltyHandler) createMerchantRoyaltyReport(merchantId bson.ObjectId) (*billing.RoyaltyReport, error) {
	merchant, err := h.merchant.GetById(merchantId.Hex())

	if err != nil {
		zap.L().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", merchantId.Hex()))
		return nil, err
	}

	var entries []*billing.AccountingEntry

	query := bson.M{"merchant_id": merchantId, "created_at": bson.M{"$gte": h.from, "$lte": h.to}}
	err = h.db.Collection(collectionAccountingEntry).Find(query).All(&entries)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.L().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(errorFieldCollection, collectionRoyaltyReport),
				zap.Any(errorFieldQuery, query),
			)
		}

		return nil, err
	}

	var grossDebitAmount, grossCreditAmount, feeAmount, taxFeeAmount float64
	var transactionsCount int64

	for _, v := range entries {
		if _, ok := grossAmountDebitEntities[v.Type]; ok {
			if v.Type == pkg.AccountingEntryTypePayment {
				transactionsCount++
			}

			grossDebitAmount += v.Amount
			continue
		}

		if _, ok := grossAmountCreditEntities[v.Type]; ok {
			grossCreditAmount += v.Amount
			continue
		}

		if v.Type == pkg.AccountingEntryTypeMethodFee || v.Type == pkg.AccountingEntryTypeMethodFixedFee {
			feeAmount += v.Amount
			continue
		}

		if v.Type == pkg.AccountingEntryTypeTaxFee {
			taxFeeAmount += v.Amount
		}
	}

	grossAmount := grossDebitAmount - grossCreditAmount

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchantId.Hex(),
		Deleted:    false,
		Amounts: &billing.RoyaltyReportDetails{
			GrossAmount:       tools.FormatAmount(grossAmount),
			TransactionsCount: transactionsCount,
			FeeAmount:         feeAmount,
			VatAmount:         taxFeeAmount,
			PayoutAmount:      tools.FormatAmount(grossAmount - feeAmount - taxFeeAmount),
			Currency:          merchant.GetPayoutCurrency().CodeA3,
		},
		Status:         pkg.RoyaltyReportStatusNew,
		CreatedAt:      ptypes.TimestampNow(),
		IsAutoAccepted: false,
	}

	report.PeriodFrom, _ = ptypes.TimestampProto(h.from)
	report.PeriodTo, _ = ptypes.TimestampProto(h.to)
	report.AcceptExpireAt, _ = ptypes.TimestampProto(time.Now().Add(time.Duration(h.cfg.RoyaltyReportAcceptTimeout) * time.Second))

	err = h.db.Collection(collectionRoyaltyReport).Insert(report)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, report),
		)

		return nil, err
	}

	h.onRoyaltyReportChange(nil, report, "", pkg.RoyaltyReportChangeSourceAuto)

	return report, nil
}

func (s *Service) sendRoyaltyReportNotification(report *billing.RoyaltyReport) {
	merchant, err := s.merchant.GetById(report.MerchantId)

	if err != nil {
		zap.L().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", report.MerchantId))
		return
	}

	if merchant.HasAuthorizedEmail() == true {
		m := gomail.NewMessage()
		m.SetHeader("Subject", pkg.EmailRoyaltyReportSubject)
		m.SetBody(pkg.EmailContentType, pkg.EmailRoyaltyReportMessage)

		err = s.smtpCl.Send(s.cfg.EmailNotificationSender, []string{merchant.GetAuthorizedEmail()}, m)

		if err != nil {
			zap.L().Error(
				"[SMTP] Send merchant notification about new royalty report failed",
				zap.Error(err),
				zap.String("merchant_id", merchant.Id),
				zap.String("royalty_report_id", report.Id),
			)
		}
	}

	msg := map[string]interface{}{"id": report.Id, "code": "rr00001", "message": pkg.EmailRoyaltyReportMessage}
	b, _ := json.Marshal(msg)

	err = s.centrifugoClient.Publish(context.Background(), fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, report.MerchantId), b)

	if err != nil {
		zap.L().Error(
			"[Centrifugo] Send merchant notification about new royalty report failed",
			zap.Error(err),
			zap.String("merchant_id", merchant.Id),
			zap.String("royalty_report_id", report.Id),
		)

		return
	}

	return
}
