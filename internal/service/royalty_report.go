package service

//1) крон для формирования - 1 раз в неделю (после 18 часов понедельника!)
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
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	errorFieldCollection = "service"
	errorFieldQuery      = "method"

	collectionRoyaltyReport        = "royalty_report"
	collectionRoyaltyReportChanges = "royalty_report_changes"

	royaltyReportErrorNoTransactions        = "no transactions for the period"
	royaltyReportErrorTimezoneIncorrect     = "incorrect time zone"
	royaltyReportErrorEndOfPeriodIsInFuture = "end of royalty report period is in future"
	royaltyReportErrorAlreadyExists         = "report for this merchant and period already exists"
)

var (
	royaltyReportErrorReportNotFound                  = newBillingServerErrorMsg("rr00001", "royalty report with specified identifier not found")
	royaltyReportErrorReportStatusChangeDenied        = newBillingServerErrorMsg("rr00002", "change royalty report to new status denied")
	royaltyReportErrorReportDisputeCorrectionRequired = newBillingServerErrorMsg("rr00003", "for change royalty report status to dispute fields with correction amount and correction reason is required")
	royaltyReportEntryErrorUnknown                    = newBillingServerErrorMsg("rr00004", "unknown error. try request later")
	royaltyReportUpdateBalanceError                   = newBillingServerErrorMsg("rr00005", "update balance failed")

	orderStatusForRoyaltyReports = []string{
		constant.OrderPublicStatusProcessed,
		constant.OrderPublicStatusRefunded,
		constant.OrderPublicStatusChargeback,
	}

	royaltyReportsStatusActive = []string{
		pkg.RoyaltyReportStatusPending,
		pkg.RoyaltyReportStatusAccepted,
		pkg.RoyaltyReportStatusDispute,
	}
)

type RoyaltyReportMerchant struct {
	Id bson.ObjectId `bson:"_id"`
}

type royaltyHandler struct {
	*Service
	from time.Time
	to   time.Time
}

type RoyaltyReportServiceInterface interface {
	GetNonPayoutReports(merchantId, currency string, excludeIdsString []string) ([]*billing.RoyaltyReport, error)
	GetBalanceAmount(merchantId, currency string) (float64, error)
	CheckReportExists(merchantId, currency string, from, to time.Time) (exists bool, err error)
}

func newRoyaltyReport(svc *Service) RoyaltyReportServiceInterface {
	s := &RoyaltyReport{svc: svc}
	return s
}

func (s *Service) CreateRoyaltyReport(
	ctx context.Context,
	req *grpc.CreateRoyaltyReportRequest,
	rsp *grpc.CreateRoyaltyReportRequest,
) error {
	zap.S().Info("start royalty reports processing")

	loc, err := time.LoadLocation(s.cfg.RoyaltyReportTimeZone)

	if err != nil {
		zap.S().Errorf(royaltyReportErrorTimezoneIncorrect, "err", err)
		return errors.New(royaltyReportErrorTimezoneIncorrect)
	}

	to := now.Monday().In(loc).Add(time.Duration(s.cfg.RoyaltyReportPeriodEndHour) * time.Hour)
	if to.After(time.Now().In(loc)) {
		zap.S().Errorf(royaltyReportErrorTimezoneIncorrect)
		return errors.New(royaltyReportErrorEndOfPeriodIsInFuture)
	}

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
		zap.S().Info(royaltyReportErrorNoTransactions)
		return nil
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
			err := handler.createMerchantRoyaltyReport(ctx, merchantId)

			if err == nil {
				rsp.Merchants = append(rsp.Merchants, merchantId.Hex())
			} else {
				zap.S().Error(
					pkg.ErrorRoyaltyReportGenerationFailed,
					zap.Error(err),
					zap.String(pkg.ErrorRoyaltyReportFieldMerchantId, merchantId.Hex()),
					zap.Any(pkg.ErrorRoyaltyReportFieldFrom, from),
					zap.Any(pkg.ErrorRoyaltyReportFieldTo, to),
				)
			}

			wg.Done()
		}(v.Id)
	}
	wg.Wait()

	zap.S().Info("royalty reports processing finished successfully")

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
	}

	var reports []*billing.RoyaltyReport
	err := s.db.Collection(collectionRoyaltyReport).Find(query).All(&reports)
	if err != nil && err != mgo.ErrNotFound {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)
		return err
	}

	for _, report := range reports {
		report.Status = pkg.RoyaltyReportStatusAccepted
		report.AcceptedAt = ptypes.TimestampNow()
		report.UpdatedAt = ptypes.TimestampNow()

		err = s.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)

		if err != nil {
			zap.S().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(errorFieldCollection, collectionRoyaltyReport),
			)
			return err
		}

		s.onRoyaltyReportChange(report, "", pkg.RoyaltyReportChangeSourceAuto)

		_, err = s.updateMerchantBalance(report.MerchantId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ListRoyaltyReports(
	ctx context.Context,
	req *grpc.ListRoyaltyReportsRequest,
	rsp *grpc.ListRoyaltyReportsResponse,
) error {
	rsp.Status = pkg.ResponseStatusOk

	query := bson.M{}

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
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown

		return nil
	}

	if count <= 0 {
		rsp.Data = &grpc.RoyaltyReportsPaginate{}
		return nil
	}

	var reports []*billing.RoyaltyReport
	err = s.db.Collection(collectionRoyaltyReport).Find(query).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&reports)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown

		return nil
	}

	rsp.Data = &grpc.RoyaltyReportsPaginate{
		Count: int32(count),
		Items: reports,
	}

	return nil
}

func (s *Service) MerchantReviewRoyaltyReport(
	ctx context.Context,
	req *grpc.MerchantReviewRoyaltyReportRequest,
	rsp *grpc.ResponseError,
) error {
	var report *billing.RoyaltyReport
	err := s.db.Collection(collectionRoyaltyReport).FindId(bson.ObjectIdHex(req.ReportId)).One(&report)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.S().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(errorFieldCollection, collectionRoyaltyReport),
			)
		}

		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = royaltyReportErrorReportNotFound

		return nil
	}

	if report.Status != pkg.RoyaltyReportStatusPending {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportStatusChangeDenied
		return nil
	}

	if req.IsAccepted == true {
		report.Status = pkg.RoyaltyReportStatusAccepted
		report.AcceptedAt = ptypes.TimestampNow()
	} else {
		report.Status = pkg.RoyaltyReportStatusDispute
	}

	report.UpdatedAt = ptypes.TimestampNow()

	err = s.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown

		return nil
	}

	s.onRoyaltyReportChange(report, req.Ip, pkg.RoyaltyReportChangeSourceMerchant)

	if req.IsAccepted {
		_, err = s.updateMerchantBalance(report.MerchantId)
		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportUpdateBalanceError

			return nil
		}
	}

	rsp.Status = pkg.ResponseStatusOk

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
			zap.S().Error(
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

	if report.Status == pkg.RoyaltyReportStatusDispute && req.Correction != nil && (req.Correction.Reason == "" ||
		req.Correction.Amount == 0) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportDisputeCorrectionRequired

		return nil
	}

	if req.Correction != nil {
		from, err := ptypes.Timestamp(report.PeriodFrom)
		if err != nil {
			zap.L().Error("time conversion error", zap.Error(err))
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}
		to, err := ptypes.Timestamp(report.PeriodTo)
		if err != nil {
			zap.L().Error("time conversion error", zap.Error(err))
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}

		reqAe := &grpc.CreateAccountingEntryRequest{
			MerchantId: report.MerchantId,
			Amount:     req.Correction.Amount,
			Currency:   report.Currency,
			Reason:     req.Correction.Reason,
			Date:       to.Add(-time.Duration(1) * time.Second).Unix(),
			Type:       pkg.AccountingEntryTypeMerchantRoyaltyCorrection,
		}
		resAe := &grpc.CreateAccountingEntryResponse{}
		err = s.CreateAccountingEntry(ctx, reqAe, resAe)
		if err != nil {
			zap.L().Error("create correction accounting entry failed", zap.Error(err))
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}
		if resAe.Status != pkg.ResponseStatusOk {
			zap.L().Error("create correction accounting entry failed")
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}

		if report.Totals == nil {
			report.Totals = &billing.RoyaltyReportTotals{}
		}
		if report.Summary == nil {
			report.Summary = &billing.RoyaltyReportSummary{}
		}

		handler := &royaltyHandler{
			Service: s,
			from:    from,
			to:      to,
		}
		report.Summary.Corrections, report.Totals.CorrectionAmount, err = handler.getRoyaltyReportCorrections(report.MerchantId, report.Currency)
		if err != nil {
			zap.L().Error("get royalty report corrections error", zap.Error(err))
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}
	}

	report.Status = req.Status
	report.UpdatedAt = ptypes.TimestampNow()

	if req.Status == pkg.RoyaltyReportStatusAccepted {
		report.AcceptedAt = ptypes.TimestampNow()
	}

	s.onRoyaltyReportChange(report, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
	err = s.db.Collection(collectionRoyaltyReport).UpdateId(bson.ObjectIdHex(report.Id), report)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown

		return nil
	}

	if req.Status == pkg.RoyaltyReportStatusPending {
		s.sendRoyaltyReportNotification(ctx, report)
	}

	_, err = s.updateMerchantBalance(report.MerchantId)
	if err != nil {
		return err
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) ListRoyaltyReportOrders(
	ctx context.Context,
	req *grpc.ListRoyaltyReportOrdersRequest,
	res *grpc.TransactionsResponse,
) error {

	res.Status = pkg.ResponseStatusOk

	var report *billing.RoyaltyReport
	err := s.db.Collection(collectionRoyaltyReport).FindId(bson.ObjectIdHex(req.ReportId)).One(&report)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = royaltyReportErrorReportNotFound
			return nil
		}

		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionRoyaltyReport),
			zap.Any("report_id", req.ReportId),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = royaltyReportEntryErrorUnknown
		return nil
	}

	from, _ := ptypes.Timestamp(report.PeriodFrom)
	to, _ := ptypes.Timestamp(report.PeriodTo)

	match := bson.M{
		"merchant_id":         bson.ObjectIdHex(report.MerchantId),
		"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
		"status":              bson.M{"$in": orderStatusForRoyaltyReports},
	}

	ts, err := s.orderView.GetTransactionsPublic(match, int(req.Limit), int(req.Offset))
	if err != nil {
		return err
	}

	res.Data = &grpc.TransactionsPaginate{
		Count: int32(len(ts)),
		Items: ts,
	}

	return nil
}

func (s *Service) getRoyaltyReportMerchantsByPeriod(from, to time.Time) []*RoyaltyReportMerchant {
	var merchants []*RoyaltyReportMerchant

	query := []bson.M{
		{
			"$match": bson.M{
				"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
				"status":              bson.M{"$in": orderStatusForRoyaltyReports},
			},
		},
		{"$project": bson.M{"project.merchant_id": true}},
		{"$group": bson.M{"_id": "$project.merchant_id"}},
	}

	err := s.db.Collection(collectionOrderView).Pipe(query).All(&merchants)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrderView),
			zap.Any(errorFieldQuery, query),
		)
	}

	return merchants
}

func (s *Service) onRoyaltyReportChange(reportNew *billing.RoyaltyReport, ip, source string) {
	change := &billing.RoyaltyReportChanges{
		Id:              bson.NewObjectId().Hex(),
		RoyaltyReportId: reportNew.Id,
		Source:          source,
		Ip:              ip,
	}

	b, _ := json.Marshal(reportNew)
	hash := md5.New()
	hash.Write(b)

	change.Hash = hex.EncodeToString(hash.Sum(nil))

	err := s.db.Collection(collectionRoyaltyReportChanges).Insert(change)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReportChanges),
			zap.Any(errorFieldQuery, "insert"),
		)
	}

	return
}

func (h *royaltyHandler) getRoyaltyReportCorrections(merchantId, currency string) (
	entries []*billing.RoyaltyReportCorrectionItem,
	total float64,
	err error) {

	accuntingEntries, err := h.accounting.GetCorrectionsForRoyaltyReport(merchantId, currency, h.from, h.to)
	if err != nil {
		return
	}

	for _, e := range accuntingEntries {
		entries = append(entries, &billing.RoyaltyReportCorrectionItem{
			AccountingEntryId: e.Id,
			Amount:            e.Amount,
			Reason:            e.Reason,
			EntryDate:         e.CreatedAt,
		})
		total += e.Amount
	}

	return
}

func (h *royaltyHandler) getRoyaltyReportRollingReserves(merchantId, currency string) (
	entries []*billing.RoyaltyReportCorrectionItem,
	total float64,
	err error) {

	accuntingEntries, err := h.accounting.GetRollingReservesForRoyaltyReport(merchantId, currency, h.from, h.to)
	if err != nil {
		return
	}

	for _, e := range accuntingEntries {
		entries = append(entries, &billing.RoyaltyReportCorrectionItem{
			AccountingEntryId: e.Id,
			Amount:            e.Amount,
			Reason:            e.Reason,
			EntryDate:         e.CreatedAt,
		})
		total += e.Amount
	}

	return
}

func (h *royaltyHandler) createMerchantRoyaltyReport(ctx context.Context, merchantId bson.ObjectId) error {
	zap.S().Infow("generating royalty report for merchant", "merchantId", merchantId.Hex())

	merchant, err := h.merchant.GetById(merchantId.Hex())
	if err != nil {
		return err
	}

	isExists, err := h.royaltyReport.CheckReportExists(merchant.Id, merchant.GetPayoutCurrency(), h.from, h.to)
	if isExists {
		return errors.New(royaltyReportErrorAlreadyExists)
	}

	summaryItems, summaryTotal, err := h.orderView.GetRoyaltySummary(merchant.Id, merchant.GetPayoutCurrency(), h.from, h.to)
	if err != nil {
		return err
	}

	corrections, correctionsTotal, err := h.getRoyaltyReportCorrections(merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return err
	}

	reserves, reservesTotal, err := h.getRoyaltyReportRollingReserves(merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return err
	}

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchantId.Hex(),
		Currency:   merchant.GetPayoutCurrency(),
		Status:     pkg.RoyaltyReportStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
		Totals: &billing.RoyaltyReportTotals{
			TransactionsCount:    summaryTotal.TotalTransactions,
			FeeAmount:            summaryTotal.TotalFees,
			VatAmount:            summaryTotal.TotalVat,
			PayoutAmount:         summaryTotal.PayoutAmount,
			CorrectionAmount:     correctionsTotal,
			RollingReserveAmount: reservesTotal,
		},
		Summary: &billing.RoyaltyReportSummary{
			ProductsItems:   summaryItems,
			ProductsTotal:   summaryTotal,
			Corrections:     corrections,
			RollingReserves: reserves,
		},
	}

	report.PeriodFrom, err = ptypes.TimestampProto(h.from)
	if err != nil {
		return err
	}

	report.PeriodTo, err = ptypes.TimestampProto(h.to)
	if err != nil {
		return err
	}

	report.AcceptExpireAt, err = ptypes.TimestampProto(time.Now().Add(time.Duration(h.cfg.RoyaltyReportAcceptTimeout) * time.Second))
	if err != nil {
		return err
	}

	err = h.db.Collection(collectionRoyaltyReport).Insert(report)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, report),
		)
		return err
	}

	h.Service.sendRoyaltyReportNotification(ctx, report)

	zap.S().Infow("generating royalty report for merchant finished", "merchantId", merchantId.Hex())

	return nil
}

func (s *Service) sendRoyaltyReportNotification(ctx context.Context, report *billing.RoyaltyReport) {
	merchant, err := s.merchant.GetById(report.MerchantId)

	if err != nil {
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", report.MerchantId))
		return
	}

	if merchant.HasAuthorizedEmail() == true {
		payload := &postmarkSdrPkg.Payload{
			TemplateAlias: s.cfg.EmailNewRoyaltyReportTemplate,
			TemplateModel: map[string]string{
				"merchant_id":       merchant.Id,
				"royalty_report_id": report.Id,
			},
			To: merchant.GetAuthorizedEmail(),
		}

		err := s.broker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

		if err != nil {
			zap.S().Error(
				"Publication message about merchant new royalty report to queue failed",
				zap.Error(err),
				zap.Any("report", report),
			)
		}
	}

	msg := map[string]interface{}{"id": report.Id, "code": "rr00001", "message": pkg.EmailRoyaltyReportMessage}
	err = s.centrifugo.Publish(ctx, fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, report.MerchantId), msg)

	if err != nil {
		zap.S().Error(
			"[Centrifugo] Send merchant notification about new royalty report failed",
			zap.Error(err),
			zap.Any("msg", msg),
		)
	}

	return
}

func (r RoyaltyReport) GetNonPayoutReports(merchantId, currency string, excludeIdsString []string) (result []*billing.RoyaltyReport, err error) {
	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"currency":    currency,
		"status":      bson.M{"$in": royaltyReportsStatusActive},
	}

	if len(excludeIdsString) > 0 {
		excludeIds := []bson.ObjectId{}
		for _, v := range excludeIdsString {
			excludeIds = append(excludeIds, bson.ObjectIdHex(v))
		}

		query["_id"] = bson.M{"$nin": excludeIds}
	}
	sorts := "period_from"
	err = r.svc.db.Collection(collectionRoyaltyReport).Find(query).Sort(sorts).All(&result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
	}

	return

}

func (r RoyaltyReport) GetBalanceAmount(merchantId, currency string) (float64, error) {
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id": bson.ObjectIdHex(merchantId),
				"currency":    currency,
				"status":      pkg.RoyaltyReportStatusAccepted,
			},
		},
		{
			"$group": bson.M{
				"_id":               "currency",
				"payout_amount":     bson.M{"$sum": "$totals.payout_amount"},
				"correction_amount": bson.M{"$sum": "$totals.correction_total_amount"},
			},
		},
		{
			"$project": bson.M{
				"_id":    0,
				"amount": bson.M{"$subtract": []interface{}{"$payout_amount", "$correction_amount"}},
			},
		},
	}

	res := &balanceQueryResItem{}

	err := r.svc.db.Collection(collectionRoyaltyReport).Pipe(query).One(&res)
	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionRoyaltyReport),
			zap.Any("query", query),
		)
		return 0, err
	}

	return res.Amount, nil
}

func (r RoyaltyReport) CheckReportExists(merchantId, currency string, from, to time.Time) (bool, error) {
	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"period_from": bson.M{"$gte": from},
		"period_to":   bson.M{"$lte": to},
		"currency":    currency,
	}
	var report *billing.RoyaltyReport
	err := r.svc.db.Collection(collectionRoyaltyReport).Find(query).One(&report)
	if err == mgo.ErrNotFound || report == nil {
		return false, nil
	}

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionRoyaltyReport),
			zap.Any("query", query),
		)
		return false, err
	}

	return true, nil
}
