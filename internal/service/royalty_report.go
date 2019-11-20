package service

//1) крон для формирования - 1 раз в неделю (после 18 часов понедельника!)
//2) крон для проверки не пропущена ли дата - каждый день

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
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	reporterConst "github.com/paysuper/paysuper-reporter/pkg"
	reporterProto "github.com/paysuper/paysuper-reporter/pkg/proto"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"mime"
	"path/filepath"
	"sync"
	"time"
)

const (
	collectionRoyaltyReport        = "royalty_report"
	collectionRoyaltyReportChanges = "royalty_report_changes"

	cacheKeyRoyaltyReport = "royalty_report:id:%s"
)

var (
	royaltyReportErrorNoTransactions = "no transactions for the period"

	royaltyReportErrorReportNotFound           = newBillingServerErrorMsg("rr00001", "royalty report with specified identifier not found")
	royaltyReportErrorReportStatusChangeDenied = newBillingServerErrorMsg("rr00002", "change royalty report to new status denied")
	royaltyReportErrorCorrectionReasonRequired = newBillingServerErrorMsg("rr00003", "correction reason required")
	royaltyReportEntryErrorUnknown             = newBillingServerErrorMsg("rr00004", "unknown error. try request later")
	royaltyReportUpdateBalanceError            = newBillingServerErrorMsg("rr00005", "update balance failed")
	royaltyReportErrorEndOfPeriodIsInFuture    = newBillingServerErrorMsg("rr00006", "end of royalty report period is in future")
	royaltyReportErrorTimezoneIncorrect        = newBillingServerErrorMsg("rr00007", "incorrect time zone")
	royaltyReportErrorAlreadyExists            = newBillingServerErrorMsg("rr00008", "report for this merchant and period already exists")
	royaltyReportErrorCorrectionAmountRequired = newBillingServerErrorMsg("rr00009", "correction amount required and must be not zero")
	royaltyReportErrorPayoutDocumentIdInvalid  = newBillingServerErrorMsg("rr00010", "payout document id is invalid")

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

	royaltyReportsStatusForBalance = []string{
		pkg.RoyaltyReportStatusAccepted,
		pkg.RoyaltyReportStatusWaitForPayment,
		pkg.RoyaltyReportStatusPaid,
	}
)

type RoyaltyReportMerchant struct {
	Id primitive.ObjectID `bson:"_id"`
}

type royaltyHandler struct {
	*Service
	from time.Time
	to   time.Time
}

type RoyaltyReportServiceInterface interface {
	Insert(ctx context.Context, document *billing.RoyaltyReport, ip, source string) error
	Update(ctx context.Context, document *billing.RoyaltyReport, ip, source string) error
	GetById(ctx context.Context, id string) (*billing.RoyaltyReport, error)
	GetNonPayoutReports(ctx context.Context, merchantId, operatingCompanyId, currency string) ([]*billing.RoyaltyReport, error)
	GetNonPayoutReportsOperatingCompaniesIds(ctx context.Context, merchantId, currency string) ([]string, error)
	GetByPayoutId(ctx context.Context, payoutId string) ([]*billing.RoyaltyReport, error)
	GetBalanceAmount(ctx context.Context, merchantId, currency string) (float64, error)
	CheckReportExists(ctx context.Context, merchantId, operatingCompanyId, currency string, from, to time.Time) (exists bool, err error)
	SetPayoutDocumentId(ctx context.Context, reportIds []string, payoutDocumentId, ip, source string) (err error)
	UnsetPayoutDocumentId(ctx context.Context, reportIds []string, ip, source string) (err error)
	SetPaid(ctx context.Context, reportIds []string, payoutDocumentId, ip, source string) (err error)
	UnsetPaid(ctx context.Context, reportIds []string, ip, source string) (err error)
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
	zap.L().Info("start royalty reports processing")

	loc, err := time.LoadLocation(s.cfg.RoyaltyReportTimeZone)

	if err != nil {
		zap.L().Error(royaltyReportErrorTimezoneIncorrect.Error(), zap.Error(err))
		return royaltyReportErrorTimezoneIncorrect
	}

	to := now.Monday().In(loc).Add(time.Duration(s.cfg.RoyaltyReportPeriodEndHour) * time.Hour)
	if to.After(time.Now().In(loc)) {
		return royaltyReportErrorEndOfPeriodIsInFuture
	}

	from := to.Add(-time.Duration(s.cfg.RoyaltyReportPeriod) * time.Second).In(loc)

	var merchants []*RoyaltyReportMerchant

	if len(req.Merchants) > 0 {
		for _, v := range req.Merchants {
			oid, err := primitive.ObjectIDFromHex(v)

			if err != nil {
				continue
			}

			merchants = append(merchants, &RoyaltyReportMerchant{Id: oid})
		}
	} else {
		merchants = s.getRoyaltyReportMerchantsByPeriod(ctx, from, to)
	}

	if len(merchants) <= 0 {
		zap.L().Info(royaltyReportErrorNoTransactions)
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
		go func(merchantId primitive.ObjectID) {
			err := handler.createMerchantRoyaltyReport(ctx, merchantId)

			if err == nil {
				rsp.Merchants = append(rsp.Merchants, merchantId.Hex())
			} else {
				zap.L().Error(
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

	zap.L().Info("royalty reports processing finished successfully")

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
	cursor, err := s.db.Collection(collectionRoyaltyReport).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return err
	}

	err = cursor.All(ctx, &reports)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return err
	}

	for _, report := range reports {
		report.Status = pkg.RoyaltyReportStatusAccepted
		report.AcceptedAt = ptypes.TimestampNow()
		report.UpdatedAt = ptypes.TimestampNow()
		report.IsAutoAccepted = true

		err = s.royaltyReport.Update(ctx, report, "", pkg.RoyaltyReportChangeSourceAuto)
		if err != nil {
			return err
		}

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

	if req.MerchantId != "" {
		query["merchant_id"], _ = primitive.ObjectIDFromHex(req.MerchantId)
	}

	if len(req.Status) > 0 {
		query["status"] = bson.M{"$in": req.Status}
	}

	if req.PeriodFrom != 0 {
		query["period_from"] = bson.M{"$gte": time.Unix(req.PeriodFrom, 0)}
	}

	if req.PeriodTo != 0 {
		query["period_to"] = bson.M{"$gte": time.Unix(req.PeriodFrom, 0)}
	}

	count, err := s.db.Collection(collectionRoyaltyReport).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
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

	opts := options.Find().
		SetLimit(req.Limit).
		SetSkip(req.Offset)
	cursor, err := s.db.Collection(collectionRoyaltyReport).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown
		return nil
	}

	err = cursor.All(ctx, &reports)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = royaltyReportEntryErrorUnknown
		return nil
	}

	rsp.Data = &grpc.RoyaltyReportsPaginate{
		Count: count,
		Items: reports,
	}

	return nil
}

func (s *Service) MerchantReviewRoyaltyReport(
	ctx context.Context,
	req *grpc.MerchantReviewRoyaltyReportRequest,
	rsp *grpc.ResponseError,
) error {
	report, err := s.royaltyReport.GetById(ctx, req.ReportId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = royaltyReportErrorReportNotFound
			return nil
		}
		return err
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
		report.DisputeReason = req.DisputeReason
		report.DisputeStartedAt = ptypes.TimestampNow()
	}

	report.UpdatedAt = ptypes.TimestampNow()

	err = s.royaltyReport.Update(ctx, report, req.Ip, pkg.RoyaltyReportChangeSourceMerchant)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

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

func (s *Service) GetRoyaltyReport(
	ctx context.Context,
	req *grpc.GetRoyaltyReportRequest,
	rsp *grpc.GetRoyaltyReportResponse,
) error {
	report, err := s.royaltyReport.GetById(ctx, req.ReportId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = royaltyReportErrorReportNotFound
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = report

	return nil
}

func (s *Service) ChangeRoyaltyReport(
	ctx context.Context,
	req *grpc.ChangeRoyaltyReportRequest,
	rsp *grpc.ResponseError,
) error {
	report, err := s.royaltyReport.GetById(ctx, req.ReportId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = royaltyReportErrorReportNotFound
			return nil
		}
		return err
	}

	if req.Status != "" && report.ChangesAvailable(req.Status) == false {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportStatusChangeDenied

		return nil
	}

	hasChanges := false

	if report.Status == pkg.RoyaltyReportStatusDispute && req.Correction != nil {

		if req.Correction.Reason == "" {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = royaltyReportErrorCorrectionReasonRequired

			return nil
		}

		if req.Correction.Amount == 0 {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = royaltyReportErrorCorrectionAmountRequired

			return nil
		}

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
			Date:       to.Add(-1 * time.Second).Unix(),
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
		report.Summary.Corrections, report.Totals.CorrectionAmount, err = handler.getRoyaltyReportCorrections(
			ctx,
			report.MerchantId,
			report.OperatingCompanyId,
			report.Currency,
		)

		if err != nil {
			zap.L().Error("get royalty report corrections error", zap.Error(err))
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = royaltyReportEntryErrorUnknown
			return nil
		}

		hasChanges = true
	}

	if req.Status != "" && req.Status != report.Status {
		if report.Status == pkg.RoyaltyReportStatusDispute {
			report.DisputeClosedAt = ptypes.TimestampNow()
		}

		if req.Status == pkg.RoyaltyReportStatusAccepted {
			report.AcceptedAt = ptypes.TimestampNow()
		}

		report.Status = req.Status

		hasChanges = true
	}

	if hasChanges != true {
		rsp.Status = pkg.ResponseStatusNotModified
		return nil
	}

	report.UpdatedAt = ptypes.TimestampNow()

	err = s.royaltyReport.Update(ctx, report, req.Ip, pkg.RoyaltyReportChangeSourceAdmin)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	s.sendRoyaltyReportNotification(ctx, report)

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
	report, err := s.royaltyReport.GetById(ctx, req.ReportId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = royaltyReportErrorReportNotFound
			return nil
		}
		return err
	}

	from, _ := ptypes.Timestamp(report.PeriodFrom)
	to, _ := ptypes.Timestamp(report.PeriodTo)
	oid, _ := primitive.ObjectIDFromHex(report.MerchantId)

	match := bson.M{
		"merchant_id":         oid,
		"pm_order_close_date": bson.M{"$gte": from, "$lte": to},
		"status":              bson.M{"$in": orderStatusForRoyaltyReports},
	}

	ts, err := s.orderView.GetTransactionsPublic(ctx, match, req.Limit, req.Offset)
	if err != nil {
		return err
	}

	res.Data = &grpc.TransactionsPaginate{
		Count: int32(len(ts)),
		Items: ts,
	}

	return nil
}

func (s *Service) getRoyaltyReportMerchantsByPeriod(ctx context.Context, from, to time.Time) []*RoyaltyReportMerchant {
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

	cursor, err := s.db.Collection(collectionOrderView).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
	} else {
		err = cursor.All(ctx, &merchants)

		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
		}
	}

	return merchants
}

func (h *royaltyHandler) getRoyaltyReportCorrections(ctx context.Context, merchantId, operatingCompanyId, currency string) (
	entries []*billing.RoyaltyReportCorrectionItem,
	total float64,
	err error) {

	accountingEntries, err := h.accounting.GetCorrectionsForRoyaltyReport(ctx, merchantId, operatingCompanyId, currency, h.from, h.to)
	if err != nil {
		return
	}

	for _, e := range accountingEntries {
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

func (h *royaltyHandler) getRoyaltyReportRollingReserves(merchantId, operatingCompanyId, currency string) (
	entries []*billing.RoyaltyReportCorrectionItem,
	total float64,
	err error) {

	accountingEntries, err := h.accounting.GetRollingReservesForRoyaltyReport(
		h.ctx,
		merchantId,
		operatingCompanyId,
		currency,
		h.from,
		h.to,
	)

	if err != nil {
		return
	}

	for _, e := range accountingEntries {
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

func (h *royaltyHandler) createMerchantRoyaltyReport(ctx context.Context, merchantId primitive.ObjectID) error {
	zap.L().Info("generating royalty reports for merchant", zap.String("merchant_id", merchantId.Hex()))

	merchant, err := h.merchant.GetById(merchantId.Hex())
	if err != nil {
		return err
	}

	ocIds, err := h.orderView.GetRoyaltyOperatingCompaniesIds(ctx, merchant.Id, merchant.GetPayoutCurrency(), h.from, h.to)

	for _, operatingCompanyId := range ocIds {

		isExists, err := h.royaltyReport.CheckReportExists(ctx, merchant.Id, operatingCompanyId, merchant.GetPayoutCurrency(), h.from, h.to)
		if isExists {
			return royaltyReportErrorAlreadyExists
		}

		summaryItems, summaryTotal, err := h.orderView.GetRoyaltySummary(ctx, merchant.Id, operatingCompanyId, merchant.GetPayoutCurrency(), h.from, h.to)
		if err != nil {
			return err
		}

		corrections, correctionsTotal, err := h.getRoyaltyReportCorrections(
			h.ctx,
			merchant.Id,
			operatingCompanyId,
			merchant.GetPayoutCurrency(),
		)

		if err != nil {
			return err
		}

		reserves, reservesTotal, err := h.getRoyaltyReportRollingReserves(merchant.Id, operatingCompanyId, merchant.GetPayoutCurrency())
		if err != nil {
			return err
		}

		report := &billing.RoyaltyReport{
			Id:                 primitive.NewObjectID().Hex(),
			MerchantId:         merchantId.Hex(),
			OperatingCompanyId: operatingCompanyId,
			Currency:           merchant.GetPayoutCurrency(),
			Status:             pkg.RoyaltyReportStatusPending,
			CreatedAt:          ptypes.TimestampNow(),
			Totals: &billing.RoyaltyReportTotals{
				TransactionsCount:    summaryTotal.TotalTransactions,
				FeeAmount:            summaryTotal.TotalFees,
				VatAmount:            summaryTotal.TotalVat,
				PayoutAmount:         summaryTotal.PayoutAmount,
				CorrectionAmount:     tools.ToPrecise(correctionsTotal),
				RollingReserveAmount: tools.ToPrecise(reservesTotal),
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

		err = h.royaltyReport.Insert(ctx, report, "", pkg.RoyaltyReportChangeSourceAuto)
		err = h.Service.renderRoyaltyReport(ctx, report, merchant)

		if err != nil {
			return err
		}

	}

	zap.L().Info("generating royalty reports for merchant finished", zap.String("merchant_id", merchantId.Hex()))

	return nil
}

func (s *Service) renderRoyaltyReport(
	ctx context.Context,
	report *billing.RoyaltyReport,
	merchant *billing.Merchant,
) error {
	params, err := json.Marshal(map[string]interface{}{reporterConst.ParamsFieldId: report.Id})
	if err != nil {
		zap.L().Error(
			"Unable to marshal the params of royalty report for the reporting service.",
			zap.Error(err),
		)
		return err
	}

	fileReq := &reporterProto.ReportFile{
		UserId:           merchant.User.Id,
		MerchantId:       merchant.Id,
		ReportType:       reporterConst.ReportTypeRoyalty,
		FileType:         reporterConst.OutputExtensionPdf,
		Params:           params,
		SendNotification: true,
	}

	if _, err = s.reporterService.CreateFile(ctx, fileReq); err != nil {
		zap.L().Error(
			"Unable to create file in the reporting service for royalty report.",
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (s *Service) RoyaltyReportPdfUploaded(
	ctx context.Context,
	req *grpc.RoyaltyReportPdfUploadedRequest,
	res *grpc.RoyaltyReportPdfUploadedResponse,
) error {
	report, err := s.royaltyReport.GetById(ctx, req.RoyaltyReportId)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = royaltyReportErrorReportNotFound
			return nil
		}
		return err
	}

	merchant, err := s.merchant.GetById(report.MerchantId)

	if err != nil {
		zap.L().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", report.MerchantId))
		return err
	}

	if merchant.HasAuthorizedEmail() == false {
		zap.L().Warn("Merchant has no authorized email", zap.String("merchant_id", merchant.Id))
		res.Status = pkg.ResponseStatusOk
		return nil
	}

	content := base64.StdEncoding.EncodeToString(req.Content)
	contentType := mime.TypeByExtension(filepath.Ext(req.Filename))

	periodFrom, err := ptypes.Timestamp(report.PeriodFrom)
	if err != nil {
		zap.L().Error(
			pkg.ErrorTimeConversion,
			zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
			zap.Any(pkg.ErrorTimeConversionValue, report.PeriodFrom),
			zap.Error(err),
		)
		return err
	}
	periodTo, err := ptypes.Timestamp(report.PeriodTo)
	if err != nil {
		zap.L().Error(
			pkg.ErrorTimeConversion,
			zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
			zap.Any(pkg.ErrorTimeConversionValue, report.PeriodTo),
			zap.Error(err),
		)
		return err
	}

	operatingCompany, err := s.operatingCompany.GetById(report.OperatingCompanyId)
	if err != nil {
		zap.L().Error("Operating company not found", zap.Error(err), zap.String("operating_company_id", report.OperatingCompanyId))
		return err
	}

	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: s.cfg.EmailNewRoyaltyReportTemplate,
		TemplateModel: map[string]string{
			"merchant_id":            merchant.Id,
			"royalty_report_id":      report.Id,
			"period_from":            periodFrom.Format("2006-01-02"),
			"period_to":              periodTo.Format("2006-01-02"),
			"license_agreement":      merchant.AgreementNumber,
			"status":                 report.Status,
			"merchant_greeting":      merchant.GetAuthorizedName(),
			"royalty_reports_url":    s.cfg.RoyaltyReportsUrl,
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
			zap.Any("report", report),
		)
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) sendRoyaltyReportNotification(ctx context.Context, report *billing.RoyaltyReport) {
	merchant, err := s.merchant.GetById(report.MerchantId)

	if err != nil {
		zap.L().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", report.MerchantId))
		return
	}

	if merchant.HasAuthorizedEmail() == true {
		periodFrom, err := ptypes.Timestamp(report.PeriodFrom)
		if err != nil {
			zap.L().Error(
				pkg.ErrorTimeConversion,
				zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
				zap.Any(pkg.ErrorTimeConversionValue, report.PeriodFrom),
				zap.Error(err),
			)
			return
		}
		periodTo, err := ptypes.Timestamp(report.PeriodTo)
		if err != nil {
			zap.L().Error(
				pkg.ErrorTimeConversion,
				zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.Timestamp"),
				zap.Any(pkg.ErrorTimeConversionValue, report.PeriodTo),
				zap.Error(err),
			)
			return
		}

		payload := &postmarkSdrPkg.Payload{
			TemplateAlias: s.cfg.EmailUpdateRoyaltyReportTemplate,
			TemplateModel: map[string]string{
				"merchant_id":         merchant.Id,
				"royalty_report_id":   report.Id,
				"period_from":         periodFrom.Format(time.RFC822),
				"period_to":           periodTo.Format(time.RFC822),
				"license_agreement":   merchant.AgreementNumber,
				"status":              report.Status,
				"merchant_greeting":   merchant.GetAuthorizedName(),
				"royalty_reports_url": s.cfg.RoyaltyReportsUrl,
			},
			To: merchant.GetAuthorizedEmail(),
		}

		err = s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

		if err != nil {
			zap.L().Error(
				"Publication message about merchant new royalty report to queue failed",
				zap.Error(err),
				zap.Any("report", report),
			)
		}
	}

	msg := map[string]interface{}{"id": report.Id, "code": "rr00001", "message": pkg.EmailRoyaltyReportMessage}
	err = s.centrifugo.Publish(ctx, fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, report.MerchantId), msg)

	if err != nil {
		zap.L().Error(
			"[Centrifugo] Send merchant notification about new royalty report failed",
			zap.Error(err),
			zap.Any("msg", msg),
		)
	}

	return
}

func (r *RoyaltyReport) GetNonPayoutReports(
	ctx context.Context,
	merchantId, operatingCompanyId, currency string,
) (result []*billing.RoyaltyReport, err error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id":          oid,
		"currency":             currency,
		"status":               bson.M{"$in": royaltyReportsStatusActive},
		"operating_company_id": operatingCompanyId,
		"payout_document_id":   "",
	}

	sorts := bson.M{"period_from": 1}
	opts := options.Find().SetSort(sorts)
	cursor, err := r.svc.db.Collection(collectionRoyaltyReport).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
	} else {
		err = cursor.All(ctx, &result)

		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
				zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
			)
		}
	}

	return
}

func (r *RoyaltyReport) GetNonPayoutReportsOperatingCompaniesIds(
	ctx context.Context,
	merchantId, currency string,
) ([]string, error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id":        oid,
		"currency":           currency,
		"status":             bson.M{"$in": royaltyReportsStatusActive},
		"payout_document_id": "",
	}

	res, err := r.svc.db.Collection(collectionRoyaltyReport).Distinct(ctx, "operating_company_id", query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	result := make([]string, len(res))

	for _, v := range res {
		result = append(result, v.(string))
	}

	return result, nil
}

func (r *RoyaltyReport) GetByPayoutId(ctx context.Context, payoutId string) ([]*billing.RoyaltyReport, error) {
	query := bson.M{
		"payout_document_id": payoutId,
	}

	sorts := bson.M{"period_from": 1}
	opts := options.Find().SetSort(sorts)
	cursor, err := r.svc.db.Collection(collectionRoyaltyReport).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
		return nil, err
	}

	var result []*billing.RoyaltyReport
	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return result, nil
}

func (r *RoyaltyReport) GetBalanceAmount(ctx context.Context, merchantId, currency string) (float64, error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id": oid,
				"currency":    currency,
				"status":      bson.M{"$in": royaltyReportsStatusForBalance},
			},
		},
		{
			"$group": bson.M{
				"_id":               "currency",
				"payout_amount":     bson.M{"$sum": "$totals.payout_amount"},
				"correction_amount": bson.M{"$sum": "$totals.correction_amount"},
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
	cursor, err := r.svc.db.Collection(collectionRoyaltyReport).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionRoyaltyReport),
			zap.Any("query", query),
		)
		return 0, err
	}

	err = cursor.Decode(&res)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, err
	}

	return res.Amount, nil
}

func (r *RoyaltyReport) CheckReportExists(
	ctx context.Context,
	merchantId, operatingCompanyId, currency string,
	from, to time.Time,
) (bool, error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id":          oid,
		"period_from":          bson.M{"$gte": from},
		"period_to":            bson.M{"$lte": to},
		"currency":             currency,
		"operating_company_id": operatingCompanyId,
	}
	var report *billing.RoyaltyReport
	err := r.svc.db.Collection(collectionRoyaltyReport).FindOne(ctx, query).Decode(&report)

	if err == mongo.ErrNoDocuments || report == nil {
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

func (r *RoyaltyReport) Insert(ctx context.Context, rr *billing.RoyaltyReport, ip, source string) (err error) {
	_, err = r.svc.db.Collection(collectionRoyaltyReport).InsertOne(ctx, rr)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, rr),
		)
		return
	}

	err = r.onRoyaltyReportChange(ctx, rr, ip, source)
	if err != nil {
		return
	}

	key := fmt.Sprintf(cacheKeyRoyaltyReport, rr.Id)
	err = r.svc.cacher.Set(key, rr, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, rr),
		)
	}
	return
}

func (r *RoyaltyReport) Update(ctx context.Context, rr *billing.RoyaltyReport, ip, source string) error {
	oid, _ := primitive.ObjectIDFromHex(rr.Id)
	_, err := r.svc.db.Collection(collectionRoyaltyReport).UpdateOne(ctx, bson.M{"_id": oid}, rr)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldDocument, rr),
		)
		return err
	}

	err = r.onRoyaltyReportChange(ctx, rr, ip, source)
	if err != nil {
		return err
	}

	key := fmt.Sprintf(cacheKeyRoyaltyReport, rr.Id)
	err = r.svc.cacher.Set(fmt.Sprintf(cacheKeyRoyaltyReport, rr.Id), rr, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, rr),
		)
	}

	return nil
}

func (r *RoyaltyReport) GetById(ctx context.Context, id string) (rr *billing.RoyaltyReport, err error) {
	var c billing.RoyaltyReport
	key := fmt.Sprintf(cacheKeyRoyaltyReport, id)
	if err := r.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	err = r.svc.db.Collection(collectionRoyaltyReport).FindOne(ctx, bson.M{"_id": oid}).Decode(&rr)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReport),
			zap.String(pkg.ErrorDatabaseFieldDocumentId, id),
		)
		return
	}

	err = r.svc.cacher.Set(key, rr, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, rr),
		)
		// suppress error returning here
		err = nil
	}
	return
}

func (r *RoyaltyReport) onRoyaltyReportChange(
	ctx context.Context,
	document *billing.RoyaltyReport,
	ip, source string,
) (err error) {
	change := &billing.RoyaltyReportChanges{
		Id:              primitive.NewObjectID().Hex(),
		RoyaltyReportId: document.Id,
		Source:          source,
		Ip:              ip,
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

	_, err = r.svc.db.Collection(collectionRoyaltyReportChanges).InsertOne(ctx, change)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionRoyaltyReportChanges),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, change),
		)
		return
	}

	return
}

func (r *RoyaltyReport) SetPayoutDocumentId(
	ctx context.Context,
	reportIds []string,
	payoutDocumentId, ip, source string,
) error {
	_, err := primitive.ObjectIDFromHex(payoutDocumentId)

	if err != nil {
		return royaltyReportErrorPayoutDocumentIdInvalid
	}

	for _, id := range reportIds {
		rr, err := r.GetById(ctx, id)

		if err != nil {
			return err
		}

		rr.PayoutDocumentId = payoutDocumentId
		rr.Status = pkg.RoyaltyReportStatusWaitForPayment

		err = r.Update(ctx, rr, ip, source)

		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RoyaltyReport) UnsetPayoutDocumentId(ctx context.Context, reportIds []string, ip, source string) (err error) {
	for _, id := range reportIds {
		rr, err := r.GetById(ctx, id)
		if err != nil {
			return err
		}

		rr.PayoutDocumentId = ""
		rr.Status = pkg.RoyaltyReportStatusAccepted

		err = r.Update(ctx, rr, ip, source)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RoyaltyReport) SetPaid(ctx context.Context, reportIds []string, payoutDocumentId, ip, source string) (err error) {
	_, err = primitive.ObjectIDFromHex(payoutDocumentId)

	if err != nil {
		return royaltyReportErrorPayoutDocumentIdInvalid
	}

	for _, id := range reportIds {
		rr, err := r.GetById(ctx, id)
		if err != nil {
			return err
		}

		rr.PayoutDocumentId = payoutDocumentId
		rr.Status = pkg.RoyaltyReportStatusPaid
		rr.PayoutDate = ptypes.TimestampNow()

		err = r.Update(ctx, rr, ip, source)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RoyaltyReport) UnsetPaid(ctx context.Context, reportIds []string, ip, source string) (err error) {
	for _, id := range reportIds {
		rr, err := r.GetById(ctx, id)
		if err != nil {
			return err
		}

		rr.PayoutDocumentId = ""
		rr.Status = pkg.RoyaltyReportStatusAccepted
		rr.PayoutDate = nil

		err = r.Update(ctx, rr, ip, source)
		if err != nil {
			return err
		}
	}
	return nil
}
