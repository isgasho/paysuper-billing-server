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

	royaltyReportErrorNoTransactions    = "no transactions for the period"
	royaltyReportErrorTimezoneIncorrect = "incorrect time zone"
	royaltyReportErrorAlreadyExists     = "report for this merchant and period already exists"
)

var (
	royaltyReportErrorReportNotFound                  = newBillingServerErrorMsg("rr00001", "royalty report with specified identifier not found")
	royaltyReportErrorReportStatusChangeDenied        = newBillingServerErrorMsg("rr00002", "change royalty report to new status denied")
	royaltyReportErrorReportDisputeCorrectionRequired = newBillingServerErrorMsg("rr00003", "for change royalty report status to dispute fields with correction amount and correction reason is required")
	royaltyReportEntryErrorUnknown                    = newBillingServerErrorMsg("rr00004", "unknown error. try request later")
	royaltyReportUpdateBalanceError                   = newBillingServerErrorMsg("rr00005", "update balance failed")

	royaltyReportsStatusActive = []string{
		pkg.RoyaltyReportStatusPending,
		pkg.RoyaltyReportStatusAccepted,
		pkg.RoyaltyReportStatusDispute,
	}
)

type royaltyReportQueryResItem struct {
	Id                   string  `bson:"_id"`
	Count                int32   `bson:"count"`
	GrossRevenue         float64 `bson:"merchant_gross_revenue"`
	RefundGrossRevenue   float64 `bson:"refund_gross_revenue"`
	NetRevenue           float64 `bson:"net_revenue"`
	RefundReverseRevenue float64 `bson:"refund_reverse_revenue"`
	Payout               float64 `bson:"merchant_payout"`
	FeesTotal            float64 `bson:"fees_total"`
	RefundReesTotal      float64 `bson:"refund_fees_total"`
	TaxFeeTotal          float64 `bson:"tax_fee_total"`
	RefundTaxFeeTotal    float64 `bson:"refund_tax_fee_total"`
}

type RoyaltyReportMerchant struct {
	Id bson.ObjectId `bson:"_id"`
}

type royaltyHandler struct {
	*Service
	from time.Time
	to   time.Time
}

type RoyaltyReportServiceInterface interface {
	GetNonPayoutReports(merchantId, currency string) ([]*billing.RoyaltyReport, error)
	GetBalanceAmount(merchantId, currency string) (float64, error)
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
			err := handler.createMerchantRoyaltyReport(merchantId)

			if err != nil {
				rsp.Merchants = append(rsp.Merchants, merchantId.Hex())
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

	if req.Status == pkg.RoyaltyReportStatusDispute && (req.Correction == nil || req.Correction.Reason == "" ||
		req.Correction.Amount <= 0) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = royaltyReportErrorReportDisputeCorrectionRequired

		return nil
	}

	report.Status = req.Status
	report.Correction = req.Correction
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
		s.sendRoyaltyReportNotification(report)
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
		"status":              constant.OrderPublicStatusProcessed,
	}

	ts, err := s.getTransactionsPublic(match, int(req.Limit), int(req.Offset))
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
				"status":              constant.OrderPublicStatusProcessed,
			},
		},
		{"$project": bson.M{"project.merchant_id": true}},
		{"$group": bson.M{"_id": "$project.merchant_id"}},
	}

	err := s.db.Collection(collectionOrder).Pipe(query).All(&merchants)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionOrder),
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

func (h *royaltyHandler) createMerchantRoyaltyReport(merchantId bson.ObjectId) error {
	zap.S().Infow("generating royalty report for merchant", "merchantId", merchantId.Hex())

	merchant, err := h.merchant.GetById(merchantId.Hex())

	if err != nil {
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", merchantId.Hex()))
		return err
	}

	// If the report already exists - this is error!
	checkQuery := bson.M{
		"merchant_id": merchantId,
		"period_from": bson.M{"$gte": h.from},
		"period_to":   bson.M{"$lte": h.to},
	}
	var previous *billing.RoyaltyReport
	err = h.db.Collection(collectionRoyaltyReport).Find(checkQuery).One(&previous)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	if previous != nil {
		zap.S().Errorw("Merchant not found", "merchant_id", merchantId.Hex, "from", h.from, "to", h.to)
		return errors.New(royaltyReportErrorAlreadyExists)
	}

	matchQuery := bson.M{
		"merchant_id":         merchantId,
		"pm_order_close_date": bson.M{"$gte": h.from, "$lte": h.to},
		"status":              constant.OrderPublicStatusProcessed,
	}

	query := []bson.M{
		{
			"$match": &matchQuery,
		},
		{
			"$group": bson.M{
				"_id":                    "$merchant_id",
				"count":                  bson.M{"$sum": 1},
				"merchant_gross_revenue": bson.M{"$sum": "$gross_revenue.amount"},
				"refund_gross_revenue":   bson.M{"$sum": "$refund_gross_revenue.amount"},
				"net_revenue":            bson.M{"$sum": "$net_revenue.amount"},
				"refund_reverse_revenue": bson.M{"$sum": "$refund_reverse_revenue.amount"},
				"fees_total":             bson.M{"$sum": "$fees_total.amount"},
				"refund_fees_total":      bson.M{"$sum": "$refund_fees_total.amount"},
				"tax_fee_total":          bson.M{"$sum": "$tax_fee_total.amount"},
				"refund_tax_fee_total":   bson.M{"$sum": "$refund_tax_fee_total.amount"},
				// todo: UNDONE calculate payouts amount (after cardpay settlement reports will be implemented)
			},
		},
	}

	var res []*royaltyReportQueryResItem
	err = h.Service.db.Collection(collectionOrderView).Pipe(query).All(&res)
	if err != nil && err != mgo.ErrNotFound {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrderView),
			zap.Any("query", query),
		)
		return err
	}

	amounts := &billing.RoyaltyReportDetails{
		Currency: merchant.GetPayoutCurrency(),
	}

	if len(res) == 1 {
		amounts.TransactionsCount = res[0].Count
		amounts.GrossAmount = tools.FormatAmount(res[0].GrossRevenue - res[0].RefundGrossRevenue)
		amounts.PayoutAmount = tools.FormatAmount(res[0].NetRevenue - res[0].RefundReverseRevenue - res[0].Payout)
		amounts.VatAmount = tools.FormatAmount(res[0].FeesTotal + res[0].RefundReesTotal)
		amounts.FeeAmount = res[0].TaxFeeTotal + res[0].RefundTaxFeeTotal
	}

	report := &billing.RoyaltyReport{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: merchantId.Hex(),
		Amounts:    amounts,
		Status:     pkg.RoyaltyReportStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	report.PeriodFrom, _ = ptypes.TimestampProto(h.from)
	report.PeriodTo, _ = ptypes.TimestampProto(h.to)
	report.AcceptExpireAt, _ = ptypes.TimestampProto(time.Now().Add(time.Duration(h.cfg.RoyaltyReportAcceptTimeout) * time.Second))

	err = h.db.Collection(collectionRoyaltyReport).Insert(report)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, report),
		)

		return err
	}

	h.Service.sendRoyaltyReportNotification(report)

	zap.S().Infow("generating royalty report for merchant finished", "merchantId", merchantId.Hex())

	return nil
}

func (s *Service) sendRoyaltyReportNotification(report *billing.RoyaltyReport) {
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
	b, _ := json.Marshal(msg)

	err = s.centrifugoClient.Publish(context.Background(), fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, report.MerchantId), b)

	if err != nil {
		zap.S().Error(
			"[Centrifugo] Send merchant notification about new royalty report failed",
			zap.Error(err),
			zap.String("merchant_id", merchant.Id),
			zap.String("royalty_report_id", report.Id),
		)

		return
	}

	return
}

func (r RoyaltyReport) GetNonPayoutReports(merchantId, currency string) (result []*billing.RoyaltyReport, err error) {
	query := bson.M{
		"merchant_id":      bson.ObjectIdHex(merchantId),
		"amounts.currency": currency,
		"status":           bson.M{"$in": royaltyReportsStatusActive},
	}

	excludeIdsString, err := r.svc.payoutDocument.GetAllSourcesIdHex(merchantId, currency)
	if err != nil {
		return nil, err
	}

	excludeIds := []bson.ObjectId{}
	for _, v := range excludeIdsString {
		excludeIds = append(excludeIds, bson.ObjectIdHex(v))
	}

	if len(excludeIds) > 0 {
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
				"merchant_id":      bson.ObjectIdHex(merchantId),
				"amounts.currency": currency,
				"status":           pkg.RoyaltyReportStatusAccepted,
			},
		},
		{
			"$group": bson.M{
				"_id":    "$amounts.currency",
				"amount": bson.M{"$sum": "$amounts.payout_amount"},
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
