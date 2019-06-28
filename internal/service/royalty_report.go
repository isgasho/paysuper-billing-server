package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"gopkg.in/mgo.v2"
	"sync"
	"time"
)

const (
	errorFieldCollection = "service"
	errorFieldQuery      = "method"

	collectionRoyaltyReport = "royalty_report"

	royaltyReportErrorNoTransactions = "no transactions for the period"
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
)

type RoyaltyReportMerchant struct {
	Id bson.ObjectId `bson:"_id"`
}

type royaltyHandler struct {
	*Service
	from   time.Time
	to     time.Time
	errors map[string]error
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

	merchants := s.getRoyaltyReportMerchantsByPeriod(from, to)

	if len(merchants) <= 0 {
		return errors.New(royaltyReportErrorNoTransactions)
	}

	var wg sync.WaitGroup
	wg.Add(len(merchants))

	handler := &royaltyHandler{
		Service: s,
		from:    from,
		to:      to,
		errors:  make(map[string]error),
	}

	for _, v := range merchants {
		go func(merchantId bson.ObjectId) {
			err := handler.processMerchantRoyaltyReport(merchantId)

			if err != nil {
				rsp.Merchants = append(rsp.Merchants, merchantId.Hex())
				handler.errors[merchantId.Hex()] = err
			}

			wg.Done()
		}(v.Id)
	}

	return nil
}

func (s *Service) ListRoyaltyReports() error {
	return nil
}

func (s *Service) ChangeRoyaltyReportStatus() error {
	return nil
}

func (s *Service) ListRoyaltyReportOrders(
	ctx context.Context,
	req *grpc.ListRoyaltyReportsRequest,
	rsp *grpc.ListRoyaltyReportsResponse,
) error {
	query := make(bson.M)

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
		zap.S().Errorf("Query from table ended with error", "err", err.Error(), "table", collectionOrder)
		return err
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
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrder),
			zap.Any("query", query),
		)
	}

	return merchants
}

func (h *royaltyHandler) processMerchantRoyaltyReport(merchantId bson.ObjectId) error {
	//If the report isn't generated the first time, then mark the previous report as deleted
	query := bson.M{
		"merchant_id": merchantId,
		"period_from": bson.M{"$gte": h.from},
		"period_to":   bson.M{"$lte": h.to},
	}
	update := bson.M{"deleted": true}
	err := h.db.Collection(collectionRoyaltyReport).Update(query, update)

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

	return h.sendRoyaltyReportNotification(report)
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
		Status:    pkg.RoyaltyReportStatusNew,
		CreatedAt: ptypes.TimestampNow(),
	}

	report.PeriodFrom, _ = ptypes.TimestampProto(h.from)
	report.PeriodTo, _ = ptypes.TimestampProto(h.to)
	report.AcceptExpireAt, _ = ptypes.TimestampProto(time.Now().AddDate(0, 0, h.cfg.RoyaltyReportAcceptTimeout))

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

	return report, nil
}

func (s *Service) sendRoyaltyReportNotification(report *billing.RoyaltyReport) error {
	merchant, err := s.merchant.GetById(report.MerchantId)

	if err != nil {
		zap.L().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", report.MerchantId))
		return err
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
	b, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	err = s.centrifugoClient.Publish(context.Background(), fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, report.MerchantId), b)

	if err != nil {
		zap.L().Error(
			"[Centrifugo] Send merchant notification about new royalty report failed",
			zap.Error(err),
			zap.String("merchant_id", merchant.Id),
			zap.String("royalty_report_id", report.Id),
		)

		return err
	}

	return nil
}
