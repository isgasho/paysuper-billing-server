package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	tax_service "github.com/paysuper/paysuper-tax-service/proto"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
	"strconv"
	"time"
)

const (
	collectionVatTransactions = "vat_transactions"
	collectionVatReports      = "vat_reports"
)

var (
	errorVatReportNotEnabledForCountry          = newBillingServerErrorMsg("vr000001", "vat not enabled for country")
	errorVatReportPeriodNotConfiguredForCountry = newBillingServerErrorMsg("vr000002", "vat period not configured for country")
	errorVatReportRatesPolicyNotImplemented     = newBillingServerErrorMsg("vr000003", "selected currency rates policy not implemented yet")
	errorVatReportCurrencyExchangeFailed        = newBillingServerErrorMsg("vr000004", "currency exchange failed")
	errorVatReportTransactionsInconsistent      = newBillingServerErrorMsg("vr000005", "vat transactions inconsistent")
	errorVatReportStatusChangeNotAllowed        = newBillingServerErrorMsg("vr000006", "vat report status change not allowed")
	errorVatReportStatusChangeFailed            = newBillingServerErrorMsg("vr000007", "vat report status change failed")
	errorVatReportQueryError                    = newBillingServerErrorMsg("vr000008", "vat report db query error")
	errorVatReportNotFound                      = newBillingServerErrorMsg("vr000009", "vat report not found")
	errorVatReportInternal                      = newBillingServerErrorMsg("vr000010", "vat report internal error")

	VatReportOnStatusNotifyToCentrifugo = []string{
		pkg.VatReportStatusNeedToPay,
		pkg.VatReportStatusPaid,
		pkg.VatReportStatusOverdue,
		pkg.VatReportStatusCanceled,
	}

	VatReportOnStatusNotifyToEmail = []string{
		pkg.VatReportStatusNeedToPay,
		pkg.VatReportStatusOverdue,
		pkg.VatReportStatusCanceled,
	}

	VatReportStatusAllowManualChangeFrom = []string{
		pkg.VatReportStatusNeedToPay,
		pkg.VatReportStatusOverdue,
	}

	VatReportStatusAllowManualChangeTo = []string{
		pkg.VatReportStatusPaid,
		pkg.VatReportStatusCanceled,
	}
)

type vatReportQueryResItem struct {
	Id           string  `bson:"_id"`
	Count        int32   `bson:"count"`
	GrossRevenue float64 `bson:"gross_revenue"`
	VatAmount    float64 `bson:"vat_amount"`
	FeesAmount   float64 `bson:"fees_amount"`
}

type vatReportProcessor struct {
	*Service
	ctx       context.Context
	date      time.Time
	ts        *timestamp.Timestamp
	countries []*billing.Country
}

func NewVatReportProcessor(s *Service, ctx context.Context, date *timestamp.Timestamp) (*vatReportProcessor, error) {
	ts, err := ptypes.Timestamp(date)
	if err != nil {
		return nil, err
	}
	eod := now.New(ts).EndOfDay()
	eodTimestamp, err := ptypes.TimestampProto(eod)
	if err != nil {
		return nil, err
	}
	countries, err := s.country.GetCountriesWithVatEnabled()
	if err != nil {
		return nil, err
	}

	processor := &vatReportProcessor{
		Service:   s,
		ctx:       ctx,
		date:      eod,
		ts:        eodTimestamp,
		countries: countries.Countries,
	}

	return processor, nil
}

func (s *Service) GetVatReportsDashboard(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.VatReportsResponse,
) error {

	res.Status = pkg.ResponseStatusOk

	query := bson.M{
		"status": bson.M{"$in": []string{pkg.VatReportStatusThreshold, pkg.VatReportStatusNeedToPay, pkg.VatReportStatusOverdue}},
	}

	sort := []string{"country", "status"}
	var reports []*billing.VatReport
	err := s.db.Collection(collectionVatReports).Find(query).Sort(sort...).All(&reports)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorVatReportNotFound
			return nil
		}

		zap.L().Error(
			errorVatReportQueryError.Message,
			zap.Error(err),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportQueryError
		return nil
	}

	res.Data = &grpc.VatReportsPaginate{
		Count: int32(len(reports)),
		Items: reports,
	}

	return nil
}

func (s *Service) GetVatReportsForCountry(
	ctx context.Context,
	req *grpc.VatReportsRequest,
	res *grpc.VatReportsResponse,
) error {

	res.Status = pkg.ResponseStatusOk

	query := bson.M{
		"country": req.Country,
	}

	sort := req.Sort
	if len(sort) == 0 {
		sort = []string{"-date_from"}
	}

	var reports []*billing.VatReport
	err := s.db.Collection(collectionVatReports).
		Find(query).
		Sort(sort...).
		Limit(int(req.Limit)).
		Skip(int(req.Offset)).
		All(&reports)

	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorVatReportNotFound
			return nil
		}

		zap.L().Error(
			errorVatReportQueryError.Message,
			zap.Error(err),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportQueryError
		return nil
	}

	res.Data = &grpc.VatReportsPaginate{
		Count: int32(len(reports)),
		Items: reports,
	}

	return nil
}

func (s *Service) GetVatReportTransactions(
	ctx context.Context,
	req *grpc.VatTransactionsRequest,
	res *grpc.VatTransactionsResponse,
) error {
	/*res.Status = pkg.ResponseStatusOk

	query := bson.M{
		"_id": bson.ObjectIdHex(req.VatReportId),
	}

	var vr *billing.VatReport
	err := s.db.Collection(collectionVatReports).Find(query).One(&vr)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorVatReportNotFound
			return nil
		}

		zap.L().Error(
			errorVatReportQueryError.Message,
			zap.Error(err),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportQueryError
		return nil
	}

	from, err := ptypes.Timestamp(vr.DateFrom)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportInternal
		return nil
	}
	to, err := ptypes.Timestamp(vr.DateTo)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportInternal
		return nil
	}

	vts, err := s.getVatTransactions(from, to, vr.Country, int(req.Limit), int(req.Offset))
	if err != nil {
		return err
	}

	res.Data = &grpc.VatTransactionsPaginate{
		Count: int32(len(vts)),
		Items: vts,
	}*/

	return nil
}

func (s *Service) ProcessVatReports(
	ctx context.Context,
	req *grpc.ProcessVatReportsRequest,
	res *grpc.EmptyResponse,
) error {

	err := s.CalcAnnualTurnovers(ctx, &grpc.EmptyRequest{}, &grpc.EmptyResponse{})
	if err != nil {
		return err
	}

	handler, err := NewVatReportProcessor(s, ctx, req.Date)
	if err != nil {
		return err
	}
	// todo: использовать проводки вместо налоговых транзакций
	/*
		err = handler.ProcessVatTransactions()
		if err != nil {
			return err
		}
	*/
	err = handler.ProcessVatReports()
	if err != nil {
		return err
	}

	return handler.ProcessVatReportsStatus()
}

func (s *Service) UpdateVatReportStatus(
	ctx context.Context,
	req *grpc.UpdateVatReportStatusRequest,
	res *grpc.ResponseError,
) error {

	res.Status = pkg.ResponseStatusOk

	query := bson.M{
		"_id": bson.ObjectIdHex(req.Id),
	}

	var vr *billing.VatReport
	err := s.db.Collection(collectionVatReports).Find(query).One(&vr)
	if err != nil {
		if err == mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorVatReportNotFound
			return nil
		}

		zap.L().Error(
			errorVatReportQueryError.Message,
			zap.Error(err),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportQueryError
		return nil
	}

	if !contains(VatReportStatusAllowManualChangeFrom, vr.Status) {
		res.Status = pkg.StatusErrorValidation
		res.Message = errorVatReportStatusChangeNotAllowed
		return nil
	}

	if !contains(VatReportStatusAllowManualChangeTo, req.Status) {
		res.Status = pkg.StatusErrorValidation
		res.Message = errorVatReportStatusChangeNotAllowed
		return nil
	}

	vr.Status = req.Status

	err = s.updateVatReport(vr)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorVatReportStatusChangeFailed
		return nil
	}

	return nil
}

/*
func (s *Service) insertVatTransaction(vt *billing.VatTransaction) error {
	return s.db.Collection(collectionVatTransactions).Insert(vt)
}

func (s *Service) updateVatTransaction(vt *billing.VatTransaction) error {
	return s.db.Collection(collectionVatTransactions).UpdateId(bson.ObjectIdHex(vt.Id), vt)
}

func (s *Service) getVatTransactions(from, to time.Time, country string, pagination ...int) ([]*billing.VatTransaction, error) {
	query := bson.M{
		"date_time": bson.M{
			"$gte": bod(from),
			"$lte": eod(to),
		},
		"country": country,
	}

	var transactions []*billing.VatTransaction
	dbRequest := s.db.Collection(collectionVatTransactions).Find(query).Sort("date_time")

	if pagination != nil {
		if val := pagination[0]; val > 0 {
			dbRequest.Limit(val)
		}
		if val := pagination[1]; val > 0 {
			dbRequest.Skip(val)
		}
	}

	err := dbRequest.All(&transactions)
	if err != nil {
		return nil, err
	}

	return transactions, nil
}
*/
func (s *Service) insertVatReport(vr *billing.VatReport) error {
	return s.db.Collection(collectionVatReports).Insert(vr)
}

func (s *Service) updateVatReport(vr *billing.VatReport) error {
	err := s.db.Collection(collectionVatReports).UpdateId(bson.ObjectIdHex(vr.Id), vr)
	if err != nil {
		return err
	}

	if contains(VatReportOnStatusNotifyToCentrifugo, vr.Status) {
		b, err := json.Marshal(vr)
		if err != nil {
			return err
		}
		err = s.centrifugoClient.Publish(context.Background(), s.cfg.CentrifugoFinancierChannel, b)

		if err != nil {
			zap.L().Error(
				"[Centrifugo] Send financier notification about vat report status change failed",
				zap.Error(err),
				zap.Any("vat_report", vr),
			)

			return err
		}
	}

	if contains(VatReportOnStatusNotifyToEmail, vr.Status) {
		m := gomail.NewMessage()
		m.SetHeader("Subject", pkg.EmailVatReportSubject)

		message := fmt.Sprintf(pkg.EmailVatReportMessage, vr.Country, vr.Status)

		m.SetBody(pkg.EmailContentType, message)

		err = s.smtpCl.Send(s.cfg.EmailNotificationSender, []string{s.cfg.EmailNotificationFinancierRecipient}, m)

		if err != nil {
			zap.L().Error(
				"[SMTP] Send financier notification about vat report status change failed",
				zap.Error(err),
				zap.Any("vat_report", vr),
			)
		}
	}
	return nil
}

func (s *Service) getVatReportTime(VatPeriodMonth int32, date time.Time) (from, to time.Time, err error) {
	var nowTime *now.Now
	if date.IsZero() {
		nowTime = now.New(time.Now())
	} else {
		nowTime = now.New(date)
	}

	switch VatPeriodMonth {
	case 1:
		from = nowTime.BeginningOfMonth()
		to = nowTime.EndOfMonth()
		return
	case 2:
		from = nowTime.BeginningOfMonth()
		to = nowTime.EndOfMonth()
		if from.Month()%2 == 0 {
			from = now.New(from.AddDate(0, 0, -1)).BeginningOfMonth()
		} else {
			to = now.New(to.AddDate(0, 0, 1)).EndOfMonth()
		}
		return
	case 3:
		from = nowTime.BeginningOfQuarter()
		to = nowTime.EndOfQuarter()
		return
	}

	err = errorVatReportPeriodNotConfiguredForCountry
	return
}

func (s *Service) getVatReportTimeForDate(VatPeriodMonth int32, date time.Time) (from, to time.Time, err error) {
	return s.getVatReportTime(VatPeriodMonth, date)
}

func (s *Service) getLastVatReportTime(VatPeriodMonth int32) (from, to time.Time, err error) {
	return s.getVatReportTime(VatPeriodMonth, time.Time{})
}

func (h *vatReportProcessor) ProcessVatReportsStatus() error {
	currentUnixTime := time.Now().Unix()

	query := bson.M{
		"status": bson.M{"$in": []string{pkg.VatReportStatusThreshold, pkg.VatReportStatusNeedToPay}},
	}

	var reports []*billing.VatReport
	err := h.Service.db.Collection(collectionVatReports).Find(query).All(&reports)
	if err != nil {
		return err
	}
	for _, report := range reports {
		country := h.getCountry(report.Country)
		currentFrom, _, err := h.Service.getLastVatReportTime(country.VatPeriodMonth)
		if err != nil {
			return err
		}
		reportDateFrom, err := ptypes.Timestamp(report.DateFrom)
		if err != nil {
			return err
		}

		if reportDateFrom.Unix() >= currentFrom.Unix() {
			continue
		}

		if report.Status == pkg.VatReportStatusNeedToPay {
			reportDeadline, err := ptypes.Timestamp(report.PayUntilDate)
			if err != nil {
				return err
			}
			if currentUnixTime >= reportDeadline.Unix() {
				report.Status = pkg.VatReportStatusOverdue
				return h.Service.updateVatReport(report)
			}
		}

		thresholdExceeded := (country.VatThreshold.Year > 0 && report.CountryAnnualTurnover >= country.VatThreshold.Year) ||
			(country.VatThreshold.World > 0 && report.WorldAnnualTurnover >= country.VatThreshold.World)

		if thresholdExceeded {
			report.Status = pkg.VatReportStatusNeedToPay
		} else {
			report.Status = pkg.VatReportStatusExpired
		}
		return h.Service.updateVatReport(report)
	}
	return nil
}

func (h *vatReportProcessor) getCountry(countryCode string) *billing.Country {
	for _, c := range h.countries {
		if c.IsoCodeA2 == countryCode {
			return c
		}
	}
	return nil
}

func (h *vatReportProcessor) ProcessVatReports() error {
	for _, c := range h.countries {
		if c.IsoCodeA2 == "US" {
			continue
		}
		err := h.processVatReportForPeriod(c)
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *vatReportProcessor) ProcessVatTransactions() error {
	for _, c := range h.countries {
		if c.IsoCodeA2 == "US" {
			continue
		}
		err := h.processVatTransactionsForPeriod(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *vatReportProcessor) processVatReportForPeriod(country *billing.Country) error {

	req := &tax_service.GetRateRequest{
		IpData: &tax_service.GeoIdentity{
			Country: country.IsoCodeA2,
		},
		UserData: &tax_service.GeoIdentity{},
	}

	rsp, err := h.Service.tax.GetRate(h.ctx, req)
	if err != nil {
		zap.L().Error("tax service get rate error", zap.Error(err))
		return err
	}

	// converting directly from float32 to float64 adds some new digits at the and of value
	// this is strong and precision conversion through string
	// todo: get float64 value form tax service
	rate, err := strconv.ParseFloat(fmt.Sprintf("%f", rsp.Rate.Rate), 64)
	if err != nil {
		zap.L().Error("tax service get rate error", zap.Error(err))
		return err
	}

	report := &billing.VatReport{
		Country:          country.IsoCodeA2,
		VatRate:          rate,
		Currency:         country.Currency,
		Status:           pkg.VatReportStatusThreshold,
		CorrectionAmount: 0,
	}

	from, to, err := h.Service.getVatReportTimeForDate(country.VatPeriodMonth, h.date)
	if err != nil {
		return err
	}
	report.DateFrom, err = ptypes.TimestampProto(from)
	if err != nil {
		return err
	}
	report.DateTo, err = ptypes.TimestampProto(to)
	if err != nil {
		return err
	}
	report.PayUntilDate, err = ptypes.TimestampProto(to.AddDate(0, 0, int(country.VatDeadlineDays)))
	if err != nil {
		return err
	}

	countryTurnover, err := h.Service.turnover.Get(country.IsoCodeA2, from.Year())
	if err != nil {
		return err
	}
	report.CountryAnnualTurnover = countryTurnover.Amount

	worldTurnover, err := h.Service.turnover.Get("", from.Year())
	if err != nil {
		return err
	}
	report.WorldAnnualTurnover = worldTurnover.Amount
	if worldTurnover.Currency != country.Currency {
		report.WorldAnnualTurnover, err = h.exchangeAmount(worldTurnover.Currency, country.Currency, worldTurnover.Amount)
		if err != nil {
			return err
		}
	}
	report.AmountsApproximate = h.date.Unix() < to.Unix()

	matchQuery := bson.M{
		"date_time": bson.M{
			"$gte": bod(from),
			"$lte": eod(to),
		},
		"country":      country.IsoCodeA2,
		"is_deduction": false,
	}

	query := []bson.M{
		{
			"$match": &matchQuery,
			"$group": bson.M{
				"_id":           "$local_currency",
				"count":         bson.M{"$sum": 1},
				"gross_revenue": bson.M{"$sum": "$local_transaction_amount"},
				"vat_amount":    bson.M{"$sum": "$local_vat_amount"},
				"fees_amount":   bson.M{"$sum": "$local_fees_amount"},
			},
		},
	}

	// todo: использовать проводки вместо налоговых транзакций

	var res []*vatReportQueryResItem
	err = h.Service.db.Collection(collectionVatTransactions).Pipe(query).All(&res)

	localCurrenciesCount := len(res)
	if localCurrenciesCount > 1 {
		return errorVatReportTransactionsInconsistent
	}
	if localCurrenciesCount == 1 {
		report.TransactionsCount = res[0].Count
		report.GrossRevenue = res[0].GrossRevenue
		report.VatAmount = res[0].VatAmount
		report.FeesAmount = res[0].FeesAmount
	}

	matchQuery["deduction"] = true
	// todo: использовать проводки вместо налоговых транзакций
	err = h.Service.db.Collection(collectionVatTransactions).Pipe(query).All(&res)

	localCurrenciesCount = len(res)
	if localCurrenciesCount > 1 {
		return errorVatReportTransactionsInconsistent
	}
	if localCurrenciesCount == 1 {
		report.DeductionAmount = res[0].VatAmount
	}

	// todo: нужно ли считать в общем TransactionsCount те транзакции, которые идут в deduction?

	selector := bson.M{
		"country":   report.Country,
		"date_from": report.DateFrom,
		"date_to":   report.DateTo,
		"status":    pkg.VatReportStatusThreshold,
	}

	var vr *billing.VatReport
	// todo: использовать проводки вместо налоговых транзакций
	err = h.Service.db.Collection(collectionVatTransactions).Find(selector).One(&vr)

	if err == mgo.ErrNotFound {
		report.Id = bson.NewObjectId().Hex()
		return h.Service.insertVatReport(report)
	}

	if err != nil {
		return err
	}

	report.Id = vr.Id
	return h.Service.updateVatReport(report)

}

func (h *vatReportProcessor) processVatTransactionsForPeriod(country *billing.Country) error {
	/*if !country.VatEnabled {
		return errorVatReportNotEnabledForCountry
	}

	if country.VatCurrencyRatesPolicy == pkg.VatCurrencyRatesPolicyOnDay {
		return nil
	}

	if country.VatCurrencyRatesPolicy == pkg.VatCurrencyRatesPolicyAvgMonth {
		return errorVatReportRatesPolicyNotImplemented
	}

	from, to, err := h.Service.getVatReportTimeForDate(country.VatPeriodMonth, h.date)
	if err != nil {
		return err
	}

	vts, err := h.Service.getVatTransactions(from, to, country.IsoCodeA2)
	if err != nil {
		return err
	}

	if len(vts) == 0 {
		return nil
	}

	isLastDay := h.date.Unix() == to.Unix()

	for _, vt := range vts {
		if vt.LocalCurrency != vt.TransactionCurrency {
			vt.TransactionAmount, err = h.exchangeAmount(vt.TransactionCurrency, vt.LocalCurrency, vt.TransactionAmount)
			if err != nil {
				return err
			}
		}
		if vt.LocalCurrency != vt.VatCurrency {
			vt.VatAmount, err = h.exchangeAmount(vt.VatCurrency, vt.LocalCurrency, vt.VatAmount)
			if err != nil {
				return err
			}
		}
		if vt.LocalCurrency != vt.FeesCurrency {
			vt.FeesAmount, err = h.exchangeAmount(vt.FeesCurrency, vt.LocalCurrency, vt.FeesAmount)
			if err != nil {
				return err
			}
		}

		if isLastDay {
			vt.LocalAmountsApproximate = false
		}

		err = h.Service.updateVatTransaction(vt)
		if err != nil {
			return err
		}
	}*/

	return nil
}

func (h *vatReportProcessor) exchangeAmount(from, to string, amount float64) (float64, error) {
	req := &currencies.ExchangeCurrencyByDateCommonRequest{
		From:     from,
		To:       to,
		RateType: curPkg.RateTypeOxr,
		Amount:   amount,
		Datetime: h.ts,
	}

	rsp, err := h.Service.curService.ExchangeCurrencyByDateCommon(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return 0, errorVatReportCurrencyExchangeFailed
	}
	return rsp.ExchangedAmount, nil
}
