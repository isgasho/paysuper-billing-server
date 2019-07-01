package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"time"
)

const (
	collectionVatTransactions = "vat_transactions"

	VatTransactionTypePayment    = "payment"
	VatTransactionTypeRefund     = "refund"
	VatTransactionTypeChargeback = "chargeback"
)

var (
	errorVatReportNotEnabledForCountry          = newBillingServerErrorMsg("vr000001", "vat not enabled for country")
	errorVatReportPeriodNotConfiguredForCountry = newBillingServerErrorMsg("vr000002", "vat period not configured for country")
)

func (s *Service) GetVatReportsDashboard(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.VatReportsResponse,
) error {
	return nil
}

func (s *Service) GetVatReportsForCountry(
	ctx context.Context,
	req *grpc.VatReportsRequest,
	res *grpc.VatReportsResponse,
) error {
	return nil
}

func (s *Service) GetVatReportTransactions(
	ctx context.Context,
	req *grpc.VatTransactionsRequest,
	res *grpc.VatTransactionsResponse,
) error {
	return nil
}

func (s *Service) getVatTransactions(
	from time.Time,
	to time.Time,
	country string,
) ([]*billing.VatTransaction, error) {

	query := bson.M{
		"date_time": bson.M{
			"$gte": bod(from),
			"$lte": eod(to),
		},
	}

	if country != "" {
		query["billing_address.country"] = country
	}

	var transactions []*billing.VatTransaction
	err := s.db.Collection(collectionVatTransactions).Find(query).All(&transactions)
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

func (s *Service) getLastVatReportTime(countryCode string) (from, to time.Time, err error) {
	country, err := s.country.GetByIsoCodeA2(countryCode)
	if err != nil {
		return from, to, errorCountryNotFound
	}

	if !country.VatEnabled {
		err = errorVatReportNotEnabledForCountry
		return
	}

	switch country.VatPeriodMonth {
	case 1:
		from = now.BeginningOfMonth()
		to = now.EndOfMonth()
		return
	case 2:
		from = now.BeginningOfMonth()
		to = now.EndOfMonth()

		if from.Month()%2 == 0 {
			from = from.AddDate(0, -1, 0)
		} else {
			to = to.AddDate(0, 1, 0)
		}
		return
	case 3:
		from = now.BeginningOfQuarter()
		to = now.EndOfQuarter()
		return
	}

	err = errorVatReportPeriodNotConfiguredForCountry
	return
}

func (s *Service) getPreviousVatReportTime(countryCode string, count int) (from, to time.Time, err error) {
	from, to, err = s.getLastVatReportTime(countryCode)
	if err != nil {
		return
	}
	country, err := s.country.GetByIsoCodeA2(countryCode)
	if err != nil {
		return from, to, errorCountryNotFound
	}

	from = from.AddDate(0, -1*int(country.VatPeriodMonth)*count, 0)
	to = to.AddDate(0, -1*int(country.VatPeriodMonth)*count, 0)
	return
}
