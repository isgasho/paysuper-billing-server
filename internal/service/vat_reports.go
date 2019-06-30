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

func (s *Service) createVatTransaction(order *billing.Order) error {
	country, err := s.country.GetByIsoCodeA2(order.GetCountry())
	// todo return country-not-found error here after merge with master
	if err != nil {
		return err
	}

	if !country.VatEnabled {
		return nil
	}

	t := &billing.VatTransaction{
		OrderId:                order.Id,
		TransactionId:          order.Transaction,
		BillingAddressCriteria: "user", // todo?
		UserId:                 order.User.Id,
		PaymentMethod:          order.PaymentMethod.Name,
		BillingAddress:         order.User.Address,
	}
	if order.BillingAddress != nil {
		t.BillingAddressCriteria = "form"
		t.BillingAddress = order.BillingAddress
	}

	if order.Refund != nil {
		t.TransactionType = "refund"
		t.TransactionAmount = order.Refund.Amount * -1
		t.TransactionCurrency = order.Refund.Currency
		t.VatAmount = order.Tax.Amount * -1
		t.VatCurrency = order.Tax.Currency
		// t.FeesAmount = order.PspFeeAmount  // todo
		// t.FeesCurrency = order.Currency  // todo
		t.DateTime = order.RefundedAt
		t.InitialTransactionDateTime = order.PaymentMethodOrderClosedAt

	} else {

		t.TransactionType = "payment"
		t.TransactionAmount = order.PaymentMethodIncomeAmount
		t.TransactionCurrency = order.PaymentMethodIncomeCurrency.CodeA3
		t.VatAmount = order.Tax.Amount
		t.VatCurrency = order.Tax.Currency
		// t.FeesAmount = order.PspFeeAmount  // todo
		// t.FeesCurrency = order.Currency  // todo
		t.DateTime = order.PaymentMethodOrderClosedAt
		t.InitialTransactionDateTime = order.PaymentMethodOrderClosedAt
	}

	if t.TransactionCurrency == country.Currency {
		t.LocalTransactionAmount = t.TransactionAmount
	} else {
		if country.VatCurrencyRatesPolicy == "on-day" {
			// todo: request today currency rate for TransactionAmount
		}
	}

	if t.VatCurrency == country.Currency {
		t.LocalVatAmount = t.VatAmount
	} else {
		if country.VatCurrencyRatesPolicy == "on-day" {
			// todo: request today currency rate for VatAmount
		}
	}

	if t.FeesCurrency == country.Currency {
		t.LocalFeesAmount = t.FeesAmount
	} else {
		if country.VatCurrencyRatesPolicy == "on-day" {
			// todo: request today currency rate for LocalFeesAmount
		}
	}

	return s.db.Collection(collectionVatTransactions).Insert(t)
}

func (s *Service) updateVatTransaction(t *billing.VatTransaction) error {
	return s.db.Collection(collectionVatTransactions).UpdateId(bson.ObjectIdHex(t.Id), t)
}

func (s *Service) getVatTransactions(
	from time.Time,
	to time.Time,
	country string,
	unexchanged bool,
	unsettled bool,
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

	if unexchanged {
		query["$or"] = []bson.M{{"local_transaction_amount": nil}, {"local_vat_amount": nil}, {"local_fees_amount": nil}}
	}

	if unsettled {
		query["is_settled"] = false
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
	// todo return country-not-found error here after merge with master
	if err != nil {
		return
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
	// todo return country-not-found error here after merge with master
	if err != nil {
		return
	}

	from = from.AddDate(0, -1*int(country.VatPeriodMonth)*count, 0)
	to = to.AddDate(0, -1*int(country.VatPeriodMonth)*count, 0)
	return
}
