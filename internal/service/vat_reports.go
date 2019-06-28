package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"time"
)

const (
	collectionVatTransactions = "vat_transactions"
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
