package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

var (
	availableAccountingEntry = map[string]func(h *accountingEntry) error{
		pkg.AccountingEntryTypePayment:                func(h *accountingEntry) error { return h.payment() },
		pkg.AccountingEntryTypePsMarkupPaymentFx:      func(h *accountingEntry) error { return h.psMarkupPaymentFx() },
		pkg.AccountingEntryTypeMethodFee:              func(h *accountingEntry) error { return h.methodFee() },
		pkg.AccountingEntryTypePsMarkupMethodFee:      func(h *accountingEntry) error { return h.psMarkupMethodFee() },
		pkg.AccountingEntryTypeMethodFixedFee:         func(h *accountingEntry) error { return h.methodFixedFee() },
		pkg.AccountingEntryTypePsMarkupMethodFixedFee: func(h *accountingEntry) error { return h.psMarkupMethodFixedFee() },
		pkg.AccountingEntryTypePsFee:                  func(h *accountingEntry) error { return h.psFee() },
		pkg.AccountingEntryTypePsFixedFee:             func(h *accountingEntry) error { return h.psFixedFee() },
		pkg.AccountingEntryTypePsMarkupFixedFeeFx:     func(h *accountingEntry) error { return h.psMarkupFixedFeeFx() },
		pkg.AccountingEntryTypeTaxFee:                 func(h *accountingEntry) error { return h.taxFee() },
		pkg.AccountingEntryTypePsTaxFxFee:             func(h *accountingEntry) error { return h.psTaxFxFee() },
	}
)

type accountingEntry struct {
	order             *billing.Order
	accountingEntries []interface{}
}

func (s *Service) CreateAccountingEntry(
	ctx context.Context,
	req *grpc.CreateAccountingEntryRequest,
	rsp *grpc.CreateAccountingEntryRequest,
) error {
	fn, ok := availableAccountingEntry[req.Type]

	if !ok {
		//вернуть ошибку
	}

	err := fn

	if err != nil {
		//вернуть ошибку
	}

	return nil
}

func (h *accountingEntry) payment() error {
	return nil
}

func (h *accountingEntry) psMarkupPaymentFx() error {
	return nil
}

func (h *accountingEntry) methodFee() error {
	return nil
}

func (h *accountingEntry) psMarkupMethodFee() error {
	return nil
}

func (h *accountingEntry) methodFixedFee() error {
	return nil
}

func (h *accountingEntry) psMarkupMethodFixedFee() error {
	return nil
}

func (h *accountingEntry) psFee() error {
	return nil
}

func (h *accountingEntry) psFixedFee() error {
	return nil
}

func (h *accountingEntry) psMarkupFixedFeeFx() error {
	return nil
}

func (h *accountingEntry) taxFee() error {
	return nil
}

func (h *accountingEntry) psTaxFxFee() error {
	return nil
}
