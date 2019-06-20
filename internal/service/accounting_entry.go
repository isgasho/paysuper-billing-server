package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

var (
	availableAccountingEntry = map[string]func() ([]*billing.AccountingEntry, error){
		pkg.AccountingEntryTypePayment:                (accountingEntry{}).payment,
		pkg.AccountingEntryTypePsMarkupPaymentFx:      (accountingEntry{}).psMarkupPaymentFx,
		pkg.AccountingEntryTypeMethodFee:              (accountingEntry{}).methodFee,
		pkg.AccountingEntryTypePsMarkupMethodFee:      (accountingEntry{}).psMarkupMethodFee,
		pkg.AccountingEntryTypeMethodFixedFee:         (accountingEntry{}).methodFixedFee,
		pkg.AccountingEntryTypePsMarkupMethodFixedFee: (accountingEntry{}).psMarkupMethodFixedFee,
		pkg.AccountingEntryTypePsFee:                  (accountingEntry{}).psFee,
		pkg.AccountingEntryTypePsFixedFee:             (accountingEntry{}).psFixedFee,
		pkg.AccountingEntryTypePsMarkupFixedFeeFx:     (accountingEntry{}).psMarkupFixedFeeFx,
		pkg.AccountingEntryTypeTaxFee:                 (accountingEntry{}).taxFee,
		pkg.AccountingEntryTypePsTaxFxFee:             (accountingEntry{}).psTaxFxFee,
	}
)

type accountingEntry struct {
	order *billing.Order
}

func (s *Service) CreateAccountingEntry(
	ctx context.Context,
	req *grpc.CreateAccountingEntryRequest,
	rsp *grpc.CreateAccountingEntryRequest,
) error {

}

func (h *accountingEntry) payment() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psMarkupPaymentFx() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) methodFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psMarkupMethodFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) methodFixedFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psMarkupMethodFixedFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psFixedFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psMarkupFixedFeeFx() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) taxFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}

func (h *accountingEntry) psTaxFxFee() ([]*billing.AccountingEntry, error) {
	return nil, nil
}
