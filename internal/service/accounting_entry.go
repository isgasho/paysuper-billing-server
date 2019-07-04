package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"go.uber.org/zap"
)

const (
	errorFieldService = "service"
	errorFieldMethod  = "method"
	errorFieldRequest = "request"

	accountingEntryErrorCodeOrderNotFound         = "ae00001"
	accountingEntryErrorCodeRefundNotFound        = "ae00002"
	accountingEntryErrorCodeMerchantNotFound      = "ae00003"
	accountingEntryErrorCodeUnknown               = "ae00004"
	accountingEntryErrorCodeCommissionNotFound    = "ae00005"
	accountingEntryErrorCodeExchangeFailed        = "ae00006"
	accountingEntryErrorCodeGetExchangeRateFailed = "ae00007"
	accountingEntryErrorCodeUnknownEntry          = "ae00008"
	accountingEntryErrorCodeVatCurrencyConflict   = "ae00009"

	accountingEntryErrorTextOrderNotFound         = "Order not found for creating accounting entry"
	accountingEntryErrorTextRefundNotFound        = "Refund not found for creating accounting entry"
	accountingEntryErrorTextMerchantNotFound      = "Merchant not found for creating accounting entry"
	accountingEntryErrorTextUnknown               = "unknown error. try request later"
	accountingEntryErrorTextCommissionNotFound    = "Commission to merchant and payment method not found"
	accountingEntryErrorTextExchangeFailed        = "Currency exchange failed"
	accountingEntryErrorTextGetExchangeRateFailed = "Get exchange rate for currencies pair failed"
	accountingEntryErrorTextUnknownEntry          = "Unknown accounting entry type"
	accountingEntryErrorTextVatCurrencyConflict   = "vat transaction currency conflict"

	collectionAccountingEntry = "accounting_entry"

	accountingEventTypePayment    = "payment"
	accountingEventTypeRefund     = "refund"
	accountingEventTypeChageback  = "chargeback"
	accountingEventTypeSettlement = "settlement"
)

var (
	availableAccountingEntry = map[string]bool{
		pkg.AccountingEntryTypeRealGrossRevenue:                    true,
		pkg.AccountingEntryTypeRealTaxFee:                          true,
		pkg.AccountingEntryTypeCentralBankTaxFee:                   true,
		pkg.AccountingEntryTypePsGrossRevenueFx:                    true,
		pkg.AccountingEntryTypePsGrossRevenueFxTaxFee:              true,
		pkg.AccountingEntryTypePsGrossRevenueFxProfit:              true,
		pkg.AccountingEntryTypeMerchantGrossRevenue:                true,
		pkg.AccountingEntryTypeMerchantTaxFeeCostValue:             true,
		pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx:         true,
		pkg.AccountingEntryTypeMerchantTaxFee:                      true,
		pkg.AccountingEntryTypePsMethodFee:                         true,
		pkg.AccountingEntryTypeMerchantMethodFee:                   true,
		pkg.AccountingEntryTypeMerchantMethodFeeCostValue:          true,
		pkg.AccountingEntryTypePsMarkupMerchantMethodFee:           true,
		pkg.AccountingEntryTypeMerchantMethodFixedFee:              true,
		pkg.AccountingEntryTypeRealMerchantMethodFixedFee:          true,
		pkg.AccountingEntryTypeMarkupMerchantMethodFixedFeeFx:      true,
		pkg.AccountingEntryTypeRealMerchantMethodFixedFeeCostValue: true,
		pkg.AccountingEntryTypePsMethodFixedFeeProfit:              true,
		pkg.AccountingEntryTypeMerchantPsFixedFee:                  true,
		pkg.AccountingEntryTypeRealMerchantPsFixedFee:              true,
		pkg.AccountingEntryTypeMarkupMerchantPsFixedFee:            true,
		pkg.AccountingEntryTypePsMethodProfit:                      true,
		pkg.AccountingEntryTypeMerchantNetRevenue:                  true,
	}

	/*availableAccountingEntry = map[string]func(h *accountingEntry) error{
		pkg.AccountingEntryTypePayment:                    func(h *accountingEntry) error { return h.payment() },
		pkg.AccountingEntryTypePsMarkupPaymentFx:          func(h *accountingEntry) error { return h.psMarkupPaymentFx() },
		pkg.AccountingEntryTypeMethodFee:                  func(h *accountingEntry) error { return h.methodFee() },
		pkg.AccountingEntryTypePsMarkupMethodFee:          func(h *accountingEntry) error { return h.psMarkupMethodFee() },
		pkg.AccountingEntryTypeMethodFixedFee:             func(h *accountingEntry) error { return h.methodFixedFee() },
		pkg.AccountingEntryTypePsMarkupMethodFixedFee:     func(h *accountingEntry) error { return h.psMarkupMethodFixedFee() },
		pkg.AccountingEntryTypePsFee:                      func(h *accountingEntry) error { return h.psFee() },
		pkg.AccountingEntryTypePsFixedFee:                 func(h *accountingEntry) error { return h.psFixedFee() },
		pkg.AccountingEntryTypePsMarkupFixedFeeFx:         func(h *accountingEntry) error { return h.psMarkupFixedFeeFx() },
		pkg.AccountingEntryTypeTaxFee:                     func(h *accountingEntry) error { return h.taxFee() },
		pkg.AccountingEntryTypePsTaxFxFee:                 func(h *accountingEntry) error { return h.psTaxFxFee() },
		pkg.AccountingEntryTypeRefund:                     func(h *accountingEntry) error { return h.refundEntry() },
		pkg.AccountingEntryTypeRefundFee:                  func(h *accountingEntry) error { return h.refundFee() },
		pkg.AccountingEntryTypeRefundFixedFee:             func(h *accountingEntry) error { return h.refundFixedFee() },
		pkg.AccountingEntryTypePsMarkupRefundFx:           func(h *accountingEntry) error { return h.psMarkupRefundFx() },
		pkg.AccountingEntryTypeRefundBody:                 func(h *accountingEntry) error { return h.refundBody() },
		pkg.AccountingEntryTypeReverseTaxFee:              func(h *accountingEntry) error { return h.reverseTaxFee() },
		pkg.AccountingEntryTypePsMarkupReverseTaxFee:      func(h *accountingEntry) error { return h.psMarkupReverseTaxFee() },
		pkg.AccountingEntryTypeReverseTaxFeeDelta:         func(h *accountingEntry) error { return h.reverseTaxFeeDelta() },
		pkg.AccountingEntryTypePsReverseTaxFeeDelta:       func(h *accountingEntry) error { return h.psReverseTaxFeeDelta() },
		pkg.AccountingEntryTypeChargeback:                 func(h *accountingEntry) error { return h.chargeback() },
		pkg.AccountingEntryTypePsMarkupChargebackFx:       func(h *accountingEntry) error { return h.psMarkupChargebackFx() },
		pkg.AccountingEntryTypeChargebackFee:              func(h *accountingEntry) error { return h.chargebackFee() },
		pkg.AccountingEntryTypePsMarkupChargebackFee:      func(h *accountingEntry) error { return h.psMarkupChargebackFee() },
		pkg.AccountingEntryTypeChargebackFixedFee:         func(h *accountingEntry) error { return h.chargebackFixedFee() },
		pkg.AccountingEntryTypePsMarkupChargebackFixedFee: func(h *accountingEntry) error { return h.psMarkupChargebackFixedFee() },
		pkg.AccountingEntryTypeRefundFailure:              func(h *accountingEntry) error { return h.refundFailure() },
		pkg.AccountingEntryTypeChargebackFailure:          func(h *accountingEntry) error { return h.chargebackFailure() },
		pkg.AccountingEntryTypePsAdjustment:               func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePsAdjustment) },
		pkg.AccountingEntryTypeAdjustment:                 func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypeAdjustment) },
		pkg.AccountingEntryTypeReserved:                   func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypeReserved) },
		pkg.AccountingEntryTypePayout:                     func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePayout) },
		pkg.AccountingEntryTypeTaxPayout:                  func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypeTaxPayout) },
		pkg.AccountingEntryTypePayoutFee:                  func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePayoutFee) },
		pkg.AccountingEntryTypePayoutTaxFee:               func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePayoutTaxFee) },
		pkg.AccountingEntryTypePsMarkupPayoutFee:          func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePsMarkupPayoutFee) },
		pkg.AccountingEntryTypePayoutFailure:              func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePayoutFailure) },
		pkg.AccountingEntryTypeTaxPayoutFailure:           func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypeTaxPayoutFailure) },
		pkg.AccountingEntryTypePayoutCancel:               func(h *accountingEntry) error { return h.createEntry(pkg.AccountingEntryTypePayoutCancel) },
	}*/

	accountingEntryErrorOrderNotFound         = newBillingServerErrorMsg(accountingEntryErrorCodeOrderNotFound, accountingEntryErrorTextOrderNotFound)
	accountingEntryErrorRefundNotFound        = newBillingServerErrorMsg(accountingEntryErrorCodeRefundNotFound, accountingEntryErrorTextRefundNotFound)
	accountingEntryErrorMerchantNotFound      = newBillingServerErrorMsg(accountingEntryErrorCodeMerchantNotFound, accountingEntryErrorTextMerchantNotFound)
	accountingEntryErrorCommissionNotFound    = newBillingServerErrorMsg(accountingEntryErrorCodeCommissionNotFound, accountingEntryErrorTextCommissionNotFound)
	accountingEntryErrorExchangeFailed        = newBillingServerErrorMsg(accountingEntryErrorCodeExchangeFailed, accountingEntryErrorTextExchangeFailed)
	accountingEntryErrorGetExchangeRateFailed = newBillingServerErrorMsg(accountingEntryErrorCodeGetExchangeRateFailed, accountingEntryErrorTextGetExchangeRateFailed)
	accountingEntryErrorUnknownEntry          = newBillingServerErrorMsg(accountingEntryErrorCodeUnknownEntry, accountingEntryErrorTextUnknownEntry)
	accountingEntryErrorUnknown               = newBillingServerErrorMsg(accountingEntryErrorCodeUnknown, accountingEntryErrorTextUnknown)
	accountingEntryErrorVatCurrencyConflict   = newBillingServerErrorMsg(accountingEntryErrorCodeVatCurrencyConflict, accountingEntryErrorTextVatCurrencyConflict)
	/*
		onPaymentAccountingEntries = []string{
			pkg.AccountingEntryTypePayment,
			pkg.AccountingEntryTypePsMarkupPaymentFx,
			pkg.AccountingEntryTypeMethodFee,
			pkg.AccountingEntryTypePsMarkupMethodFee,
			pkg.AccountingEntryTypeMethodFixedFee,
			pkg.AccountingEntryTypePsMarkupMethodFixedFee,
			pkg.AccountingEntryTypePsFee,
			pkg.AccountingEntryTypePsFixedFee,
			pkg.AccountingEntryTypePsMarkupFixedFeeFx,
			pkg.AccountingEntryTypeTaxFee,
		}

		onRefundAccountingEntries = []string{
			pkg.AccountingEntryTypeRefund,
			pkg.AccountingEntryTypeRefundFee,
			pkg.AccountingEntryTypeRefundFixedFee,
			pkg.AccountingEntryTypePsMarkupRefundFx,
			pkg.AccountingEntryTypeRefundBody,
			pkg.AccountingEntryTypeReverseTaxFee,
			pkg.AccountingEntryTypePsMarkupReverseTaxFee,
			pkg.AccountingEntryTypeReverseTaxFeeDelta,
			pkg.AccountingEntryTypePsReverseTaxFeeDelta,
		}

		onChargebackAccountingEntries = []string{
			pkg.AccountingEntryTypeChargeback,
			pkg.AccountingEntryTypePsMarkupChargebackFx,
			pkg.AccountingEntryTypeChargebackFee,
			pkg.AccountingEntryTypePsMarkupChargebackFee,
			pkg.AccountingEntryTypeChargebackFixedFee,
			pkg.AccountingEntryTypePsMarkupChargebackFixedFee,
			pkg.AccountingEntryTypeReverseTaxFee,
			pkg.AccountingEntryTypePsMarkupReverseTaxFee,
			pkg.AccountingEntryTypeReverseTaxFeeDelta,
			pkg.AccountingEntryTypePsReverseTaxFeeDelta,
		}

		vatAmountAccountingEntries = []string{
			pkg.AccountingEntryTypePayment,
			pkg.AccountingEntryTypeRefund,
			pkg.AccountingEntryTypeChargeback,
		}

		vatTaxAccountingEntries = []string{
			pkg.AccountingEntryTypeTaxFee,
			pkg.AccountingEntryTypeReverseTaxFee,
			pkg.AccountingEntryTypeReverseTaxFeeDelta,
		}

		vatFeesAccountingEntries = []string{
			pkg.AccountingEntryTypeMethodFee,
			pkg.AccountingEntryTypeMethodFixedFee,
			pkg.AccountingEntryTypePsFee,
			pkg.AccountingEntryTypePsFixedFee,
			pkg.AccountingEntryTypeRefundFee,
			pkg.AccountingEntryTypeRefundFixedFee,
			pkg.AccountingEntryTypeChargebackFee,
			pkg.AccountingEntryTypeChargebackFixedFee,
		}

		vatAccountingEntries = map[string][]string{
			"amounts": vatAmountAccountingEntries,
			"fees":    vatFeesAccountingEntries,
			"taxes":   vatTaxAccountingEntries,
		}*/
)

type vatAmount struct {
	Currency string
	Amount   float64
}

type accountingEntry struct {
	*Service
	ctx context.Context

	order             *billing.Order
	refund            *billing.Refund
	merchant          *billing.Merchant
	country           *billing.Country
	accountingEntries []interface{}
	req               *grpc.CreateAccountingEntryRequest
}

func (s *Service) CreateAccountingEntry(
	ctx context.Context,
	req *grpc.CreateAccountingEntryRequest,
	rsp *grpc.CreateAccountingEntryResponse,
) error {
	handler := &accountingEntry{Service: s, req: req, ctx: ctx}

	if req.OrderId != "" && bson.IsObjectIdHex(req.OrderId) == true {
		order, err := s.getOrderById(req.OrderId)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorOrderNotFound

			return nil
		}

		handler.order = order
	}

	if req.RefundId != "" && bson.IsObjectIdHex(req.RefundId) == true {
		refund, err := s.getRefundById(req.RefundId)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorRefundNotFound

			return nil
		}

		order, err := s.getOrderById(refund.Order.Id)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorOrderNotFound

			return nil
		}

		handler.order = order
		handler.refund = refund
	}

	if req.MerchantId != "" && bson.IsObjectIdHex(req.MerchantId) == true {
		merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorMerchantNotFound

			return nil
		}

		handler.merchant = merchant
	}

	if handler.order != nil && handler.order.RoyaltyData == nil {
		handler.order.RoyaltyData = &billing.RoyaltyData{}
	}

	// todo: refactor here

	/*
		fn, ok := availableAccountingEntry[req.Type]

		if !ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = accountingEntryErrorUnknownEntry

			return nil
		}

		_ = fn(handler)
		err := handler.saveAccountingEntries()

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = accountingEntryErrorUnknown

			return nil
		}

		err = handler.updateVatTransaction()

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = accountingEntryErrorUnknown

			return nil
		}*/

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = handler.accountingEntries[0].(*billing.AccountingEntry)

	return nil
}

func (s *Service) onPaymentNotify(ctx context.Context, order *billing.Order) error {
	if order.RoyaltyData == nil {
		order.RoyaltyData = &billing.RoyaltyData{}
	}

	country, err := s.country.GetByIsoCodeA2(order.GetCountry())
	if err != nil {
		return err
	}

	handler := &accountingEntry{
		Service: s,
		order:   order,
		ctx:     ctx,
		country: country,
	}

	return s.processEvent(handler, accountingEventTypePayment)
}

func (s *Service) onRefundNotify(ctx context.Context, refund *billing.Refund, order *billing.Order) error {

	country, err := s.country.GetByIsoCodeA2(order.GetCountry())
	if err != nil {
		return err
	}

	handler := &accountingEntry{
		Service: s,
		refund:  refund,
		order:   order,
		ctx:     ctx,
		country: country,
	}

	if refund.IsChargeback == true {
		return s.processEvent(handler, accountingEventTypeChageback)
	}

	return s.processEvent(handler, accountingEventTypeRefund)
}

func (s *Service) processEvent(handler *accountingEntry, eventType string) error {

	var err error

	switch eventType {
	case accountingEventTypePayment:
		err = handler.processPaymentEvent()
		break

	case accountingEventTypeRefund:
		// err = handler.processRefundEvent()
		break

	case accountingEventTypeChageback:
		// err = handler.processChargebackEvent()
		break

	case accountingEventTypeSettlement:
		// err = handler.processSettlementEvent()
		break

	default:
		return accountingEntryErrorUnknown
	}

	if err != nil {
		return err
	}

	err = handler.saveAccountingEntries()
	if err != nil {
		return err
	}

	return handler.createVatTransaction()
}

func (h *accountingEntry) processPaymentEvent() error {
	var (
		amount float64
		err    error
	)

	// 1. realGrossRevenue
	realGrossRevenue := h.newEntry(pkg.AccountingEntryTypeRealGrossRevenue)
	realGrossRevenue.Amount, err = h.GetExchangePsCurrentCommon(h.order.Currency, h.order.TotalPaymentAmount)
	if err != nil {
		return err
	}
	realGrossRevenue.OriginalAmount = h.order.TotalPaymentAmount
	realGrossRevenue.OriginalCurrency = h.order.Currency
	if err = h.addEntry(realGrossRevenue); err != nil {
		return err
	}

	// 2. realTaxFee
	realTaxFee := h.newEntry(pkg.AccountingEntryTypeRealTaxFee)
	realTaxFee.Amount, err = h.GetExchangePsCurrentCommon(h.order.Tax.Currency, h.order.Tax.Amount)
	if err != nil {
		return err
	}
	realTaxFee.OriginalAmount = h.order.Tax.Amount
	realTaxFee.OriginalCurrency = h.order.Tax.Currency
	if err = h.addEntry(realTaxFee); err != nil {
		return err
	}

	// 3. centralBankTaxFee
	centralBankTaxFee := h.newEntry(pkg.AccountingEntryTypeCentralBankTaxFee)
	centralBankTaxFee.Amount = 0
	if err = h.addEntry(centralBankTaxFee); err != nil {
		return err
	}

	// 4. psGrossRevenueFx
	psGrossRevenueFx := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFx)
	amount, err = h.GetExchangePsCurrentMerchant(h.order.Currency, h.order.TotalPaymentAmount)
	if err != nil {
		return err
	}
	psGrossRevenueFx.Amount = amount - realGrossRevenue.Amount
	if err = h.addEntry(psGrossRevenueFx); err != nil {
		return err
	}

	// 5. psGrossRevenueFxTaxFee
	psGrossRevenueFxTaxFee := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFxTaxFee)
	psGrossRevenueFxTaxFee.Amount = psGrossRevenueFx.Amount * h.country.VatRate
	if err = h.addEntry(psGrossRevenueFxTaxFee); err != nil {
		return err
	}

	// 6. psGrossRevenueFxProfit
	psGrossRevenueFxProfit := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFxProfit)
	psGrossRevenueFxProfit.Amount = psGrossRevenueFx.Amount - psGrossRevenueFxTaxFee.Amount
	if err = h.addEntry(psGrossRevenueFxProfit); err != nil {
		return err
	}

	// 7. merchantGrossRevenue
	merchantGrossRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantGrossRevenue)
	merchantGrossRevenue.Amount = realGrossRevenue.Amount - psGrossRevenueFx.Amount
	if err = h.addEntry(merchantGrossRevenue); err != nil {
		return err
	}

	// 8. merchantTaxFeeCostValue
	merchantTaxFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFeeCostValue)
	merchantTaxFeeCostValue.Amount = merchantGrossRevenue.Amount * h.country.VatRate
	if err = h.addEntry(merchantTaxFeeCostValue); err != nil {
		return err
	}

	// 9. merchantTaxFeeCentralBankFx
	merchantTaxFeeCentralBankFx := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx)
	amount, err = h.GetExchangeCbCurrentMerchant(h.order.Tax.Currency, h.order.Tax.Amount)
	if err != nil {
		return err
	}
	amount, err = h.GetExchangeStockCurrentCommon(h.country.VatCurrency, amount)
	if err != nil {
		return err
	}
	merchantTaxFeeCentralBankFx.Amount = amount - merchantTaxFeeCostValue.Amount
	if err = h.addEntry(merchantTaxFeeCentralBankFx); err != nil {
		return err
	}

	// 10. merchantTaxFee
	merchantTaxFee := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFee)
	merchantTaxFee.Amount = merchantTaxFeeCostValue.Amount + merchantTaxFeeCentralBankFx.Amount
	if err = h.addEntry(merchantTaxFee); err != nil {
		return err
	}

	paymentChannelCostMerchant, err := h.getPaymentChannelCostMerchant(realGrossRevenue.Amount)
	if err != nil {
		return err
	}

	paymentChannelCostSystem, err := h.getPaymentChannelCostSystem()
	if err != nil {
		return err
	}

	// 11. psMethodFee
	psMethodFee := h.newEntry(pkg.AccountingEntryTypePsMethodFee)
	psMethodFee.Amount = merchantGrossRevenue.Amount * paymentChannelCostMerchant.PsPercent
	if err = h.addEntry(psMethodFee); err != nil {
		return err
	}

	// 12. merchantMethodFee
	merchantMethodFee := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFee)
	merchantMethodFee.Amount = merchantGrossRevenue.Amount * paymentChannelCostMerchant.MethodPercent
	if err = h.addEntry(merchantMethodFee); err != nil {
		return err
	}

	// 13. merchantMethodFeeCostValue
	merchantMethodFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFeeCostValue)
	merchantMethodFeeCostValue.Amount = realGrossRevenue.Amount * paymentChannelCostSystem.Percent
	if err = h.addEntry(merchantMethodFeeCostValue); err != nil {
		return err
	}

	// 14. psMarkupMerchantMethodFee
	psMarkupMerchantMethodFee := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFeeCostValue)
	psMarkupMerchantMethodFee.Amount = merchantMethodFee.Amount - merchantMethodFeeCostValue.Amount
	if err = h.addEntry(psMarkupMerchantMethodFee); err != nil {
		return err
	}

	// 15. merchantMethodFixedFee
	merchantMethodFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFixedFee)
	merchantMethodFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(paymentChannelCostMerchant.MethodFixAmountCurrency, paymentChannelCostMerchant.MethodFixAmount)
	if err = h.addEntry(merchantMethodFixedFee); err != nil {
		return err
	}

	// 16. realMerchantMethodFixedFee
	realMerchantMethodFixedFee := h.newEntry(pkg.AccountingEntryTypeRealMerchantMethodFixedFee)
	realMerchantMethodFixedFee.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostMerchant.MethodFixAmountCurrency, paymentChannelCostMerchant.MethodFixAmount)
	if err = h.addEntry(realMerchantMethodFixedFee); err != nil {
		return err
	}

	// 17. psMarkupMerchantMethodFee
	markupMerchantMethodFixedFeeFx := h.newEntry(pkg.AccountingEntryTypeMarkupMerchantMethodFixedFeeFx)
	markupMerchantMethodFixedFeeFx.Amount = merchantMethodFixedFee.Amount - realMerchantMethodFixedFee.Amount
	if err = h.addEntry(markupMerchantMethodFixedFeeFx); err != nil {
		return err
	}

	// 18. realMerchantMethodFixedFeeCostValue
	realMerchantMethodFixedFeeCostValue := h.newEntry(pkg.AccountingEntryTypeRealMerchantMethodFixedFeeCostValue)
	realMerchantMethodFixedFeeCostValue.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostSystem.FixAmountCurrency, paymentChannelCostSystem.FixAmount)
	if err != nil {
		return err
	}
	if err = h.addEntry(realMerchantMethodFixedFeeCostValue); err != nil {
		return err
	}

	// 19. psMarkupMerchantMethodFee
	psMethodFixedFeeProfit := h.newEntry(pkg.AccountingEntryTypePsMethodFixedFeeProfit)
	psMethodFixedFeeProfit.Amount = realMerchantMethodFixedFee.Amount - realMerchantMethodFixedFeeCostValue.Amount
	if err = h.addEntry(psMethodFixedFeeProfit); err != nil {
		return err
	}

	// 20. merchantPsFixedFee
	merchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantPsFixedFee)
	merchantPsFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(paymentChannelCostMerchant.PsFixedFeeCurrency, paymentChannelCostMerchant.PsFixedFee)
	if err != nil {
		return err
	}
	if err = h.addEntry(merchantPsFixedFee); err != nil {
		return err
	}

	// 21. realMerchantPsFixedFee
	realMerchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeRealMerchantPsFixedFee)
	realMerchantPsFixedFee.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostMerchant.PsFixedFeeCurrency, paymentChannelCostMerchant.PsFixedFee)
	if err != nil {
		return err
	}
	if err = h.addEntry(realMerchantPsFixedFee); err != nil {
		return err
	}

	// 22. psMarkupMerchantMethodFee
	markupMerchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeMarkupMerchantPsFixedFee)
	markupMerchantPsFixedFee.Amount = merchantPsFixedFee.Amount - realMerchantPsFixedFee.Amount
	if err = h.addEntry(markupMerchantPsFixedFee); err != nil {
		return err
	}

	// 23. psMethodProfit
	psMethodProfit := h.newEntry(pkg.AccountingEntryTypePsMethodProfit)
	psMethodProfit.Amount = psMethodFee.Amount - merchantMethodFeeCostValue.Amount - realMerchantMethodFixedFeeCostValue.Amount + merchantPsFixedFee.Amount
	if err = h.addEntry(psMethodProfit); err != nil {
		return err
	}

	// 24. merchantNetRevenue
	merchantNetRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantNetRevenue)
	merchantNetRevenue.Amount = realGrossRevenue.Amount - merchantTaxFee.Amount - psMethodFee.Amount - merchantPsFixedFee.Amount
	if err = h.addEntry(merchantNetRevenue); err != nil {
		return err
	}

	return nil
}

func (h *accountingEntry) GetExchangePsCurrentCommon(from string, amount float64) (float64, error) {
	to := h.order.GetMerchantRoyaltyCurrency()

	if to == from {
		return amount, nil
	}
	return h.GetExchangeCurrentCommon(&currencies.ExchangeCurrencyCurrentCommonRequest{
		From:     from,
		To:       to,
		RateType: curPkg.RateTypePaysuper,
		Amount:   amount,
	})
}

func (h *accountingEntry) GetExchangeStockCurrentCommon(from string, amount float64) (float64, error) {
	to := h.order.GetMerchantRoyaltyCurrency()

	if to == from {
		return amount, nil
	}
	return h.GetExchangeCurrentCommon(&currencies.ExchangeCurrencyCurrentCommonRequest{
		From:     from,
		To:       to,
		RateType: curPkg.RateTypeStock,
		Amount:   amount,
	})
}

func (h *accountingEntry) GetExchangePsCurrentMerchant(from string, amount float64) (float64, error) {
	to := h.order.GetMerchantRoyaltyCurrency()

	if to == from {
		return amount, nil
	}

	return h.GetExchangeCurrentMerchant(&currencies.ExchangeCurrencyCurrentForMerchantRequest{
		From:       from,
		To:         to,
		RateType:   curPkg.RateTypePaysuper,
		MerchantId: h.order.GetMerchantId(),
		Amount:     amount,
	})
}

func (h *accountingEntry) GetExchangeCbCurrentMerchant(from string, amount float64) (float64, error) {
	to := h.country.VatCurrency

	if to == from {
		return amount, nil
	}

	return h.GetExchangeCurrentMerchant(&currencies.ExchangeCurrencyCurrentForMerchantRequest{
		From:       from,
		To:         to,
		RateType:   curPkg.RateTypeCentralbanks,
		MerchantId: h.order.GetMerchantId(),
		Amount:     amount,
	})
}

func (h *accountingEntry) GetExchangeCurrentMerchant(req *currencies.ExchangeCurrencyCurrentForMerchantRequest) (float64, error) {

	rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchantRequest"),
			zap.Any(errorFieldRequest, req),
		)

		return 0, accountingEntryErrorExchangeFailed
	}

	return rsp.ExchangedAmount, nil
}

func (h *accountingEntry) GetExchangeCurrentCommon(req *currencies.ExchangeCurrencyCurrentCommonRequest) (float64, error) {
	rsp, err := h.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return 0, accountingEntryErrorExchangeFailed
	}

	return rsp.ExchangedAmount, nil
}

func (h *accountingEntry) addEntry(entry *billing.AccountingEntry) error {
	if entry.OriginalAmount == 0 && entry.OriginalCurrency == "" {
		entry.OriginalAmount = entry.Amount
		entry.OriginalCurrency = entry.Currency
	}
	if entry.LocalAmount == 0 && entry.LocalCurrency == "" && entry.Country != "" {
		if h.country.Currency == entry.OriginalCurrency {
			entry.LocalAmount = entry.OriginalAmount
			entry.LocalCurrency = h.country.Currency
		} else {
			req := &currencies.ExchangeCurrencyCurrentCommonRequest{
				From:     entry.OriginalCurrency,
				To:       h.country.Currency,
				RateType: curPkg.RateTypeOxr,
				Amount:   entry.OriginalAmount,
			}

			rsp, err := h.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "CurrencyRatesService"),
					zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
					zap.Any(errorFieldRequest, req),
				)

				return accountingEntryErrorExchangeFailed
			} else {
				entry.LocalAmount = rsp.ExchangedAmount
				entry.LocalCurrency = h.country.Currency
			}
		}
	}

	entry.Amount = toPrecise(entry.Amount)
	entry.OriginalAmount = toPrecise(entry.OriginalAmount)
	entry.LocalAmount = toPrecise(entry.LocalAmount)

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) saveAccountingEntries() error {
	if h.order != nil {
		err := h.db.Collection(collectionOrder).UpdateId(bson.ObjectIdHex(h.order.Id), h.order)

		if err != nil {
			zap.L().Error(
				"Order update failed",
				zap.Error(err),
				zap.Any("data", h.order),
			)

			return err
		}
	}

	err := h.db.Collection(collectionAccountingEntry).Insert(h.accountingEntries...)

	if err != nil {
		zap.L().Error(
			"Accounting entries insert failed",
			zap.Error(err),
			zap.Any("accounting_entries", h.accountingEntries),
		)

		return err
	}

	return nil
}

func (h *accountingEntry) newEntry(entryType string) *billing.AccountingEntry {

	var source *billing.AccountingEntrySource
	if h.refund != nil {
		source = &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		}
	} else {
		source = &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		}
	}

	return &billing.AccountingEntry{
		Id:         bson.NewObjectId().Hex(),
		Object:     pkg.ObjectTypeBalanceTransaction,
		Type:       entryType,
		Source:     source,
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
		Country:    h.country.IsoCodeA2,
		Currency:   h.order.GetMerchantRoyaltyCurrency(),
	}
}

/*func (h *accountingEntry) createEntry(entryType string) error {
	if h.merchant == nil {
		return accountingEntryErrorMerchantNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   entryType,
		Source: &billing.AccountingEntrySource{
			Id:   h.merchant.Id,
			Type: collectionMerchant,
		},
		MerchantId: h.merchant.Id,
	}
	h.mapRequestToEntry(entry)
	if err := h.addEntry(entry); err != nil {
		return err
	}

	return nil
}

func (h *accountingEntry) mapRequestToEntry(entry *billing.AccountingEntry) {
	entry.Amount = h.req.Amount
	entry.Currency = h.req.Currency
	entry.Reason = h.req.Reason
	entry.Status = h.req.Status

	entry.OriginalAmount = entry.OriginalAmount
	entry.OriginalCurrency = entry.OriginalCurrency
	entry.LocalAmount = entry.LocalAmount
	entry.LocalCurrency = entry.LocalCurrency

	t := time.Unix(h.req.Date, 0)
	entry.CreatedAt, _ = ptypes.TimestampProto(t)
}*/

func (h *accountingEntry) getPaymentChannelCostSystem() (*billing.PaymentChannelCostSystem, error) {
	name, err := h.order.GetCostPaymentMethodName()
	if err != nil {
		return nil, err
	}

	cost, err := h.Service.paymentChannelCostSystem.Get(name, h.country.Region, h.country.IsoCodeA2)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return nil, accountingEntryErrorCommissionNotFound
	}
	return cost, nil
}

func (h *accountingEntry) getPaymentChannelCostMerchant(amount float64) (*billing.PaymentChannelCostMerchant, error) {
	name, err := h.order.GetCostPaymentMethodName()
	if err != nil {
		return nil, err
	}

	req := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     h.order.GetMerchantId(),
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		Amount:         amount,
		Region:         h.country.Region,
		Country:        h.country.IsoCodeA2,
	}
	cost, err := h.Service.getPaymentChannelCostMerchant(req)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return nil, accountingEntryErrorCommissionNotFound
	}
	return cost, nil
}

func (h *accountingEntry) getMoneyBackCostMerchant(reason string) (*billing.MoneyBackCostMerchant, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	paymentAt, _ := ptypes.Timestamp(h.order.PaymentMethodOrderClosedAt)
	refundAt, _ := ptypes.Timestamp(h.refund.CreatedAt)

	data := &billing.MoneyBackCostMerchantRequest{
		MerchantId:     h.order.GetMerchantId(),
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		UndoReason:     reason,
		Region:         h.country.Region,
		Country:        h.country.IsoCodeA2,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
	}
	return h.Service.getMoneyBackCostMerchant(data)
}

func (h *accountingEntry) getMoneyBackCostSystem(reason string) (*billing.MoneyBackCostSystem, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	paymentAt, _ := ptypes.Timestamp(h.order.PaymentMethodOrderClosedAt)
	refundAt, _ := ptypes.Timestamp(h.refund.CreatedAt)

	data := &billing.MoneyBackCostSystemRequest{
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		Region:         h.country.Region,
		Country:        h.country.IsoCodeA2,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
		UndoReason:     reason,
	}
	return h.Service.getMoneyBackCostSystem(data)
}

func (h *accountingEntry) createVatTransaction() error {
	/*	order := h.order

		if order == nil {
			return nil
		}

		country, err := h.Service.country.GetByIsoCodeA2(order.GetCountry())
		if err != nil {
			return errorCountryNotFound
		}

		if !country.VatEnabled {
			return nil
		}

		t := &billing.VatTransaction{
			Id:                      bson.NewObjectId().Hex(),
			OrderId:                 order.Id,
			TransactionId:           order.Transaction,
			BillingAddressCriteria:  "user", // todo?
			UserId:                  order.User.Id,
			PaymentMethod:           order.PaymentMethod.Name,
			BillingAddress:          order.User.Address,
			Country:                 country.IsoCodeA2,
			LocalCurrency:           country.Currency,
			LocalAmountsApproximate: country.VatCurrencyRatesPolicy != pkg.VatCurrencyRatesPolicyOnDay,
		}
		if order.BillingAddress != nil {
			t.BillingAddressCriteria = "form"
			t.BillingAddress = order.BillingAddress
		}

		multiplier := float64(1)

		if h.refund != nil {
			multiplier = float64(-1)

			if h.refund.IsChargeback {
				t.TransactionType = pkg.VatTransactionTypeChargeback
			} else {
				t.TransactionType = pkg.VatTransactionTypeRefund
			}
			t.DateTime = h.refund.UpdatedAt

			orderPaidAt, err := ptypes.Timestamp(order.PaymentMethodOrderClosedAt)
			if err != nil {
				return err
			}

			from, _, err := h.Service.getLastVatReportTime(country.VatPeriodMonth)
			if err != nil {
				return err
			}

			t.IsDeduction = orderPaidAt.Unix() < from.Unix()

		} else {
			t.TransactionType = pkg.VatTransactionTypePayment
			t.DateTime = order.PaymentMethodOrderClosedAt
		}

		vatAmounts := make(map[string]*vatAmount, len(vatAccountingEntries))
		for key := range vatAccountingEntries {
			vatAmounts[key] = &vatAmount{}
		}

		for _, e := range h.accountingEntries {
			entry := e.(*billing.AccountingEntry)

			for key, entriesTypes := range vatAccountingEntries {
				if contains(entriesTypes, entry.Type) {
					amount := entry.OriginalAmount
					currency := entry.OriginalCurrency
					if key == "fees" {
						amount = entry.Amount
						currency = entry.Currency
					}

					if vatAmounts[key].Currency != "" && vatAmounts[key].Currency != currency {
						return accountingEntryErrorVatCurrencyConflict
					}

					vatAmounts[key].Amount += amount
					vatAmounts[key].Currency = currency
				}
			}
		}

		t.TransactionAmount = toPrecise(vatAmounts["amounts"].Amount * multiplier)
		t.TransactionCurrency = vatAmounts["amounts"].Currency

		t.VatAmount = toPrecise(vatAmounts["taxes"].Amount * multiplier)
		t.VatCurrency = vatAmounts["taxes"].Currency

		t.FeesAmount = toPrecise(vatAmounts["fees"].Amount) // we newer returns fees?
		t.FeesCurrency = vatAmounts["fees"].Currency

		if t.TransactionCurrency == country.Currency {
			t.LocalTransactionAmount = t.TransactionAmount
		} else {
			req := &currencies.ExchangeCurrencyCurrentCommonRequest{
				From:     t.TransactionCurrency,
				To:       country.Currency,
				RateType: curPkg.RateTypeOxr,
				Amount:   t.TransactionAmount,
			}

			rsp, err := h.Service.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "CurrencyRatesService"),
					zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
					zap.Any(errorFieldRequest, req),
				)

				return accountingEntryErrorGetExchangeRateFailed
			} else {
				t.LocalTransactionAmount = toPrecise(rsp.ExchangedAmount)
			}
		}

		if t.VatCurrency == country.Currency {
			t.LocalVatAmount = t.VatAmount
		} else {
			req := &currencies.ExchangeCurrencyCurrentCommonRequest{
				From:     t.VatCurrency,
				To:       country.Currency,
				RateType: curPkg.RateTypeOxr,
				Amount:   t.VatAmount,
			}

			rsp, err := h.Service.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "CurrencyRatesService"),
					zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
					zap.Any(errorFieldRequest, req),
				)

				return accountingEntryErrorGetExchangeRateFailed
			} else {
				t.LocalVatAmount = toPrecise(rsp.ExchangedAmount)
			}
		}

		if t.FeesCurrency == country.Currency {
			t.LocalFeesAmount = t.FeesAmount
		} else {
			req := &currencies.ExchangeCurrencyCurrentCommonRequest{
				From:     t.FeesCurrency,
				To:       country.Currency,
				RateType: curPkg.RateTypeOxr,
				Amount:   t.FeesAmount,
			}

			rsp, err := h.Service.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "CurrencyRatesService"),
					zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
					zap.Any(errorFieldRequest, req),
				)

				return accountingEntryErrorGetExchangeRateFailed
			} else {
				t.LocalFeesAmount = toPrecise(rsp.ExchangedAmount)
			}
		}

		return h.Service.insertVatTransaction(t)
	*/
	return nil
}

func (h *accountingEntry) updateVatTransaction() error {
	return nil
}
