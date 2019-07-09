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
	"time"
)

const (
	errorFieldService = "service"
	errorFieldMethod  = "method"
	errorFieldRequest = "request"

	collectionAccountingEntry = "accounting_entry"

	accountingEventTypePayment          = "payment"
	accountingEventTypeRefund           = "refund"
	accountingEventTypeSettlement       = "settlement"
	accountingEventTypeManualCorrection = "manual-correction"
)

var (
	accountingEntryErrorOrderNotFound            = newBillingServerErrorMsg("ae00001", "order not found for creating accounting entry")
	accountingEntryErrorRefundNotFound           = newBillingServerErrorMsg("ae00002", "refund not found for creating accounting entry")
	accountingEntryErrorMerchantNotFound         = newBillingServerErrorMsg("ae00003", "merchant not found for creating accounting entry")
	accountingEntryErrorCommissionNotFound       = newBillingServerErrorMsg("ae00004", "commission to merchant and payment method not found")
	accountingEntryErrorExchangeFailed           = newBillingServerErrorMsg("ae00005", "currency exchange failed")
	accountingEntryErrorUnknownEntry             = newBillingServerErrorMsg("ae00006", "unknown accounting entry type")
	accountingEntryErrorUnknown                  = newBillingServerErrorMsg("ae00007", "unknown error. try request later")
	accountingEntryErrorRefundExceedsOrderAmount = newBillingServerErrorMsg("ae00008", "refund exceeds order amount")
	accountingEntryErrorCountryNotFound          = newBillingServerErrorMsg("ae00009", "country not found")
	accountingEntryUnknownEvent                  = newBillingServerErrorMsg("ae00010", "accounting unknown event")

	availableAccountingEntries = map[string]bool{
		pkg.AccountingEntryTypeRealGrossRevenue:                    true,
		pkg.AccountingEntryTypePsGrossRevenueFx:                    true,
		pkg.AccountingEntryTypeRealTaxFee:                          true,
		pkg.AccountingEntryTypeRealTaxFeeTotal:                     true,
		pkg.AccountingEntryTypeCentralBankTaxFee:                   true,
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
		pkg.AccountingEntryTypePsProfitTotal:                       true,
		pkg.AccountingEntryTypeRealRefund:                          true,
		pkg.AccountingEntryTypeRealRefundFee:                       true,
		pkg.AccountingEntryTypeRealRefundFixedFee:                  true,
		pkg.AccountingEntryTypeMerchantRefund:                      true,
		pkg.AccountingEntryTypePsMerchantRefundFx:                  true,
		pkg.AccountingEntryTypeMerchantRefundFee:                   true,
		pkg.AccountingEntryTypePsMarkupMerchantRefundFee:           true,
		pkg.AccountingEntryTypeMerchantRefundFixedFeeCostValue:     true,
		pkg.AccountingEntryTypeMerchantRefundFixedFee:              true,
		pkg.AccountingEntryTypePsMerchantRefundFixedFeeFx:          true,
		pkg.AccountingEntryTypePsMerchantRefundFixedFeeProfit:      true,
		pkg.AccountingEntryTypeReverseTaxFee:                       true,
		pkg.AccountingEntryTypeReverseTaxFeeDelta:                  true,
		pkg.AccountingEntryTypePsReverseTaxFeeDelta:                true,
		pkg.AccountingEntryTypeMerchantReverseTaxFee:               true,
		pkg.AccountingEntryTypeMerchantReverseRevenue:              true,
		pkg.AccountingEntryTypePsRefundProfit:                      true,
	}
)

/*type vatAmount struct {
	Currency string
	Amount   float64
}*/

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
	if _, ok := availableAccountingEntries[req.Type]; !ok {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = accountingEntryErrorUnknownEntry

		return nil
	}

	handler := &accountingEntry{Service: s, req: req, ctx: ctx}

	countryCode := ""

	if req.OrderId != "" && bson.IsObjectIdHex(req.OrderId) == true {
		order, err := s.getOrderById(req.OrderId)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorOrderNotFound

			return nil
		}

		handler.order = order
		countryCode = order.GetCountry()
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
		countryCode = order.GetCountry()
	}

	if req.MerchantId != "" && bson.IsObjectIdHex(req.MerchantId) == true {
		merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorMerchantNotFound

			return nil
		}

		handler.merchant = merchant
		countryCode = merchant.Country
	}

	if countryCode == "" {
		countryCode = req.Country
	}

	country, err := s.country.GetByIsoCodeA2(countryCode)
	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = accountingEntryErrorCountryNotFound

		return nil
	}

	handler.country = country

	err = s.processEvent(handler, accountingEventTypeManualCorrection)
	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = accountingEntryErrorUnknown

		return nil
	}

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

	return s.processEvent(handler, accountingEventTypeRefund)
}

func (s *Service) processEvent(handler *accountingEntry, eventType string) error {

	var err error

	createVatTransaction := false
	updateVatTransaction := false

	switch eventType {
	case accountingEventTypePayment:
		err = handler.processPaymentEvent()
		createVatTransaction = true
		break

	case accountingEventTypeRefund:
		err = handler.processRefundEvent()
		createVatTransaction = true
		break

	case accountingEventTypeManualCorrection:
		err = handler.processManualCorrectionEvent()
		break

	case accountingEventTypeSettlement:
		// err = handler.processSettlementEvent()
		updateVatTransaction = true
		break

	default:
		return accountingEntryUnknownEvent
	}

	if err != nil {
		return err
	}

	err = handler.saveAccountingEntries()
	if err != nil {
		return err
	}

	if createVatTransaction {
		return handler.createVatTransaction()
	}

	if updateVatTransaction {
		return handler.updateVatTransaction()
	}
	return nil
}

func (h *accountingEntry) processManualCorrectionEvent() error {
	var err error

	entry := h.newEntry(h.req.Type)

	entry.Amount = h.req.Amount
	entry.Currency = h.req.Currency
	entry.Reason = h.req.Reason
	entry.Status = h.req.Status
	t := time.Unix(h.req.Date, 0)
	entry.CreatedAt, err = ptypes.TimestampProto(t)
	if err != nil {
		return err
	}

	if h.merchant != nil {
		entry.Source.Type = collectionMerchant
		entry.Source.Id = h.merchant.Id
	}

	if err = h.addEntry(entry); err != nil {
		return err
	}

	return nil
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

	// 4. realTaxFeeTotal
	realTaxFeeTotal := h.newEntry(pkg.AccountingEntryTypeRealTaxFeeTotal)
	realTaxFeeTotal.Amount = realTaxFee.Amount + centralBankTaxFee.Amount
	if err = h.addEntry(realTaxFeeTotal); err != nil {
		return err
	}

	// 5. psGrossRevenueFx
	psGrossRevenueFx := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFx)
	amount, err = h.GetExchangePsCurrentMerchant(h.order.Currency, h.order.TotalPaymentAmount)
	if err != nil {
		return err
	}
	psGrossRevenueFx.Amount = amount - realGrossRevenue.Amount
	if err = h.addEntry(psGrossRevenueFx); err != nil {
		return err
	}

	// 6. psGrossRevenueFxTaxFee
	psGrossRevenueFxTaxFee := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFxTaxFee)
	psGrossRevenueFxTaxFee.Amount = psGrossRevenueFx.Amount * h.order.Tax.Rate
	if err = h.addEntry(psGrossRevenueFxTaxFee); err != nil {
		return err
	}

	// 7. psGrossRevenueFxProfit
	psGrossRevenueFxProfit := h.newEntry(pkg.AccountingEntryTypePsGrossRevenueFxProfit)
	psGrossRevenueFxProfit.Amount = psGrossRevenueFx.Amount - psGrossRevenueFxTaxFee.Amount
	if err = h.addEntry(psGrossRevenueFxProfit); err != nil {
		return err
	}

	// 8. merchantGrossRevenue
	merchantGrossRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantGrossRevenue)
	merchantGrossRevenue.Amount = realGrossRevenue.Amount - psGrossRevenueFx.Amount
	if err = h.addEntry(merchantGrossRevenue); err != nil {
		return err
	}

	// 9. merchantTaxFeeCostValue
	merchantTaxFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFeeCostValue)
	merchantTaxFeeCostValue.Amount = merchantGrossRevenue.Amount * h.order.Tax.Rate
	if err = h.addEntry(merchantTaxFeeCostValue); err != nil {
		return err
	}

	// 10. merchantTaxFeeCentralBankFx
	merchantTaxFeeCentralBankFx := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx)
	amount, err = h.GetExchangeCbCurrentCommon(h.order.GetMerchantRoyaltyCurrency(), merchantTaxFeeCostValue.Amount)
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

	// 11. merchantTaxFee
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

	// 12. psMethodFee
	psMethodFee := h.newEntry(pkg.AccountingEntryTypePsMethodFee)
	psMethodFee.Amount = merchantGrossRevenue.Amount * paymentChannelCostMerchant.PsPercent
	if err = h.addEntry(psMethodFee); err != nil {
		return err
	}

	// 13. merchantMethodFee
	merchantMethodFee := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFee)
	merchantMethodFee.Amount = merchantGrossRevenue.Amount * paymentChannelCostMerchant.MethodPercent
	if err = h.addEntry(merchantMethodFee); err != nil {
		return err
	}

	// 14. merchantMethodFeeCostValue
	merchantMethodFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFeeCostValue)
	merchantMethodFeeCostValue.Amount = realGrossRevenue.Amount * paymentChannelCostSystem.Percent
	if err = h.addEntry(merchantMethodFeeCostValue); err != nil {
		return err
	}

	// 15. psMarkupMerchantMethodFee
	psMarkupMerchantMethodFee := h.newEntry(pkg.AccountingEntryTypePsMarkupMerchantMethodFee)
	psMarkupMerchantMethodFee.Amount = merchantMethodFee.Amount - merchantMethodFeeCostValue.Amount
	if err = h.addEntry(psMarkupMerchantMethodFee); err != nil {
		return err
	}

	// 16. merchantMethodFixedFee
	merchantMethodFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantMethodFixedFee)
	merchantMethodFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(paymentChannelCostMerchant.MethodFixAmountCurrency, paymentChannelCostMerchant.MethodFixAmount)
	if err = h.addEntry(merchantMethodFixedFee); err != nil {
		return err
	}

	// 17. realMerchantMethodFixedFee
	realMerchantMethodFixedFee := h.newEntry(pkg.AccountingEntryTypeRealMerchantMethodFixedFee)
	realMerchantMethodFixedFee.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostMerchant.MethodFixAmountCurrency, paymentChannelCostMerchant.MethodFixAmount)
	if err = h.addEntry(realMerchantMethodFixedFee); err != nil {
		return err
	}

	// 18. markupMerchantMethodFixedFeeFx
	markupMerchantMethodFixedFeeFx := h.newEntry(pkg.AccountingEntryTypeMarkupMerchantMethodFixedFeeFx)
	markupMerchantMethodFixedFeeFx.Amount = merchantMethodFixedFee.Amount - realMerchantMethodFixedFee.Amount
	if err = h.addEntry(markupMerchantMethodFixedFeeFx); err != nil {
		return err
	}

	// 19. realMerchantMethodFixedFeeCostValue
	realMerchantMethodFixedFeeCostValue := h.newEntry(pkg.AccountingEntryTypeRealMerchantMethodFixedFeeCostValue)
	realMerchantMethodFixedFeeCostValue.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostSystem.FixAmountCurrency, paymentChannelCostSystem.FixAmount)
	if err != nil {
		return err
	}
	if err = h.addEntry(realMerchantMethodFixedFeeCostValue); err != nil {
		return err
	}

	// 20. psMethodFixedFeeProfit
	psMethodFixedFeeProfit := h.newEntry(pkg.AccountingEntryTypePsMethodFixedFeeProfit)
	psMethodFixedFeeProfit.Amount = realMerchantMethodFixedFee.Amount - realMerchantMethodFixedFeeCostValue.Amount
	if err = h.addEntry(psMethodFixedFeeProfit); err != nil {
		return err
	}

	// 21. merchantPsFixedFee
	merchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantPsFixedFee)
	merchantPsFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(paymentChannelCostMerchant.PsFixedFeeCurrency, paymentChannelCostMerchant.PsFixedFee)
	if err != nil {
		return err
	}
	if err = h.addEntry(merchantPsFixedFee); err != nil {
		return err
	}

	// 22. realMerchantPsFixedFee
	realMerchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeRealMerchantPsFixedFee)
	realMerchantPsFixedFee.Amount, err = h.GetExchangePsCurrentCommon(paymentChannelCostMerchant.PsFixedFeeCurrency, paymentChannelCostMerchant.PsFixedFee)
	if err != nil {
		return err
	}
	if err = h.addEntry(realMerchantPsFixedFee); err != nil {
		return err
	}

	// 23. markupMerchantPsFixedFee
	markupMerchantPsFixedFee := h.newEntry(pkg.AccountingEntryTypeMarkupMerchantPsFixedFee)
	markupMerchantPsFixedFee.Amount = merchantPsFixedFee.Amount - realMerchantPsFixedFee.Amount
	if err = h.addEntry(markupMerchantPsFixedFee); err != nil {
		return err
	}

	// 24. psMethodProfit
	psMethodProfit := h.newEntry(pkg.AccountingEntryTypePsMethodProfit)
	psMethodProfit.Amount = psMethodFee.Amount - merchantMethodFeeCostValue.Amount - realMerchantMethodFixedFeeCostValue.Amount + merchantPsFixedFee.Amount
	if err = h.addEntry(psMethodProfit); err != nil {
		return err
	}

	// 25. merchantNetRevenue
	merchantNetRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantNetRevenue)
	merchantNetRevenue.Amount = merchantGrossRevenue.Amount - merchantTaxFee.Amount - psMethodFee.Amount - merchantPsFixedFee.Amount
	if err = h.addEntry(merchantNetRevenue); err != nil {
		return err
	}

	// 26. psProfitTotal
	psProfitTotal := h.newEntry(pkg.AccountingEntryTypePsProfitTotal)
	psProfitTotal.Amount = psMethodProfit.Amount + psGrossRevenueFxProfit.Amount - centralBankTaxFee.Amount
	if err = h.addEntry(psProfitTotal); err != nil {
		return err
	}

	return nil
}

func (h *accountingEntry) processRefundEvent() error {
	var (
		err error
	)

	// info: reversal rates are applied after the transaction has been physically processed by the payment method
	// bur refund is the return of payment _before_ of the transaction was physically processed by the payment method.
	// Now, at this moment we can't determine that it is a refund or reversal
	// But we will be able to determine it after getting a settlement from Cardpay
	reason := "reversal"
	if h.refund.IsChargeback {
		reason = "chargeback"
	}
	moneyBackCostMerchant, err := h.getMoneyBackCostMerchant(reason)
	if err != nil {
		return err
	}

	moneyBackCostSystem, err := h.getMoneyBackCostSystem(reason)
	if err != nil {
		return err
	}

	partialRefundCorrection := h.refund.Amount / h.order.TotalPaymentAmount
	if partialRefundCorrection > 1 {
		return accountingEntryErrorRefundExceedsOrderAmount
	}
	// todo: check for past partial returns for a given order?

	// 1. realRefund
	realRefund := h.newEntry(pkg.AccountingEntryTypeRealRefund)
	realRefund.Amount, err = h.GetExchangePsCurrentCommon(h.refund.Currency, h.refund.Amount)
	if err != nil {
		return err
	}
	realRefund.OriginalAmount = h.refund.Amount
	realRefund.OriginalCurrency = h.refund.Currency
	if err = h.addEntry(realRefund); err != nil {
		return err
	}

	// 2. realRefundFee
	realRefundFee := h.newEntry(pkg.AccountingEntryTypeRealRefundFee)
	realRefundFee.Amount = realRefund.Amount * moneyBackCostSystem.Percent
	if err = h.addEntry(realRefundFee); err != nil {
		return err
	}

	// 3. realRefundFixedFee
	realRefundFixedFee := h.newEntry(pkg.AccountingEntryTypeRealRefundFixedFee)
	realRefundFixedFee.Amount, err = h.GetExchangePsCurrentCommon(moneyBackCostSystem.PayoutCurrency, moneyBackCostSystem.FixAmount)
	if err = h.addEntry(realRefundFixedFee); err != nil {
		return err
	}

	// 4. merchantRefund
	merchantRefund := h.newEntry(pkg.AccountingEntryTypeMerchantRefund)
	merchantRefund.Amount, err = h.GetExchangePsCurrentMerchant(h.refund.Currency, h.refund.Amount)
	if err != nil {
		return err
	}
	if err = h.addEntry(merchantRefund); err != nil {
		return err
	}

	// 5. psMerchantRefundFx
	psMerchantRefundFx := h.newEntry(pkg.AccountingEntryTypePsMerchantRefundFx)
	psMerchantRefundFx.Amount = merchantRefund.Amount - realRefund.Amount
	if err = h.addEntry(psMerchantRefundFx); err != nil {
		return err
	}

	// 6. merchantRefundFee
	merchantRefundFee := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFee)
	if moneyBackCostMerchant.IsPaidByMerchant {
		merchantRefundFee.Amount = merchantRefund.Amount * moneyBackCostMerchant.Percent
	}
	if err = h.addEntry(merchantRefundFee); err != nil {
		return err
	}

	// 7. psMarkupMerchantRefundFee
	psMarkupMerchantRefundFee := h.newEntry(pkg.AccountingEntryTypePsMarkupMerchantRefundFee)
	psMarkupMerchantRefundFee.Amount = merchantRefundFee.Amount - realRefundFee.Amount
	if err = h.addEntry(psMarkupMerchantRefundFee); err != nil {
		return err
	}

	merchantRefundFixedFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFixedFeeCostValue)
	merchantRefundFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFixedFee)
	psMerchantRefundFixedFeeFx := h.newEntry(pkg.AccountingEntryTypePsMerchantRefundFixedFeeFx)

	if moneyBackCostMerchant.IsPaidByMerchant {

		// 8. merchantRefundFixedFeeCostValue
		merchantRefundFixedFeeCostValue.Amount, err = h.GetExchangePsCurrentCommon(moneyBackCostMerchant.FixAmountCurrency, moneyBackCostMerchant.FixAmount)
		if err != nil {
			return err
		}

		// 9. merchantRefundFixedFee
		merchantRefundFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(moneyBackCostMerchant.FixAmountCurrency, moneyBackCostMerchant.FixAmount)
		if err != nil {
			return err
		}

		// 10. psMerchantRefundFixedFeeFx
		psMerchantRefundFixedFeeFx.Amount = merchantRefundFixedFee.Amount - merchantRefundFixedFeeCostValue.Amount
	}

	if err = h.addEntry(merchantRefundFixedFeeCostValue); err != nil {
		return err
	}
	if err = h.addEntry(merchantRefundFixedFee); err != nil {
		return err
	}
	if err = h.addEntry(psMerchantRefundFixedFeeFx); err != nil {
		return err
	}

	// 11. psMerchantRefundFixedFeeProfit
	psMerchantRefundFixedFeeProfit := h.newEntry(pkg.AccountingEntryTypePsMerchantRefundFixedFeeProfit)
	psMerchantRefundFixedFeeProfit.Amount = merchantRefundFixedFee.Amount - realRefundFixedFee.Amount
	if err = h.addEntry(psMerchantRefundFixedFeeProfit); err != nil {
		return err
	}

	// 12. reverseTaxFee
	merchantTaxFee := h.newEntry("")
	query := bson.M{
		"object":      pkg.ObjectTypeBalanceTransaction,
		"type":        pkg.AccountingEntryTypeMerchantTaxFee,
		"source.id":   bson.ObjectIdHex(h.order.Id),
		"source.type": collectionOrder,
	}
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).One(&merchantTaxFee)
	if err != nil {
		return err
	}
	reverseTaxFee := h.newEntry(pkg.AccountingEntryTypeReverseTaxFee)
	reverseTaxFee.Amount = merchantTaxFee.Amount * partialRefundCorrection
	if err = h.addEntry(reverseTaxFee); err != nil {
		return err
	}

	// 13. reverseTaxFeeDelta
	// 14. psReverseTaxFeeDelta
	reverseTaxFeeDelta := h.newEntry(pkg.AccountingEntryTypeReverseTaxFeeDelta)
	psReverseTaxFeeDelta := h.newEntry(pkg.AccountingEntryTypePsReverseTaxFeeDelta)

	amount := reverseTaxFee.Amount - (merchantRefund.Amount * h.order.Tax.Rate)
	if amount < 0 {
		psReverseTaxFeeDelta.Amount = -1 * amount
	} else {
		reverseTaxFeeDelta.Amount = amount
	}

	if err = h.addEntry(reverseTaxFeeDelta); err != nil {
		return err
	}
	if err = h.addEntry(psReverseTaxFeeDelta); err != nil {
		return err
	}

	// 15. merchantReverseTaxFee
	merchantReverseTaxFee := h.newEntry(pkg.AccountingEntryTypeMerchantReverseTaxFee)
	merchantReverseTaxFee.Amount = reverseTaxFee.Amount + reverseTaxFeeDelta.Amount
	if err = h.addEntry(merchantReverseTaxFee); err != nil {
		return err
	}

	// 16. merchantReverseRevenue
	merchantReverseRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantReverseRevenue)
	merchantReverseRevenue.Amount = merchantRefund.Amount + merchantRefundFee.Amount + merchantRefundFixedFee.Amount - merchantReverseTaxFee.Amount
	if err = h.addEntry(merchantReverseRevenue); err != nil {
		return err
	}

	// 17. psRefundProfit
	psRefundProfit := h.newEntry(pkg.AccountingEntryTypePsRefundProfit)
	psRefundProfit.Amount = psReverseTaxFeeDelta.Amount + psMerchantRefundFixedFeeProfit.Amount + psMarkupMerchantRefundFee.Amount
	if err = h.addEntry(psRefundProfit); err != nil {
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

func (h *accountingEntry) GetExchangeCbCurrentCommon(from string, amount float64) (float64, error) {
	to := h.country.VatCurrency

	if to == from {
		return amount, nil
	}

	return h.GetExchangeCurrentCommon(&currencies.ExchangeCurrencyCurrentCommonRequest{
		From:     from,
		To:       to,
		RateType: curPkg.RateTypeCentralbanks,
		Amount:   amount,
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
	if _, ok := availableAccountingEntries[entry.Type]; !ok {
		return accountingEntryErrorUnknownEntry
	}

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

func (h *accountingEntry) getPaymentChannelCostSystem() (*billing.PaymentChannelCostSystem, error) {
	name, err := h.order.GetCostPaymentMethodName()
	if err != nil {
		return nil, err
	}

	cost, err := h.Service.paymentChannelCostSystem.Get(name, h.country.Region, h.country.IsoCodeA2)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorCommissionNotFound.Message,
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
			accountingEntryErrorCommissionNotFound.Message,
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
