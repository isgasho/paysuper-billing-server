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
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"time"
)

const (
	errorFieldService = "service"
	errorFieldMethod  = "method"
	errorFieldEntry   = "entry"
	errorFieldRequest = "request"

	collectionAccountingEntry = "accounting_entry"

	accountingEventTypePayment          = "payment"
	accountingEventTypeRefund           = "refund"
	accountingEventTypeSettlement       = "settlement"
	accountingEventTypeManualCorrection = "manual-correction"
)

var (
	accountingEntryErrorOrderNotFound              = newBillingServerErrorMsg("ae00001", "order not found for creating accounting entry")
	accountingEntryErrorRefundNotFound             = newBillingServerErrorMsg("ae00002", "refund not found for creating accounting entry")
	accountingEntryErrorMerchantNotFound           = newBillingServerErrorMsg("ae00003", "merchant not found for creating accounting entry")
	accountingEntryErrorMerchantCommissionNotFound = newBillingServerErrorMsg("ae00004", "commission to merchant and payment method not found")
	accountingEntryErrorExchangeFailed             = newBillingServerErrorMsg("ae00005", "currency exchange failed")
	accountingEntryErrorUnknownEntry               = newBillingServerErrorMsg("ae00006", "unknown accounting entry type")
	accountingEntryErrorUnknown                    = newBillingServerErrorMsg("ae00007", "unknown error. try request later")
	accountingEntryErrorRefundExceedsOrderAmount   = newBillingServerErrorMsg("ae00008", "refund exceeds order amount")
	accountingEntryErrorCountryNotFound            = newBillingServerErrorMsg("ae00009", "country not found")
	accountingEntryUnknownEvent                    = newBillingServerErrorMsg("ae00010", "accounting unknown event")
	accountingEntryErrorUnknownSourceType          = newBillingServerErrorMsg("ae00011", "unknown accounting entry source type")
	accountingEntryErrorInvalidSourceId            = newBillingServerErrorMsg("ae00012", "accounting entry invalid source id")
	accountingEntryErrorSystemCommissionNotFound   = newBillingServerErrorMsg("ae00013", "system commission for payment method not found")
	accountingEntryAlreadyCreated                  = newBillingServerErrorMsg("ae00014", "accounting entries already created")

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
		pkg.AccountingEntryTypeRealRefundTaxFee:                    true,
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

	availableAccountingEntriesSourceTypes = map[string]bool{
		collectionOrder:    true,
		collectionRefund:   true,
		collectionMerchant: true,
	}
)

type accountingEntry struct {
	*Service
	ctx context.Context

	order             *billing.Order
	refund            *billing.Refund
	refundOrder       *billing.Order
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

		order, err := s.getOrderById(refund.OriginalOrder.Id)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = accountingEntryErrorOrderNotFound

			return nil
		}

		refundOrder, err := s.getOrderById(refund.CreatedOrderId)
		if err != nil {
			return err
		}

		handler.order = order
		handler.refund = refund
		handler.refundOrder = refundOrder
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
		countryCode = merchant.Company.Country
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

	refundOrder, err := s.getOrderById(refund.CreatedOrderId)
	if err != nil {
		return err
	}

	handler := &accountingEntry{
		Service:     s,
		refund:      refund,
		order:       order,
		refundOrder: refundOrder,
		ctx:         ctx,
		country:     country,
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
		err = handler.processRefundEvent()
		break

	case accountingEventTypeManualCorrection:
		err = handler.processManualCorrectionEvent()
		break

	case accountingEventTypeSettlement:
		// err = handler.processSettlementEvent()
		break

	default:
		return accountingEntryUnknownEvent
	}

	if err != nil {
		return err
	}

	return handler.saveAccountingEntries()
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

	query := bson.M{
		"object":      pkg.ObjectTypeBalanceTransaction,
		"source.id":   bson.ObjectIdHex(h.order.Id),
		"source.type": collectionOrder,
	}
	var aes []*billing.AccountingEntry
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).All(&aes)
	foundCount := len(aes)
	if foundCount > 0 {
		zap.S().Error(
			accountingEntryAlreadyCreated.Message,
			zap.Error(err),
			zap.String("source.type", collectionOrder),
			zap.String("source.id", h.order.Id),
			zap.Int("entries found", foundCount),
		)
		return accountingEntryAlreadyCreated
		// todo: is there must be an update of existing entry, instead of error?
	}

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
	// calculated in order_view

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
	psGrossRevenueFxTaxFee.Amount = psGrossRevenueFx.Amount / (1 + h.order.Tax.Rate) * h.order.Tax.Rate
	if err = h.addEntry(psGrossRevenueFxTaxFee); err != nil {
		return err
	}

	// 7. psGrossRevenueFxProfit
	// calculated in order_view

	// 8. merchantGrossRevenue
	merchantGrossRevenue := h.newEntry(pkg.AccountingEntryTypeMerchantGrossRevenue)
	merchantGrossRevenue.Amount = realGrossRevenue.Amount - psGrossRevenueFx.Amount
	// not store in DB - calculated in order_view, but used further in the method code

	// 9. merchantTaxFeeCostValue
	merchantTaxFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantTaxFeeCostValue)
	merchantTaxFeeCostValue.Amount = merchantGrossRevenue.Amount / (1 + h.order.Tax.Rate) * h.order.Tax.Rate
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
	// calculated in order_view

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
	// calculated in order_view

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
	// calculated in order_view

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
	// calculated in order_view

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
	// calculated in order_view

	// 24. psMethodProfit
	// calculated in order_view

	// 25. merchantNetRevenue
	// calculated in order_view

	// 26. psProfitTotal
	// calculated in order_view

	return nil
}

func (h *accountingEntry) processRefundEvent() error {
	var (
		err error
	)

	query := bson.M{
		"object":      pkg.ObjectTypeBalanceTransaction,
		"source.id":   bson.ObjectIdHex(h.refund.CreatedOrderId),
		"source.type": collectionRefund,
	}
	var aes []*billing.AccountingEntry
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).All(&aes)
	foundCount := len(aes)
	if foundCount > 0 {
		zap.S().Error(
			accountingEntryAlreadyCreated.Message,
			zap.Error(err),
			zap.String("source.type", collectionRefund),
			zap.String("source.id", h.refund.CreatedOrderId),
			zap.Int("entries found", foundCount),
		)
		return accountingEntryAlreadyCreated
		// todo: is there must be an update of existing entry, instead of error?
	}

	// info: reversal rates are applied after the transaction has been physically processed by the payment method
	// but refund is the return of payment _before_ of the transaction was physically processed by the payment method.
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
	// todo: check for past partial refunds for a given order?

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

	// 2. realRefundTaxFee
	realTaxFee := h.newEntry("")
	query = bson.M{
		"object":      pkg.ObjectTypeBalanceTransaction,
		"type":        pkg.AccountingEntryTypeRealTaxFee,
		"source.id":   bson.ObjectIdHex(h.order.Id),
		"source.type": collectionOrder,
	}
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).One(&realTaxFee)
	if err != nil {
		return err
	}
	realRefundTaxFee := h.newEntry(pkg.AccountingEntryTypeRealRefundTaxFee)
	realRefundTaxFee.Amount = realTaxFee.Amount * partialRefundCorrection
	realRefundTaxFee.Currency = realTaxFee.Currency
	realRefundTaxFee.OriginalAmount = realTaxFee.OriginalAmount * partialRefundCorrection
	realRefundTaxFee.OriginalCurrency = realTaxFee.OriginalCurrency

	// fills with original values, if not deduction, to substract the same vat amount that was added on payment
	// otherwise local values will be automatically re-calculated with exchange rates for current vat period
	if !h.refundOrder.IsVatDeduction {
		realRefundTaxFee.LocalAmount = realTaxFee.LocalAmount * partialRefundCorrection
		realRefundTaxFee.LocalCurrency = realTaxFee.LocalCurrency
	}

	if err = h.addEntry(realRefundTaxFee); err != nil {
		return err
	}

	// 3. realRefundFee
	realRefundFee := h.newEntry(pkg.AccountingEntryTypeRealRefundFee)
	realRefundFee.Amount = realRefund.Amount * moneyBackCostSystem.Percent
	if err = h.addEntry(realRefundFee); err != nil {
		return err
	}

	// 4. realRefundFixedFee
	realRefundFixedFee := h.newEntry(pkg.AccountingEntryTypeRealRefundFixedFee)
	realRefundFixedFee.Amount, err = h.GetExchangePsCurrentCommon(moneyBackCostSystem.PayoutCurrency, moneyBackCostSystem.FixAmount)
	if err = h.addEntry(realRefundFixedFee); err != nil {
		return err
	}

	// 5. merchantRefund
	merchantRefund := h.newEntry(pkg.AccountingEntryTypeMerchantRefund)
	merchantRefund.Amount, err = h.GetExchangePsCurrentMerchant(h.refund.Currency, h.refund.Amount)
	if err != nil {
		return err
	}
	if err = h.addEntry(merchantRefund); err != nil {
		return err
	}

	// 6. psMerchantRefundFx
	// calculated in order_view

	// 7. merchantRefundFee
	merchantRefundFee := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFee)
	if moneyBackCostMerchant.IsPaidByMerchant {
		merchantRefundFee.Amount = merchantRefund.Amount * moneyBackCostMerchant.Percent
	}
	if err = h.addEntry(merchantRefundFee); err != nil {
		return err
	}

	// 8. psMarkupMerchantRefundFee
	// calculated in order_view

	merchantRefundFixedFeeCostValue := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFixedFeeCostValue)
	merchantRefundFixedFee := h.newEntry(pkg.AccountingEntryTypeMerchantRefundFixedFee)

	if moneyBackCostMerchant.IsPaidByMerchant {

		// 9. merchantRefundFixedFeeCostValue
		merchantRefundFixedFeeCostValue.Amount, err = h.GetExchangePsCurrentCommon(moneyBackCostMerchant.FixAmountCurrency, moneyBackCostMerchant.FixAmount)
		if err != nil {
			return err
		}

		// 10. merchantRefundFixedFee
		merchantRefundFixedFee.Amount, err = h.GetExchangePsCurrentMerchant(moneyBackCostMerchant.FixAmountCurrency, moneyBackCostMerchant.FixAmount)
		if err != nil {
			return err
		}

		// 11. psMerchantRefundFixedFeeFx
		// calculated in order_view
	}

	if err = h.addEntry(merchantRefundFixedFeeCostValue); err != nil {
		return err
	}
	if err = h.addEntry(merchantRefundFixedFee); err != nil {
		return err
	}

	// 12. psMerchantRefundFixedFeeProfit
	// calculated in order_view

	// 13. reverseTaxFee
	merchantTaxFeeCostValue := h.newEntry("")
	query["type"] = pkg.AccountingEntryTypeMerchantTaxFeeCostValue
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).One(&merchantTaxFeeCostValue)
	if err != nil {
		return err
	}

	merchantTaxFeeCentralBankFx := h.newEntry("")
	query["type"] = pkg.AccountingEntryTypeMerchantTaxFeeCentralBankFx
	err = h.Service.db.Collection(collectionAccountingEntry).Find(query).One(&merchantTaxFeeCentralBankFx)
	if err != nil {
		return err
	}

	reverseTaxFee := h.newEntry(pkg.AccountingEntryTypeReverseTaxFee)
	reverseTaxFee.Amount = (merchantTaxFeeCostValue.Amount + merchantTaxFeeCentralBankFx.Amount) * partialRefundCorrection
	reverseTaxFee.OriginalAmount = (merchantTaxFeeCostValue.OriginalAmount + merchantTaxFeeCentralBankFx.Amount) * partialRefundCorrection
	reverseTaxFee.OriginalCurrency = merchantTaxFeeCostValue.OriginalCurrency
	reverseTaxFee.LocalAmount = (merchantTaxFeeCostValue.LocalAmount + merchantTaxFeeCentralBankFx.Amount) * partialRefundCorrection
	reverseTaxFee.LocalCurrency = merchantTaxFeeCostValue.LocalCurrency
	if err = h.addEntry(reverseTaxFee); err != nil {
		return err
	}

	// 14. reverseTaxFeeDelta
	// 15. psReverseTaxFeeDelta
	reverseTaxFeeDelta := h.newEntry(pkg.AccountingEntryTypeReverseTaxFeeDelta)
	psReverseTaxFeeDelta := h.newEntry(pkg.AccountingEntryTypePsReverseTaxFeeDelta)

	amount := reverseTaxFee.Amount - (merchantRefund.Amount / (1 + h.order.Tax.Rate) * h.order.Tax.Rate)
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

	// 16. merchantReverseTaxFee
	// calculated in order_view

	// 17. merchantReverseRevenue
	// calculated in order_view

	// 18. psRefundProfit
	// calculated in order_view

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
		Source:   h.country.VatCurrencyRatesSource,
		Amount:   amount,
	})
}

func (h *accountingEntry) GetExchangeCurrentMerchant(req *currencies.ExchangeCurrencyCurrentForMerchantRequest) (float64, error) {

	rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.S().Error(
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
		zap.S().Error(
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

	if _, ok := availableAccountingEntriesSourceTypes[entry.Source.Type]; !ok {
		return accountingEntryErrorUnknownSourceType
	}

	if entry.Source.Id == "" {
		return accountingEntryErrorInvalidSourceId
	}

	if entry.OriginalAmount == 0 && entry.OriginalCurrency == "" {
		entry.OriginalAmount = entry.Amount
		entry.OriginalCurrency = entry.Currency
	}
	if entry.LocalAmount == 0 && entry.LocalCurrency == "" && entry.Country != "" {
		var rateType string
		var rateSource string
		if h.country.VatEnabled {
			// Use VatCurrency as local currency, instead of country currency.
			// It because of some countries of EU,
			// that use national currencies but pays vat in euro
			entry.LocalCurrency = h.country.VatCurrency
			rateType = curPkg.RateTypeCentralbanks
			rateSource = h.country.VatCurrencyRatesSource
		} else {
			entry.LocalCurrency = h.country.Currency
			rateType = curPkg.RateTypeOxr
			rateSource = ""
		}

		if entry.LocalCurrency == entry.OriginalCurrency {
			entry.LocalAmount = entry.OriginalAmount
		} else {
			req := &currencies.ExchangeCurrencyCurrentCommonRequest{
				From:     entry.OriginalCurrency,
				To:       entry.LocalCurrency,
				RateType: rateType,
				Source:   rateSource,
				Amount:   entry.OriginalAmount,
			}

			rsp, err := h.curService.ExchangeCurrencyCurrentCommon(h.ctx, req)

			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "CurrencyRatesService"),
					zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
					zap.String(errorFieldEntry, entry.Type),
					zap.Any(errorFieldRequest, req),
				)

				return accountingEntryErrorExchangeFailed
			} else {
				entry.LocalAmount = rsp.ExchangedAmount
			}
		}
	}

	entry.Amount = tools.ToPrecise(entry.Amount)
	entry.OriginalAmount = tools.ToPrecise(entry.OriginalAmount)
	entry.LocalAmount = tools.ToPrecise(entry.LocalAmount)

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) saveAccountingEntries() error {
	err := h.db.Collection(collectionAccountingEntry).Insert(h.accountingEntries...)

	if err != nil {
		zap.S().Error(
			"Accounting entries insert failed",
			zap.Error(err),
			zap.Any("accounting_entries", h.accountingEntries),
		)

		return err
	}

	var ids []string
	if h.order != nil {
		ids = append(ids, h.order.Id)
	}

	if h.refund != nil && h.refundOrder != nil {
		ids = append(ids, h.refundOrder.Id)

	}

	if len(ids) == 0 {
		return nil
	}

	return h.Service.updateOrderView(ids)

}

func (h *accountingEntry) newEntry(entryType string) *billing.AccountingEntry {

	var (
		createdTime = ptypes.TimestampNow()
		source      *billing.AccountingEntrySource
	)
	if h.refund != nil {
		if h.refundOrder != nil {
			createdTime = h.refundOrder.PaymentMethodOrderClosedAt
		}
		source = &billing.AccountingEntrySource{
			Id:   h.refund.CreatedOrderId,
			Type: collectionRefund,
		}
	} else {
		createdTime = h.order.PaymentMethodOrderClosedAt
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
		CreatedAt:  createdTime,
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
		zap.S().Error(
			accountingEntryErrorSystemCommissionNotFound.Message,
			zap.Error(err),
			zap.String("payment_method", name),
			zap.String("region", h.country.Region),
			zap.String("country", h.country.IsoCodeA2),
		)

		return nil, accountingEntryErrorSystemCommissionNotFound
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
		zap.S().Error(
			accountingEntryErrorMerchantCommissionNotFound.Message,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return nil, accountingEntryErrorMerchantCommissionNotFound
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
