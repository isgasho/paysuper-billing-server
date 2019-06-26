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
	"math"
	"time"
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

	accountingEntryErrorTextOrderNotFound         = "Order not found for creating accounting entry"
	accountingEntryErrorTextRefundNotFound        = "Refund not found for creating accounting entry"
	accountingEntryErrorTextMerchantNotFound      = "Merchant not found for creating accounting entry"
	accountingEntryErrorTextUnknown               = "unknown error. try request later"
	accountingEntryErrorTextCommissionNotFound    = "Commission to merchant and payment method not found"
	accountingEntryErrorTextExchangeFailed        = "Currency exchange failed"
	accountingEntryErrorTextGetExchangeRateFailed = "Get exchange rate for currencies pair failed"
	accountingEntryErrorTextUnknownEntry          = "Unknown accounting entry type"

	collectionAccountingEntry = "accounting_entry"
)

var (
	availableAccountingEntry = map[string]func(h *accountingEntry) error{
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
	}

	accountingEntryErrorOrderNotFound         = newBillingServerErrorMsg(accountingEntryErrorCodeOrderNotFound, accountingEntryErrorTextOrderNotFound)
	accountingEntryErrorRefundNotFound        = newBillingServerErrorMsg(accountingEntryErrorCodeRefundNotFound, accountingEntryErrorTextRefundNotFound)
	accountingEntryErrorMerchantNotFound      = newBillingServerErrorMsg(accountingEntryErrorCodeMerchantNotFound, accountingEntryErrorTextMerchantNotFound)
	accountingEntryErrorCommissionNotFound    = newBillingServerErrorMsg(accountingEntryErrorCodeCommissionNotFound, accountingEntryErrorTextCommissionNotFound)
	accountingEntryErrorExchangeFailed        = newBillingServerErrorMsg(accountingEntryErrorCodeExchangeFailed, accountingEntryErrorTextExchangeFailed)
	accountingEntryErrorGetExchangeRateFailed = newBillingServerErrorMsg(accountingEntryErrorCodeGetExchangeRateFailed, accountingEntryErrorTextGetExchangeRateFailed)
	accountingEntryErrorUnknownEntry          = newBillingServerErrorMsg(accountingEntryErrorCodeUnknownEntry, accountingEntryErrorTextUnknownEntry)
	accountingEntryErrorUnknown               = newBillingServerErrorMsg(accountingEntryErrorCodeUnknown, accountingEntryErrorTextUnknown)

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
	}
)

type accountingEntry struct {
	*Service
	ctx context.Context

	order             *billing.Order
	refund            *billing.Refund
	merchant          *billing.Merchant
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

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = handler.accountingEntries[0].(*billing.AccountingEntry)

	return nil
}

func (s *Service) onPaymentNotify(ctx context.Context, order *billing.Order) error {
	if order.RoyaltyData == nil {
		order.RoyaltyData = &billing.RoyaltyData{}
	}

	handler := &accountingEntry{
		Service: s,
		order:   order,
		ctx:     ctx,
	}

	return s.processEvent(handler, onPaymentAccountingEntries)
}

func (s *Service) onRefundNotify(ctx context.Context, refund *billing.Refund, order *billing.Order) error {
	handler := &accountingEntry{
		Service: s,
		refund:  refund,
		order:   order,
		ctx:     ctx,
	}

	entries := onRefundAccountingEntries

	if refund.IsChargeback == true {
		entries = onChargebackAccountingEntries
	}

	return s.processEvent(handler, entries)
}

func (s *Service) processEvent(handler *accountingEntry, list []string) error {
	for _, v := range list {
		fn, ok := availableAccountingEntry[v]

		if !ok {
			return accountingEntryErrorUnknownEntry
		}

		err := fn(handler)

		if err != nil {
			return err
		}
	}

	return handler.saveAccountingEntries()
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

func (h *accountingEntry) payment() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePayment,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
		From:       h.order.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.GetMerchantId(),
		RateType:   curPkg.RateTypePaysuper,
		Amount:     h.order.TotalPaymentAmount,
	}

	rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorExchangeFailed
	}

	h.order.RoyaltyData.AmountInRoyaltyCurrency = rsp.ExchangedAmount

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupPaymentFx() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupPaymentFx,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       h.order.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.Project.MerchantId,
		RateType:   curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.order.Currency,
		To:       h.order.GetMerchantRoyaltyCurrency(),
		RateType: curPkg.RateTypePaysuper,
	}
	rsp1, err := h.curService.GetRateCurrentCommon(h.ctx, req1)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	entry.Amount = rsp1.Rate - rsp.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) methodFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMethodFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	commission, err := h.commission.GetByProjectIdAndMethod(h.order.GetProjectId(), h.order.GetPaymentMethodId())

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency = h.order.RoyaltyData.AmountInRoyaltyCurrency * commission.Fee
	h.order.RoyaltyData.MerchantTotalCommissionInRoyaltyCurrency = h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency

	entry.Amount = h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupMethodFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupMethodFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getPaymentChannelCostMerchant()

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	entry.Amount = h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency - (h.order.RoyaltyData.AmountInRoyaltyCurrency * cost.MethodPercent)
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) methodFixedFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeMethodFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	commission, err := h.commission.GetByProjectIdAndMethod(h.order.GetProjectId(), h.order.GetPaymentMethodId())

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency = commission.PerTransaction.Fee

	if commission.PerTransaction.Currency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       commission.PerTransaction.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     commission.PerTransaction.Fee,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency = rsp.ExchangedAmount
	}

	h.order.RoyaltyData.MerchantTotalCommissionInRoyaltyCurrency += h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency

	entry.Amount = h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupMethodFixedFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupMethodFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getPaymentChannelCostMerchant()

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	entry.Amount = h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency - cost.MethodFixAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getPaymentChannelCostMerchant()

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	amount := h.order.RoyaltyData.AmountInRoyaltyCurrency * cost.PsPercent

	entry.Amount = h.order.RoyaltyData.MerchantTotalCommissionInRoyaltyCurrency - amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psFixedFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getPaymentChannelCostMerchant()

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	amount := cost.PsFixedFee

	if cost.PsFixedFeeCurrency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     amount,
		}

		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		amount = rsp.ExchangedAmount
	}

	entry.Amount = amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupFixedFeeFx() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupFixedFeeFx,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getPaymentChannelCostMerchant()

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       cost.PsFixedFeeCurrency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.Project.MerchantId,
		RateType:   curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     cost.PsFixedFeeCurrency,
		To:       h.order.GetMerchantRoyaltyCurrency(),
		RateType: curPkg.RateTypePaysuper,
	}
	rsp1, err := h.curService.GetRateCurrentCommon(h.ctx, req1)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) taxFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	amount := h.order.Tax.Amount

	if h.order.Tax.Currency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Tax.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     amount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		amount = rsp.ExchangedAmount
	}

	h.order.RoyaltyData.PaymentTaxAmountInRoyaltyCurrency = amount

	entry.Amount = amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psTaxFxFee() error {
	if h.order == nil {
		return accountingEntryErrorOrderNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsTaxFxFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
		},
		MerchantId: h.order.GetMerchantId(),
	}
	h.mapRequestToEntry(entry)
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundEntry() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRefund,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
		From:       h.refund.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.GetMerchantId(),
		RateType:   curPkg.RateTypePaysuper,
		Amount:     h.refund.Amount,
	}

	rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorExchangeFailed
	}

	h.order.RoyaltyData.RefundTotalAmountInRoyaltyCurrency += rsp.ExchangedAmount

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRefundFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostMerchant(pkg.AccountingEntryTypeRefund)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	entry.Amount = h.refund.Amount * cost.Percent
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFixedFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRefundFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostMerchant(pkg.AccountingEntryTypeRefund)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	amount := cost.FixAmount

	if cost.FixAmountCurrency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       cost.FixAmountCurrency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     amount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		amount = rsp.ExchangedAmount
	}

	entry.Amount = amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupRefundFx() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupRefundFx,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       h.refund.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.Project.MerchantId,
		RateType:   curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.refund.Currency,
		To:       h.order.GetMerchantRoyaltyCurrency(),
		RateType: curPkg.RateTypePaysuper,
	}
	rsp1, err := h.curService.GetRateCurrentCommon(h.ctx, req1)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundBody() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRefundBody,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	refundAmount := h.refund.Amount

	if h.refund.Currency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Tax.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     refundAmount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		refundAmount = rsp.ExchangedAmount
	}

	taxAmount := h.order.Tax.Amount

	if h.order.Tax.Currency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Tax.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     taxAmount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		taxAmount = rsp.ExchangedAmount
	}

	entry.Amount = refundAmount - taxAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) reverseTaxFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeReverseTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	amount := h.order.Tax.Amount

	if h.order.Tax.Currency != h.order.GetMerchantRoyaltyCurrency() {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Tax.Currency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     amount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		amount = rsp.ExchangedAmount
	}

	h.order.RoyaltyData.RefundTaxAmountInRoyaltyCurrency = amount

	entry.Amount = amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupReverseTaxFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupReverseTaxFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       h.refund.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.Project.MerchantId,
		RateType:   curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.refund.Currency,
		To:       h.order.GetMerchantRoyaltyCurrency(),
		RateType: curPkg.RateTypePaysuper,
	}
	rsp1, err := h.curService.GetRateCurrentCommon(h.ctx, req1)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) reverseTaxFeeDelta() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeReverseTaxFeeDelta,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	amount := h.order.RoyaltyData.PaymentTaxAmountInRoyaltyCurrency - h.order.RoyaltyData.RefundTaxAmountInRoyaltyCurrency

	if amount <= 0 {
		return nil
	}

	entry.Amount = amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psReverseTaxFeeDelta() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsReverseTaxFeeDelta,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	amount := h.order.RoyaltyData.PaymentTaxAmountInRoyaltyCurrency - h.order.RoyaltyData.RefundTaxAmountInRoyaltyCurrency

	if amount > 0 {
		return nil
	}

	entry.Amount = math.Abs(amount)
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargeback() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargeback,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
		From:       h.order.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.GetMerchantId(),
		RateType:   curPkg.RateTypePaysuper,
		Amount:     h.order.TotalPaymentAmount,
	}

	rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorExchangeFailed
	}

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFx() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFx,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       h.order.Currency,
		To:         h.order.GetMerchantRoyaltyCurrency(),
		MerchantId: h.order.GetMerchantId(),
		RateType:   curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentForMerchant(h.ctx, req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentForMerchant"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.order.Currency,
		To:       h.order.GetMerchantRoyaltyCurrency(),
		RateType: curPkg.RateTypePaysuper,
	}
	rsp1, err := h.curService.GetRateCurrentCommon(h.ctx, req1)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommonRequest"),
			zap.Any(errorFieldRequest, req),
		)

		return accountingEntryErrorGetExchangeRateFailed
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargebackFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargebackFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Amount:     h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency,
		Currency:   h.order.GetMerchantRoyaltyCurrency(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostMerchant(pkg.AccountingEntryTypeChargeback)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency = h.order.TotalPaymentAmount * cost.Percent

	entry.Amount = h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostSystem(pkg.AccountingEntryTypeChargeback)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	entry.Amount = h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency - (h.order.TotalPaymentAmount * cost.Percent)
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargebackFixedFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargebackFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostMerchant(pkg.AccountingEntryTypeChargeback)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency = cost.FixAmount

	if h.order.GetMerchantRoyaltyCurrency() != cost.FixAmountCurrency {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       cost.FixAmountCurrency,
			To:         h.order.GetMerchantRoyaltyCurrency(),
			MerchantId: h.order.GetMerchantId(),
			RateType:   curPkg.RateTypePaysuper,
			Amount:     cost.FixAmount,
		}
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(h.ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return accountingEntryErrorExchangeFailed
		}

		h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency = rsp.ExchangedAmount
	}

	entry.Amount = h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFixedFee() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFixedFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
		Status:     pkg.BalanceTransactionStatusPending,
		CreatedAt:  ptypes.TimestampNow(),
	}

	if h.req != nil {
		h.mapRequestToEntry(entry)
		h.accountingEntries = append(h.accountingEntries, entry)

		return nil
	}

	cost, err := h.getMoneyBackCostSystem(pkg.AccountingEntryTypeChargeback)

	if err != nil {
		zap.L().Error(
			accountingEntryErrorTextCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return accountingEntryErrorCommissionNotFound
	}

	amount := cost.FixAmount

	entry.Amount = h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency - amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFailure() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeRefundFailure,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
	}
	h.mapRequestToEntry(entry)
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargebackFailure() error {
	if h.refund == nil {
		return accountingEntryErrorRefundNotFound
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargebackFailure,
		Source: &billing.AccountingEntrySource{
			Id:   h.refund.Id,
			Type: collectionRefund,
		},
		MerchantId: h.order.GetMerchantId(),
	}
	h.mapRequestToEntry(entry)
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) createEntry(entryType string) error {
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
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) mapRequestToEntry(entry *billing.AccountingEntry) {
	entry.Amount = h.req.Amount
	entry.Currency = h.req.Currency
	entry.Reason = h.req.Reason
	entry.Status = h.req.Status

	t := time.Unix(h.req.Date, 0)
	entry.CreatedAt, _ = ptypes.TimestampProto(t)
}

func (h *accountingEntry) getPaymentChannelCostMerchant() (*billing.PaymentChannelCostMerchant, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	userCountry := h.order.GetUserCountry()
	country, err := h.country.GetByIsoCodeA2(userCountry)

	if err != nil {
		return nil, err
	}

	req := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     h.order.GetMerchantId(),
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		Amount:         h.order.RoyaltyData.AmountInRoyaltyCurrency,
		Region:         country.Region,
		Country:        h.order.GetUserCountry(),
	}
	return h.Service.getPaymentChannelCostMerchant(req)
}

func (h *accountingEntry) getMoneyBackCostMerchant(reason string) (*billing.MoneyBackCostMerchant, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	userCountry := h.order.GetUserCountry()
	country, err := h.country.GetByIsoCodeA2(userCountry)

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
		Region:         country.Region,
		Country:        userCountry,
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

	userCountry := h.order.GetUserCountry()
	country, err := h.country.GetByIsoCodeA2(userCountry)

	if err != nil {
		return nil, err
	}

	paymentAt, _ := ptypes.Timestamp(h.order.PaymentMethodOrderClosedAt)
	refundAt, _ := ptypes.Timestamp(h.refund.CreatedAt)

	data := &billing.MoneyBackCostSystemRequest{
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		Region:         country.Region,
		Country:        userCountry,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
		UndoReason:     reason,
	}
	return h.Service.getMoneyBackCostSystem(data)
}
