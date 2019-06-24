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
	errorAccountingEntryCommissionNotFound = "Commission to merchant and payment method not found"

	errorFieldService = "service"
	errorFieldMethod  = "method"
	errorFieldRequest = "request"
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
	}
)

type accountingEntry struct {
	*Service
	curService currencies.CurrencyratesService
	ctx        context.Context

	order             *billing.Order
	refund            *billing.Refund
	merchant          *billing.Merchant
	accountingEntries []interface{}
	req               *grpc.CreateAccountingEntryRequest
}

func (s *Service) CreateAccountingEntry(
	ctx context.Context,
	req *grpc.CreateAccountingEntryRequest,
	rsp *grpc.CreateAccountingEntryRequest,
) error {
	fn, ok := availableAccountingEntry[req.Type]

	if !ok {
		//вернуть ошибку
		return nil
	}

	handler := &accountingEntry{
		Service: s,
		req:     req,
		ctx:     ctx,
	}

	if req.OrderId != "" && bson.IsObjectIdHex(req.OrderId) == true {
		order, err := s.getOrderById(req.OrderId)

		if err != nil {
			//вернуть ошибку
			return nil
		}

		handler.order = order
	}

	if req.RefundId != "" && bson.IsObjectIdHex(req.RefundId) == true {
		refund, err := s.getRefundById(req.RefundId)

		if err != nil {
			//вернуть ошибку
			return nil
		}

		order, err := s.getOrderById(refund.Order.Id)

		if err != nil {
			//вернуть ошибку
			return nil
		}

		handler.order = order
		handler.refund = refund
	}

	if req.MerchantId != "" && bson.IsObjectIdHex(req.MerchantId) == true {
		merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.OrderId)})

		if err != nil {
			//вернуть ошибку
		}

		handler.merchant = merchant
	}

	if handler.order != nil && handler.order.RoyaltyData == nil {
		handler.order.RoyaltyData = &billing.RoyaltyData{}
	}

	err := fn(handler)

	if err != nil {
		//вернуть ошибку
	}

	return nil
}

func (h *accountingEntry) payment() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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

		return err
	}

	h.order.RoyaltyData.AmountInRoyaltyCurrency = rsp.ExchangedAmount

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupPaymentFx() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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

		return err
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

		return err
	}

	entry.Amount = rsp1.Rate - rsp.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) methodFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
			errorAccountingEntryCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return err
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
		//вернуть ошибку
		return nil
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
		return err
	}

	entry.Amount = h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency - (h.order.RoyaltyData.AmountInRoyaltyCurrency * cost.MethodPercent)
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) methodFixedFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
			errorAccountingEntryCommissionNotFound,
			zap.Error(err),
			zap.String("project", h.order.GetProjectId()),
			zap.String("payment_method", h.order.GetPaymentMethodId()),
		)

		return err
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
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(context.Background(), req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return err
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
		//вернуть ошибку
		return nil
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
		return err
	}

	entry.Amount = h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency - cost.MethodFixAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
		return err
	}

	amount := h.order.RoyaltyData.AmountInRoyaltyCurrency * cost.PsPercent

	entry.Amount = h.order.RoyaltyData.MerchantTotalCommissionInRoyaltyCurrency - amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()
	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psFixedFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
		return err
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

			return err
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
		//вернуть ошибку
		return nil
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
		return err
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

		return err
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

		return err
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) taxFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
		rsp, err := h.curService.ExchangeCurrencyCurrentForMerchant(context.Background(), req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
				zap.Any(errorFieldRequest, req),
			)

			return err
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
		//вернуть ошибку
		return nil
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
		Amount:     h.req.Amount,
		Currency:   h.req.Currency,
		Status:     h.req.Status,
	}

	if h.req.Date == 0 {
		entry.CreatedAt = ptypes.TimestampNow()
	} else {
		t := time.Unix(h.req.Date, 0)
		entry.CreatedAt, _ = ptypes.TimestampProto(t)
	}

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypePsTaxFxFee,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     h.req.Amount,
			Currency:   h.req.Currency,
			Status:     h.req.Status,
		},
	)

	return nil
}

func (h *accountingEntry) refundEntry() error {
	if h.refund == nil {
		//вернуть ошибку
		return nil
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
		From:       h.refund.Currency.CodeA3,
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

		return err
	}

	h.order.RoyaltyData.RefundTotalAmountInRoyaltyCurrency += rsp.ExchangedAmount

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFee() error {
	if h.refund == nil {
		//вернуть ошибку
		return nil
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

	cost, err := h.getMoneyBackCostMerchant()

	if err != nil {
		return err
	}

	entry.Amount = h.refund.Amount * cost.Percent
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFixedFee() error {
	if h.refund == nil {
		//вернуть ошибку
		return nil
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

	cost, err := h.getMoneyBackCostMerchant()

	if err != nil {
		return err
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

			return err
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
		//вернуть ошибку
		return nil
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
		From:       h.refund.Currency.CodeA3,
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

		return err
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.refund.Currency.CodeA3,
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

		return err
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundBody() error {
	if h.refund == nil {
		//вернуть ошибку
		return nil
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

	if h.refund.Currency.CodeA3 != h.order.GetMerchantRoyaltyCurrency() {
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

			return err
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

			return err
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
		//вернуть ошибку
		return nil
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

			return err
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
		//вернуть ошибку
		return nil
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
		From:       h.refund.Currency.CodeA3,
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

		return err
	}

	req1 := &currencies.GetRateCurrentCommonRequest{
		From:     h.refund.Currency.CodeA3,
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

		return err
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) reverseTaxFeeDelta() error {
	if h.refund == nil {
		//вернуть ошибку
		return nil
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
		//вернуть ошибку
		return nil
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
	if h.order == nil {
		//вернуть ошибку
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargeback,
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

		return err
	}

	entry.Amount = rsp.ExchangedAmount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFx() error {
	if h.order == nil {
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFx,
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

		return err
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

		return err
	}

	entry.Amount = rsp.Rate - rsp1.Rate
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargebackFee() error {
	if h.order == nil {
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargebackFee,
		Source: &billing.AccountingEntrySource{
			Id:   h.order.Id,
			Type: collectionOrder,
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

	cost, err := h.getMoneyBackCostMerchant()

	if err != nil {
		return err
	}

	h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency = h.order.TotalPaymentAmount * cost.Percent
	entry.Amount = h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFee() error {
	if h.order == nil {
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFee,
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

	cost, err := h.getMoneyBackCostSystem()

	if err != nil {
		return err
	}

	entry.Amount = h.order.RoyaltyData.ChargebackPercentCommissionInRoyaltyCurrency - (h.order.TotalPaymentAmount * cost.Percent)
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) chargebackFixedFee() error {
	if h.order == nil {
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypeChargebackFixedFee,
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

	cost, err := h.getMoneyBackCostMerchant()

	if err != nil {
		return err
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

			return err
		}

		h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency = rsp.ExchangedAmount
	}

	entry.Amount = h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) psMarkupChargebackFixedFee() error {
	if h.order == nil {
		return nil
	}

	entry := &billing.AccountingEntry{
		Id:     bson.NewObjectId().Hex(),
		Object: pkg.ObjectTypeBalanceTransaction,
		Type:   pkg.AccountingEntryTypePsMarkupChargebackFixedFee,
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

	cost, err := h.getMoneyBackCostSystem()

	if err != nil {
		return err
	}

	amount := cost.FixAmount

	entry.Amount = h.order.RoyaltyData.ChargebackFixedCommissionInRoyaltyCurrency - amount
	entry.Currency = h.order.GetMerchantRoyaltyCurrency()

	h.accountingEntries = append(h.accountingEntries, entry)

	return nil
}

func (h *accountingEntry) refundFailure() error {
	if h.refund == nil {
		return nil
	}

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypeRefundFailure,
			Source: &billing.AccountingEntrySource{
				Id:   h.refund.Id,
				Type: collectionRefund,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     h.req.Amount,
			Currency:   h.req.Currency,
			Reason:     h.req.Reason,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	return nil
}

func (h *accountingEntry) chargebackFailure() error {
	if h.order == nil {
		return nil
	}

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypeChargebackFailure,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     h.req.Amount,
			Currency:   h.req.Currency,
			Reason:     h.req.Reason,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

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

func (h *accountingEntry) getMoneyBackCostMerchant() (*billing.MoneyBackCostMerchant, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	userCountry := h.order.GetUserCountry()
	country, err := h.country.GetByIsoCodeA2(userCountry)

	if err != nil {
		return nil, err
	}

	paymentAt, err := ptypes.Timestamp(h.order.PaymentMethodOrderClosedAt)
	refundAt, err := ptypes.Timestamp(h.refund.CreatedAt)

	if err != nil {
		return nil, err
	}

	data := &billing.MoneyBackCostMerchantRequest{
		MerchantId:     h.order.GetMerchantId(),
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		UndoReason:     h.refund.Reason,
		Region:         country.Region,
		Country:        userCountry,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
	}
	return h.Service.getMoneyBackCostMerchant(data)
}

func (h *accountingEntry) getMoneyBackCostSystem() (*billing.MoneyBackCostSystem, error) {
	name, err := h.order.GetCostPaymentMethodName()

	if err != nil {
		return nil, err
	}

	userCountry := h.order.GetUserCountry()
	country, err := h.country.GetByIsoCodeA2(userCountry)

	if err != nil {
		return nil, err
	}

	paymentAt, err := ptypes.Timestamp(h.order.PaymentMethodOrderClosedAt)
	refundAt, err := ptypes.Timestamp(h.refund.CreatedAt)

	if err != nil {
		return nil, err
	}

	data := &billing.MoneyBackCostSystemRequest{
		Name:           name,
		PayoutCurrency: h.order.GetMerchantRoyaltyCurrency(),
		Region:         country.Region,
		Country:        userCountry,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
	}
	return h.Service.getMoneyBackCostSystem(data)
}
