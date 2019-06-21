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
	errorAccountingEntryCommissionNotFound = "Commission to merchant and payment method not found"

	errorFieldService = "service"
	errorFieldMethod  = "method"
	errorFieldRequest = "request"
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
	*Service
	curService currencies.CurrencyratesService
	ctx        context.Context

	order             *billing.Order
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

		if order.RoyaltyData == nil || order.GetMerchantRoyaltyCurrency() == "" {
			//вернуть ошибку
			return nil
		}

		handler.order = order
	}

	if req.MerchantId != "" && bson.IsObjectIdHex(req.MerchantId) == true {
		merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.OrderId)})

		if err != nil {
			//вернуть ошибку
		}

		handler.merchant = merchant
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
	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypePayment,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     rsp.ExchangedAmount,
			Currency:   h.order.GetMerchantRoyaltyCurrency(),
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	return nil
}

func (h *accountingEntry) psMarkupPaymentFx() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
	}

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       h.order.Currency,
		To:         h.order.RoyaltyData.MerchantRoyaltyCurrency,
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
		To:       h.order.RoyaltyData.MerchantRoyaltyCurrency,
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

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypePsMarkupPaymentFx,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     rsp1.Rate - rsp.Rate,
			Currency:   h.order.RoyaltyData.MerchantRoyaltyCurrency,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	return nil
}

func (h *accountingEntry) methodFee() error {
	if h.order == nil {
		//вернуть ошибку
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

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypeMethodFee,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency,
			Currency:   h.order.RoyaltyData.MerchantRoyaltyCurrency,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency = h.order.RoyaltyData.AmountInRoyaltyCurrency * commission.Fee
	h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency = commission.PerTransaction.Fee

	if commission.PerTransaction.Currency != h.order.RoyaltyData.MerchantRoyaltyCurrency {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       commission.PerTransaction.Currency,
			To:         h.order.RoyaltyData.MerchantRoyaltyCurrency,
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

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypeMethodFixedFee,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency,
			Currency:   h.order.RoyaltyData.MerchantRoyaltyCurrency,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	h.order.RoyaltyData.MerchantTotalCommissionInRoyaltyCurrency = h.order.RoyaltyData.MerchantPercentCommissionInRoyaltyCurrency + h.order.RoyaltyData.MerchantFixedCommissionInRoyaltyCurrency

	return nil
}

func (h *accountingEntry) psMarkupMethodFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
	}

	var err error

	name := h.order.GetPaymentMethodName()

	if h.order.PaymentMethod.IsBankCard() == true {
		name, err = h.order.GetBankCardBrand()

		if err != nil {
			return err
		}
	}

	req := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     h.order.GetMerchantId(),
		Name:           name,
		PayoutCurrency: h.order.RoyaltyData.MerchantRoyaltyCurrency,
		Amount:         h.order.TotalPaymentAmount,
		// @inject_tag: json:"region" bson:"region" validate:"required,alpha"
		//Region string `protobuf:"bytes,5,opt,name=region,proto3" json:"region" bson:"region" validate:"required,alpha"`
		// @inject_tag: json:"country" bson:"country" validate:"omitempty,alpha,len=2"
		//Country
	}
	h.getPaymentChannelCostMerchant(req)

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
	if h.order == nil {
		//вернуть ошибку
		return nil
	}

	amount := h.order.Tax.Amount

	if h.order.Tax.Currency != h.order.RoyaltyData.MerchantRoyaltyCurrency {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       h.order.Tax.Currency,
			To:         h.order.RoyaltyData.MerchantRoyaltyCurrency,
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

	h.accountingEntries = append(
		h.accountingEntries,
		&billing.AccountingEntry{
			Id:     bson.NewObjectId().Hex(),
			Object: pkg.ObjectTypeBalanceTransaction,
			Type:   pkg.AccountingEntryTypeTaxFee,
			Source: &billing.AccountingEntrySource{
				Id:   h.order.Id,
				Type: collectionOrder,
			},
			MerchantId: h.order.GetMerchantId(),
			Amount:     amount,
			Currency:   h.order.RoyaltyData.MerchantRoyaltyCurrency,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	return nil
}

func (h *accountingEntry) psTaxFxFee() error {
	if h.order == nil {
		//вернуть ошибку
		return nil
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
			CreatedAt:  ptypes.TimestampNow(),
		},
	)

	return nil
}
