package service

/*import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"go.uber.org/zap"
	"time"
)

const (
	errorCommissionNotFound = "Commission by merchant and payment method not found"
)

type BalanceTransaction struct {
	Id          bson.ObjectId `bson:"_id"`
	Object      string        `bson:"object"`
	Type        string        `bson:"type"`
	Source      bson.ObjectId `bson:"source"`
	MerchantId  bson.ObjectId `bson:"merchant_id"`
	Amount      float64       `bson:"amount"`
	Currency    string        `bson:"currency"`
	Reason      string        `bson:"reason"`
	Status      string        `bson:"status"`
	CreatedAt   time.Time     `bson:"created_at"`
	AvailableOn time.Time     `bson:"available_on"`
}

type btProcessor struct {
	*Service
	curService currencies.CurrencyratesService
	ctx        context.Context
}

func (h *btProcessor) prepareOrderData(order *billing.Order) error {
	merchant, err := h.merchant.GetById(order.GetMerchantId())

	if err != nil {
		zap.L().Error(
			merchantErrorNotFound.Error(),
			zap.Error(err),
			zap.String("merchant_id", order.GetMerchantId()),
		)

		return err
	}

	order.PaymentRoyaltyData.RoyaltyCur = merchant.GetPayoutCurrency().CodeA3

	req := &currencies.GetRateCurrentForMerchantRequest{
		From:       order.Currency,
		To:         order.PaymentRoyaltyData.RoyaltyCur,
		MerchantId: order.Project.MerchantId,
		RateType:   curPkg.RateTypeOxr,
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

	order.PaymentRoyaltyData.ExRatePaymentToRoyaltyInstant = rsp.Rate

	req.RateType = curPkg.RateTypePaysuper
	rsp, err = h.curService.GetRateCurrentForMerchant(context.Background(), req)

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

	order.PaymentRoyaltyData.ExRatePaymentToRoyaltyPrediction = rsp.Rate

	req.RateType = curPkg.RateTypeCentralbanks
	rsp, err = h.curService.GetRateCurrentForMerchant(context.Background(), req)

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

	order.PaymentRoyaltyData.ExRatePaymentToVat = rsp.Rate
	order.PaymentRoyaltyData.Vat = order.Tax.Amount * order.PaymentRoyaltyData.ExRatePaymentToVat
	order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction = order.TotalPaymentAmount * order.PaymentRoyaltyData.ExRatePaymentToRoyaltyPrediction

	commission, err := h.commission.GetByProjectIdAndMethod(order.GetProjectId(), order.GetPaymentMethodId())

	if err != nil {
		zap.L().Error(
			errorCommissionNotFound,
			zap.Error(err),
			zap.String("project_id", order.GetProjectId()),
			zap.String("payment_method", order.GetPaymentMethodId()),
		)

		return err
	}

	order.PaymentRoyaltyData.OverallCutRate = commission.Fee
	order.PaymentRoyaltyData.OverallFee = commission.PerTransaction.Fee

	if commission.PerTransaction.Currency != order.PaymentRoyaltyData.RoyaltyCur {
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:       commission.PerTransaction.Currency,
			To:         order.PaymentRoyaltyData.RoyaltyCur,
			MerchantId: order.GetMerchantId(),
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

		order.PaymentRoyaltyData.OverallFee = rsp.ExchangedAmount
	}

	order.PaymentRoyaltyData.OverallDeductionPrediction = order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction*order.PaymentRoyaltyData.OverallCutRate + order.PaymentRoyaltyData.OverallFee

	req = &currencies.GetRateCurrentForMerchantRequest{
		From:       order.Tax.Currency,
		To:         order.PaymentRoyaltyData.RoyaltyCur,
		MerchantId: order.Project.MerchantId,
		RateType:   curPkg.RateTypeStock,
	}
	rsp, err = h.curService.GetRateCurrentForMerchant(h.ctx, req)

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

	order.PaymentRoyaltyData.ExRateVatToRoyaltyInstant = rsp.Rate

	req.RateType = curPkg.RateTypePaysuper
	rsp, err = h.curService.GetRateCurrentForMerchant(h.ctx, req)

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

	order.PaymentRoyaltyData.ExRateVatToRoyaltyPrediction = rsp.Rate
	order.PaymentRoyaltyData.VatInRoyaltyCurPrediction = order.PaymentRoyaltyData.Vat * order.PaymentRoyaltyData.ExRateVatToRoyaltyPrediction
	//royaltyPrediction = order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction - order.PaymentRoyaltyData.VatInRoyaltyCurPrediction - order.PaymentRoyaltyData.OverallDeductionPrediction
	//costFeeCur
	//costRatePublic
	//costFeePublic
	//exRateCostFeeToRoyaltyInstant
	//exRateCostFeeToRoyaltyPrediction
	//costPublicRateAmountPrediction
	//costPublicPrediction
	//paySuperCutAmountPrediction
	//paySuperCutRatePrediction
	//costRate
	//costFee
	//costPrediction
	//costDeltaPrediction
	//paySuperRevenuePrediction

	return nil
}

func (h *btProcessor) payment(order *billing.Order) (transactions []*BalanceTransaction) {
	order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction = order.GetTotalPaymentAmount() * order.PaymentRoyaltyData.ExRatePaymentToRoyaltyPrediction
	transactions = append(
		transactions,
		&BalanceTransaction{
			Id:         bson.NewObjectId(),
			Object:     pkg.ObjectTypeBalanceTransaction,
			Type:       pkg.BalanceTransactionTypePayment,
			Source:     bson.ObjectIdHex(order.Id),
			MerchantId: bson.ObjectIdHex(order.GetMerchantId()),
			Amount:     order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction,
			Currency:   order.PaymentRoyaltyData.RoyaltyCur,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  time.Now(),
		},
	)

	return transactions
}

func (h *btProcessor) psMarkupPaymentFx(order *billing.Order) (transactions []*BalanceTransaction) {
	req := &currencies.GetRateCurrentCommonRequest{
		From:     order.Currency,
		To:       order.PaymentRoyaltyData.RoyaltyCur,
		RateType: curPkg.RateTypePaysuper,
	}
	rsp, err := h.curService.GetRateCurrentCommon(context.Background(), req)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetRateCurrentCommon"),
			zap.Any(errorFieldRequest, req),
		)

		return transactions
	}

	transactions = append(
		transactions,
		&BalanceTransaction{
			Id:         bson.NewObjectId(),
			Object:     pkg.ObjectTypeBalanceTransaction,
			Type:       pkg.BalanceTransactionTypePsMarkupPaymentFx,
			Source:     bson.ObjectIdHex(order.Id),
			MerchantId: bson.ObjectIdHex(order.GetMerchantId()),
			Amount:     order.PaymentRoyaltyData.ExRatePaymentToRoyaltyPrediction - rsp.Rate,
			Currency:   order.PaymentRoyaltyData.RoyaltyCur,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  time.Now(),
		},
	)

	return transactions
}

func (h *btProcessor) methodFee(order *billing.Order) (transactions []*BalanceTransaction) {
	c, err := h.commission.GetByProjectIdAndMethod(order.GetProjectId(), order.GetPaymentMethodId())

	if err != nil {
		zap.L().Error(
			"Commission to merchant and payment method not found",
			zap.Error(err),
			zap.String("project", order.GetProjectId()),
			zap.String("payment_method", order.GetPaymentMethodId()),
		)

		return transactions
	}

	order.PaymentRoyaltyData.OverallCutRate = c.Fee
	transactions = append(
		transactions,
		&BalanceTransaction{
			Id:         bson.NewObjectId(),
			Object:     pkg.ObjectTypeBalanceTransaction,
			Type:       pkg.BalanceTransactionTypeMethodFee,
			Source:     bson.ObjectIdHex(order.Id),
			MerchantId: bson.ObjectIdHex(order.GetMerchantId()),
			Amount:     order.PaymentRoyaltyData.GrossInRoyaltyCurPrediction * c.Fee,
			Currency:   order.PaymentRoyaltyData.RoyaltyCur,
			Status:     pkg.BalanceTransactionStatusPending,
			CreatedAt:  time.Now(),
		},
	)

	if c.PerTransaction.Fee > 0 {
		order.PaymentRoyaltyData.OverallFee = c.PerTransaction.Fee

		if c.PerTransaction.Currency != order.PaymentRoyaltyData.RoyaltyCur {
			req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
				From:       c.PerTransaction.Currency,
				To:         order.PaymentRoyaltyData.RoyaltyCur,
				MerchantId: order.GetMerchantId(),
				RateType:   curPkg.RateTypePaysuper,
				Amount:     c.PerTransaction.Fee,
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

				return transactions
			}

			order.PaymentRoyaltyData.OverallFee = rsp.ExchangedAmount
		}

		transactions = append(
			transactions,
			&BalanceTransaction{
				Id:         bson.NewObjectId(),
				Object:     pkg.ObjectTypeBalanceTransaction,
				Type:       pkg.BalanceTransactionTypeMethodFixedFee,
				Source:     bson.ObjectIdHex(order.Id),
				MerchantId: bson.ObjectIdHex(order.GetMerchantId()),
				Amount:     order.PaymentRoyaltyData.OverallFee,
				Currency:   order.PaymentRoyaltyData.RoyaltyCur,
				Status:     pkg.BalanceTransactionStatusPending,
				CreatedAt:  time.Now(),
			},
		)
	}

	return transactions
}

func (h *btProcessor) psMarkupMethodFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) gatewayMethodFixedFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) psMarkupMethodFixedFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) psFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) psFixedFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) psMarkupFixedFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) taxFee() (transactions []*BalanceTransaction) {
	return transactions
}

func (h *btProcessor) psTaxFxFee() {

}*/
