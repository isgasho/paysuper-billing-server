package service

import (
	"context"
	"encoding/json"
	"fmt"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/jinzhu/copier"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	tools "github.com/paysuper/paysuper-tools/number"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"time"
)

const (
	refundDefaultReasonMask = "Refund by order #%s"
)

var (
	refundErrorUnknown            = newBillingServerErrorMsg("rf000001", "refund can't be create. try request later")
	refundErrorNotAllowed         = newBillingServerErrorMsg("rf000002", "create refund for order not allowed")
	refundErrorAlreadyRefunded    = newBillingServerErrorMsg("rf000003", "amount by order was fully refunded")
	refundErrorPaymentAmountLess  = newBillingServerErrorMsg("rf000004", "refund unavailable, because payment amount less than total refunds amount")
	refundErrorNotFound           = newBillingServerErrorMsg("rf000005", "refund with specified data not found")
	refundErrorOrderNotFound      = newBillingServerErrorMsg("rf000006", "information about payment for refund with specified data not found")
	refundErrorCostsRatesNotFound = newBillingServerErrorMsg("rf000007", "settings to calculate commissions for refund not found")
)

type createRefundChecked struct {
	order *billingpb.Order
}

type createRefundProcessor struct {
	service *Service
	request *billingpb.CreateRefundRequest
	checked *createRefundChecked
	ctx     context.Context
}

func (s *Service) CreateRefund(
	ctx context.Context,
	req *billingpb.CreateRefundRequest,
	rsp *billingpb.CreateRefundResponse,
) error {
	processor := &createRefundProcessor{
		service: s,
		request: req,
		checked: &createRefundChecked{},
		ctx:     ctx,
	}

	refund, err := processor.processCreateRefund()

	if err != nil {
		rsp.Status = err.(*billingpb.ResponseError).Status
		rsp.Message = err.(*billingpb.ResponseError).Message

		return nil
	}

	h, err := s.paymentSystemGateway.getGateway(processor.checked.order.PaymentMethod.Handler)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	err = h.CreateRefund(processor.checked.order, refund)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = refundErrorUnknown

		return nil
	}

	if err = s.refundRepository.Update(ctx, refund); err != nil {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = refund

	return nil
}

func (s *Service) ListRefunds(
	ctx context.Context,
	req *billingpb.ListRefundsRequest,
	rsp *billingpb.ListRefundsResponse,
) error {
	order, err := s.orderRepository.GetByUuid(ctx, req.OrderId)

	if err != nil {
		return nil
	}

	if order.GetMerchantId() != req.MerchantId {
		zap.S().Errorw("Merchant ID does not match requested.", "uuid", req.OrderId, "merchantId", req.MerchantId)
		return nil
	}

	refunds, err := s.refundRepository.FindByOrderUuid(ctx, req.OrderId, req.Limit, req.Offset)

	if err != nil {
		return nil
	}

	count, err := s.refundRepository.CountByOrderUuid(ctx, req.OrderId)

	if err != nil {
		return nil
	}

	if refunds != nil {
		rsp.Count = count
		rsp.Items = refunds
	}

	return nil
}

func (s *Service) GetRefund(
	ctx context.Context,
	req *billingpb.GetRefundRequest,
	rsp *billingpb.CreateRefundResponse,
) error {
	order, err := s.orderRepository.GetByUuid(ctx, req.OrderId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	if order.GetMerchantId() != req.MerchantId {
		zap.S().Errorw("Merchant ID does not match requested.", "uuid", req.OrderId, "merchantId", req.MerchantId)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	refund, err := s.refundRepository.GetById(ctx, req.RefundId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = refund

	return nil
}

func (s *Service) ProcessRefundCallback(
	ctx context.Context,
	req *billingpb.CallbackRequest,
	rsp *billingpb.PaymentNotifyResponse,
) error {
	var data protobuf.Message
	var refundId string

	switch req.Handler {
	case billingpb.PaymentSystemHandlerCardPay:
		data = &billingpb.CardPayRefundCallback{}
		err := json.Unmarshal(req.Body, &data)

		if err != nil ||
			data.(*billingpb.CardPayRefundCallback).RefundData == nil {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Error = callbackRequestIncorrect

			return nil
		}

		refundId = data.(*billingpb.CardPayRefundCallback).MerchantOrder.Id
		break
	default:
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Error = callbackHandlerIncorrect

		return nil
	}

	refund, err := s.refundRepository.GetById(ctx, refundId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Error = refundErrorNotFound.Error()

		return nil
	}

	order, err := s.getOrderById(ctx, refund.OriginalOrder.Id)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Error = refundErrorOrderNotFound.Error()

		return nil
	}

	h, err := s.paymentSystemGateway.getGateway(order.PaymentMethod.Handler)

	if err != nil {
		zap.L().Error(
			"s.NewPaymentSystem method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Error = orderErrorUnknown.Error()

		return nil
	}

	pErr := h.ProcessRefund(order, refund, data, string(req.Body), req.Signature)

	if pErr != nil {
		rsp.Error = pErr.Error()
		rsp.Status = pErr.(*billingpb.ResponseError).Status

		if rsp.Status == billingpb.ResponseStatusTemporary {
			rsp.Status = billingpb.ResponseStatusOk

			return nil
		}
	}

	refundOrder := &billingpb.Order{}

	if pErr == nil {
		if refund.CreatedOrderId == "" {
			refundOrder, err = s.createOrderByRefund(ctx, order, refund)

			if err != nil {
				rsp.Status = billingpb.ResponseStatusSystemError
				rsp.Error = err.Error()
				return nil
			}

			refund.CreatedOrderId = refundOrder.Id
		} else {
			refundOrder, err = s.getOrderById(ctx, refund.CreatedOrderId)
			if err != nil {
				zap.L().Error(
					pkg.MethodFinishedWithError,
					zap.String("method", "getOrderById"),
					zap.Error(err),
					zap.String("refundId", refund.Id),
					zap.String("refund-orderId", refund.CreatedOrderId),
				)

				rsp.Error = err.Error()
				rsp.Status = billingpb.ResponseStatusSystemError

				return nil
			}
		}
	}

	if err = s.refundRepository.Update(ctx, refund); err != nil {
		rsp.Error = orderErrorUnknown.Error()
		rsp.Status = billingpb.ResponseStatusSystemError

		return nil
	}

	if pErr == nil {
		processor := &createRefundProcessor{service: s, ctx: ctx}
		refundedAmount, _ := processor.service.refundRepository.GetAmountByOrderId(ctx, order.Id)

		if refundedAmount == order.ChargeAmount {
			if refund.IsChargeback == true {
				order.PrivateStatus = recurringpb.OrderStatusChargeback
				order.Status = recurringpb.OrderPublicStatusChargeback
			} else {
				order.PrivateStatus = recurringpb.OrderStatusRefund
				order.Status = recurringpb.OrderPublicStatusRefunded
			}

			order.UpdatedAt = ptypes.TimestampNow()
			order.RefundedAt = ptypes.TimestampNow()
			order.Refunded = true
			order.IsRefundAllowed = false
			order.Refund = &billingpb.OrderNotificationRefund{
				Amount:        refundedAmount,
				Currency:      order.ChargeCurrency,
				Reason:        refund.Reason,
				ReceiptNumber: refund.Id,
			}

			err = s.updateOrder(ctx, order)

			if err != nil {
				zap.S().Errorf("Update order data failed", "err", err.Error(), "order", order)
			}
		}

		err = s.onRefundNotify(ctx, refund, order)

		if err != nil {
			zap.L().Error(
				pkg.MethodFinishedWithError,
				zap.String("method", "onRefundNotify"),
				zap.Error(err),
				zap.String("refundId", refund.Id),
				zap.String("refund-orderId", refundOrder.Id),
			)

			rsp.Error = err.Error()
			rsp.Status = billingpb.ResponseStatusSystemError

			return nil
		}

		s.sendMailWithReceipt(ctx, refundOrder)

		rsp.Status = billingpb.ResponseStatusOk
	}

	return nil
}

func (s *Service) createOrderByRefund(ctx context.Context, order *billingpb.Order, refund *billingpb.Refund) (*billingpb.Order, error) {
	refundOrder := new(billingpb.Order)
	err := copier.Copy(&refundOrder, &order)

	if err != nil {
		zap.S().Error(
			"Copy order to new structure order by refund failed",
			zap.Error(err),
			zap.Any("refund", refund),
		)

		return nil, refundErrorUnknown
	}

	country, err := s.country.GetByIsoCodeA2(ctx, order.GetCountry())
	if err != nil {
		zap.S().Error(
			"country not found",
			zap.Error(err),
		)
		return nil, refundErrorUnknown
	}

	isVatDeduction := false

	if country.VatEnabled {
		from, _, err := s.getLastVatReportTime(country.VatPeriodMonth)
		if err != nil {
			zap.S().Error(
				"cannot get last vat report time",
				zap.Error(err),
			)
			return nil, refundErrorUnknown
		}

		orderPayedAt, err := ptypes.Timestamp(order.PaymentMethodOrderClosedAt)

		if err != nil {
			zap.S().Error(
				"cannot get convert PaymentMethodOrderClosedAt date to time",
				zap.Error(err),
			)
			return nil, refundErrorUnknown
		}

		if orderPayedAt.Unix() < from.Unix() {
			isVatDeduction = true
		}
	}

	refundOrder.Id = primitive.NewObjectID().Hex()
	refundOrder.Uuid = uuid.New().String()
	refundOrder.Type = pkg.OrderTypeRefund
	refundOrder.PrivateStatus = recurringpb.OrderStatusRefund
	refundOrder.Status = recurringpb.OrderPublicStatusRefunded

	if refund.IsChargeback {
		refundOrder.PrivateStatus = recurringpb.OrderStatusChargeback
		refundOrder.Status = recurringpb.OrderPublicStatusChargeback
	}

	refundOrder.CreatedAt = ptypes.TimestampNow()
	refundOrder.UpdatedAt = ptypes.TimestampNow()
	refundOrder.RefundedAt = ptypes.TimestampNow()
	refundOrder.IsRefundAllowed = false
	refundOrder.Refunded = true
	refundOrder.Refund = &billingpb.OrderNotificationRefund{
		Amount:        refund.Amount,
		Currency:      refund.Currency,
		Reason:        refund.Reason,
		ReceiptNumber: refund.Id,
	}
	refundOrder.ParentOrder = &billingpb.ParentOrder{
		Id:   order.Id,
		Uuid: order.Uuid,
	}
	refundOrder.IsVatDeduction = isVatDeduction
	refundOrder.ParentPaymentAt = order.PaymentMethodOrderClosedAt

	refundOrder.ChargeAmount = refund.Amount

	refundOrder.Tax.Amount = tools.FormatAmount(tools.GetPercentPartFromAmount(refund.Amount, refundOrder.Tax.Rate))
	refundOrder.OrderAmount = tools.FormatAmount(refundOrder.TotalPaymentAmount - refundOrder.Tax.Amount)
	refundOrder.ReceiptId = uuid.New().String()
	refundOrder.ReceiptUrl = s.cfg.GetReceiptRefundUrl(refundOrder.Uuid, refundOrder.ReceiptId)

	if err = s.orderRepository.Insert(ctx, refundOrder); err != nil {
		return nil, refundErrorUnknown
	}

	return refundOrder, nil
}

func (p *createRefundProcessor) processCreateRefund() (*billingpb.Refund, error) {
	err := p.processOrder()

	if err != nil {
		return nil, err
	}

	if !p.hasMoneyBackCosts(p.ctx, p.checked.order) {
		return nil, newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorCostsRatesNotFound)
	}

	err = p.processRefundsByOrder()

	if err != nil {
		return nil, err
	}

	order := p.checked.order

	if order.GetMerchantId() != p.request.MerchantId {
		return nil, newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorOrderNotFound)
	}

	refund := &billingpb.Refund{
		Id: primitive.NewObjectID().Hex(),
		OriginalOrder: &billingpb.RefundOrder{
			Id:   order.Id,
			Uuid: order.Uuid,
		},
		Amount:    order.ChargeAmount,
		CreatorId: p.request.CreatorId,
		Reason:    fmt.Sprintf(refundDefaultReasonMask, p.checked.order.Id),
		Currency:  order.ChargeCurrency,
		Status:    pkg.RefundStatusCreated,
		CreatedAt: ptypes.TimestampNow(),
		UpdatedAt: ptypes.TimestampNow(),
		PayerData: &billingpb.RefundPayerData{
			Country: order.GetCountry(),
			Zip:     order.GetPostalCode(),
			State:   order.GetState(),
		},
		IsChargeback: p.request.IsChargeback,
	}

	if refund.IsChargeback == true {
		refund.Amount = p.checked.order.ChargeAmount
		refund.IsChargeback = p.request.IsChargeback
	}

	if order.Tax != nil {
		refund.SalesTax = float32(order.Tax.Amount)
	}

	if p.request.Reason != "" {
		refund.Reason = p.request.Reason
	}

	if err = p.service.refundRepository.Insert(p.ctx, refund); err != nil {
		return nil, newBillingServerResponseError(billingpb.ResponseStatusBadData, orderErrorUnknown)
	}

	return refund, nil
}

func (p *createRefundProcessor) processOrder() error {
	order, err := p.service.getOrderByUuid(p.ctx, p.request.OrderId)

	if err != nil {
		return newBillingServerResponseError(billingpb.ResponseStatusNotFound, refundErrorNotFound)
	}

	if order.PrivateStatus == recurringpb.OrderStatusRefund {
		return newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorAlreadyRefunded)
	}

	if order.RefundAllowed() == false {
		return newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorNotAllowed)
	}

	p.checked.order = order

	return nil
}

func (p *createRefundProcessor) processRefundsByOrder() error {
	refundedAmount, err := p.service.refundRepository.GetAmountByOrderId(p.ctx, p.checked.order.Id)

	if err != nil {
		return newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorUnknown)
	}

	if refundedAmount > 0 {
		return newBillingServerResponseError(billingpb.ResponseStatusBadData, refundErrorPaymentAmountLess)
	}

	return nil
}

func (p *createRefundProcessor) hasMoneyBackCosts(ctx context.Context, order *billingpb.Order) bool {
	country, err := p.service.country.GetByIsoCodeA2(ctx, order.GetCountry())

	if err != nil {
		return false
	}

	methodName, err := order.GetCostPaymentMethodName()

	if err != nil {
		return false
	}

	paymentAt, _ := ptypes.Timestamp(order.PaymentMethodOrderClosedAt)
	refundAt := time.Now()
	reason := pkg.UndoReasonReversal

	if p.request.IsChargeback {
		reason = pkg.UndoReasonChargeback
	}

	data := &billingpb.MoneyBackCostSystemRequest{
		Name:               methodName,
		PayoutCurrency:     order.GetMerchantRoyaltyCurrency(),
		Region:             country.PayerTariffRegion,
		Country:            country.IsoCodeA2,
		PaymentStage:       1,
		Days:               int32(refundAt.Sub(paymentAt).Hours() / 24),
		UndoReason:         reason,
		MccCode:            order.MccCode,
		OperatingCompanyId: order.OperatingCompanyId,
	}
	_, err = p.service.getMoneyBackCostSystem(ctx, data)

	if err != nil {
		return false
	}

	data1 := &billingpb.MoneyBackCostMerchantRequest{
		MerchantId:     order.GetMerchantId(),
		Name:           methodName,
		PayoutCurrency: order.GetMerchantRoyaltyCurrency(),
		UndoReason:     reason,
		Region:         country.PayerTariffRegion,
		Country:        country.IsoCodeA2,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
		MccCode:        order.MccCode,
	}
	_, err = p.service.getMoneyBackCostMerchant(ctx, data1)
	return err == nil
}
