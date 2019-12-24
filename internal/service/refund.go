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
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
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
	order *billing.Order
}

type createRefundProcessor struct {
	service *Service
	request *grpc.CreateRefundRequest
	checked *createRefundChecked
	ctx     context.Context
}

func (s *Service) CreateRefund(
	ctx context.Context,
	req *grpc.CreateRefundRequest,
	rsp *grpc.CreateRefundResponse,
) error {
	processor := &createRefundProcessor{
		service: s,
		request: req,
		checked: &createRefundChecked{},
		ctx:     ctx,
	}

	refund, err := processor.processCreateRefund()

	if err != nil {
		rsp.Status = err.(*grpc.ResponseError).Status
		rsp.Message = err.(*grpc.ResponseError).Message

		return nil
	}

	h, err := s.NewPaymentSystem(ctx, s.cfg.PaymentSystemConfig, processor.checked.order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	err = h.CreateRefund(processor.checked.order, refund)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = refundErrorUnknown

		return nil
	}

	if err = s.refundRepository.Update(ctx, refund); err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = refund

	return nil
}

func (s *Service) ListRefunds(
	ctx context.Context,
	req *grpc.ListRefundsRequest,
	rsp *grpc.ListRefundsResponse,
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
	req *grpc.GetRefundRequest,
	rsp *grpc.CreateRefundResponse,
) error {
	order, err := s.orderRepository.GetByUuid(ctx, req.OrderId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	if order.GetMerchantId() != req.MerchantId {
		zap.S().Errorw("Merchant ID does not match requested.", "uuid", req.OrderId, "merchantId", req.MerchantId)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	refund, err := s.refundRepository.GetById(ctx, req.RefundId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = refundErrorNotFound

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = refund

	return nil
}

func (s *Service) ProcessRefundCallback(
	ctx context.Context,
	req *grpc.CallbackRequest,
	rsp *grpc.PaymentNotifyResponse,
) error {
	var data protobuf.Message
	var refundId string

	switch req.Handler {
	case pkg.PaymentSystemHandlerCardPay:
		data = &billing.CardPayRefundCallback{}
		err := json.Unmarshal(req.Body, &data)

		if err != nil ||
			data.(*billing.CardPayRefundCallback).RefundData == nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Error = callbackRequestIncorrect

			return nil
		}

		refundId = data.(*billing.CardPayRefundCallback).MerchantOrder.Id
		break
	default:
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Error = callbackHandlerIncorrect

		return nil
	}

	refund, err := s.refundRepository.GetById(ctx, refundId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Error = refundErrorNotFound.Error()

		return nil
	}

	order, err := s.getOrderById(ctx, refund.OriginalOrder.Id)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Error = refundErrorOrderNotFound.Error()

		return nil
	}

	h, err := s.NewPaymentSystem(ctx, s.cfg.PaymentSystemConfig, order)

	if err != nil {
		zap.L().Error(
			"s.NewPaymentSystem method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Error = orderErrorUnknown.Error()

		return nil
	}

	pErr := h.ProcessRefund(order, refund, data, string(req.Body), req.Signature)

	if pErr != nil {
		rsp.Error = pErr.Error()
		rsp.Status = pErr.(*grpc.ResponseError).Status

		if rsp.Status == pkg.ResponseStatusTemporary {
			rsp.Status = pkg.ResponseStatusOk

			return nil
		}
	}

	refundOrder := &billing.Order{}

	if pErr == nil {
		if refund.CreatedOrderId == "" {
			refundOrder, err = s.createOrderByRefund(ctx, order, refund)

			if err != nil {
				rsp.Status = pkg.ResponseStatusSystemError
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
				rsp.Status = pkg.ResponseStatusSystemError

				return nil
			}
		}
	}

	if err = s.refundRepository.Update(ctx, refund); err != nil {
		rsp.Error = orderErrorUnknown.Error()
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	if pErr == nil {
		processor := &createRefundProcessor{service: s, ctx: ctx}
		refundedAmount, _ := processor.service.refundRepository.GetAmountByOrderId(ctx, order.Id)

		if refundedAmount == order.ChargeAmount {
			if refund.IsChargeback == true {
				order.PrivateStatus = constant.OrderStatusChargeback
				order.Status = constant.OrderPublicStatusChargeback
			} else {
				order.PrivateStatus = constant.OrderStatusRefund
				order.Status = constant.OrderPublicStatusRefunded
			}

			order.UpdatedAt = ptypes.TimestampNow()
			order.RefundedAt = ptypes.TimestampNow()
			order.Refunded = true
			order.IsRefundAllowed = false
			order.Refund = &billing.OrderNotificationRefund{
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
			rsp.Status = pkg.ResponseStatusSystemError

			return nil
		}

		s.sendMailWithReceipt(ctx, refundOrder)

		rsp.Status = pkg.ResponseStatusOk
	}

	return nil
}

func (s *Service) createOrderByRefund(ctx context.Context, order *billing.Order, refund *billing.Refund) (*billing.Order, error) {
	refundOrder := new(billing.Order)
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
	refundOrder.PrivateStatus = constant.OrderStatusRefund
	refundOrder.Status = constant.OrderPublicStatusRefunded

	if refund.IsChargeback {
		refundOrder.PrivateStatus = constant.OrderStatusChargeback
		refundOrder.Status = constant.OrderPublicStatusChargeback
	}

	refundOrder.CreatedAt = ptypes.TimestampNow()
	refundOrder.UpdatedAt = ptypes.TimestampNow()
	refundOrder.RefundedAt = ptypes.TimestampNow()
	refundOrder.IsRefundAllowed = false
	refundOrder.Refunded = true
	refundOrder.Refund = &billing.OrderNotificationRefund{
		Amount:        refund.Amount,
		Currency:      refund.Currency,
		Reason:        refund.Reason,
		ReceiptNumber: refund.Id,
	}
	refundOrder.ParentOrder = &billing.ParentOrder{
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

func (p *createRefundProcessor) processCreateRefund() (*billing.Refund, error) {
	err := p.processOrder()

	if err != nil {
		return nil, err
	}

	if !p.hasMoneyBackCosts(p.ctx, p.checked.order) {
		return nil, newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorCostsRatesNotFound)
	}

	err = p.processRefundsByOrder()

	if err != nil {
		return nil, err
	}

	order := p.checked.order

	if order.GetMerchantId() != p.request.MerchantId {
		return nil, newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorOrderNotFound)
	}

	refund := &billing.Refund{
		Id: primitive.NewObjectID().Hex(),
		OriginalOrder: &billing.RefundOrder{
			Id:   order.Id,
			Uuid: order.Uuid,
		},
		Amount:    p.request.Amount,
		CreatorId: p.request.CreatorId,
		Reason:    fmt.Sprintf(refundDefaultReasonMask, p.checked.order.Id),
		Currency:  p.checked.order.ChargeCurrency,
		Status:    pkg.RefundStatusCreated,
		CreatedAt: ptypes.TimestampNow(),
		UpdatedAt: ptypes.TimestampNow(),
		PayerData: &billing.RefundPayerData{
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
		return nil, newBillingServerResponseError(pkg.ResponseStatusBadData, orderErrorUnknown)
	}

	return refund, nil
}

func (p *createRefundProcessor) processOrder() error {
	order, err := p.service.getOrderByUuid(p.ctx, p.request.OrderId)

	if err != nil {
		return newBillingServerResponseError(pkg.ResponseStatusNotFound, refundErrorNotFound)
	}

	if order.PrivateStatus == constant.OrderStatusRefund {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorAlreadyRefunded)
	}

	if order.RefundAllowed() == false {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorNotAllowed)
	}

	p.checked.order = order

	return nil
}

func (p *createRefundProcessor) processRefundsByOrder() error {
	refundedAmount, err := p.service.refundRepository.GetAmountByOrderId(p.ctx, p.checked.order.Id)

	if err != nil {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorUnknown)
	}

	if p.checked.order.ChargeAmount < (refundedAmount + p.request.Amount) {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorPaymentAmountLess)
	}

	return nil
}

func (p *createRefundProcessor) hasMoneyBackCosts(ctx context.Context, order *billing.Order) bool {
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

	data := &billing.MoneyBackCostSystemRequest{
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

	data1 := &billing.MoneyBackCostMerchantRequest{
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
