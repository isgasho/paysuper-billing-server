package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/jinzhu/copier"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"time"
)

const (
	refundDefaultReasonMask = "Refund by order #%s"

	collectionRefund = "refund"
)

var (
	refundErrorUnknown            = newBillingServerErrorMsg("rf000001", "refund can't be create. try request later")
	refundErrorNotAllowed         = newBillingServerErrorMsg("rf000002", "create refund for order not allowed")
	refundErrorAlreadyRefunded    = newBillingServerErrorMsg("rf000003", "amount by order was fully refunded")
	refundErrorPaymentAmountLess  = newBillingServerErrorMsg("rf000004", "refund unavailable, because payment amount less than total refunds amount")
	refundErrorNotFound           = newBillingServerErrorMsg("rf000005", "refund with specified data not found")
	refundErrorOrderNotFound      = newBillingServerErrorMsg("rf000006", "information about payment for refund with specified data not found")
	refundErrorCostsRatesNotFound = newBillingServerErrorMsg("rf000007", "settings to calculate commissions not found")
)

type createRefundChecked struct {
	order *billing.Order
}

type createRefundProcessor struct {
	service *Service
	request *grpc.CreateRefundRequest
	checked *createRefundChecked
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
	}

	refund, err := processor.processCreateRefund()

	if err != nil {
		rsp.Status = err.(*grpc.ResponseError).Status
		rsp.Message = err.(*grpc.ResponseError).Message

		return nil
	}

	h, err := s.NewPaymentSystem(s.cfg.PaymentSystemConfig, processor.checked.order)

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

	err = s.db.Collection(collectionRefund).UpdateId(bson.ObjectIdHex(refund.Id), refund)

	if err != nil {
		zap.S().Errorf("Query to update refund failed", "err", err.Error(), "data", refund)

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
	var refunds []*billing.Refund

	query := bson.M{"original_order.uuid": req.OrderId}
	err := s.db.Collection(collectionRefund).Find(query).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&refunds)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.S().Errorf("Query to find refunds by order failed", "err", err.Error(), "query", query)
		}

		return nil
	}

	count, err := s.db.Collection(collectionRefund).Find(query).Count()

	if err != nil {
		zap.S().Errorf("Query to count refunds by order failed", "err", err.Error(), "query", query)
		return nil
	}

	if refunds != nil {
		rsp.Count = int32(count)
		rsp.Items = refunds
	}

	return nil
}

func (s *Service) GetRefund(
	ctx context.Context,
	req *grpc.GetRefundRequest,
	rsp *grpc.CreateRefundResponse,
) error {
	var refund *billing.Refund

	query := bson.M{"_id": bson.ObjectIdHex(req.RefundId), "original_order.uuid": req.OrderId}
	err := s.db.Collection(collectionRefund).Find(query).One(&refund)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "query", query)
		}

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
	var refund *billing.Refund

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

	err := s.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(refundId)).One(&refund)

	if err != nil || refund == nil {
		if err != nil && err != mgo.ErrNotFound {
			zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", refundId)
		}

		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Error = refundErrorNotFound.Error()

		return nil
	}

	order, err := s.getOrderById(refund.OriginalOrder.Id)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Error = refundErrorOrderNotFound.Error()

		return nil
	}

	h, err := s.NewPaymentSystem(s.cfg.PaymentSystemConfig, order)

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

	if pErr == nil && refund.CreatedOrderId == "" {
		refund.CreatedOrderId, err = s.createOrderByRefund(order, refund)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Error = err.Error()
			return nil
		}
	}

	err = s.db.Collection(collectionRefund).UpdateId(bson.ObjectIdHex(refundId), refund)

	if err != nil {
		zap.S().Errorf("Update refund data failed", "err", err.Error(), "refund", refund)

		rsp.Error = orderErrorUnknown.Error()
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	if pErr == nil {
		err = s.onRefundNotify(ctx, refund, order)

		if err != nil {
			zap.L().Error(
				pkg.MethodFinishedWithError,
				zap.String("method", "onRefundNotify"),
				zap.Error(err),
				zap.String("refundId", refund.Id),
				zap.String("refund-orderId", order.Id),
			)

			rsp.Error = err.Error()
			rsp.Status = pkg.ResponseStatusSystemError

			return nil
		}

		processor := &createRefundProcessor{service: s}
		refundedAmount, _ := processor.getRefundedAmount(order)

		if refundedAmount == order.TotalPaymentAmount {
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
			order.Refund = &billing.OrderNotificationRefund{
				Amount:        refundedAmount,
				Currency:      order.Currency,
				Reason:        refund.Reason,
				ReceiptNumber: refund.Id,
			}

			err = s.updateOrder(order)

			if err != nil {
				zap.S().Errorf("Update order data failed", "err", err.Error(), "order", order)
			}
		}

		rsp.Status = pkg.ResponseStatusOk
	}

	return nil
}

func (s *Service) createOrderByRefund(order *billing.Order, refund *billing.Refund) (string, error) {
	refundOrder := new(billing.Order)
	err := copier.Copy(&refundOrder, &order)

	if err != nil {
		zap.S().Error(
			"Copy order to new structure order by refund failed",
			zap.Error(err),
			zap.Any("refund", refund),
		)

		return "", refundErrorUnknown
	}

	country, err := s.country.GetByIsoCodeA2(order.GetCountry())
	if err != nil {
		zap.S().Error(
			"country not found",
			zap.Error(err),
		)
		return "", refundErrorUnknown
	}

	isVatDeduction := false

	if country.VatEnabled {
		from, _, err := s.getLastVatReportTime(country.VatPeriodMonth)
		if err != nil {
			zap.S().Error(
				"cannot get last vat report time",
				zap.Error(err),
			)
			return "", refundErrorUnknown
		}

		orderPayedAt, err := ptypes.Timestamp(order.PaymentMethodOrderClosedAt)

		if err != nil {
			zap.S().Error(
				"cannot get convert PaymentMethodOrderClosedAt date to time",
				zap.Error(err),
			)
			return "", refundErrorUnknown
		}

		if orderPayedAt.Unix() < from.Unix() {
			isVatDeduction = true
		}
	}

	refundOrder.Id = bson.NewObjectId().Hex()
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
	refundOrder.Refunded = true
	refundOrder.PaymentMethodOrderClosedAt = ptypes.TimestampNow()
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
	refundOrder.ParentPaymentAt = refundOrder.PaymentMethodOrderClosedAt
	refundOrder.PaymentMethodOrderClosedAt = ptypes.TimestampNow()

	refundOrder.TotalPaymentAmount = refund.Amount

	refundOrder.Tax.Amount = tools.FormatAmount(refund.Amount / (1 + refundOrder.Tax.Rate) * refundOrder.Tax.Rate)
	refundOrder.OrderAmount = tools.FormatAmount(refundOrder.TotalPaymentAmount - refundOrder.Tax.Amount)
	refundOrder.ReceiptId = uuid.New().String()

	err = s.db.Collection(collectionOrder).Insert(refundOrder)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrder),
			zap.Any("query", refundOrder),
		)

		return "", refundErrorUnknown
	}

	return refundOrder.Id, nil
}

func (p *createRefundProcessor) processCreateRefund() (*billing.Refund, error) {
	err := p.processOrder()

	if err != nil {
		return nil, err
	}

	if !p.hasMoneyBackCosts(p.checked.order) {
		return nil, newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorCostsRatesNotFound)
	}

	err = p.processRefundsByOrder()

	if err != nil {
		return nil, err
	}

	order := p.checked.order

	refund := &billing.Refund{
		Id: bson.NewObjectId().Hex(),
		OriginalOrder: &billing.RefundOrder{
			Id:   order.Id,
			Uuid: order.Uuid,
		},
		Amount:    p.request.Amount,
		CreatorId: p.request.CreatorId,
		Reason:    fmt.Sprintf(refundDefaultReasonMask, p.checked.order.Id),
		Currency:  p.checked.order.Currency,
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
		refund.Amount = p.checked.order.TotalPaymentAmount
		refund.IsChargeback = p.request.IsChargeback
	}

	if order.Tax != nil {
		refund.SalesTax = float32(order.Tax.Amount)
	}

	if p.request.Reason != "" {
		refund.Reason = p.request.Reason
	}

	err = p.service.db.Collection(collectionRefund).Insert(refund)

	if err != nil {
		p.service.logError("Query to insert refund failed", []interface{}{"err", err.Error(), "data", refund})
		return nil, newBillingServerResponseError(pkg.ResponseStatusBadData, orderErrorUnknown)
	}

	return refund, nil
}

func (p *createRefundProcessor) processOrder() error {
	order, err := p.service.getOrderByUuid(p.request.OrderId)

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
	refundedAmount, err := p.getRefundedAmount(p.checked.order)

	if err != nil {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorUnknown)
	}

	if p.checked.order.TotalPaymentAmount < (refundedAmount + p.request.Amount) {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, refundErrorPaymentAmountLess)
	}

	return nil
}

func (p *createRefundProcessor) getRefundedAmount(order *billing.Order) (float64, error) {
	var res struct {
		Id     bson.ObjectId `bson:"_id"`
		Amount float64       `bson:"amount"`
	}

	query := []bson.M{
		{
			"$match": bson.M{
				"status":            bson.M{"$nin": []int32{pkg.RefundStatusRejected}},
				"original_order.id": bson.ObjectIdHex(order.Id),
			},
		},
		{"$group": bson.M{"_id": "$order.id", "amount": bson.M{"$sum": "$amount"}}},
	}

	err := p.service.db.Collection(collectionRefund).Pipe(query).One(&res)

	if err != nil && !p.service.IsDbNotFoundError(err) {
		p.service.logError("Query to calculate refunded amount by order failed", []interface{}{"err", err.Error(), "query", query})
		return 0, refundErrorUnknown
	}

	return res.Amount, nil
}

func (s *Service) getRefundById(id string) (*billing.Refund, error) {
	var refund *billing.Refund

	err := s.db.Collection(collectionRefund).FindId(bson.ObjectIdHex(id)).One(&refund)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", id)
		}

		return nil, err
	}

	return refund, nil
}

func (p *createRefundProcessor) hasMoneyBackCosts(order *billing.Order) bool {
	country, err := p.service.country.GetByIsoCodeA2(order.GetCountry())

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
		Name:           methodName,
		PayoutCurrency: order.GetMerchantRoyaltyCurrency(),
		Region:         country.Region,
		Country:        country.IsoCodeA2,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
		UndoReason:     reason,
	}
	_, err = p.service.getMoneyBackCostSystem(data)

	if err != nil {
		return false
	}

	data1 := &billing.MoneyBackCostMerchantRequest{
		MerchantId:     order.GetMerchantId(),
		Name:           methodName,
		PayoutCurrency: order.GetMerchantRoyaltyCurrency(),
		UndoReason:     reason,
		Region:         country.Region,
		Country:        country.IsoCodeA2,
		PaymentStage:   1,
		Days:           int32(refundAt.Sub(paymentAt).Hours() / 24),
	}
	_, err = p.service.getMoneyBackCostMerchant(data1)
	return err == nil
}
