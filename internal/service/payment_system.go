package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"sync"
)

const (
	paymentSystemHandlerMockOk      = "mock_ok"
	paymentSystemHandlerMockError   = "mock_error"
	paymentSystemHandlerCardPayMock = "cardpay_mock"

	defaultHttpClientTimeout = 10

	cachePaymentSystem      = "payment_system:id:%s"
	collectionPaymentSystem = "payment_system"
)

var (
	paymentSystemErrorHandlerNotFound                        = newBillingServerErrorMsg("ph000001", "handler for specified payment system not found")
	paymentSystemErrorAuthenticateFailed                     = newBillingServerErrorMsg("ph000002", "authentication failed")
	paymentSystemErrorUnknownPaymentMethod                   = newBillingServerErrorMsg("ph000003", "unknown payment Method")
	paymentSystemErrorCreateRequestFailed                    = newBillingServerErrorMsg("ph000004", "order can't be create. try request later")
	paymentSystemErrorEWalletIdentifierIsInvalid             = newBillingServerErrorMsg("ph000005", "wallet identifier is invalid")
	paymentSystemErrorRequestSignatureIsInvalid              = newBillingServerErrorMsg("ph000006", "request signature is invalid")
	paymentSystemErrorRequestTimeFieldIsInvalid              = newBillingServerErrorMsg("ph000007", "time field in request is invalid")
	paymentSystemErrorRequestRecurringIdFieldIsInvalid       = newBillingServerErrorMsg("ph000008", "recurring id field in request is invalid")
	paymentSystemErrorRequestStatusIsInvalid                 = newBillingServerErrorMsg("ph000009", "status is invalid")
	paymentSystemErrorRequestPaymentMethodIsInvalid          = newBillingServerErrorMsg("ph000010", "payment Method from request not match with value in order")
	paymentSystemErrorRequestAmountOrCurrencyIsInvalid       = newBillingServerErrorMsg("ph000011", "amount or currency from request not match with value in order")
	paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid = newBillingServerErrorMsg("ph000012", "amount or currency from request not match with value in refund")
	paymentSystemErrorRequestTemporarySkipped                = newBillingServerErrorMsg("ph000013", "notification skipped with temporary status")
	paymentSystemErrorRecurringFailed                        = newBillingServerErrorMsg("ph000014", "recurring payment failed")

	registry = map[string]func() Gate{
		billingpb.PaymentSystemHandlerCardPay: newCardPayHandler,
		paymentSystemHandlerMockOk:            NewPaymentSystemMockOk,
		paymentSystemHandlerMockError:         NewPaymentSystemMockError,
		paymentSystemHandlerCardPayMock:       NewCardPayMock,
	}
)

type Gate interface {
	CreatePayment(order *billingpb.Order, successUrl, failUrl string, requisites map[string]string) (string, error)
	ProcessPayment(order *billingpb.Order, message proto.Message, raw, signature string) error
	IsRecurringCallback(request proto.Message) bool
	GetRecurringId(request proto.Message) string
	CreateRefund(order *billingpb.Order, refund *billingpb.Refund) error
	ProcessRefund(order *billingpb.Order, refund *billingpb.Refund, message proto.Message, raw, signature string) error
}

type Gateway struct {
	gateways map[string]Gate
	mx       sync.Mutex
}

func (s *Service) newPaymentSystemGateway() *Gateway {
	paymentSystem := &Gateway{
		gateways: make(map[string]Gate),
	}
	return paymentSystem
}

func (m *Gateway) getGateway(name string) (Gate, error) {
	initFn, ok := registry[name]

	if !ok {
		return nil, paymentSystemErrorHandlerNotFound
	}

	m.mx.Lock()
	gateway, ok := m.gateways[name]

	if !ok {
		gateway = initFn()
		m.gateways[name] = gateway
	}

	m.mx.Unlock()
	return gateway, nil
}

type PaymentSystemServiceInterface interface {
	GetById(context.Context, string) (*billingpb.PaymentSystem, error)
	Insert(context.Context, *billingpb.PaymentSystem) error
	MultipleInsert(context.Context, []*billingpb.PaymentSystem) error
	Update(context.Context, *billingpb.PaymentSystem) error
}

func newPaymentSystemService(svc *Service) *PaymentSystemService {
	s := &PaymentSystemService{svc: svc}
	return s
}

func (h PaymentSystemService) GetById(ctx context.Context, id string) (*billingpb.PaymentSystem, error) {
	var c billingpb.PaymentSystem
	key := fmt.Sprintf(cachePaymentSystem, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "is_active": true}
	err := h.svc.db.Collection(collectionPaymentSystem).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentSystem)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h *PaymentSystemService) Insert(ctx context.Context, ps *billingpb.PaymentSystem) error {
	_, err := h.svc.db.Collection(collectionPaymentSystem).InsertOne(ctx, ps)

	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0)
	if err != nil {
		return err
	}

	return nil
}

func (h PaymentSystemService) MultipleInsert(ctx context.Context, ps []*billingpb.PaymentSystem) error {
	c := make([]interface{}, len(ps))
	for i, v := range ps {
		c[i] = v
	}

	_, err := h.svc.db.Collection(collectionPaymentSystem).InsertMany(ctx, c)

	if err != nil {
		return err
	}

	return nil
}

func (h *PaymentSystemService) Update(ctx context.Context, ps *billingpb.PaymentSystem) error {
	oid, _ := primitive.ObjectIDFromHex(ps.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPaymentSystem).ReplaceOne(ctx, filter, ps)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0); err != nil {
		return err
	}

	return nil
}
