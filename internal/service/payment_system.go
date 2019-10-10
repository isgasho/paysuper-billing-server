package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
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
	paymentSystemErrorUnknownPaymentMethod                   = newBillingServerErrorMsg("ph000003", "unknown payment method")
	paymentSystemErrorCreateRequestFailed                    = newBillingServerErrorMsg("ph000004", "order can't be create. try request later")
	paymentSystemErrorEWalletIdentifierIsInvalid             = newBillingServerErrorMsg("ph000005", "wallet identifier is invalid")
	paymentSystemErrorRequestSignatureIsInvalid              = newBillingServerErrorMsg("ph000006", "request signature is invalid")
	paymentSystemErrorRequestTimeFieldIsInvalid              = newBillingServerErrorMsg("ph000007", "time field in request is invalid")
	paymentSystemErrorRequestRecurringIdFieldIsInvalid       = newBillingServerErrorMsg("ph000008", "recurring id field in request is invalid")
	paymentSystemErrorRequestStatusIsInvalid                 = newBillingServerErrorMsg("ph000009", "status is invalid")
	paymentSystemErrorRequestPaymentMethodIsInvalid          = newBillingServerErrorMsg("ph000010", "payment method from request not match with value in order")
	paymentSystemErrorRequestAmountOrCurrencyIsInvalid       = newBillingServerErrorMsg("ph000011", "amount or currency from request not match with value in order")
	paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid = newBillingServerErrorMsg("ph000012", "amount or currency from request not match with value in refund")
	paymentSystemErrorRequestTemporarySkipped                = newBillingServerErrorMsg("ph000013", "notification skipped with temporary status")
	paymentSystemErrorRecurringFailed                        = newBillingServerErrorMsg("ph000014", "recurring payment failed")

	paymentSystemHandlers = map[string]func(cfg *config.PaymentSystemConfig) PaymentSystem{
		pkg.PaymentSystemHandlerCardPay: newCardPayHandler,
		paymentSystemHandlerMockOk:      NewPaymentSystemMockOk,
		paymentSystemHandlerMockError:   NewPaymentSystemMockError,
		paymentSystemHandlerCardPayMock: NewCardPayMock,
	}
)

type Path struct {
	path   string
	method string
}

type PaymentSystem interface {
	CreatePayment(order *billing.Order, requisites map[string]string) (string, error)
	ProcessPayment(order *billing.Order, message proto.Message, raw, signature string) error
	IsRecurringCallback(request proto.Message) bool
	GetRecurringId(request proto.Message) string
	CreateRefund(order *billing.Order, refund *billing.Refund) error
	ProcessRefund(order *billing.Order, refund *billing.Refund, message proto.Message, raw, signature string) error
}

func (s *Service) NewPaymentSystem(
	cfg *config.PaymentSystemConfig,
	order *billing.Order,
) (PaymentSystem, error) {
	ps, err := s.paymentSystem.GetById(order.PaymentMethod.PaymentSystemId)

	if err != nil {
		return nil, err
	}

	h, ok := paymentSystemHandlers[ps.Handler]

	if !ok {
		return nil, paymentSystemErrorHandlerNotFound
	}

	return h(cfg), nil
}

type PaymentSystemServiceInterface interface {
	GetById(string) (*billing.PaymentSystem, error)
	Insert(*billing.PaymentSystem) error
	MultipleInsert([]*billing.PaymentSystem) error
	Update(*billing.PaymentSystem) error
}

func newPaymentSystemService(svc *Service) *PaymentSystemService {
	s := &PaymentSystemService{svc: svc}
	return s
}

func (h PaymentSystemService) GetById(id string) (*billing.PaymentSystem, error) {
	var c billing.PaymentSystem
	key := fmt.Sprintf(cachePaymentSystem, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionPaymentSystem).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentSystem)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h *PaymentSystemService) Insert(ps *billing.PaymentSystem) error {
	if err := h.svc.db.Collection(collectionPaymentSystem).Insert(ps); err != nil {
		return err
	}

	err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0)
	if err != nil {
		return err
	}

	return nil
}

func (h PaymentSystemService) MultipleInsert(ps []*billing.PaymentSystem) error {
	c := make([]interface{}, len(ps))
	for i, v := range ps {
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentSystem).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h *PaymentSystemService) Update(ps *billing.PaymentSystem) error {
	err := h.svc.db.Collection(collectionPaymentSystem).UpdateId(bson.ObjectIdHex(ps.Id), ps)
	if err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0); err != nil {
		return err
	}

	return nil
}
