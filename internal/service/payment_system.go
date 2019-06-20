package service

import (
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	paymentSystemHandlerMockOk    = "mock_ok"
	paymentSystemHandlerMockError = "mock_error"

	paymentSystemErrorHandlerNotFound                        = "handler for specified payment system not found"
	paymentSystemErrorAuthenticateFailed                     = "authentication failed"
	paymentSystemErrorUnknownPaymentMethod                   = "unknown payment method"
	paymentSystemErrorCreateRequestFailed                    = "order can't be create. try request later"
	paymentSystemErrorEWalletIdentifierIsInvalid             = "wallet identifier is invalid"
	paymentSystemErrorRequestSignatureIsInvalid              = "request signature is invalid"
	paymentSystemErrorRequestTimeFieldIsInvalid              = "time field in request is invalid"
	paymentSystemErrorRequestRecurringIdFieldIsInvalid       = "recurring id field in request is invalid"
	paymentSystemErrorRequestStatusIsInvalid                 = "status is invalid"
	paymentSystemErrorRequestPaymentMethodIsInvalid          = "payment method from request not match with value in order"
	paymentSystemErrorRequestAmountOrCurrencyIsInvalid       = "amount or currency from request not match with value in order"
	paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid = "amount or currency from request not match with value in refund"
	paymentSystemErrorRequestTemporarySkipped                = "notification skipped with temporary status"
	paymentSystemErrorRecurringFailed                        = "recurring payment failed"

	defaultHttpClientTimeout = 10
	defaultResponseBodyLimit = 512

	cachePaymentSystem      = "payment_system:id:%s"
	collectionPaymentSystem = "payment_system"
)

var paymentSystemHandlers = map[string]func(*paymentProcessor) PaymentSystem{
	pkg.PaymentSystemHandlerCardPay: newCardPayHandler,
	paymentSystemHandlerMockOk:      NewPaymentSystemMockOk,
	paymentSystemHandlerMockError:   NewPaymentSystemMockError,
}

type Error struct {
	err    string
	status int32
}

type Path struct {
	path   string
	method string
}

type PaymentSystem interface {
	CreatePayment(map[string]string) (string, error)
	ProcessPayment(request proto.Message, rawRequest string, signature string) error
	IsRecurringCallback(request proto.Message) bool
	GetRecurringId(request proto.Message) string
	CreateRefund(refund *billing.Refund) error
	ProcessRefund(refund *billing.Refund, message proto.Message, raw, signature string) (err error)
}

type paymentProcessor struct {
	cfg     *config.PaymentSystemConfig
	order   *billing.Order
	service *Service
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
		return nil, errors.New(paymentSystemErrorHandlerNotFound)
	}

	processor := &paymentProcessor{cfg: cfg, order: order, service: s}

	return h(processor), nil
}

func NewError(text string, status int32) error {
	return &Error{err: text, status: status}
}

func (e *Error) Error() string {
	return e.err
}

func (e *Error) Status() int32 {
	return e.status
}

func (h *paymentProcessor) cutBytes(body []byte, limit int) string {
	sBody := string(body)
	r := []rune(sBody)

	if len(r) > limit {
		return string(r[:limit])
	}

	return sBody
}

func (h *paymentProcessor) httpHeadersToString(headers map[string][]string) string {
	var out string

	for k, v := range headers {
		out += k + ":" + v[0] + "\n "
	}

	return out
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

	if err := h.svc.db.Collection(collectionPaymentSystem).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c); err != nil {
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

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0); err != nil {
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
	if err := h.svc.db.Collection(collectionPaymentSystem).UpdateId(bson.ObjectIdHex(ps.Id), ps); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentSystem, ps.Id), ps, 0); err != nil {
		return err
	}

	return nil
}
