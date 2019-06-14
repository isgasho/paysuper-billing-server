package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

const (
	cachePaymentMethodId    = "payment_method:id:%s"
	cachePaymentMethodGroup = "payment_method:group:%s"
	cachePaymentMethodAll   = "payment_method:all"

	collectionPaymentMethod = "payment_method"

	paymentMethodErrorName                       = "payment method must contain name"
	paymentMethodErrorPaymentSystem              = "payment method must contain of payment system"
	paymentMethodErrorIncorrectCurrencyA3        = "payment method must contain currency code a3 for production settings"
	paymentMethodErrorIncorrectTerminal          = "payment method must contain terminal settings for production settings"
	paymentMethodErrorEmptyPassword              = "payment method must contain password for production settings"
	paymentMethodErrorEmptyCallbackPassword      = "payment method must contain callback password for production settings"
	paymentMethodErrorUnknownMethod              = "payment method is unknown"
	paymentMethodErrorNotFoundProductionSettings = "payment method is not contain requesting settings"
)

func (s *Service) CreateOrUpdatePaymentMethod(
	ctx context.Context,
	req *billing.PaymentMethod,
	rsp *grpc.ChangePaymentMethodResponse,
) error {
	var pm *billing.PaymentMethod
	var err error

	if req.Name == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorName

		return nil
	}

	if req.PaymentSystem == nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorPaymentSystem

		return nil
	}

	if req.Id != "" {
		pm, err = s.paymentMethod.GetById(req.Id)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = err.Error()

			return nil
		}
	}

	pm.UpdatedAt = ptypes.TimestampNow()

	if pm == nil {
		pm.CreatedAt = ptypes.TimestampNow()
		err = s.paymentMethod.Insert(req)
	} else {
		err = s.paymentMethod.Update(pm)
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	return nil
}

func (s *Service) CreateOrUpdatePaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.ChangePaymentMethodRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	var pm *billing.PaymentMethod
	var err error

	pm, err = s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if req.Params.CurrencyCodeA3 == "" || len(req.Params.CurrencyCodeA3) != 2 {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorIncorrectCurrencyA3

		return nil
	}

	if req.Params.Password == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorEmptyPassword

		return nil
	}

	if req.Params.CallbackPassword == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorEmptyCallbackPassword

		return nil
	}

	if req.Params.Terminal == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorIncorrectTerminal

		return nil
	}

	pm.ProductionSettings[req.Params.CurrencyCodeA3] = req.Params
	if err := s.paymentMethod.Update(pm); err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	return nil
}

func (s *Service) GetPaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodProductionSettingsRequest,
	rsp *billing.PaymentMethodParams,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		return nil
	}

	rsp, _ = pm.ProductionSettings[req.CurrencyA3]

	return nil
}

func (s *Service) DeletePaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodProductionSettingsRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if _, ok := pm.ProductionSettings[req.CurrencyA3]; !ok {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorNotFoundProductionSettings

		return nil
	}

	delete(pm.ProductionSettings, req.CurrencyA3)

	if err := s.paymentMethod.Update(pm); err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	return nil
}

type paymentMethods struct {
	Methods map[string]*billing.PaymentMethod
}

func newPaymentMethodService(svc *Service) *PaymentMethod {
	s := &PaymentMethod{svc: svc}
	return s
}

func (h PaymentMethod) GetByGroupAndCurrency(group string, currency int32) (*billing.PaymentMethod, error) {
	var c billing.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodGroup, group)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(collectionPaymentMethod).
			Find(bson.M{"group_alias": group, "currencies": currency}).
			One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
		}
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentMethod) GetById(id string) (*billing.PaymentMethod, error) {
	var c billing.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodId, id)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(collectionPaymentMethod).
			Find(bson.M{"_id": bson.ObjectIdHex(id)}).
			One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
		}
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentMethod) GetAll() (map[string]*billing.PaymentMethod, error) {
	var c paymentMethods
func (h PaymentMethod) GetByIdAndCurrency(id string, currencyCodeA3 string) (*billing.PaymentMethodParams, error) {
	pm, err := h.GetById(id)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	if _, ok := pm.ProductionSettings[currencyCodeA3]; !ok {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	return pm.ProductionSettings[currencyCodeA3], nil
}

	if err := s.paymentMethod.Update(pm); err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

func (h PaymentMethod) Groups() (map[string]map[int32]*billing.PaymentMethod, error) {
	pool, err := h.GetAll()
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, nil
	}

	groups := make(map[string]map[int32]*billing.PaymentMethod, len(pool))
	for _, r := range pool {
		group := make(map[int32]*billing.PaymentMethod, len(r.Currencies))
		for _, v := range r.Currencies {
			group[v] = r
		}
		groups[r.Group] = group
	}

	return groups, nil
}

func (h PaymentMethod) MultipleInsert(pm []*billing.PaymentMethod) error {
	pms := make([]interface{}, len(pm))
	for i, v := range pm {
		pms[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentMethod).Insert(pms...); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentMethod) Insert(pm *billing.PaymentMethod) error {
	if err := h.svc.db.Collection(collectionPaymentMethod).Insert(pm); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentMethod) Update(pm *billing.PaymentMethod) error {
	if err := h.svc.db.Collection(collectionPaymentMethod).UpdateId(pm.Id, pm); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}
