package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newMerchantService(svc *Service) *Merchant {
	s := &Merchant{svc: svc}
	s.loadAllToCache()

	return s
}

func (h *Merchant) Update(merchant *billing.Merchant) error {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return err
	}

	if _, ok := pool[merchant.Id]; !ok {
		pool[merchant.Id] = &billing.Merchant{}
	}
	pool[merchant.Id] = merchant

	if err := h.svc.cacher.Set(pkg.CollectionMerchant, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Merchant) Get(id string) (*billing.Merchant, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	c, ok := pool[id]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionMerchant)
	}

	return c, nil
}

func (h Merchant) GetAll() map[string]*billing.Merchant {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h Merchant) GetMerchantPaymentMethod(merchantId string, pmId string) (*billing.MerchantPaymentMethod, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, errors.New(orderErrorPaymentMethodNotAllowed)
	}

	m, ok := pool[merchantId]
	if !ok {
		return nil, errors.New(orderErrorPaymentMethodNotAllowed)
	}

	merchantPaymentMethods := make(map[string]*billing.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm := h.svc.paymentMethod.GetAll()
	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billing.MerchantPaymentMethod{
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: DefaultPaymentMethodFee,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      DefaultPaymentMethodPerTransactionFee,
						Currency: DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[pmId]; !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionMerchant)
	}

	return merchantPaymentMethods[pmId], nil
}

func (h Merchant) GetMerchantPaymentMethodTerminalId(merchantId, pmId string) (string, error) {
	pm, err := h.GetMerchantPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalId == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalId, nil
}

func (h Merchant) GetMerchantPaymentMethodTerminalPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetMerchantPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalPassword == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalPassword, nil
}

func (h Merchant) GetMerchantPaymentMethodTerminalCallbackPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetMerchantPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalCallbackPassword == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalCallbackPassword, nil
}

func (h Merchant) getMerchantPaymentMethod(m *billing.Merchant, pmId string) (*billing.MerchantPaymentMethod, error) {
	merchantPaymentMethods := make(map[string]*billing.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm := h.svc.paymentMethod.GetAll()
	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billing.MerchantPaymentMethod{
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: DefaultPaymentMethodFee,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      DefaultPaymentMethodPerTransactionFee,
						Currency: DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[pmId]; !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionMerchant)
	}

	return merchantPaymentMethods[pmId], nil
}

func (h *Merchant) loadAllToCache() error {
	var data []*billing.Merchant

	err := h.svc.db.Collection(pkg.CollectionCountry).Find(bson.M{}).All(&data)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	pool := map[string]*billing.Merchant{}
	if data != nil {
		pool = make(map[string]*billing.Merchant, len(data))
		for _, v := range data {
			pool[v.Id] = v
		}
	}

	if err := h.svc.cacher.Set(pkg.CollectionMerchant, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Merchant) loadAllFromCache() (map[string]*billing.Merchant, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionMerchant)
	if err != nil {
		return nil, err
	}

	var a map[string]*billing.Merchant
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
