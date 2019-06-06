package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newPaymentMethodService(svc *Service) *PaymentMethod {
	s := &PaymentMethod{svc: svc}
	s.loadAllToCache()

	return s
}

func (h PaymentMethod) GetPaymentMethodByGroupAndCurrency(group string, currency int32) (*billing.PaymentMethod, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	for _, r := range pool {
		for _, v := range r.Currencies {
			if v == currency && r.Group == group {
				return r, nil
			}
		}
	}

	return nil, fmt.Errorf(errorNotFound, pkg.CollectionPaymentMethod)
}

func (h PaymentMethod) GetPaymentMethodById(id string) (*billing.PaymentMethod, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	c, ok := pool[id]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionPaymentMethod)
	}

	return c, nil
}

func (h PaymentMethod) GetAll() map[string]*billing.PaymentMethod {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h PaymentMethod) Groups() map[string]map[int32]*billing.PaymentMethod {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	groups := make(map[string]map[int32]*billing.PaymentMethod, len(pool))
	for _, r := range pool {
		group := make(map[int32]*billing.PaymentMethod, len(r.Currencies))
		for _, v := range r.Currencies {
			group[v] = r
		}
		groups[r.Group] = group
	}

	return groups
}

func (h *PaymentMethod) loadAllToCache() error {
	var data []*billing.PaymentMethod

	err := h.svc.db.Collection(pkg.CollectionPaymentMethod).Find(bson.M{}).All(&data)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	pool := map[string]*billing.PaymentMethod{}
	if data != nil {
		pool = make(map[string]*billing.PaymentMethod, len(data))
		for _, v := range data {
			pool[v.Id] = v
		}
	}

	if err := h.svc.cacher.Set(pkg.CollectionPaymentMethod, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) loadAllFromCache() (map[string]*billing.PaymentMethod, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionPaymentMethod)
	if err != nil {
		return nil, err
	}

	var a map[string]*billing.PaymentMethod
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
