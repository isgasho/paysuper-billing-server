package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	paymentMethodErrorInsert = "unable to insert payment method"
)

func newPaymentMethodService(svc *Service) *PaymentMethod {
	s := &PaymentMethod{svc: svc}
	return s
}

func (h PaymentMethod) GetByGroupAndCurrency(group string, currency int32) (*billing.PaymentMethod, error) {
	c := &billing.PaymentMethod{}
	key := fmt.Sprintf(pkg.CachePaymentMethodGroup, group)
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil, fmt.Errorf(errorInterfaceCast, pkg.CollectionPaymentMethod)
		}
		return c, nil
	}

	err = h.svc.db.Collection(pkg.CollectionPaymentMethod).Find(bson.M{"group_alias": group, "currencies": currency}).One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionPaymentMethod)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c, nil
}

func (h PaymentMethod) GetById(id string) (*billing.PaymentMethod, error) {
	c := &billing.PaymentMethod{}
	key := fmt.Sprintf(pkg.CachePaymentMethodId, id)
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil, fmt.Errorf(errorInterfaceCast, pkg.CollectionPaymentMethod)
		}
		return c, nil
	}

	err = h.svc.db.Collection(pkg.CollectionPaymentMethod).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionPaymentMethod)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c, nil
}

func (h PaymentMethod) GetAll() map[string]*billing.PaymentMethod {
	c := map[string]*billing.PaymentMethod{}
	key := pkg.CachePaymentMethodAll
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil
		}
		return c
	}

	var data []*billing.PaymentMethod
	err = h.svc.db.Collection(pkg.CollectionPaymentMethod).Find(bson.M{}).All(&data)
	if err != nil || data == nil {
		return nil
	}

	pool := make(map[string]*billing.PaymentMethod, len(data))
	for _, v := range data {
		pool[v.Id] = v
	}

	_ = h.svc.cacher.Set(key, pool, 0)
	return pool
}

func (h PaymentMethod) Groups() map[string]map[int32]*billing.PaymentMethod {
	pool := h.GetAll()
	if pool == nil {
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

func (h PaymentMethod) MultipleInsert(pm []*billing.PaymentMethod) error {
	pms := make([]interface{}, len(pm))
	for i, v := range pm {
		pms[i] = v
	}

	if err := h.svc.db.Collection(pkg.CollectionPaymentMethod).Insert(pms...); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(pkg.CachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentMethod) Insert(pm *billing.PaymentMethod) error {
	if err := h.svc.db.Collection(pkg.CollectionPaymentMethod).Insert(pm); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(pkg.CachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}
