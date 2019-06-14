package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	cachePaymentMethodId    = "payment_method:id:%s"
	cachePaymentMethodGroup = "payment_method:group:%s"
	cachePaymentMethodAll   = "payment_method:all"

	collectionPaymentMethod = "payment_method"
)

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
	key := cachePaymentMethodAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		var data []*billing.PaymentMethod
		if err = h.svc.db.Collection(collectionPaymentMethod).Find(bson.M{}).All(&data); err != nil {
			return nil, err
		}

		pool := make(map[string]*billing.PaymentMethod, len(data))
		for _, v := range data {
			pool[v.Id] = v
		}
		c.Methods = pool
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c.Methods, nil
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
