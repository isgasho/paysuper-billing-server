package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	cacheCurrencyA3 = "currency:code_a3:%s"

	collectionCurrency = "currency"
)

func newCurrencyService(svc *Service) *Currency {
	s := &Currency{svc: svc}
	return s
}

func (h *Currency) Insert(currency *billing.Currency) error {
	if err := h.svc.db.Collection(collectionCurrency).Insert(currency); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cacheCurrencyA3, currency.CodeA3), currency, 0); err != nil {
		return err
	}

	return nil
}

func (h Currency) MultipleInsert(currency []*billing.Currency) error {
	c := make([]interface{}, len(currency))
	for i, v := range currency {
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionCurrency).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h Currency) GetByCodeA3(code string) (*billing.Currency, error) {
	var c billing.Currency
	key := fmt.Sprintf(cacheCurrencyA3, code)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionCurrency).
		Find(bson.M{"is_active": true, "code_a3": code}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionCurrency)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}
