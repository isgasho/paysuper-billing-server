package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	CacheCurrencyA3 = pkg.CollectionCurrency + ":code_a3:%s"
)

func newCurrencyService(svc *Service) *Currency {
	s := &Currency{svc: svc}
	return s
}

func (h *Currency) Insert(currency *billing.Currency) error {
	if err := h.svc.db.Collection(pkg.CollectionCurrency).Insert(currency); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(CacheCurrencyA3, currency.CodeA3), currency, 0); err != nil {
		return err
	}

	return nil
}

func (h Currency) GetByCodeA3(code string) (*billing.Currency, error) {
	var c billing.Currency
	key := fmt.Sprintf(CacheCurrencyA3, code)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(pkg.CollectionCurrency).Find(bson.M{"is_active": true, "code_a3": code}).One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrency)
		}
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}
