package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newCurrencyService(svc *Service) *Currency {
	s := &Currency{svc: svc}
	return s
}

func (h *Currency) Insert(currency *billing.Currency) error {
	if err := h.svc.db.Collection(pkg.CollectionCurrency).Insert(currency); err != nil {
		return err
	}

	key := fmt.Sprintf(pkg.CacheCurrencyA3, currency.CodeA3)
	if err := h.svc.cacher.Set(key, currency, 0); err != nil {
		return err
	}

	return nil
}

func (h Currency) GetByCodeA3(code string) (*billing.Currency, error) {
	c := &billing.Currency{}
	key := fmt.Sprintf(pkg.CacheCurrencyA3, code)
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil, fmt.Errorf(errorInterfaceCast, pkg.CollectionCurrency)
		}
		return c, nil
	}

	err = h.svc.db.Collection(pkg.CollectionCurrency).Find(bson.M{"is_active": true, "code_a3": code}).One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrency)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c, nil
}
