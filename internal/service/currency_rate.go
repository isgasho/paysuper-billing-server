package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

func newCurrencyRateService(svc *Service) *CurrencyRate {
	s := &CurrencyRate{svc: svc}
	return s
}

func (h *CurrencyRate) GetFromTo(from int32, to int32) (*billing.CurrencyRate, error) {
	c := &billing.CurrencyRate{}
	key := fmt.Sprintf(pkg.CacheCurrencyRateFromTo, from, to)
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil, fmt.Errorf(errorInterfaceCast, pkg.CollectionCurrencyRate)
		}
		return c, nil
	}

	err = h.svc.db.Collection(pkg.CollectionCurrencyRate).Find(bson.M{"is_active": true, "currency_from": from, "currency_to": to}).One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c, nil
}

func (h *CurrencyRate) Convert(from int32, to int32, value float64) (float64, error) {
	rate, err := h.GetFromTo(from, to)
	if err != nil {
		return float64(0), err
	}

	if rate.Id == "" {
		return 0, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
	}

	return tools.FormatAmount(value / rate.Rate), nil
}
