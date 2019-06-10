package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

const (
	cacheCurrencyRateFromTo = "currency_rate:from:%d:to:%d"

	collectionCurrencyRate = "currency_rate"
)

func newCurrencyRateService(svc *Service) *CurrencyRate {
	s := &CurrencyRate{svc: svc}
	return s
}

func (h *CurrencyRate) Insert(rate *billing.CurrencyRate) error {
	if err := h.svc.db.Collection(collectionCurrencyRate).Insert(rate); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cacheCurrencyRateFromTo, rate.CurrencyFrom, rate.CurrencyTo), rate, 0); err != nil {
		return err
	}

	return nil
}

func (h CurrencyRate) MultipleInsert(country []*billing.CurrencyRate) error {
	c := make([]interface{}, len(country))
	for i, v := range country {
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionCurrencyRate).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h *CurrencyRate) GetFromTo(from int32, to int32) (*billing.CurrencyRate, error) {
	var c billing.CurrencyRate
	key := fmt.Sprintf(cacheCurrencyRateFromTo, from, to)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(collectionCurrencyRate).Find(bson.M{"is_active": true, "currency_from": from, "currency_to": to}).One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, collectionCurrencyRate)
		}
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h *CurrencyRate) Convert(from int32, to int32, value float64) (float64, error) {
	rate, err := h.GetFromTo(from, to)
	if err != nil {
		return float64(0), err
	}

	if rate.Id == "" {
		return 0, fmt.Errorf(errorNotFound, collectionCurrencyRate)
	}

	return tools.FormatAmount(value / rate.Rate), nil
}
