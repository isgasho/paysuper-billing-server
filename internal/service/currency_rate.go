package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

const (
	CacheCurrencyRateFromTo = pkg.CollectionCurrencyRate + ":from:%d:to:%d"
)

func newCurrencyRateService(svc *Service) *CurrencyRate {
	s := &CurrencyRate{svc: svc}
	return s
}

func (h *CurrencyRate) GetFromTo(from int32, to int32) (*billing.CurrencyRate, error) {
	var c billing.CurrencyRate
	key := fmt.Sprintf(CacheCurrencyRateFromTo, from, to)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(pkg.CollectionCurrencyRate).Find(bson.M{"is_active": true, "currency_from": from, "currency_to": to}).One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
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
		return 0, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
	}

	return tools.FormatAmount(value / rate.Rate), nil
}
