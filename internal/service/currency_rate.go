package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

func newCurrencyRateService(svc *Service) *CurrencyRate {
	s := &CurrencyRate{svc: svc}
	s.loadAllToCache()

	return s
}

func (h *CurrencyRate) Get(from int32, to int32) (*billing.CurrencyRate, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	rate := &billing.CurrencyRate{}
	for _, cr := range pool {
		if cr.CurrencyFrom == from && cr.CurrencyTo == to {
			rate = cr
		}
	}

	if rate.Id == "" {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
	}

	return rate, nil
}

func (h *CurrencyRate) Convert(from int32, to int32, value float64) (float64, error) {
	rate, err := h.Get(from, to)
	if err != nil {
		return float64(0), err
	}

	if rate.Id == "" {
		return 0, fmt.Errorf(errorNotFound, pkg.CollectionCurrencyRate)
	}

	return tools.FormatAmount(value / rate.Rate), nil
}

func (h *CurrencyRate) GetAll() map[string]*billing.CurrencyRate {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h *CurrencyRate) loadAllToCache() error {
	var data []*billing.CurrencyRate

	err := h.svc.db.Collection(pkg.CollectionCurrencyRate).Find(bson.M{"is_active": true}).All(&data)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	pool := make(map[string]*billing.CurrencyRate, len(data))
	for _, v := range data {
		pool[string(v.Id)] = v
	}

	if err := h.svc.cacher.Set(pkg.CollectionCurrencyRate, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *CurrencyRate) loadAllFromCache() (map[string]*billing.CurrencyRate, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionCurrencyRate)
	if err != nil {
		return nil, err
	}

	var a map[string]*billing.CurrencyRate
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
