package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newCurrencyService(svc *Service) *Currency {
	s := &Currency{svc: svc}
	s.loadAllToCache()

	return s
}

func (h Currency) GetCurrencyByCodeA3(code string) (*billing.Currency, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	c, ok := pool[code]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCurrency)
	}

	return c, nil
}

func (h Currency) GetAll() map[string]*billing.Currency {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h *Currency) loadAllToCache() error {
	var data []*billing.Currency

	err := h.svc.db.Collection(pkg.CollectionCurrency).Find(bson.M{"is_active": true}).All(&data)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	pool := map[string]*billing.Currency{}
	if data != nil {
		pool = make(map[string]*billing.Currency, len(data))
		for _, v := range data {
			pool[v.CodeA3] = v
		}
	}

	if err := h.svc.cacher.Set(pkg.CollectionCurrency, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Currency) loadAllFromCache() (map[string]*billing.Currency, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionCurrency)
	if err != nil {
		return nil, err
	}

	var a map[string]*billing.Currency
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
