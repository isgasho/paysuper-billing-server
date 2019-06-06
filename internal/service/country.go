package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	s.loadAllToCache()

	return s
}

func (h Country) GetCountryByCodeA2(code string) (*billing.Country, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	c, ok := pool[code]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCountry)
	}

	return c, nil
}

func (h Country) GetAll() map[string]*billing.Country {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h *Country) loadAllToCache() error {
	var data []*billing.Country

	err := h.svc.db.Collection(pkg.CollectionCountry).Find(bson.M{"is_active": true}).All(&data)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}

	pool := map[string]*billing.Country{}
	if data != nil {
		pool = make(map[string]*billing.Country, len(data))
		for _, v := range data {
			pool[v.CodeA2] = v
		}
	}

	if err := h.svc.cacher.Set(pkg.CollectionCountry, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Country) loadAllFromCache() (map[string]*billing.Country, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionCountry)
	if err != nil {
		return nil, err
	}

	var a map[string]*billing.Country
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
