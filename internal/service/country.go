package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	cacheCountryCodeA2 = "country:code_a2:%s"

	collectionCountry = "country"
)

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	return s
}

func (h *Country) Insert(country *billing.Country) error {
	if err := h.svc.db.Collection(collectionCountry).Insert(country); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cacheCountryCodeA2, country.CodeA2), country, 0); err != nil {
		return err
	}

	return nil
}

func (h Country) MultipleInsert(country []*billing.Country) error {
	c := make([]interface{}, len(country))
	for i, v := range country {
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionCountry).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h Country) GetByCodeA2(code string) (*billing.Country, error) {
	var c billing.Country
	key := fmt.Sprintf(cacheCountryCodeA2, code)

	if err := h.svc.cacher.Get(key, c); err != nil {
		if err = h.svc.db.Collection(collectionCountry).Find(bson.M{"is_active": true, "code_a2": code}).One(&c); err != nil {
			return nil, fmt.Errorf(errorNotFound, collectionCountry)
		}
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}
