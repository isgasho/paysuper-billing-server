package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	return s
}

func (h Country) GetCountryByCodeA2(code string) (*billing.Country, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	res, err := h.svc.cacher.GetSet(
		fmt.Sprintf(pkg.CacheCountryCodeA2, code),
		func() (interface{}, error) {
			var c *billing.Country
			err := h.svc.db.Collection(pkg.CollectionCountry).Find(bson.M{"is_active": true, "code_a2": code}).One(&c)
			if err != nil {
				return nil, fmt.Errorf(errorNotFound, pkg.CollectionCountry)
			}

			return c, nil
		},
		0,
	)
	if err != nil || res == nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCountry)
	}

	c := &billing.Country{}
	err = json.Unmarshal(*res, c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCountry)
	}

	return c, nil
}
