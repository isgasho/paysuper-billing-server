package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

const (
	cacheCountryCodeA2 = "country:code_a2:%s"
	cacheCountryAll    = "country:all"

	collectionCountry = "country"
)

func (s *Service) GetCountriesList(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *billing.CountriesList,
) error {
	countries, err := s.country.GetAll()
	if err != nil {
		return err
	}

	res.Countries = countries.Countries

	return nil
}

func (s *Service) GetCountry(
	ctx context.Context,
	req *billing.GetCountryRequest,
	res *billing.Country,
) error {
	country, err := s.country.GetByIsoCodeA2(req.IsoCode)
	if err != nil {
		return err
	}
	res.IsoCodeA2 = country.IsoCodeA2
	res.Region = country.Region
	res.Currency = country.Currency
	res.PaymentsAllowed = country.PaymentsAllowed
	res.ChangeAllowed = country.ChangeAllowed
	res.VatEnabled = country.VatEnabled
	res.VatCurrency = country.VatCurrency
	res.PriceGroupId = country.PriceGroupId
	res.CreatedAt = country.CreatedAt
	res.UpdatedAt = country.UpdatedAt

	return nil
}

func (s *Service) UpdateCountry(
	ctx context.Context,
	req *billing.Country,
	res *billing.Country,
) error {

	country, err := s.country.GetByIsoCodeA2(req.IsoCodeA2)
	if err != nil {
		return err
	}

	pg, err := s.priceGroup.GetById(req.PriceGroupId)
	if err != nil {
		return err
	}

	update := &billing.Country{
		Id:              country.Id,
		IsoCodeA2:       country.IsoCodeA2,
		Region:          req.Region,
		Currency:        req.Currency,
		PaymentsAllowed: req.PaymentsAllowed,
		ChangeAllowed:   req.ChangeAllowed,
		VatEnabled:      req.VatEnabled,
		VatCurrency:     req.VatCurrency,
		PriceGroupId:    pg.Id,
		CreatedAt:       country.CreatedAt,
		UpdatedAt:       ptypes.TimestampNow(),
	}

	err = s.country.Update(update)
	if err != nil {
		s.logError("update country failed", []interface{}{"err", err.Error(), "data", update})
		return err
	}

	res.IsoCodeA2 = update.IsoCodeA2
	res.Region = update.Region
	res.Currency = update.Currency
	res.PaymentsAllowed = update.PaymentsAllowed
	res.ChangeAllowed = update.ChangeAllowed
	res.VatEnabled = update.VatEnabled
	res.VatCurrency = update.VatCurrency
	res.PriceGroupId = update.PriceGroupId
	res.CreatedAt = update.CreatedAt
	res.UpdatedAt = update.UpdatedAt

	return nil
}

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	return s
}

func (h *Country) Insert(country *billing.Country) error {
	if err := h.svc.db.Collection(collectionCountry).Insert(country); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, 0); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheCountryAll); err != nil {
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

	if err := h.svc.cacher.Delete(cacheCountryAll); err != nil {
		return err
	}

	return nil
}

func (h Country) Update(country *billing.Country) error {
	if err := h.svc.db.Collection(collectionCountry).UpdateId(bson.ObjectIdHex(country.Id), country); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, 0); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheCountryAll); err != nil {
		return err
	}

	return nil
}

func (h Country) GetByIsoCodeA2(code string) (*billing.Country, error) {
	var c billing.Country
	key := fmt.Sprintf(cacheCountryCodeA2, code)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionCountry).
		Find(bson.M{"iso_code_a2": code}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionCountry)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h Country) GetAll() (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountryAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		err = h.svc.db.Collection(collectionCountry).Find(nil).Sort("iso_code_a2").All(&c.Countries)
		if err != nil {
			return nil, err
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}
