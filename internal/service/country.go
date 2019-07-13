package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	cacheCountryCodeA2  = "country:code_a2:%s"
	cacheCountryAll     = "country:all"
	cacheCountryRegions = "country:regions"

	collectionCountry = "country"
)

var (
	errorCountryNotFound        = newBillingServerErrorMsg("co000001", "country not found")
	errorCountryRegionNotExists = newBillingServerErrorMsg("co000002", "region not exists")
)

type countryRegions struct {
	Regions map[string]bool
}

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
		zap.S().Errorf("update country failed", "err", err.Error(), "data", update)
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

type CountryServiceInterface interface {
	Insert(*billing.Country) error
	MultipleInsert([]*billing.Country) error
	Update(*billing.Country) error
	GetByIsoCodeA2(string) (*billing.Country, error)
	GetAll() (*billing.CountriesList, error)
	IsRegionExists(string) (bool, error)
}

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	return s
}

func (h *Country) Insert(country *billing.Country) error {
	err := h.svc.db.Collection(collectionCountry).Insert(country)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, 0)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Delete(cacheCountryAll)
	if err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cacheCountryRegions); err != nil {
		return err
	}

	return nil
}

func (h Country) MultipleInsert(country []*billing.Country) error {
	c := make([]interface{}, len(country))
	for i, v := range country {
		c[i] = v
	}

	err := h.svc.db.Collection(collectionCountry).Insert(c...)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Delete(cacheCountryAll)
	if err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cacheCountryRegions); err != nil {
		return err
	}

	return nil
}

func (h Country) Update(country *billing.Country) error {
	err := h.svc.db.Collection(collectionCountry).UpdateId(bson.ObjectIdHex(country.Id), country)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, 0)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Delete(cacheCountryAll)
	if err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cacheCountryRegions); err != nil {
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

	err := h.svc.db.Collection(collectionCountry).
		Find(bson.M{"iso_code_a2": code}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionCountry)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h Country) GetAll() (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountryAll

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	err := h.svc.db.Collection(collectionCountry).Find(nil).Sort("iso_code_a2").All(&c.Countries)
	if err != nil {
		return nil, err
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return c, nil
}

func (h Country) IsRegionExists(region string) (bool, error) {
	var c = &countryRegions{}
	key := cacheCountryRegions
	if err := h.svc.cacher.Get(key, c); err != nil {
		countries, err := h.GetAll()
		if err != nil {
			return false, err
		}
		c.Regions = make(map[string]bool)
		for _, country := range countries.Countries {
			c.Regions[country.Region] = true
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	_, ok := c.Regions[region]
	return ok, nil
}
