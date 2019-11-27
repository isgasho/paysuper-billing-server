package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	cacheCountryCodeA2                    = "country:code_a2:%s"
	cacheCountryAll                       = "country:all"
	cacheCountryRegions                   = "country:regions"
	cacheCountriesWithVatEnabled          = "country:with_vat"
	cacheTariffRegionsCountriesAndRegions = "country:tariff_region:%s"

	collectionCountry = "country"
)

var (
	errorCountryNotFound        = newBillingServerErrorMsg("co000001", "country not found")
	errorCountryRegionNotExists = newBillingServerErrorMsg("co000002", "region not exists")

	errorTariffsNotFound = errors.New("tariffs not found")
)

type countryRegions struct {
	Regions map[string]bool
}

type countryWithVat struct {
	Countries map[string]bool
}

func (s *Service) GetCountriesList(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *billing.CountriesList,
) error {
	countries, err := s.country.GetAll(ctx)
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
	country, err := s.country.GetByIsoCodeA2(ctx, req.IsoCode)
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
	res.VatThreshold = country.VatThreshold
	res.VatPeriodMonth = country.VatPeriodMonth
	res.VatDeadlineDays = country.VatDeadlineDays
	res.VatStoreYears = country.VatStoreYears
	res.VatCurrencyRatesPolicy = country.VatCurrencyRatesPolicy
	res.VatCurrencyRatesSource = country.VatCurrencyRatesSource
	res.CreatedAt = country.CreatedAt
	res.UpdatedAt = country.UpdatedAt
	res.PayerTariffRegion = country.PayerTariffRegion
	res.HighRiskPaymentsAllowed = country.HighRiskPaymentsAllowed
	res.HighRiskChangeAllowed = country.HighRiskChangeAllowed

	return nil
}

func (s *Service) UpdateCountry(
	ctx context.Context,
	req *billing.Country,
	res *billing.Country,
) error {

	country, err := s.country.GetByIsoCodeA2(ctx, req.IsoCodeA2)
	if err != nil {
		return err
	}

	pg, err := s.priceGroup.GetById(ctx, req.PriceGroupId)
	if err != nil {
		return err
	}

	var threshold *billing.CountryVatThreshold

	if req.VatThreshold != nil {
		threshold = req.VatThreshold
	} else {
		threshold = &billing.CountryVatThreshold{
			Year:  0,
			World: 0,
		}
	}

	update := &billing.Country{
		Id:                      country.Id,
		IsoCodeA2:               country.IsoCodeA2,
		Region:                  req.Region,
		Currency:                req.Currency,
		PaymentsAllowed:         req.PaymentsAllowed,
		ChangeAllowed:           req.ChangeAllowed,
		VatEnabled:              req.VatEnabled,
		VatCurrency:             req.VatCurrency,
		PriceGroupId:            pg.Id,
		VatThreshold:            threshold,
		VatPeriodMonth:          req.VatPeriodMonth,
		VatDeadlineDays:         req.VatDeadlineDays,
		VatStoreYears:           req.VatStoreYears,
		VatCurrencyRatesPolicy:  req.VatCurrencyRatesPolicy,
		VatCurrencyRatesSource:  req.VatCurrencyRatesSource,
		CreatedAt:               country.CreatedAt,
		UpdatedAt:               ptypes.TimestampNow(),
		HighRiskPaymentsAllowed: req.HighRiskPaymentsAllowed,
		HighRiskChangeAllowed:   req.HighRiskChangeAllowed,
	}

	err = s.country.Update(ctx, update)
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
	res.VatThreshold = update.VatThreshold
	res.VatPeriodMonth = update.VatPeriodMonth
	res.VatDeadlineDays = update.VatDeadlineDays
	res.VatStoreYears = update.VatStoreYears
	res.VatCurrencyRatesPolicy = update.VatCurrencyRatesPolicy
	res.VatCurrencyRatesSource = update.VatCurrencyRatesSource
	res.CreatedAt = update.CreatedAt
	res.UpdatedAt = update.UpdatedAt
	res.HighRiskPaymentsAllowed = update.HighRiskPaymentsAllowed
	res.HighRiskChangeAllowed = update.HighRiskChangeAllowed

	return nil
}

type CountryServiceInterface interface {
	Insert(context.Context, *billing.Country) error
	MultipleInsert(context.Context, []*billing.Country) error
	Update(context.Context, *billing.Country) error
	GetByIsoCodeA2(context.Context, string) (*billing.Country, error)
	GetAll(context.Context) (*billing.CountriesList, error)
	IsRegionExists(context.Context, string) (bool, error)
	IsTariffRegionExists(string) bool
	GetCountriesWithVatEnabled(context.Context) (*billing.CountriesList, error)
	GetCountriesAndRegionsByTariffRegion(ctx context.Context, tariffRegion string) ([]*internalPkg.CountryAndRegionItem, error)
}

func newCountryService(svc *Service) *Country {
	s := &Country{svc: svc}
	return s
}

func (h *Country) Insert(ctx context.Context, country *billing.Country) error {
	_, err := h.svc.db.Collection(collectionCountry).InsertOne(ctx, country)

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
	if err := h.svc.cacher.Delete(cacheCountriesWithVatEnabled); err != nil {
		return err
	}

	return nil
}

func (h *Country) MultipleInsert(ctx context.Context, country []*billing.Country) error {
	c := make([]interface{}, len(country))
	for i, v := range country {
		if v.VatThreshold == nil {
			v.VatThreshold = &billing.CountryVatThreshold{
				Year:  0,
				World: 0,
			}
		}
		v.VatThreshold.Year = tools.FormatAmount(v.VatThreshold.Year)
		v.VatThreshold.World = tools.FormatAmount(v.VatThreshold.World)
		c[i] = v
	}

	_, err := h.svc.db.Collection(collectionCountry).InsertMany(ctx, c)
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
	if err := h.svc.cacher.Delete(cacheCountriesWithVatEnabled); err != nil {
		return err
	}

	return nil
}

func (h *Country) Update(ctx context.Context, country *billing.Country) error {
	country.VatThreshold.Year = tools.FormatAmount(country.VatThreshold.Year)
	country.VatThreshold.World = tools.FormatAmount(country.VatThreshold.World)

	oid, err := primitive.ObjectIDFromHex(country.Id)

	if err != nil {
		return err
	}

	_, err = h.svc.db.Collection(collectionCountry).ReplaceOne(ctx, bson.M{"_id": oid}, country)
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
	if err := h.svc.cacher.Delete(cacheCountriesWithVatEnabled); err != nil {
		return err
	}

	return nil
}

func (h *Country) GetByIsoCodeA2(ctx context.Context, code string) (*billing.Country, error) {
	var c billing.Country
	key := fmt.Sprintf(cacheCountryCodeA2, code)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{"iso_code_a2": code}
	err := h.svc.db.Collection(collectionCountry).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, fmt.Errorf(errorNotFound, collectionCountry)
	}

	err = h.svc.cacher.Set(key, c, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return &c, nil
}

func (h *Country) GetCountriesWithVatEnabled(ctx context.Context) (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountriesWithVatEnabled

	err := h.svc.cacher.Get(key, c)
	if err == nil {
		return c, nil
	}

	query := bson.M{
		"vat_enabled":               true,
		"iso_code_a2":               bson.M{"$ne": "US"},
		"vat_currency_rates_policy": bson.M{"$ne": ""},
		"vat_period_month":          bson.M{"$gt": 0},
	}
	opts := options.Find().SetSort(bson.M{"iso_code_a2": 1})
	cursor, err := h.svc.db.Collection(collectionCountry).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionCountry)
	}

	err = cursor.All(ctx, &c.Countries)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionCountry)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}
	return c, nil
}

func (h *Country) GetAll(ctx context.Context) (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountryAll

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	query := bson.M{}
	opts := options.Find().SetSort(bson.M{"iso_code_a2": 1})
	cursor, err := h.svc.db.Collection(collectionCountry).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c.Countries)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return c, nil
}

func (h *Country) IsRegionExists(ctx context.Context, region string) (bool, error) {
	var c = &countryRegions{}
	key := cacheCountryRegions
	if err := h.svc.cacher.Get(key, c); err != nil {
		countries, err := h.GetAll(ctx)
		if err != nil {
			return false, err
		}
		c.Regions = make(map[string]bool, len(countries.Countries))
		for _, country := range countries.Countries {
			c.Regions[country.Region] = true
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	_, ok := c.Regions[region]
	return ok, nil
}

func (h *Country) IsTariffRegionExists(region string) bool {
	return contains(pkg.SupportedTariffRegions, region)
}

func (h *Country) GetCountriesAndRegionsByTariffRegion(
	ctx context.Context,
	tariffRegion string,
) ([]*internalPkg.CountryAndRegionItem, error) {
	items := new(internalPkg.CountryAndRegionItems)
	key := fmt.Sprintf(cacheTariffRegionsCountriesAndRegions, tariffRegion)
	err := h.svc.cacher.Get(key, items)

	if err == nil {
		return items.Items, nil
	}

	query := bson.M{"payer_tariff_region": tariffRegion}
	cursor, err := h.svc.db.Collection(collectionCountry).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &items.Items)

	if err != nil || len(items.Items) <= 0 {
		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionCountry),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
		}
		return nil, errorTariffsNotFound
	}

	err = h.svc.cacher.Set(key, items, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, items),
		)
	}

	return items.Items, nil
}

func (h *Country) IsCountryVatEnabled(ctx context.Context, isoCodeA2 string) (bool, error) {
	var c = &countryWithVat{}
	key := cacheCountriesWithVatEnabled
	if err := h.svc.cacher.Get(key, c); err != nil {
		countries, err := h.GetCountriesWithVatEnabled(ctx)
		if err != nil {
			return false, err
		}
		c.Countries = make(map[string]bool, len(countries.Countries))
		for _, country := range countries.Countries {
			c.Countries[country.IsoCodeA2] = true
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	_, ok := c.Countries[isoCodeA2]
	return ok, nil
}
