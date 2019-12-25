package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

type countryRepository repository

// NewCountryRepository create and return an object for working with the country repository.
// The returned object implements the CountryRepositoryInterface interface.
func NewCountryRepository(db *mongodb.Source, cache database.CacheInterface) CountryRepositoryInterface {
	s := &countryRepository{db: db, cache: cache}
	return s
}

func (h *countryRepository) Insert(ctx context.Context, country *billing.Country) error {
	_, err := h.db.Collection(CollectionCountry).InsertOne(ctx, country)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, country),
		)
		return err
	}

	if err := h.updateCache(country); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, country),
		)
		return err
	}

	return nil
}

func (h *countryRepository) MultipleInsert(ctx context.Context, country []*billing.Country) error {
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

	_, err := h.db.Collection(CollectionCountry).InsertMany(ctx, c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
		return err
	}

	if err := h.updateCache(nil); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, country),
		)
		return err
	}

	return nil
}

func (h *countryRepository) Update(ctx context.Context, country *billing.Country) error {
	country.VatThreshold.Year = tools.FormatAmount(country.VatThreshold.Year)
	country.VatThreshold.World = tools.FormatAmount(country.VatThreshold.World)

	oid, err := primitive.ObjectIDFromHex(country.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.String(pkg.ErrorDatabaseFieldQuery, country.Id),
		)
		return err
	}

	_, err = h.db.Collection(CollectionCountry).ReplaceOne(ctx, bson.M{"_id": oid}, country)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, country),
		)
		return err
	}

	if err := h.updateCache(country); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, country),
		)
		return err
	}

	return nil
}

func (h *countryRepository) GetByIsoCodeA2(ctx context.Context, code string) (*billing.Country, error) {
	var c billing.Country
	var key = fmt.Sprintf(cacheCountryCodeA2, code)

	if err := h.cache.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{"iso_code_a2": code}
	err := h.db.Collection(CollectionCountry).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = h.cache.Set(key, c, 0); err != nil {
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

func (h *countryRepository) FindByVatEnabled(ctx context.Context) (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountriesWithVatEnabled

	if err := h.cache.Get(key, c); err == nil {
		return c, nil
	}

	query := bson.M{
		"vat_enabled":               true,
		"iso_code_a2":               bson.M{"$ne": "US"},
		"vat_currency_rates_policy": bson.M{"$ne": ""},
		"vat_period_month":          bson.M{"$gt": 0},
	}
	opts := options.Find().SetSort(bson.M{"iso_code_a2": 1})
	cursor, err := h.db.Collection(CollectionCountry).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c.Countries)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = h.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return c, nil
}

func (h *countryRepository) GetAll(ctx context.Context) (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	key := cacheCountryAll

	if err := h.cache.Get(key, c); err == nil {
		return c, nil
	}

	query := bson.M{}
	opts := options.Find().SetSort(bson.M{"iso_code_a2": 1})
	cursor, err := h.db.Collection(CollectionCountry).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c.Countries)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = h.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return c, nil
}

func (h *countryRepository) FindByHighRisk(ctx context.Context, isHighRiskOrder bool) (*billing.CountriesList, error) {
	var c = &billing.CountriesList{}
	var key = fmt.Sprintf(cacheCountryRisk, isHighRiskOrder)

	if err := h.cache.Get(key, c); err == nil {
		return c, nil
	}

	field := "payments_allowed"
	if isHighRiskOrder {
		field = "high_risk_payments_allowed"
	}

	query := bson.M{
		field: true,
	}
	opts := options.Find().SetSort(bson.M{"iso_code_a2": 1})
	cursor, err := h.db.Collection(CollectionCountry).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c.Countries)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = h.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return c, nil
}

func (h *countryRepository) IsTariffRegionSupported(region string) bool {
	return helper.Contains(pkg.SupportedTariffRegions, region)
}

func (h *countryRepository) updateCache(country *billing.Country) error {
	if country != nil {
		if err := h.cache.Set(fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, 0); err != nil {
			return err
		}
	}

	if err := h.cache.Delete(cacheCountryAll); err != nil {
		return err
	}

	if err := h.cache.Delete(cacheCountryRegions); err != nil {
		return err
	}

	if err := h.cache.Delete(cacheCountriesWithVatEnabled); err != nil {
		return err
	}

	if err := h.cache.Delete(fmt.Sprintf(cacheCountryRisk, false)); err != nil {
		return err
	}

	if err := h.cache.Delete(fmt.Sprintf(cacheCountryRisk, true)); err != nil {
		return err
	}

	return nil
}
