package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	// CollectionCountry is name of table for collection the country.
	CollectionCountry = "country"

	cacheCountryCodeA2           = "country:code_a2:%s"
	cacheCountryRisk             = "country:risk:%t"
	cacheCountryAll              = "country:all"
	cacheCountryRegions          = "country:regions"
	cacheCountriesWithVatEnabled = "country:with_vat"
)

// CountryRepositoryInterface is abstraction layer for working with country and representation in database.
type CountryRepositoryInterface interface {
	// Insert adds country to the collection.
	Insert(context.Context, *billingpb.Country) error

	// Insert adds multiple countries to the collection.
	MultipleInsert(context.Context, []*billingpb.Country) error

	// Update updates the country in the collection.
	Update(context.Context, *billingpb.Country) error

	// GetByIsoCodeA2 returns the country using a two-letter code according to the ISO standard.
	GetByIsoCodeA2(context.Context, string) (*billingpb.Country, error)

	// GetAll returns all countries.
	GetAll(context.Context) (*billingpb.CountriesList, error)

	// FindByHighRisk returns countries by high risk criteria.
	FindByHighRisk(ctx context.Context, isHighRiskOrder bool) (*billingpb.CountriesList, error)

	// IsTariffRegionSupported checks if the region is supported by country settings.
	IsTariffRegionSupported(string) bool

	// FindByVatEnabled returns countries with enabled vat (except the US).
	FindByVatEnabled(context.Context) (*billingpb.CountriesList, error)
}
