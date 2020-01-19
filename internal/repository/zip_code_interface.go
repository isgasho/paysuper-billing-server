package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionZipCode = "zip_code"

	cacheZipCodeByZipAndCountry = "zip_code:zip_country:%s_%s"
)

// ZipCodeRepositoryInterface is abstraction layer for working with zip code and representation in database.
type ZipCodeRepositoryInterface interface {
	// Insert adds zip code to the collection.
	Insert(context.Context, *billingpb.ZipCode) error

	// GetByZipAndCountry get record by full zip code and country.
	GetByZipAndCountry(context.Context, string, string) (*billingpb.ZipCode, error)

	// FindByZipAndCountry find multiple records by part of zip code and full country code.
	FindByZipAndCountry(context.Context, string, string, int64, int64) ([]*billingpb.ZipCode, error)

	// CountByZip find records by part of zip code and full country code and return found count.
	CountByZip(context.Context, string, string) (int64, error)
}
