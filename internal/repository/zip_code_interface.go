package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	collectionZipCode = "zip_code"

	cacheZipCodeByZipAndCountry = "zip_code:zip_country:%s_%s"
)

// ZipCodeRepositoryInterface is abstraction layer for working with zip code and representation in database.
type ZipCodeRepositoryInterface interface {
	// Insert adds zip code to the collection.
	Insert(context.Context, *billing.ZipCode) error

	// GetByZipAndCountry get record by full zip code and country.
	GetByZipAndCountry(context.Context, string, string) (*billing.ZipCode, error)

	// FindByZipAndCountry find multiple records by part of zip code and full country code.
	FindByZipAndCountry(context.Context, string, string, int64, int64) ([]*billing.ZipCode, error)

	// CountByZip find records by part of zip code and full country code and return found count.
	CountByZip(context.Context, string, string) (int64, error)
}
