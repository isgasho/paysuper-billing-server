package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionAnnualTurnovers = "annual_turnovers"

	cacheTurnoverKey = "turnover:company:%s:country:%s:year:%d"
)

// TurnoverRepositoryInterface is abstraction layer for working with annual turnover and representation in database.
type TurnoverRepositoryInterface interface {
	// Upsert add or update an annual turnover to the collection.
	Upsert(context.Context, *billingpb.AnnualTurnover) error

	// GetByZipAndCountry get record by full zip code and country.
	Get(context.Context, string, string, int) (*billingpb.AnnualTurnover, error)

	// CountAll return count elements in annual turnover documents.
	CountAll(context.Context) (int64, error)
}
