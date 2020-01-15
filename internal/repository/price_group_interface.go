package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	cachePriceGroupId     = "price_group:id:%s"
	cachePriceGroupAll    = "price_group:all"
	cachePriceGroupRegion = "price_group:region:%s"

	collectionPriceGroup = "price_group"
)

// PriceGroupRepositoryInterface is abstraction layer for working with price group and representation in database.
type PriceGroupRepositoryInterface interface {
	// Insert adds the price group to the collection.
	Insert(context.Context, *billing.PriceGroup) error

	// Insert adds the multiple price groups to the collection.
	MultipleInsert(context.Context, []*billing.PriceGroup) error

	// Update updates the price group in the collection.
	Update(context.Context, *billing.PriceGroup) error

	// GetById returns the price group by unique identity.
	GetById(context.Context, string) (*billing.PriceGroup, error)

	// GetByRegion returns the price group by region name.
	GetByRegion(context.Context, string) (*billing.PriceGroup, error)

	// GetByRegion returns all price groups.
	GetAll(context.Context) ([]*billing.PriceGroup, error)
}
