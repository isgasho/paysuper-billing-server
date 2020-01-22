package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	// CollectionOrder is name of table for collection the order.
	CollectionOrder = "order"
)

// OrderRepositoryInterface is abstraction layer for working with order and representation in database.
type OrderRepositoryInterface interface {
	// Insert adds order to the collection.
	Insert(context.Context, *billingpb.Order) error

	// Update updates the order in the collection.
	Update(context.Context, *billingpb.Order) error

	// GetById returns a order by its identifier.
	GetById(context.Context, string) (*billingpb.Order, error)

	// GetByUuid returns a order by its public (uuid) identifier.
	GetByUuid(context.Context, string) (*billingpb.Order, error)

	// GetByRefundReceiptNumber returns a order by its receipt number.
	GetByRefundReceiptNumber(context.Context, string) (*billingpb.Order, error)

	// GetByProjectOrderId returns a order by project and order identifiers.
	GetByProjectOrderId(context.Context, string, string) (*billingpb.Order, error)
}
