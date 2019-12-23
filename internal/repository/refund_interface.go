package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	// CollectionRefund is name of table for collection the refund.
	CollectionRefund = "refund"
)

// RefundRepositoryInterface is abstraction layer for working with refund and representation in database.
type RefundRepositoryInterface interface {
	// Insert adds refund to the collection.
	Insert(context.Context, *billing.Refund) error

	// Update updates the refund in the collection.
	Update(context.Context, *billing.Refund) error

	// GetById returns a refund by its identifier.
	GetById(context.Context, string) (*billing.Refund, error)

	// FindByOrderUuid returns a list of refunds by the public identifier of the purchase order.
	FindByOrderUuid(context.Context, string, int64, int64) ([]*billing.Refund, error)

	// CountByOrderUuid returns the number of refunds by the public identifier of the purchase order.
	CountByOrderUuid(context.Context, string) (int64, error)

	// GetAmountByOrderId returns the amount of refunds produced by order ID.
	GetAmountByOrderId(context.Context, string) (float64, error)
}
