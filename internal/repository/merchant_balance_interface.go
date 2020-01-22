package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionMerchantBalances = "merchant_balances"

	cacheKeyMerchantBalances = "balance:merchant_id:%s:currency:%s"
)

// MerchantBalanceRepositoryInterface is abstraction layer for working with merchant balance and representation in database.
type MerchantBalanceRepositoryInterface interface {
	// Insert adds the price group to the collection.
	Insert(context.Context, *billingpb.MerchantBalance) error

	// GetByIdAndCurrency get latest balance for merchant by currency
	GetByIdAndCurrency(context.Context, string, string) (*billingpb.MerchantBalance, error)

	// CountByIdAndCurrency return count balance records for merchant and currency
	CountByIdAndCurrency(context.Context, string, string) (int64, error)
}
