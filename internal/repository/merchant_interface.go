package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	CollectionMerchant = "merchant"

	cacheMerchantId       = "merchant:id:%s"
	cacheMerchantCommonId = "merchant:common:id:%s"
)

// MerchantRepositoryInterface is abstraction layer for working with merchant and representation in database.
type MerchantRepositoryInterface interface {
	// Insert add the merchant to the collection.
	Insert(ctx context.Context, merchant *billingpb.Merchant) error

	// MultipleInsert adds the multiple merchants to the collection.
	MultipleInsert(ctx context.Context, merchants []*billingpb.Merchant) error

	// Update updates the merchant in the collection.
	Update(ctx context.Context, merchant *billingpb.Merchant) error

	// Upsert add or update the merchant to the collection.
	Upsert(ctx context.Context, merchant *billingpb.Merchant) error

	// UpdateTariffs update payment tariffs.
	UpdateTariffs(context.Context, string, *billingpb.PaymentChannelCostMerchant) error

	// GetById returns the merchant by unique identity.
	GetById(ctx context.Context, id string) (*billingpb.Merchant, error)

	// GetByUserId returns the merchant by user identifier.
	GetByUserId(ctx context.Context, id string) (*billingpb.Merchant, error)

	// GetCommonById returns the common merchant information by unique identity.
	GetCommonById(ctx context.Context, id string) (*billingpb.MerchantCommon, error)

	// GetMerchantsWithAutoPayouts return list of merchants with the automatic payments option is turned off.
	GetMerchantsWithAutoPayouts(ctx context.Context) ([]*billingpb.Merchant, error)

	// GetAll returns all merchants.
	GetAll(ctx context.Context) ([]*billingpb.Merchant, error)

	// Find returns list of merchants by criteria.
	Find(context.Context, bson.M, []string, int64, int64) ([]*billingpb.Merchant, error)

	// FindCount returns count of merchants by criteria.
	FindCount(context.Context, bson.M) (int64, error)
}
