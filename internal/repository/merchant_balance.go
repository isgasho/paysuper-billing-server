package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type merchantBalanceRepository repository

// NewMerchantBalanceRepository create and return an object for working with the merchant balance repository.
// The returned object implements the MerchantBalanceRepositoryInterface interface.
func NewMerchantBalanceRepository(db mongodb.SourceInterface, cache database.CacheInterface) MerchantBalanceRepositoryInterface {
	s := &merchantBalanceRepository{db: db, cache: cache}
	return s
}

func (r merchantBalanceRepository) Insert(ctx context.Context, mb *billingpb.MerchantBalance) (err error) {
	_, err = r.db.Collection(collectionMerchantBalances).InsertOne(ctx, mb)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantBalances),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, mb),
		)
		return err
	}

	key := fmt.Sprintf(cacheKeyMerchantBalances, mb.MerchantId, mb.Currency)
	err = r.cache.Set(key, mb, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, mb),
		)
	}

	return
}

func (r merchantBalanceRepository) GetByIdAndCurrency(ctx context.Context, merchantId, currency string) (*billingpb.MerchantBalance, error) {
	var (
		mb *billingpb.MerchantBalance
		c  billingpb.MerchantBalance
	)
	key := fmt.Sprintf(cacheKeyMerchantBalances, merchantId, currency)

	if err := r.cache.Get(key, c); err == nil {
		return &c, nil
	}

	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return nil, err
	}

	query := bson.M{
		"merchant_id": oid,
		"currency":    currency,
	}

	sorts := bson.M{"_id": -1}
	opts := options.FindOne().SetSort(sorts)
	err = r.db.Collection(collectionMerchantBalances).FindOne(ctx, query, opts).Decode(&mb)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantBalances),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
		return nil, err
	}

	if err = r.cache.Set(key, mb, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, mb),
		)
	}

	return mb, nil
}

func (r merchantBalanceRepository) CountByIdAndCurrency(ctx context.Context, merchantId, currency string) (int64, error) {
	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return int64(0), err
	}

	query := bson.M{
		"merchant_id": oid,
		"currency":    currency,
	}
	count, err := r.db.Collection(collectionMerchantBalances).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantBalances),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return int64(0), err
	}

	return count, nil
}
