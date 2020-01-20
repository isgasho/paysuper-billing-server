package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type priceGroupRepository repository

// NewPriceGroupRepository create and return an object for working with the price group repository.
// The returned object implements the PriceGroupRepositoryInterface interface.
func NewPriceGroupRepository(db mongodb.SourceInterface, cache database.CacheInterface) PriceGroupRepositoryInterface {
	s := &priceGroupRepository{db: db, cache: cache}
	return s
}

func (r *priceGroupRepository) Insert(ctx context.Context, pg *billing.PriceGroup) error {
	_, err := r.db.Collection(collectionPriceGroup).InsertOne(ctx, pg)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, pg),
		)
		return err
	}

	if err := r.updateCache(pg); err != nil {
		return err
	}

	return nil
}

func (r priceGroupRepository) MultipleInsert(ctx context.Context, pg []*billing.PriceGroup) error {
	c := make([]interface{}, len(pg))
	for i, v := range pg {
		c[i] = v
	}

	_, err := r.db.Collection(collectionPriceGroup).InsertMany(ctx, c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
		return err
	}

	return nil
}

func (r priceGroupRepository) Update(ctx context.Context, pg *billing.PriceGroup) error {
	oid, err := primitive.ObjectIDFromHex(pg.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldQuery, pg.Id),
		)
		return err
	}

	filter := bson.M{"_id": oid}
	_, err = r.db.Collection(collectionPriceGroup).ReplaceOne(ctx, filter, pg)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, pg),
		)
		return err
	}

	if err := r.updateCache(pg); err != nil {
		return err
	}

	return nil
}

func (r priceGroupRepository) GetById(ctx context.Context, id string) (*billing.PriceGroup, error) {
	var c billing.PriceGroup
	key := fmt.Sprintf(cachePriceGroupId, id)
	err := r.cache.Get(key, c)

	if err == nil {
		return &c, nil
	}

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid, "is_active": true}
	err = r.db.Collection(collectionPriceGroup).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = r.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return &c, nil
}

func (r priceGroupRepository) GetByRegion(ctx context.Context, region string) (*billing.PriceGroup, error) {
	var c billing.PriceGroup
	key := fmt.Sprintf(cachePriceGroupRegion, region)

	if err := r.cache.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{"region": region, "is_active": true}
	err := r.db.Collection(collectionPriceGroup).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err := r.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return &c, nil
}

func (r priceGroupRepository) GetAll(ctx context.Context) ([]*billing.PriceGroup, error) {
	c := []*billing.PriceGroup{}

	if err := r.cache.Get(cachePriceGroupAll, c); err == nil {
		return c, nil
	}

	query := bson.M{"is_active": true}
	cursor, err := r.db.Collection(collectionPriceGroup).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPriceGroup),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = r.cache.Set(cachePriceGroupAll, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, cachePriceGroupAll),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return c, nil
}

func (r priceGroupRepository) updateCache(pg *billing.PriceGroup) error {
	if pg != nil {
		if err := r.cache.Set(fmt.Sprintf(cachePriceGroupId, pg.Id), pg, 0); err != nil {
			zap.L().Error(
				pkg.ErrorCacheQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorCacheFieldCmd, "SET"),
				zap.String(pkg.ErrorCacheFieldKey, fmt.Sprintf(cachePriceGroupId, pg.Id)),
				zap.Any(pkg.ErrorDatabaseFieldQuery, pg),
			)
			return err
		}

		if err := r.cache.Set(fmt.Sprintf(cachePriceGroupRegion, pg.Region), pg, 0); err != nil {
			zap.L().Error(
				pkg.ErrorCacheQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorCacheFieldCmd, "SET"),
				zap.String(pkg.ErrorCacheFieldKey, fmt.Sprintf(cachePriceGroupRegion, pg.Region)),
				zap.Any(pkg.ErrorDatabaseFieldQuery, pg),
			)
			return err
		}
	}

	if err := r.cache.Delete(cachePriceGroupAll); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, cachePriceGroupAll),
		)
		return err
	}

	return nil
}
