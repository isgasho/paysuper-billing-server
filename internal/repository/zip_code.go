package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type zipCodeRepository repository

// NewZipCodeRepository create and return an object for working with the zip codes repository.
// The returned object implements the ZipCodeRepositoryInterface interface.
func NewZipCodeRepository(db mongodb.SourceInterface, cache database.CacheInterface) ZipCodeRepositoryInterface {
	s := &zipCodeRepository{db: db, cache: cache}
	return s
}

func (h *zipCodeRepository) Insert(ctx context.Context, zipCode *billing.ZipCode) error {
	_, err := h.db.Collection(collectionZipCode).InsertOne(ctx, zipCode)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, zipCode),
		)
		return err
	}

	var key = fmt.Sprintf(cacheZipCodeByZipAndCountry, zipCode.Zip, zipCode.Country)

	if err = h.cache.Set(key, zipCode, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, zipCode),
		)
	}

	return nil
}

func (h *zipCodeRepository) GetByZipAndCountry(ctx context.Context, zip, country string) (*billing.ZipCode, error) {
	data := &billing.ZipCode{}
	var key = fmt.Sprintf(cacheZipCodeByZipAndCountry, zip, country)

	if err := h.cache.Get(key, data); err == nil {
		return data, nil
	}

	query := bson.M{"zip": zip, "country": country}
	err := h.db.Collection(collectionZipCode).FindOne(ctx, query).Decode(&data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, cacheZipCodeByZipAndCountry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err = h.cache.Set(key, data, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, data),
		)
	}

	return data, nil
}

func (h *zipCodeRepository) FindByZipAndCountry(ctx context.Context, zip, country string, offset, limit int64) ([]*billing.ZipCode, error) {
	var data []*billing.ZipCode

	query := bson.D{{"zip", primitive.Regex{Pattern: zip}}, {"country", country}}
	opts := options.Find().SetLimit(limit).SetSkip(offset)
	cursor, err := h.db.Collection(collectionZipCode).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return data, nil
}

func (h *zipCodeRepository) CountByZip(ctx context.Context, zip, country string) (int64, error) {
	query := bson.D{{"zip", primitive.Regex{Pattern: zip}}, {"country", country}}
	count, err := h.db.Collection(collectionZipCode).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, err
	}

	return count, nil
}
