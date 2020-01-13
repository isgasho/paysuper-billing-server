package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type turnoverRepository repository

// NewTurnoverRepository create and return an object for working with the annual turnover repository.
// The returned object implements the TurnoverRepositoryInterface interface.
func NewTurnoverRepository(db mongodb.SourceInterface, cache database.CacheInterface) TurnoverRepositoryInterface {
	s := &turnoverRepository{db: db, cache: cache}
	return s
}

func (r *turnoverRepository) Upsert(ctx context.Context, turnover *billing.AnnualTurnover) error {
	filter := bson.M{
		"year":                 turnover.Year,
		"country":              turnover.Country,
		"operating_company_id": turnover.OperatingCompanyId,
	}
	opts := options.Replace().SetUpsert(true)

	_, err := r.db.Collection(collectionAnnualTurnovers).ReplaceOne(ctx, filter, turnover, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAnnualTurnovers),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, turnover),
		)
		return err
	}

	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)

	if err = r.cache.Set(key, turnover, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, turnover),
		)
	}

	return nil
}

func (r *turnoverRepository) Get(ctx context.Context, operatingCompanyId, country string, year int) (*billing.AnnualTurnover, error) {
	data := &billing.AnnualTurnover{}
	key := fmt.Sprintf(cacheTurnoverKey, operatingCompanyId, country, year)

	if err := r.cache.Get(key, data); err == nil {
		return data, nil
	}

	query := bson.M{"operating_company_id": operatingCompanyId, "country": country, "year": year}
	err := r.db.Collection(collectionAnnualTurnovers).FindOne(ctx, query).Decode(&data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAnnualTurnovers),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = r.cache.Set(key, data, 0)
	if err != nil {
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

func (r *turnoverRepository) CountAll(ctx context.Context) (int64, error) {
	count, err := r.db.Collection(collectionAnnualTurnovers).CountDocuments(context.TODO(), bson.M{})

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAnnualTurnovers),
		)
	}

	return count, err
}
