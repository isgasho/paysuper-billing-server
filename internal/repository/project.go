package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type projectRepository repository

// NewProjectRepository create and return an object for working with the price group repository.
// The returned object implements the ProjectRepositoryInterface interface.
func NewProjectRepository(db mongodb.SourceInterface, cache database.CacheInterface) ProjectRepositoryInterface {
	s := &projectRepository{db: db, cache: cache}
	return s
}

func (r *projectRepository) Insert(ctx context.Context, project *billingpb.Project) error {
	_, err := r.db.Collection(collectionProject).InsertOne(ctx, project)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	key := fmt.Sprintf(cacheProjectId, project.Id)
	err = r.cache.Set(key, project, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	return nil
}

func (r *projectRepository) MultipleInsert(ctx context.Context, projects []*billingpb.Project) error {
	p := make([]interface{}, len(projects))
	for i, v := range projects {
		p[i] = v
	}

	_, err := r.db.Collection(collectionProject).InsertMany(ctx, p)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, p),
		)
		return err
	}

	return nil
}

func (r *projectRepository) Update(ctx context.Context, project *billingpb.Project) error {
	oid, _ := primitive.ObjectIDFromHex(project.Id)
	filter := bson.M{"_id": oid}
	_, err := r.db.Collection(collectionProject).ReplaceOne(ctx, filter, project)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	key := fmt.Sprintf(cacheProjectId, project.Id)
	err = r.cache.Set(key, project, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	return nil
}

func (r *projectRepository) GetById(ctx context.Context, id string) (*billingpb.Project, error) {
	var c billingpb.Project
	key := fmt.Sprintf(cacheProjectId, id)
	err := r.cache.Get(key, c)

	if err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid}
	err = r.db.Collection(collectionProject).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = r.cache.Set(key, c, 0)

	if err != nil {
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

func (r *projectRepository) CountByMerchantId(ctx context.Context, id string) (int64, error) {
	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"merchant_id": oid}
	count, err := r.db.Collection(collectionProject).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return int64(0), err
	}

	return count, nil
}
