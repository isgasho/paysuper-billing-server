package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

const (
	// CollectionRefund is name of table for collection the refund.
	CollectionRefund = "refund"
)

// RefundRepository is a repository for working with the Refund entity.
type RefundRepository Repository

// RefundRepositoryInterface is interface of RefundRepository.
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

func Refund(db *mongodb.Source) RefundRepositoryInterface {
	s := &RefundRepository{db: db}
	return s
}

func (h *RefundRepository) Insert(ctx context.Context, refund *billing.Refund) error {
	_, err := h.db.Collection(CollectionRefund).InsertOne(ctx, refund)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, refund),
		)
		return err
	}

	return nil
}

func (h *RefundRepository) Update(ctx context.Context, refund *billing.Refund) error {
	oid, _ := primitive.ObjectIDFromHex(refund.Id)
	filter := bson.M{"_id": oid}
	_, err := h.db.Collection(CollectionRefund).ReplaceOne(ctx, filter, refund)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, refund),
		)
		return err
	}

	return nil
}

func (h *RefundRepository) GetById(ctx context.Context, id string) (*billing.Refund, error) {
	var refund *billing.Refund

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err := h.db.Collection(CollectionRefund).FindOne(ctx, filter).Decode(&refund)

	if err != nil {
		zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", id)
		return nil, err
	}

	return refund, nil
}

func (h *RefundRepository) FindByOrderUuid(ctx context.Context, id string, limit int64, offset int64) ([]*billing.Refund, error) {
	var refunds []*billing.Refund

	query := bson.M{"original_order.uuid": id}
	opts := options.Find().
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := h.db.Collection(CollectionRefund).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &refunds)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return refunds, nil
}

func (h *RefundRepository) CountByOrderUuid(ctx context.Context, id string) (int64, error) {
	query := bson.M{"original_order.uuid": id}
	count, err := h.db.Collection(CollectionRefund).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", id)
		return 0, err
	}

	return count, nil
}

func (h *RefundRepository) GetAmountByOrderId(ctx context.Context, orderId string) (float64, error) {
	var res struct {
		Amount float64 `bson:"amount"`
	}

	oid, _ := primitive.ObjectIDFromHex(orderId)
	query := []bson.M{
		{
			"$match": bson.M{
				"status":            bson.M{"$nin": []int32{pkg.RefundStatusRejected}},
				"original_order.id": oid,
			},
		},
		{"$group": bson.M{"_id": "$original_order.id", "amount": bson.M{"$sum": "$amount"}}},
	}

	cursor, err := h.db.Collection(CollectionRefund).Aggregate(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to calculate refunded amount by order failed", "err", err.Error(), "query", query)
		return 0, err
	}

	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		err = cursor.Decode(&res)

		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
			return 0, err
		}
	}

	return res.Amount, nil
}
