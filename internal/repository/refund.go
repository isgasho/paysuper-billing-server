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

type refundRepository repository

// NewRefundRepository create and return an object for working with the refund repository.
// The returned object implements the RefundRepositoryInterface interface.
func NewRefundRepository(db *mongodb.Source) RefundRepositoryInterface {
	s := &refundRepository{db: db}
	return s
}

func (h *refundRepository) Insert(ctx context.Context, refund *billing.Refund) error {
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

func (h *refundRepository) Update(ctx context.Context, refund *billing.Refund) error {
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

func (h *refundRepository) GetById(ctx context.Context, id string) (*billing.Refund, error) {
	var refund *billing.Refund
	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionRefund),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	err = h.db.Collection(CollectionRefund).FindOne(ctx, bson.M{"_id": oid}).Decode(&refund)

	if err != nil {
		zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", id)
		return nil, err
	}

	return refund, nil
}

func (h *refundRepository) FindByOrderUuid(ctx context.Context, id string, limit int64, offset int64) ([]*billing.Refund, error) {
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

func (h *refundRepository) CountByOrderUuid(ctx context.Context, id string) (int64, error) {
	query := bson.M{"original_order.uuid": id}
	count, err := h.db.Collection(CollectionRefund).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to find refund by id failed", "err", err.Error(), "id", id)
		return 0, err
	}

	return count, nil
}

func (h *refundRepository) GetAmountByOrderId(ctx context.Context, orderId string) (float64, error) {
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
