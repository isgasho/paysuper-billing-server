package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

type orderRepository repository

// NewOrderRepository create and return an object for working with the order repository.
// The returned object implements the OrderRepositoryInterface interface.
func NewOrderRepository(db *mongodb.Source) OrderRepositoryInterface {
	s := &orderRepository{db: db}
	return s
}

func (h *orderRepository) Insert(ctx context.Context, order *billing.Order) error {
	_, err := h.db.Collection(CollectionOrder).InsertOne(ctx, order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return err
	}

	return nil
}

func (h *orderRepository) Update(ctx context.Context, order *billing.Order) error {
	oid, err := primitive.ObjectIDFromHex(order.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.String(pkg.ErrorDatabaseFieldQuery, order.Id),
		)
		return err
	}

	_, err = h.db.Collection(CollectionOrder).ReplaceOne(ctx, bson.M{"_id": oid}, order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return err
	}

	return nil
}

func (h *orderRepository) GetByUuid(ctx context.Context, uuid string) (*billing.Order, error) {
	order := &billing.Order{}
	query := bson.M{"uuid": uuid}
	err := h.db.Collection(CollectionOrder).FindOne(ctx, query).Decode(order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return order, nil
}

func (h *orderRepository) GetById(ctx context.Context, id string) (*billing.Order, error) {
	order := &billing.Order{}
	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(CollectionOrder).FindOne(ctx, query).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return order, nil
}

func (h *orderRepository) GetByRefundReceiptNumber(ctx context.Context, id string) (*billing.Order, error) {
	order := &billing.Order{}
	query := bson.M{"refund.receipt_number": id}
	err := h.db.Collection(CollectionOrder).FindOne(ctx, query).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return order, nil
}

func (h *orderRepository) GetByProjectOrderId(ctx context.Context, projectId, projectOrderId string) (*billing.Order, error) {
	order := &billing.Order{}
	id, err := primitive.ObjectIDFromHex(projectId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.String(pkg.ErrorDatabaseFieldQuery, order.Id),
		)
		return nil, err
	}

	query := bson.M{"project._id": id, "project_order_id": projectOrderId}
	err = h.db.Collection(CollectionOrder).FindOne(ctx, query).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return order, nil
}
