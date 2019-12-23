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

const (
	// CollectionOrder is name of table for collection the order.
	CollectionOrder = "order"
)

// OrderRepository is a repository for working with the Order entity.
type OrderRepository Repository

// OrderRepositoryInterface is interface of OrderRepository.
type OrderRepositoryInterface interface {
	// Insert adds order to the collection.
	Insert(context.Context, *billing.Order) error

	// Update updates the order in the collection.
	Update(context.Context, *billing.Order) error

	// GetById returns a order by its identifier.
	GetById(context.Context, string) (*billing.Order, error)

	// GetByUuid returns a order by its public (uuid) identifier.
	GetByUuid(context.Context, string) (*billing.Order, error)

	// GetByRefundReceiptNumber returns a order by its receipt number.
	GetByRefundReceiptNumber(context.Context, string) (*billing.Order, error)

	// GetByProjectOrderId returns a order by project and order identifiers.
	GetByProjectOrderId(context.Context, string, string) (*billing.Order, error)
}

func Order(db *mongodb.Source) OrderRepositoryInterface {
	s := &OrderRepository{db: db}
	return s
}

func (h *OrderRepository) Insert(ctx context.Context, order *billing.Order) error {
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

func (h *OrderRepository) Update(ctx context.Context, order *billing.Order) error {
	oid, _ := primitive.ObjectIDFromHex(order.Id)
	filter := bson.M{"_id": oid}
	_, err := h.db.Collection(CollectionOrder).ReplaceOne(ctx, filter, order)

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

func (h *OrderRepository) GetByUuid(ctx context.Context, uuid string) (*billing.Order, error) {
	order := &billing.Order{}
	err := h.db.Collection(CollectionOrder).FindOne(ctx, bson.M{"uuid": uuid}).Decode(order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return nil, err
	}

	return order, nil
}

func (h *OrderRepository) GetById(ctx context.Context, id string) (*billing.Order, error) {
	order := &billing.Order{}
	oid, _ := primitive.ObjectIDFromHex(id)

	err := h.db.Collection(CollectionOrder).FindOne(ctx, bson.M{"_id": oid}).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return nil, err
	}

	return order, nil
}

func (h *OrderRepository) GetByRefundReceiptNumber(ctx context.Context, id string) (*billing.Order, error) {
	order := &billing.Order{}
	err := h.db.Collection(CollectionOrder).FindOne(ctx, bson.M{"refund.receipt_number": id}).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return nil, err
	}

	return order, nil
}

func (h *OrderRepository) GetByProjectOrderId(ctx context.Context, projectId, projectOrderId string) (*billing.Order, error) {
	order := &billing.Order{}
	id, _ := primitive.ObjectIDFromHex(projectId)
	filter := bson.M{"project._id": id, "project_order_id": projectOrderId}
	err := h.db.Collection(CollectionOrder).FindOne(ctx, filter).Decode(&order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionOrder),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		return nil, err
	}

	return order, nil
}
