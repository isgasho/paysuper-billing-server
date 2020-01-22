package service

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/repository"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"time"
)

const (
	orderFailedNotificationQueryFieldMask = "is_notifications_sent.%s"
)

var (
	reportErrorUnknown = newBillingServerErrorMsg("rp000001", "request processing failed. try request later")
)

func (s *Service) FindAllOrdersPublic(
	ctx context.Context,
	req *billingpb.ListOrdersRequest,
	rsp *billingpb.ListOrdersPublicResponse,
) error {
	count, orders, err := s.getOrdersList(ctx, req, collectionOrderView, make([]*billingpb.OrderViewPublic, 1))

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	orderList := orders.([]*billingpb.OrderViewPublic)

	if len(orderList) > 0 && orderList[0].MerchantId != req.Merchant[0] {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = &billingpb.ListOrdersPublicResponseItem{
		Count: count,
		Items: orderList,
	}

	return nil
}

func (s *Service) FindAllOrdersPrivate(
	ctx context.Context,
	req *billingpb.ListOrdersRequest,
	rsp *billingpb.ListOrdersPrivateResponse,
) error {
	count, orders, err := s.getOrdersList(ctx, req, collectionOrderView, make([]*billingpb.OrderViewPrivate, 1))

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = &billingpb.ListOrdersPrivateResponseItem{
		Count: count,
		Items: orders.([]*billingpb.OrderViewPrivate),
	}

	return nil
}

func (s *Service) FindAllOrders(
	ctx context.Context,
	req *billingpb.ListOrdersRequest,
	rsp *billingpb.ListOrdersResponse,
) error {
	count, orders, err := s.getOrdersList(ctx, req, repository.CollectionOrder, make([]*billingpb.Order, 1))

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = &billingpb.ListOrdersResponseItem{
		Count: count,
		Items: orders.([]*billingpb.Order),
	}

	return nil
}

func (s *Service) GetOrderPublic(
	ctx context.Context,
	req *billingpb.GetOrderRequest,
	rsp *billingpb.GetOrderPublicResponse,
) error {
	order, err := s.orderView.GetOrderBy(ctx, "", req.OrderId, req.MerchantId, new(billingpb.OrderViewPublic))

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		if err == orderErrorNotFound {
			rsp.Status = billingpb.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Item = order.(*billingpb.OrderViewPublic)

	if rsp.Item.MerchantId != req.MerchantId {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

func (s *Service) GetOrderPrivate(
	ctx context.Context,
	req *billingpb.GetOrderRequest,
	rsp *billingpb.GetOrderPrivateResponse,
) error {
	order, err := s.orderView.GetOrderBy(ctx, "", req.OrderId, req.MerchantId, new(billingpb.OrderViewPrivate))

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		if err == orderErrorNotFound {
			rsp.Status = billingpb.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = order.(*billingpb.OrderViewPrivate)

	return nil
}

func (s *Service) getOrdersList(
	ctx context.Context,
	req *billingpb.ListOrdersRequest,
	source string,
	receiver interface{},
) (int64, interface{}, error) {
	query := make(bson.M)

	if len(req.Merchant) > 0 {
		var merchants []primitive.ObjectID

		for _, v := range req.Merchant {
			oid, _ := primitive.ObjectIDFromHex(v)
			merchants = append(merchants, oid)
		}

		query["project.merchant_id"] = bson.M{"$in": merchants}
	}

	if req.QuickSearch != "" {
		r := primitive.Regex{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}

		query["$or"] = []bson.M{
			{"uuid": bson.M{"$regex": r}},
			{"user.external_id": bson.M{"$regex": r, "$exists": true}},
			{"project_order_id": bson.M{"$regex": r, "$exists": true}},
			{"project.name": bson.M{"$elemMatch": bson.M{"value": r}}},
			{"payment_method.name": bson.M{"$regex": r, "$exists": true}},
		}
	} else {
		if req.Id != "" {
			query["uuid"] = req.Id
		}

		if len(req.Project) > 0 {
			var projects []primitive.ObjectID

			for _, v := range req.Project {
				oid, _ := primitive.ObjectIDFromHex(v)
				projects = append(projects, oid)
			}

			query["project._id"] = bson.M{"$in": projects}
		}

		if len(req.Country) > 0 {
			query["user.address.country"] = bson.M{"$in": req.Country}
		}

		if len(req.PaymentMethod) > 0 {
			var paymentMethod []primitive.ObjectID

			for _, v := range req.PaymentMethod {
				oid, _ := primitive.ObjectIDFromHex(v)
				paymentMethod = append(paymentMethod, oid)
			}

			query["payment_method._id"] = bson.M{"$in": paymentMethod}
		}

		if len(req.Status) > 0 {
			query["status"] = bson.M{"$in": req.Status}
		}

		if req.Account != "" {
			r := primitive.Regex{Pattern: ".*" + req.Account + ".*", Options: "i"}
			query["$or"] = []bson.M{
				{"user.external_id": r},
				{"user.phone": r},
				{"user.email": r},
				{"payment_method.card.masked": bson.M{"$regex": r, "$exists": true}},
				{"payment_method.crypto_currency.address": bson.M{"$regex": r, "$exists": true}},
				{"payment_method.wallet.account": bson.M{"$regex": r, "$exists": true}},
			}
		}

		pmDates := make(bson.M)

		if req.PmDateFrom != 0 {
			pmDates["$gte"] = time.Unix(req.PmDateFrom, 0)
		}

		if req.PmDateTo != 0 {
			pmDates["$lte"] = time.Unix(req.PmDateTo, 0)
		}

		if len(pmDates) > 0 {
			query["pm_order_close_date"] = pmDates
		}

		prjDates := make(bson.M)

		if req.ProjectDateFrom != 0 {
			prjDates["$gte"] = time.Unix(req.ProjectDateFrom, 0)
		}

		if req.ProjectDateTo != 0 {
			prjDates["$lte"] = time.Unix(req.ProjectDateTo, 0)
		}

		if len(prjDates) > 0 {
			query["created_at"] = prjDates
		}

		if req.StatusNotificationFailedFor != "" {
			field := fmt.Sprintf(orderFailedNotificationQueryFieldMask, req.StatusNotificationFailedFor)
			query[field] = false
		}
	}

	if req.HideTest == true {
		query["is_production"] = true
	}

	count, err := s.db.Collection(source).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, source),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return 0, nil, err
	}

	opts := options.Find().
		SetSort(mongodb.ToSortOption(req.Sort)).
		SetLimit(req.Limit).
		SetSkip(req.Offset)
	cursor, err := s.db.Collection(source).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, source),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, nil, err
	}

	if res, ok := receiver.([]*billingpb.OrderViewPublic); ok {
		err = cursor.All(ctx, &res)
		receiver = res
	} else if res, ok := receiver.([]*billingpb.OrderViewPrivate); ok {
		err = cursor.All(ctx, &res)
		receiver = res
	} else if res, ok := receiver.([]*billingpb.Order); ok {
		err = cursor.All(ctx, &res)
		receiver = res
	}

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, source),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, nil, err
	}

	return count, receiver, nil
}
