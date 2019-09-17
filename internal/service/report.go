package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
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
	req *grpc.ListOrdersRequest,
	rsp *grpc.ListOrdersPublicResponse,
) error {
	count, orders, err := s.getOrdersList(req, make([]*billing.OrderViewPublic, 1))

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.ListOrdersPublicResponseItem{
		Count: int32(count),
		Items: orders.([]*billing.OrderViewPublic),
	}

	return nil
}

func (s *Service) FindAllOrdersPrivate(
	ctx context.Context,
	req *grpc.ListOrdersRequest,
	rsp *grpc.ListOrdersPrivateResponse,
) error {
	count, orders, err := s.getOrdersList(req, make([]*billing.OrderViewPrivate, 1))

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = reportErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.ListOrdersPrivateResponseItem{
		Count: int32(count),
		Items: orders.([]*billing.OrderViewPrivate),
	}

	return nil
}

func (s *Service) GetOrderPublic(
	ctx context.Context,
	req *grpc.GetOrderRequest,
	rsp *grpc.GetOrderPublicResponse,
) error {
	order, err := s.getOrderBy(req.Id, req.Merchant, new(billing.OrderViewPublic))

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == orderErrorNotFound {
			rsp.Status = pkg.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = order.(*billing.OrderViewPublic)

	return nil
}

func (s *Service) GetOrderPrivate(
	ctx context.Context,
	req *grpc.GetOrderRequest,
	rsp *grpc.GetOrderPrivateResponse,
) error {
	order, err := s.getOrderBy(req.Id, req.Merchant, new(billing.OrderViewPrivate))

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == orderErrorNotFound {
			rsp.Status = pkg.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = order.(*billing.OrderViewPrivate)

	return nil
}

func (s *Service) getOrderBy(uuid, merchantId string, receiver interface{}) (interface{}, error) {
	query := bson.M{"uuid": uuid}

	if merchantId != "" {
		query["project.merchant_id"] = bson.ObjectIdHex(merchantId)
	}

	err := s.db.Collection(collectionOrderView).Find(query).One(receiver)

	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, orderErrorNotFound
		}

		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, reportErrorUnknown
	}

	return receiver, nil
}

func (s *Service) getOrdersList(req *grpc.ListOrdersRequest, receiver interface{}) (int, interface{}, error) {
	query := make(bson.M)

	if len(req.Merchant) > 0 {
		var merchants []bson.ObjectId

		for _, v := range req.Merchant {
			merchants = append(merchants, bson.ObjectIdHex(v))
		}

		query["project.merchant_id"] = bson.M{"$in": merchants}
	}

	if req.QuickSearch != "" {
		r := bson.RegEx{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}

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
			var projects []bson.ObjectId

			for _, v := range req.Project {
				projects = append(projects, bson.ObjectIdHex(v))
			}

			query["project._id"] = bson.M{"$in": projects}
		}

		if len(req.Country) > 0 {
			query["user.address.country"] = bson.M{"$in": req.Country}
		}

		if len(req.PaymentMethod) > 0 {
			var paymentMethod []bson.ObjectId

			for _, v := range req.PaymentMethod {
				paymentMethod = append(paymentMethod, bson.ObjectIdHex(v))
			}

			query["payment_method._id"] = bson.M{"$in": paymentMethod}
		}

		if len(req.Status) > 0 {
			query["status"] = bson.M{"$in": req.Status}
		}

		if req.Account != "" {
			r := bson.RegEx{Pattern: ".*" + req.Account + ".*", Options: "i"}
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

	count, err := s.db.Collection(collectionOrderView).Find(query).Count()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return 0, nil, err
	}

	err = s.db.Collection(collectionOrderView).Find(query).Sort(req.Sort...).Limit(int(req.Limit)).
		Skip(int(req.Offset)).All(&receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return 0, nil, err
	}

	return count, receiver, nil
}
