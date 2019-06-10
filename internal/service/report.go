package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"time"
)

const (
	reportErrorNotFound = "not found"
)

func (s *Service) FindAllOrders(
	ctx context.Context,
	req *grpc.ListOrdersRequest,
	rsp *billing.OrderPaginate,
) error {
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
			ar := bson.RegEx{Pattern: ".*" + req.Account + ".*", Options: "i"}
			query["$or"] = []bson.M{
				{"project_account": ar},
				{"pm_account": ar},
				{"payer_data.phone": ar},
				{"payer_data.email": ar},
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
	}

	co, err := s.db.Collection(collectionOrder).Find(query).Count()

	if err != nil {
		s.logError("Query from table ended with error", []interface{}{"table", collectionOrder, "error", err})
		return err
	}

	var o []*billing.Order
	if err := s.db.Collection(collectionOrder).Find(query).Sort(req.Sort...).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&o); err != nil {
		s.logError("Query from table ended with error", []interface{}{"table", collectionOrder, "error", err})
		return err
	}

	rsp.Count = int32(co)
	rsp.Items = o

	return nil
}

func (s *Service) GetOrder(
	ctx context.Context,
	req *grpc.GetOrderRequest,
	rsp *billing.Order,
) error {
	query := bson.M{"uuid": req.Id}

	if req.Merchant != "" {
		query["project.merchant_id"] = bson.ObjectIdHex(req.Merchant)
	}

	err := s.db.Collection(collectionOrder).Find(query).One(&rsp)

	if err != nil {
		s.logError("Query from table ended with error", []interface{}{"table", collectionOrder, "error", err, "query", query})
		return err
	}

	return nil
}
