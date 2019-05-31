package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"time"
)

func (s *Service) FindAllOrders(
	ctx context.Context,
	req *grpc.ListOrdersRequest,
	rsp *billing.OrderPaginate,
) error {
	query := make(bson.M)

	if req.QuickSearch != "" {
		r := bson.RegEx{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}

		query["$or"] = []bson.M{
			{"uuid": bson.M{"$regex": r, "$exists": true}},
			{"user.externalid": bson.M{"$regex": r, "$exists": true}},
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

	co, err := s.db.Collection(pkg.CollectionOrder).Find(query).Count()

	if err != nil {
		s.logError("Query from table ended with error", []interface{}{"table", pkg.CollectionOrder, "error", err})
		return err
	}

	var o []*billing.Order
	if err := s.db.Collection(pkg.CollectionOrder).Find(query).Sort(req.Sort...).Limit(int(req.Limit)).Skip(int(req.Offset)).All(&o); err != nil {
		s.logError("Query from table ended with error", []interface{}{"table", pkg.CollectionOrder, "error", err})
		return err
	}

	rsp.Count = int32(co)
	rsp.Items = o

	return nil
}
