package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"time"
)

const (
	collectionOrderView = "order_view"
)

func (s *Service) getOrderFromViewPublic(id string) (*billing.OrderViewPublic, error) {
	result := &billing.OrderViewPublic{}
	err := s.db.Collection(collectionOrderView).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) getOrderFromViewPrivate(id string) (*billing.OrderViewPrivate, error) {
	result := &billing.OrderViewPrivate{}
	err := s.db.Collection(collectionOrderView).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) getTransactionsForVatReport(from, to time.Time, country string, pagination ...int) ([]*billing.OrderViewPrivate, error) {
	result := []*billing.OrderViewPrivate{}

	query := bson.M{
		"pm_order_close_date": bson.M{
			"$gte": bod(from),
			"$lte": eod(to),
		},
		"country": country,
	}

	dbRequest := s.db.Collection(collectionOrderView).Find(query).Sort("date_time")

	if pagination != nil {
		if val := pagination[0]; val > 0 {
			dbRequest.Limit(val)
		}
		if val := pagination[1]; val > 0 {
			dbRequest.Skip(val)
		}
	}

	err := dbRequest.All(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
