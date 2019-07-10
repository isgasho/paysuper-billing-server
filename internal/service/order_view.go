package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
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
