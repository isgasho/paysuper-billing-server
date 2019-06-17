package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

const (
	cachePayoutCostSystemKey = "pcs:current"

	collectionPayoutCostSystem = "payout_cost_system"
)

func newPayoutCostSystemService(svc *Service) *PayoutCostSystem {
	s := &PayoutCostSystem{svc: svc}
	return s
}

func (h *PayoutCostSystem) Set(obj *billing.PayoutCostSystem) error {

	// disable all previous settings
	_, err := h.svc.db.Collection(collectionPayoutCostSystem).
		UpdateAll(bson.M{"is_active": true}, bson.M{"$set": bson.M{"is_active": false}})
	if err != nil {
		return err
	}

	// adding new setting
	obj.IntrabankCostAmount = tools.FormatAmount(obj.IntrabankCostAmount)
	obj.InterbankCostAmount = tools.FormatAmount(obj.InterbankCostAmount)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPayoutCostSystem).Insert(obj); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(cachePayoutCostSystemKey, obj, 0); err != nil {
		return err
	}

	return nil
}

func (h PayoutCostSystem) Get() (*billing.PayoutCostSystem, error) {
	var c billing.PayoutCostSystem

	if err := h.svc.cacher.Get(cachePayoutCostSystemKey, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionPayoutCostSystem).
		Find(bson.M{"is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPayoutCostSystem)
	}

	_ = h.svc.cacher.Set(cachePayoutCostSystemKey, c, 0)
	return &c, nil
}
