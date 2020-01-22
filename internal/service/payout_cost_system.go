package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	tools "github.com/paysuper/paysuper-tools/number"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	cachePayoutCostSystemKey = "pcs:current"

	collectionPayoutCostSystem = "payout_cost_system"
)

func newPayoutCostSystemService(svc *Service) *PayoutCostSystem {
	s := &PayoutCostSystem{svc: svc}
	return s
}

func (h *PayoutCostSystem) Set(ctx context.Context, obj *billingpb.PayoutCostSystem) error {
	// disable all previous settings
	filter := bson.M{"is_active": true}
	update := bson.M{"$set": bson.M{"is_active": false}}
	_, err := h.svc.db.Collection(collectionPayoutCostSystem).UpdateMany(ctx, filter, update)

	if err != nil {
		return err
	}

	// adding new setting
	obj.IntrabankCostAmount = tools.FormatAmount(obj.IntrabankCostAmount)
	obj.InterbankCostAmount = tools.FormatAmount(obj.InterbankCostAmount)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	_, err = h.svc.db.Collection(collectionPayoutCostSystem).InsertOne(ctx, obj)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Set(cachePayoutCostSystemKey, obj, 0); err != nil {
		return err
	}

	return nil
}

func (h PayoutCostSystem) Get(ctx context.Context) (*billingpb.PayoutCostSystem, error) {
	var c billingpb.PayoutCostSystem

	if err := h.svc.cacher.Get(cachePayoutCostSystemKey, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionPayoutCostSystem).FindOne(ctx, bson.M{"is_active": true}).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPayoutCostSystem)
	}

	_ = h.svc.cacher.Set(cachePayoutCostSystemKey, c, 0)
	return &c, nil
}
