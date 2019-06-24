package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

const (
	cachePaymentChannelCostSystemKey   = "pccs:n:%s:r:%s:c:%s"
	cachePaymentChannelCostSystemKeyId = "pccs:id:%s"
	cachePaymentChannelCostSystemAll   = "pccs:all"

	collectionPaymentChannelCostSystem = "payment_channel_cost_system"
)

func (s *Service) GetAllPaymentChannelCostSystem(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *billing.PaymentChannelCostSystemList,
) error {
	val, err := s.paymentChannelCostSystem.GetAll()
	if err != nil {
		return err
	}
	res.Items = val.Items
	return nil
}

func (s *Service) GetPaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentChannelCostSystemRequest,
	res *billing.PaymentChannelCostSystem,
) error {
	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country)
	if err != nil {
		return err
	}
	res.Id = val.Id
	res.Name = val.Name
	res.Region = val.Region
	res.Country = val.Country
	res.Percent = val.Percent
	res.FixAmount = val.FixAmount
	res.FixAmountCurrency = val.FixAmountCurrency
	res.CreatedAt = val.CreatedAt
	res.UpdatedAt = val.UpdatedAt
	res.IsActive = val.IsActive

	return nil
}

func (s *Service) SetPaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentChannelCostSystem,
	res *billing.PaymentChannelCostSystem,
) error {

	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country)
	if err != nil && err.Error() != fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem) {
		return err
	}

	if req.Country != "" {
		country, err := s.country.GetByIsoCodeA2(req.Country)
		if err != nil {
			return err
		}
		req.Region = country.Region
	} else {
		exists, err := s.country.IsRegionExists(req.Region)
		if err != nil {
			return err
		}
		if !exists {
			return errors.New(errorRegionNotExists)
		}
	}

	if val == nil {
		req.Id = bson.NewObjectId().Hex()
		err = s.paymentChannelCostSystem.Insert(req)
	} else {
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.paymentChannelCostSystem.Update(req)
	}
	if err != nil {
		return err
	}

	res.Id = req.Id
	res.Name = req.Name
	res.Region = req.Region
	res.Country = req.Country
	res.Percent = req.Percent
	res.FixAmount = req.FixAmount
	res.FixAmountCurrency = req.FixAmountCurrency
	res.CreatedAt = req.CreatedAt
	res.UpdatedAt = req.UpdatedAt
	res.IsActive = true

	return nil
}

func (s *Service) DeletePaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.EmptyResponse,
) error {
	pc, err := s.paymentChannelCostSystem.GetById(req.Id)
	if err != nil {
		return err
	}
	err = s.paymentChannelCostSystem.Delete(pc)
	if err != nil {
		return err
	}

	return nil
}

func newPaymentChannelCostSystemService(svc *Service) *PaymentChannelCostSystem {
	s := &PaymentChannelCostSystem{svc: svc}
	return s
}

func (h *PaymentChannelCostSystem) Insert(obj *billing.PaymentChannelCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Insert(obj); err != nil {
		return err
	}

	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, obj.Country)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cachePaymentChannelCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostSystem) MultipleInsert(obj []*billing.PaymentChannelCostSystem) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.FormatAmount(v.Percent)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Insert(c...); err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cachePaymentChannelCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostSystem) Update(obj *billing.PaymentChannelCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, obj.Country)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cachePaymentChannelCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostSystem) GetById(id string) (*billing.PaymentChannelCostSystem, error) {
	var c billing.PaymentChannelCostSystem
	key := fmt.Sprintf(cachePaymentChannelCostSystemKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostSystem) Get(name string, region string, country string) (*billing.PaymentChannelCostSystem, error) {
	var c billing.PaymentChannelCostSystem
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, name, region, country)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).
		Find(bson.M{"name": name, "region": region, "country": country, "is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostSystem) Delete(obj *billing.PaymentChannelCostSystem) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, obj.Country)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}
	if err := h.svc.cacher.Delete(cachePaymentChannelCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostSystem) GetAll() (*billing.PaymentChannelCostSystemList, error) {
	var c = &billing.PaymentChannelCostSystemList{}
	key := cachePaymentChannelCostSystemAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		err = h.svc.db.Collection(collectionPaymentChannelCostSystem).
			Find(bson.M{"is_active": true}).
			Sort("name", "region", "country").
			All(&c.Items)
		if err != nil {
			return nil, err
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}
