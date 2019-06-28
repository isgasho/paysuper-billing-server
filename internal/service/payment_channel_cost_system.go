package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
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
	res *grpc.PaymentChannelCostSystemListResponse,
) error {
	val, err := s.paymentChannelCostSystem.GetAll()
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemGetAll
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetPaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentChannelCostSystemRequest,
	res *grpc.PaymentChannelCostSystemResponse,
) error {
	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemGet
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetPaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentChannelCostSystem,
	res *grpc.PaymentChannelCostSystemResponse,
) error {

	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country)
	if err != nil && err.Error() != fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem) {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemGet
		return nil
	}

	if req.Country != "" {
		country, err := s.country.GetByIsoCodeA2(req.Country)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorCountryNotFound
			return nil
		}
		req.Region = country.Region
	} else {
		exists, err := s.country.IsRegionExists(req.Region)
		if err != nil || !exists {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorCountryRegionNotExists
			return nil
		}
	}

	req.IsActive = true

	if val == nil {
		req.Id = bson.NewObjectId().Hex()
		err = s.paymentChannelCostSystem.Insert(req)
	} else {
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.paymentChannelCostSystem.Update(req)
	}
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemSetFailed
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeletePaymentChannelCostSystem(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.ResponseError,
) error {
	pc, err := s.paymentChannelCostSystem.GetById(req.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemGet
		return nil
	}
	err = s.paymentChannelCostSystem.Delete(pc)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemDelete
		return nil
	}

	res.Status = pkg.ResponseStatusOk
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
