package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"sort"
)

const (
	cacheMoneyBackCostSystemKey   = "pucs:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d"
	cacheMoneyBackCostSystemKeyId = "pucs:id:%s"
	cacheMoneyBackCostSystemAll   = "pucs:all"

	collectionMoneyBackCostSystem = "money_back_cost_system"

	errorDaysMatchedNotFound = "days matched not found"
)

var (
	errorMoneybackSystemGetAll    = newBillingServerErrorMsg("mbs000001", "can't get list of money back setting for system")
	errorMoneybackSystemGet       = newBillingServerErrorMsg("mbs000002", "can't get money back setting for system")
	errorMoneybackSystemSetFailed = newBillingServerErrorMsg("mbs000003", "can't set money back setting for system")
	errorMoneybackSystemDelete    = newBillingServerErrorMsg("mbs000004", "can't delete money back setting for system")
)

type moneyBackCostSystems struct {
	Items []*billing.MoneyBackCostSystem
}

func (s *Service) GetAllMoneyBackCostSystem(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.MoneyBackCostSystemListResponse,
) error {
	val, err := s.moneyBackCostSystem.GetAll()
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemGetAll
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetMoneyBackCostSystem(
	ctx context.Context,
	req *billing.MoneyBackCostSystemRequest,
	res *grpc.MoneyBackCostSystemResponse,
) error {
	val, err := s.getMoneyBackCostSystem(req)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemGet
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetMoneyBackCostSystem(
	ctx context.Context,
	req *billing.MoneyBackCostSystem,
	res *grpc.MoneyBackCostSystemResponse,
) error {

	var err error

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

	// todo: check fo valid payout currency after integrations with currencies service

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostSystem.GetById(req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorMoneybackSystemDelete
			return nil
		}
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostSystem.Update(req)
	} else {
		req.Id = bson.NewObjectId().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostSystem.Insert(req)
	}
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemSetFailed
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeleteMoneyBackCostSystem(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.ResponseError,
) error {
	pc, err := s.moneyBackCostSystem.GetById(req.Id)
	if err != nil {
		return err
	}
	err = s.moneyBackCostSystem.Delete(pc)
	if err != nil {
		return err
	}
	res.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostSystem(req *billing.MoneyBackCostSystemRequest) (*billing.MoneyBackCostSystem, error) {
	val, err := s.moneyBackCostSystem.Get(req.Name, req.PayoutCurrency, req.UndoReason, req.Region, req.Country, req.PaymentStage)
	if err != nil {
		return nil, err
	}

	var matchedDays []*kvIntInt
	for k, i := range val.Items {
		if req.Days >= i.DaysFrom {
			matchedDays = append(matchedDays, &kvIntInt{k, i.DaysFrom})
		}
	}
	if len(matchedDays) == 0 {
		return nil, errors.New(errorDaysMatchedNotFound)
	}

	sort.Slice(matchedDays, func(i, j int) bool {
		return matchedDays[i].Value > matchedDays[j].Value
	})
	return val.Items[matchedDays[0].Key], nil
}

func newMoneyBackCostSystemService(svc *Service) *MoneyBackCostSystem {
	s := &MoneyBackCostSystem{svc: svc}
	return s
}

func (h *MoneyBackCostSystem) Insert(obj *billing.MoneyBackCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).Insert(obj); err != nil {
		return err
	}

	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheMoneyBackCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostSystem) MultipleInsert(obj []*billing.MoneyBackCostSystem) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.FormatAmount(v.Percent)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).Insert(c...); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheMoneyBackCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostSystem) Update(obj *billing.MoneyBackCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheMoneyBackCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostSystem) Get(
	name string,
	payout_currency string,
	undo_reason string,
	region string,
	country string,
	payment_stage int32,
) (*moneyBackCostSystems, error) {
	var c moneyBackCostSystems
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, name, payout_currency, undo_reason, region, country, payment_stage)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{
		"name":            name,
		"payout_currency": payout_currency,
		"undo_reason":     undo_reason,
		"region":          region,
		"country":         country,
		"payment_stage":   payment_stage,
		"is_active":       true,
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).
		Find(query).
		All(&c.Items); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostSystem) GetById(id string) (*billing.MoneyBackCostSystem, error) {
	var c billing.MoneyBackCostSystem
	key := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostSystem) Delete(obj *billing.MoneyBackCostSystem) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cacheMoneyBackCostSystemAll); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostSystem) GetAll() (*billing.MoneyBackCostSystemList, error) {
	var c = &billing.MoneyBackCostSystemList{}
	key := cacheMoneyBackCostSystemAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		err = h.svc.db.Collection(collectionMoneyBackCostSystem).
			Find(bson.M{"is_active": true}).
			Sort("name", "payout_currency", "undo_reason", "region", "country", "payment_stage").
			All(&c.Items)
		if err != nil {
			return nil, err
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}
