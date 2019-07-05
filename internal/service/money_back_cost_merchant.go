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
	cacheMoneyBackCostMerchantKey   = "pucm:m:%s:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d"
	cacheMoneyBackCostMerchantKeyId = "pucm:id:%s"
	cacheMoneyBackCostMerchantAll   = "pucm:all:m:%s"

	collectionMoneyBackCostMerchant = "money_back_cost_merchant"
)

var (
	errorMoneybackMerchantGetAll    = newBillingServerErrorMsg("mbm000001", "can't get list of money back setting for merchant")
	errorMoneybackMerchantGet       = newBillingServerErrorMsg("mbm000002", "can't get money back setting for merchant")
	errorMoneybackMerchantSetFailed = newBillingServerErrorMsg("mbm000003", "can't set money back setting for merchant")
	errorMoneybackMerchantDelete    = newBillingServerErrorMsg("mbm000004", "can't delete money back setting for merchant")
)

func (s *Service) GetAllMoneyBackCostMerchant(
	ctx context.Context,
	req *billing.MoneyBackCostMerchantListRequest,
	res *grpc.MoneyBackCostMerchantListResponse,
) error {
	val, err := s.moneyBackCostMerchant.GetAllForMerchant(req.MerchantId)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantGetAll
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetMoneyBackCostMerchant(
	ctx context.Context,
	req *billing.MoneyBackCostMerchantRequest,
	res *grpc.MoneyBackCostMerchantResponse,
) error {
	val, err := s.getMoneyBackCostMerchant(req)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorMoneybackMerchantGet
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetMoneyBackCostMerchant(
	ctx context.Context,
	req *billing.MoneyBackCostMerchant,
	res *grpc.MoneyBackCostMerchantResponse,
) error {

	var err error

	if _, err := s.merchant.GetById(req.MerchantId); err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = merchantErrorNotFound
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

	// todo: check fo valid payout currency after integrations with currencies service

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostMerchant.GetById(req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorMoneybackMerchantSetFailed
			return nil
		}
		req.Id = val.Id
		req.MerchantId = val.MerchantId
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostMerchant.Update(req)
	} else {
		req.Id = bson.NewObjectId().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostMerchant.Insert(req)
	}
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantSetFailed
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeleteMoneyBackCostMerchant(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.ResponseError,
) error {
	pc, err := s.moneyBackCostMerchant.GetById(req.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantDelete
		return nil
	}
	err = s.moneyBackCostMerchant.Delete(pc)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantDelete
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostMerchant(req *billing.MoneyBackCostMerchantRequest) (*billing.MoneyBackCostMerchant, error) {
	val, err := s.moneyBackCostMerchant.Get(req.MerchantId, req.Name, req.PayoutCurrency, req.UndoReason, req.Region, req.Country, req.PaymentStage)
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

func newMoneyBackCostMerchantService(svc *Service) *MoneyBackCostMerchant {
	s := &MoneyBackCostMerchant{svc: svc}
	return s
}

func (h *MoneyBackCostMerchant) Insert(obj *billing.MoneyBackCostMerchant) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).Insert(obj); err != nil {
		return err
	}

	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	key = fmt.Sprintf(cacheMoneyBackCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostMerchant) MultipleInsert(obj []*billing.MoneyBackCostMerchant) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.FormatAmount(v.Percent)
		v.IsActive = true
		c[i] = v

		key := fmt.Sprintf(cacheMoneyBackCostMerchantAll, v.MerchantId)
		if err := h.svc.cacher.Delete(key); err != nil {
			return err
		}
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostMerchant) Update(obj *billing.MoneyBackCostMerchant) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.FormatAmount(obj.Percent)
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	key = fmt.Sprintf(cacheMoneyBackCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostMerchant) Get(
	merchantId string,
	name string,
	payoutCurrency string,
	undoReason string,
	region string,
	country string,
	paymentStage int32,
) (*billing.MoneyBackCostMerchantList, error) {
	var c billing.MoneyBackCostMerchantList
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, merchantId, name, payoutCurrency, undoReason, region, country, paymentStage)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{
		"merchant_id":     bson.ObjectIdHex(merchantId),
		"name":            bson.RegEx{Pattern: "^" + name + "$", Options: "i"},
		"payout_currency": payoutCurrency,
		"undo_reason":     undoReason,
		"region":          region,
		"country":         country,
		"payment_stage":   paymentStage,
		"is_active":       true,
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).
		Find(query).
		All(&c.Items); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostMerchant) GetById(id string) (*billing.MoneyBackCostMerchant, error) {
	var c billing.MoneyBackCostMerchant
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostMerchant) Delete(obj *billing.MoneyBackCostMerchant) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionMoneyBackCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	key = fmt.Sprintf(cacheMoneyBackCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h MoneyBackCostMerchant) GetAllForMerchant(merchant_id string) (*billing.MoneyBackCostMerchantList, error) {
	var c = &billing.MoneyBackCostMerchantList{}

	key := fmt.Sprintf(cacheMoneyBackCostMerchantAll, merchant_id)

	if err := h.svc.cacher.Get(key, c); err != nil {
		err = h.svc.db.Collection(collectionMoneyBackCostMerchant).
			Find(bson.M{"merchant_id": bson.ObjectIdHex(merchant_id), "is_active": true}).
			Sort("name", "payout_currency", "undo_reason", "region", "country", "payment_stage").
			All(&c.Items)
		if err != nil {
			return nil, err
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}
