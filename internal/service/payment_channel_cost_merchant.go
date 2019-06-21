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
	"sort"
)

const (
	cachePaymentChannelCostMerchantKeyId = "pccm:id:%s"
	cachePaymentChannelCostMerchantKey   = "pccm:m:%s:n:%s:pc:%s:r:%s:c:%s"
	cachePaymentChannelCostMerchantAll   = "pccm:all:m:%s"

	collectionPaymentChannelCostMerchant = "payment_channel_cost_merchant"

	errorCostMatchedToAmountNotFound = "cost matched to amount not found"
)

func (s *Service) GetAllPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchantListRequest,
	res *billing.PaymentChannelCostMerchantList,
) error {
	val, err := s.paymentChannelCostMerchant.GetAllForMerchant(req.MerchantId)
	if err != nil {
		return err
	}
	res.Items = val.Items
	return nil
}

func (s *Service) GetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchantRequest,
	res *billing.PaymentChannelCostMerchant,
) error {
	val, err := s.getPaymentChannelCostMerchant(req)
	if err != nil {
		return err
	}
	res.Id = val.Id
	res.MerchantId = val.MerchantId
	res.Name = val.Name
	res.PayoutCurrency = val.PayoutCurrency
	res.MinAmount = val.MinAmount
	res.Region = val.Region
	res.Country = val.Country
	res.MethodPercent = val.MethodPercent
	res.MethodFixAmount = val.MethodFixAmount
	res.PsPercent = val.PsPercent
	res.PsFixedFee = val.PsFixedFee
	res.PsFixedFeeCurrency = val.PsFixedFeeCurrency
	res.CreatedAt = val.CreatedAt
	res.UpdatedAt = val.UpdatedAt
	res.IsActive = val.IsActive

	return nil
}

func (s *Service) SetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchant,
	res *billing.PaymentChannelCostMerchant,
) error {

	var err error

	if _, err := s.merchant.GetById(req.MerchantId); err != nil {
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

	// todo: 1. check for supported PayoutCurrency after integrations with currencies service
	// todo: 2. check for supported PsFixedFeeCurrency after integrations with currencies service

	if req.Id != "" {
		val, err := s.paymentChannelCostMerchant.GetById(req.Id)
		if err != nil {
			return err
		}
		req.Id = val.Id
		req.MerchantId = val.MerchantId
		req.CreatedAt = val.CreatedAt
		err = s.paymentChannelCostMerchant.Update(req)
	} else {
		req.Id = bson.NewObjectId().Hex()
		err = s.paymentChannelCostMerchant.Insert(req)
	}
	if err != nil {
		return err
	}

	res.Id = req.Id
	res.MerchantId = req.MerchantId
	res.Name = req.Name
	res.PayoutCurrency = req.PayoutCurrency
	res.MinAmount = req.MinAmount
	res.Region = req.Region
	res.Country = req.Country
	res.MethodPercent = req.MethodPercent
	res.MethodFixAmount = req.MethodFixAmount
	res.PsPercent = req.PsPercent
	res.PsFixedFee = req.PsFixedFee
	res.PsFixedFeeCurrency = req.PsFixedFeeCurrency
	res.CreatedAt = req.CreatedAt
	res.UpdatedAt = req.UpdatedAt
	res.IsActive = true

	return nil
}

func (s *Service) DeletePaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.EmptyResponse,
) error {
	pc, err := s.paymentChannelCostMerchant.GetById(req.Id)
	if err != nil {
		return err
	}
	err = s.paymentChannelCostMerchant.Delete(pc)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) getPaymentChannelCostMerchant(req *billing.PaymentChannelCostMerchantRequest) (*billing.PaymentChannelCostMerchant, error) {
	val, err := s.paymentChannelCostMerchant.Get(req.MerchantId, req.Name, req.PayoutCurrency, req.Region, req.Country)
	if err != nil {
		return nil, err
	}

	var matchedAmounts []*kvIntFloat
	for k, i := range val.Items {
		if req.Amount >= i.MinAmount {
			matchedAmounts = append(matchedAmounts, &kvIntFloat{k, i.MinAmount})
		}
	}
	if len(matchedAmounts) == 0 {
		return nil, errors.New(errorCostMatchedToAmountNotFound)
	}

	sort.Slice(matchedAmounts, func(i, j int) bool {
		return matchedAmounts[i].Value > matchedAmounts[j].Value
	})
	return val.Items[matchedAmounts[0].Key], nil
}

func newPaymentChannelCostMerchantService(svc *Service) *PaymentChannelCostMerchant {
	s := &PaymentChannelCostMerchant{svc: svc}
	return s
}

func (h *PaymentChannelCostMerchant) Insert(obj *billing.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.FormatAmount(obj.MethodPercent)
	obj.PsPercent = tools.FormatAmount(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Insert(obj); err != nil {
		return err
	}

	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, obj.Country)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	key = fmt.Sprintf(cachePaymentChannelCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostMerchant) MultipleInsert(obj []*billing.PaymentChannelCostMerchant) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		v.MinAmount = tools.FormatAmount(v.MinAmount)
		v.MethodFixAmount = tools.FormatAmount(v.MethodFixAmount)
		v.MethodPercent = tools.FormatAmount(v.MethodPercent)
		v.PsPercent = tools.FormatAmount(v.PsPercent)
		v.PsFixedFee = tools.FormatAmount(v.PsFixedFee)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v

		key := fmt.Sprintf(cachePaymentChannelCostMerchantAll, v.MerchantId)
		if err := h.svc.cacher.Delete(key); err != nil {
			return err
		}
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostMerchant) Update(obj *billing.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.FormatAmount(obj.MethodPercent)
	obj.PsPercent = tools.FormatAmount(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, obj.Country)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		return err
	}

	key = fmt.Sprintf(cachePaymentChannelCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostMerchant) GetById(id string) (*billing.PaymentChannelCostMerchant, error) {
	var c billing.PaymentChannelCostMerchant
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).
		Find(bson.M{"_id": bson.ObjectIdHex(id), "is_active": true}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostMerchant) Get(merchant_id string, name string, payout_currency string, region string, country string) (*billing.PaymentChannelCostMerchantList, error) {
	var c billing.PaymentChannelCostMerchantList
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, merchant_id, name, payout_currency, region, country)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{
		"merchant_id":     bson.ObjectIdHex(merchant_id),
		"name":            name,
		"payout_currency": payout_currency,
		"region":          region,
		"country":         country,
		"is_active":       true,
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).
		Find(query).
		All(&c.Items); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostMerchant) Delete(obj *billing.PaymentChannelCostMerchant) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, obj.Country)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	key = fmt.Sprintf(cachePaymentChannelCostMerchantAll, obj.MerchantId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	return nil
}

func (h PaymentChannelCostMerchant) GetAllForMerchant(merchant_id string) (*billing.PaymentChannelCostMerchantList, error) {
	var c = &billing.PaymentChannelCostMerchantList{}

	key := fmt.Sprintf(cachePaymentChannelCostMerchantAll, merchant_id)

	if err := h.svc.cacher.Get(key, c); err != nil {
		err = h.svc.db.Collection(collectionPaymentChannelCostMerchant).
			Find(bson.M{"merchant_id": bson.ObjectIdHex(merchant_id), "is_active": true}).
			Sort("name", "payout_currency", "region", "country").
			All(&c.Items)
		if err != nil {
			return nil, err
		}
		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}
