package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"sort"
)

const (
	cachePaymentChannelCostMerchantKeyId = "pccm:id:%s"
	cachePaymentChannelCostMerchantKey   = "pccm:m:%s:n:%s:pc:%s:r:%s:c:%s:mcc:%s"
	cachePaymentChannelCostMerchantAll   = "pccm:all:m:%s"

	collectionPaymentChannelCostMerchant = "payment_channel_cost_merchant"
)

var (
	errorPaymentChannelMerchantGetAll           = newBillingServerErrorMsg("pcm000001", "can't get list of payment channel setting for merchant")
	errorPaymentChannelMerchantGet              = newBillingServerErrorMsg("pcm000002", "can't get payment channel setting for merchant")
	errorPaymentChannelMerchantSetFailed        = newBillingServerErrorMsg("pcm000003", "can't set payment channel setting for merchant")
	errorPaymentChannelMerchantDelete           = newBillingServerErrorMsg("pcm000004", "can't delete payment channel setting for merchant")
	errorPaymentChannelMerchantCurrency         = newBillingServerErrorMsg("pcm000005", "currency not supported")
	errorPaymentChannelMerchantCostAlreadyExist = newBillingServerErrorMsg("pcm000006", "cost with specified parameters already exist")
	errorCostMatchedToAmountNotFound            = newBillingServerErrorMsg("pcm000007", "cost matched to amount not found")
	errorPaymentChannelMccCode                  = newBillingServerErrorMsg("pcm000008", "mcc code not supported")
)

type PaymentChannelCostMerchantInterface interface {
	MultipleInsert(obj []*billing.PaymentChannelCostMerchant) error
	Update(obj *billing.PaymentChannelCostMerchant) error
	GetById(id string) (*billing.PaymentChannelCostMerchant, error)
	Get(merchantId, name, payoutCurrency, region, country, mccCode string) ([]*internalPkg.PaymentChannelCostMerchantSet, error)
	Delete(obj *billing.PaymentChannelCostMerchant) error
	GetAllForMerchant(merchantId string) (*billing.PaymentChannelCostMerchantList, error)
}

func newPaymentChannelCostMerchantService(svc *Service) *PaymentChannelCostMerchant {
	s := &PaymentChannelCostMerchant{svc: svc}
	return s
}

func (s *Service) GetAllPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchantListRequest,
	res *grpc.PaymentChannelCostMerchantListResponse,
) error {
	val, err := s.paymentChannelCostMerchant.GetAllForMerchant(req.MerchantId)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantGetAll
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchantRequest,
	res *grpc.PaymentChannelCostMerchantResponse,
) error {
	val, err := s.getPaymentChannelCostMerchant(req)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorPaymentChannelMerchantGet
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentChannelCostMerchant,
	res *grpc.PaymentChannelCostMerchantResponse,
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
		req.Region = country.PayerTariffRegion
	} else {
		exists := s.country.IsTariffRegionExists(req.Region)
		if !exists {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorCountryRegionNotExists
			return nil
		}
	}

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currencies.EmptyRequest{})
	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !contains(sCurr.Currencies, req.PsFixedFeeCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !contains(sCurr.Currencies, req.MethodFixAmountCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelMccCode
		return nil
	}

	req.IsActive = true

	if req.Id != "" {
		val, err := s.paymentChannelCostMerchant.GetById(req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = errorPaymentChannelMerchantSetFailed
			return nil
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
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantSetFailed

		if mgo.IsDup(err) {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaymentChannelMerchantCostAlreadyExist
		}

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeletePaymentChannelCostMerchant(
	ctx context.Context,
	req *billing.PaymentCostDeleteRequest,
	res *grpc.ResponseError,
) error {
	pc, err := s.paymentChannelCostMerchant.GetById(req.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.paymentChannelCostMerchant.Delete(pc)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantDelete
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) getPaymentChannelCostMerchant(req *billing.PaymentChannelCostMerchantRequest) (*billing.PaymentChannelCostMerchant, error) {
	val, err := s.paymentChannelCostMerchant.Get(req.MerchantId, req.Name, req.PayoutCurrency, req.Region, req.Country, req.MccCode)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorMoneybackMerchantDaysMatchedNotFound
	}

	var matched []*kvIntFloat
	for _, set := range val {
		for k, i := range set.Set {
			if req.Amount >= i.MinAmount {
				matched = append(matched, &kvIntFloat{k, i.MinAmount})
			}
		}
		if len(matched) == 0 {
			continue
		}

		sort.Slice(matched, func(i, j int) bool {
			return matched[i].Value > matched[j].Value
		})
		return set.Set[matched[0].Key], nil
	}

	return nil, errorCostMatchedToAmountNotFound
}

func (h *PaymentChannelCostMerchant) Insert(obj *billing.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.ToPrecise(obj.MethodPercent)
	obj.PsPercent = tools.ToPrecise(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Insert(obj); err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h PaymentChannelCostMerchant) MultipleInsert(obj []*billing.PaymentChannelCostMerchant) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		if v.Id == "" {
			v.Id = bson.NewObjectId().Hex()
		}
		v.MinAmount = tools.FormatAmount(v.MinAmount)
		v.MethodFixAmount = tools.FormatAmount(v.MethodFixAmount)
		v.MethodPercent = tools.ToPrecise(v.MethodPercent)
		v.PsPercent = tools.ToPrecise(v.PsPercent)
		v.PsFixedFee = tools.FormatAmount(v.PsFixedFee)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Insert(c...); err != nil {
		return err
	}

	for _, v := range obj {
		if err := h.updateCaches(v); err != nil {
			return err
		}
	}

	return nil
}

func (h PaymentChannelCostMerchant) Update(obj *billing.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.ToPrecise(obj.MethodPercent)
	obj.PsPercent = tools.ToPrecise(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}

	return h.updateCaches(obj)
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

func (h PaymentChannelCostMerchant) Get(
	merchantId string,
	name string,
	payoutCurrency string,
	region string,
	country string,
	mccCode string,
) (c []*internalPkg.PaymentChannelCostMerchantSet, err error) {
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKey, merchantId, name, payoutCurrency, region, country, mccCode)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	matchQuery := bson.M{
		"merchant_id":     bson.ObjectIdHex(merchantId),
		"name":            bson.RegEx{Pattern: "^" + name + "$", Options: "i"},
		"payout_currency": payoutCurrency,
		"is_active":       true,
		"mcc_code":        mccCode,
		"$or": []bson.M{
			{
				"country": country,
				"region":  region,
			},
			{
				"$or": []bson.M{
					{"country": ""},
					{"country": bson.M{"$exists": false}},
				},
				"region": region,
			},
		},
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
		{
			"$group": bson.M{
				"_id": "$country",
				"set": bson.M{"$push": "$$ROOT"},
			},
		},
		{
			"$sort": bson.M{"_id": -1},
		},
	}

	err = h.svc.db.Collection(collectionPaymentChannelCostMerchant).Pipe(query).All(&c)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostMerchant),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}
	return c, nil
}

func (h PaymentChannelCostMerchant) Delete(obj *billing.PaymentChannelCostMerchant) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h PaymentChannelCostMerchant) GetAllForMerchant(merchantId string) (c *billing.PaymentChannelCostMerchantList, err error) {
	key := fmt.Sprintf(cachePaymentChannelCostMerchantAll, merchantId)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"is_active":   true,
	}

	var res = []*billing.PaymentChannelCostMerchant{}

	err = h.svc.db.Collection(collectionPaymentChannelCostMerchant).
		Find(query).
		Sort("name", "payout_currency", "region", "country", "mcc_code").
		All(&res)

	c = &billing.PaymentChannelCostMerchantList{
		Items: res,
	}

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostMerchant),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return c, nil
}

func (h PaymentChannelCostMerchant) updateCaches(obj *billing.PaymentChannelCostMerchant) (err error) {
	groupKeys := []string{
		fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, obj.Country, obj.MccCode),
		fmt.Sprintf(cachePaymentChannelCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.Region, "", obj.MccCode),
		fmt.Sprintf(cachePaymentChannelCostMerchantAll, obj.MerchantId),
	}
	for _, key := range groupKeys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}

	keys := []string{
		fmt.Sprintf(cachePaymentChannelCostMerchantKeyId, obj.Id),
	}

	for _, key := range keys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}

	if obj.IsActive {
		for _, key := range keys {
			err = h.svc.cacher.Set(key, obj, 0)
			if err != nil {
				zap.L().Error(
					pkg.ErrorCacheQueryFailed,
					zap.Error(err),
					zap.String(pkg.ErrorCacheFieldCmd, "SET"),
					zap.String(pkg.ErrorCacheFieldKey, key),
					zap.Any(pkg.ErrorCacheFieldData, obj),
				)
				return
			}
		}
		return
	}

	return
}
