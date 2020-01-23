package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/currenciespb"
	tools "github.com/paysuper/paysuper-tools/number"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
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
	errorCostMatchedNotFound                    = newBillingServerErrorMsg("pcm000009", "cost matched not found")
	errorMerchantTariffUpdate                   = newBillingServerErrorMsg("pcm000010", "can't update merchant tariffs")
)

type PaymentChannelCostMerchantInterface interface {
	MultipleInsert(ctx context.Context, obj []*billingpb.PaymentChannelCostMerchant) error
	Update(ctx context.Context, obj *billingpb.PaymentChannelCostMerchant) error
	GetById(ctx context.Context, id string) (*billingpb.PaymentChannelCostMerchant, error)
	Get(ctx context.Context, merchantId, name, payoutCurrency, region, country, mccCode string) ([]*internalPkg.PaymentChannelCostMerchantSet, error)
	Delete(ctx context.Context, obj *billingpb.PaymentChannelCostMerchant) error
	GetAllForMerchant(ctx context.Context, merchantId string) (*billingpb.PaymentChannelCostMerchantList, error)
}

func newPaymentChannelCostMerchantService(svc *Service) *PaymentChannelCostMerchant {
	s := &PaymentChannelCostMerchant{svc: svc}
	return s
}

func (s *Service) GetAllPaymentChannelCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentChannelCostMerchantListRequest,
	res *billingpb.PaymentChannelCostMerchantListResponse,
) error {
	val, err := s.paymentChannelCostMerchant.GetAllForMerchant(ctx, req.MerchantId)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantGetAll
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentChannelCostMerchantRequest,
	res *billingpb.PaymentChannelCostMerchantResponse,
) error {
	val, err := s.getPaymentChannelCostMerchant(ctx, req)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorPaymentChannelMerchantGet
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetPaymentChannelCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentChannelCostMerchant,
	res *billingpb.PaymentChannelCostMerchantResponse,
) error {
	var err error

	merchant := &billingpb.Merchant{}
	if merchant, err = s.merchantRepository.GetById(ctx, req.MerchantId); err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = merchantErrorNotFound
		return nil
	}

	if req.Country != "" {
		country, err := s.country.GetByIsoCodeA2(ctx, req.Country)
		if err != nil {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorCountryNotFound
			return nil
		}
		req.Region = country.PayerTariffRegion
	} else {
		exists := s.country.IsTariffRegionSupported(req.Region)
		if !exists {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorCountryRegionNotExists
			return nil
		}
	}

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currenciespb.EmptyRequest{})
	if err != nil {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PsFixedFeeCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.MethodFixAmountCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelMerchantCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelMccCode
		return nil
	}

	req.IsActive = true

	if req.Id != "" {
		val, err := s.paymentChannelCostMerchant.GetById(ctx, req.Id)
		if err != nil {
			res.Status = billingpb.ResponseStatusSystemError
			res.Message = errorPaymentChannelMerchantSetFailed
			return nil
		}
		req.Id = val.Id
		req.MerchantId = val.MerchantId
		req.CreatedAt = val.CreatedAt
		err = s.paymentChannelCostMerchant.Update(ctx, req)
	} else {
		req.Id = primitive.NewObjectID().Hex()
		err = s.paymentChannelCostMerchant.Insert(ctx, req)
	}
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = billingpb.ResponseStatusBadData
			res.Message = errorPaymentChannelMerchantCostAlreadyExist
		}

		return nil
	}

	if err := s.merchantRepository.UpdateTariffs(ctx, merchant.Id, req); err != nil {
		zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMerchantTariffUpdate
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeletePaymentChannelCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentCostDeleteRequest,
	res *billingpb.ResponseError,
) error {
	pc, err := s.paymentChannelCostMerchant.GetById(ctx, req.Id)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.paymentChannelCostMerchant.Delete(ctx, pc)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelMerchantDelete
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	return nil
}

func (s *Service) getPaymentChannelCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentChannelCostMerchantRequest,
) (*billingpb.PaymentChannelCostMerchant, error) {
	val, err := s.paymentChannelCostMerchant.Get(
		ctx,
		req.MerchantId,
		req.Name,
		req.PayoutCurrency,
		req.Region,
		req.Country,
		req.MccCode,
	)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorCostMatchedNotFound
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

func (h *PaymentChannelCostMerchant) Insert(ctx context.Context, obj *billingpb.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.ToPrecise(obj.MethodPercent)
	obj.PsPercent = tools.ToPrecise(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	_, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).InsertOne(ctx, obj)

	if err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h PaymentChannelCostMerchant) MultipleInsert(ctx context.Context, obj []*billingpb.PaymentChannelCostMerchant) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		if v.Id == "" {
			v.Id = primitive.NewObjectID().Hex()
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

	_, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).InsertMany(ctx, c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostMerchant),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
		return err
	}

	for _, v := range obj {
		if err := h.updateCaches(v); err != nil {
			return err
		}
	}

	return nil
}

func (h PaymentChannelCostMerchant) Update(ctx context.Context, obj *billingpb.PaymentChannelCostMerchant) error {
	obj.MinAmount = tools.FormatAmount(obj.MinAmount)
	obj.MethodFixAmount = tools.FormatAmount(obj.MethodFixAmount)
	obj.MethodPercent = tools.ToPrecise(obj.MethodPercent)
	obj.PsPercent = tools.ToPrecise(obj.PsPercent)
	obj.PsFixedFee = tools.FormatAmount(obj.PsFixedFee)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h PaymentChannelCostMerchant) GetById(ctx context.Context, id string) (*billingpb.PaymentChannelCostMerchant, error) {
	var c billingpb.PaymentChannelCostMerchant
	key := fmt.Sprintf(cachePaymentChannelCostMerchantKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "is_active": true}
	err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostMerchant) Get(
	ctx context.Context,
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

	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	matchQuery := bson.M{
		"merchant_id":     merchantOid,
		"name":            primitive.Regex{Pattern: "^" + name + "$", Options: "i"},
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

	cursor, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostMerchant),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
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

func (h PaymentChannelCostMerchant) Delete(ctx context.Context, obj *billingpb.PaymentChannelCostMerchant) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h PaymentChannelCostMerchant) GetAllForMerchant(
	ctx context.Context,
	merchantId string,
) (*billingpb.PaymentChannelCostMerchantList, error) {
	c := new(billingpb.PaymentChannelCostMerchantList)
	key := fmt.Sprintf(cachePaymentChannelCostMerchantAll, merchantId)
	err := h.svc.cacher.Get(key, c)

	if err == nil {
		return c, nil
	}

	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": merchantOid, "is_active": true}
	opts := options.Find().
		SetSort(bson.M{"name": 1, "payout_currency": 1, "region": 1, "country": 1, "mcc_code": 1})
	cursor, err := h.svc.db.Collection(collectionPaymentChannelCostMerchant).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostMerchant),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostMerchant)
	}

	err = cursor.All(ctx, &c.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
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

func (h PaymentChannelCostMerchant) updateCaches(obj *billingpb.PaymentChannelCostMerchant) (err error) {
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
