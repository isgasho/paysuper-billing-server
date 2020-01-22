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
	cacheMoneyBackCostMerchantKey   = "pucm:m:%s:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d:mcc:%s"
	cacheMoneyBackCostMerchantKeyId = "pucm:id:%s"
	cacheMoneyBackCostMerchantAll   = "pucm:all:m:%s"

	collectionMoneyBackCostMerchant = "money_back_cost_merchant"
)

var (
	errorMoneybackMerchantGetAll              = newBillingServerErrorMsg("mbm000001", "can't get list of money back setting for merchant")
	errorMoneybackMerchantGet                 = newBillingServerErrorMsg("mbm000002", "can't get money back setting for merchant")
	errorMoneybackMerchantSetFailed           = newBillingServerErrorMsg("mbm000003", "can't set money back setting for merchant")
	errorMoneybackMerchantDelete              = newBillingServerErrorMsg("mbm000004", "can't delete money back setting for merchant")
	errorMoneybackMerchantCurrency            = newBillingServerErrorMsg("mbm000005", "currency not supported")
	errorMoneybackMerchantCostAlreadyExist    = newBillingServerErrorMsg("mbm000006", "cost with specified parameters already exist")
	errorMoneybackMerchantMccCode             = newBillingServerErrorMsg("mbm000007", "mcc code not supported")
	errorMoneybackMerchantDaysMatchedNotFound = newBillingServerErrorMsg("mbm000008", "days matched not found")
	errorCostRateNotFound                     = newBillingServerErrorMsg("cr000001", "cost rate with specified identifier not found")
)

type MoneyBackCostMerchantInterface interface {
	MultipleInsert(obj []*billingpb.MoneyBackCostMerchant) error
	Update(obj *billingpb.MoneyBackCostMerchant) error
	Get(merchantId, name, payoutCurrency, undoReason, region, country, mccCode string, paymentStage int32) (*billingpb.MoneyBackCostMerchantList, error)
	GetById(id string) (*billingpb.MoneyBackCostMerchant, error)
	Delete(obj *billingpb.MoneyBackCostMerchant) error
	GetAllForMerchant(merchantId string) (*billingpb.MoneyBackCostMerchantList, error)
}

func (s *Service) GetAllMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantListRequest,
	res *billingpb.MoneyBackCostMerchantListResponse,
) error {
	val, err := s.moneyBackCostMerchant.GetAllForMerchant(ctx, req.MerchantId)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantGetAll
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantRequest,
	res *billingpb.MoneyBackCostMerchantResponse,
) error {
	val, err := s.getMoneyBackCostMerchant(ctx, req)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorMoneybackMerchantGet
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchant,
	res *billingpb.MoneyBackCostMerchantResponse,
) error {

	var err error

	if _, err := s.merchantRepository.GetById(ctx, req.MerchantId); err != nil {
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
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantMccCode
		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostMerchant.GetById(ctx, req.Id)
		if err != nil {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorMoneybackMerchantSetFailed
			return nil
		}
		req.Id = val.Id
		req.MerchantId = val.MerchantId
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostMerchant.Update(ctx, req)
	} else {
		req.Id = primitive.NewObjectID().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostMerchant.Insert(ctx, req)
	}

	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = billingpb.ResponseStatusBadData
			res.Message = errorMoneybackMerchantCostAlreadyExist
		}

		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeleteMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentCostDeleteRequest,
	res *billingpb.ResponseError,
) error {
	pc, err := s.moneyBackCostMerchant.GetById(ctx, req.Id)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.moneyBackCostMerchant.Delete(ctx, pc)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantDelete
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantRequest,
) (*billingpb.MoneyBackCostMerchant, error) {
	val, err := s.moneyBackCostMerchant.Get(
		ctx,
		req.MerchantId,
		req.Name,
		req.PayoutCurrency,
		req.UndoReason,
		req.Region,
		req.Country,
		req.MccCode,
		req.PaymentStage,
	)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorMoneybackMerchantDaysMatchedNotFound
	}

	var matched []*kvIntInt
	for _, set := range val {
		for k, i := range set.Set {
			if req.Days >= i.DaysFrom {
				matched = append(matched, &kvIntInt{k, i.DaysFrom})
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

	return nil, errorMoneybackMerchantDaysMatchedNotFound
}

func newMoneyBackCostMerchantService(svc *Service) *MoneyBackCostMerchant {
	s := &MoneyBackCostMerchant{svc: svc}
	return s
}

func (h *MoneyBackCostMerchant) Insert(ctx context.Context, obj *billingpb.MoneyBackCostMerchant) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.IsActive = true

	_, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).InsertOne(ctx, obj)

	if err != nil {
		return err
	}

	return h.updateCaches(obj)
}

func (h MoneyBackCostMerchant) MultipleInsert(ctx context.Context, obj []*billingpb.MoneyBackCostMerchant) error {
	c := make([]interface{}, len(obj))

	for i, v := range obj {
		if v.Id == "" {
			v.Id = primitive.NewObjectID().Hex()
		}
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.ToPrecise(v.Percent)
		v.IsActive = true
		c[i] = v
	}

	_, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).InsertMany(ctx, c)

	if err != nil {
		return err
	}

	for _, v := range obj {
		if err := h.updateCaches(v); err != nil {
			return err
		}
	}

	return nil
}

func (h MoneyBackCostMerchant) Update(ctx context.Context, obj *billingpb.MoneyBackCostMerchant) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.IsActive = true

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h MoneyBackCostMerchant) Get(
	ctx context.Context,
	merchantId string,
	name string,
	payoutCurrency string,
	undoReason string,
	region string,
	country string,
	mccCode string,
	paymentStage int32,
) (c []*internalPkg.MoneyBackCostMerchantSet, err error) {
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKey, merchantId, name, payoutCurrency, undoReason, region, country, paymentStage, mccCode)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	matchQuery := bson.M{
		"merchant_id":     merchantOid,
		"name":            primitive.Regex{Pattern: "^" + name + "$", Options: "i"},
		"payout_currency": payoutCurrency,
		"undo_reason":     undoReason,
		"payment_stage":   paymentStage,
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

	cursor, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
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
		return nil, err
	}
	return c, nil
}

func (h MoneyBackCostMerchant) GetById(ctx context.Context, id string) (*billingpb.MoneyBackCostMerchant, error) {
	var c billingpb.MoneyBackCostMerchant
	key := fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "is_active": true}
	err := h.svc.db.Collection(collectionMoneyBackCostMerchant).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostMerchant) Delete(ctx context.Context, obj *billingpb.MoneyBackCostMerchant) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).ReplaceOne(ctx, filter, obj)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)
		return err
	}
	return h.updateCaches(obj)
}

func (h MoneyBackCostMerchant) GetAllForMerchant(
	ctx context.Context,
	merchantId string,
) (*billingpb.MoneyBackCostMerchantList, error) {
	item := new(billingpb.MoneyBackCostMerchantList)
	key := fmt.Sprintf(cacheMoneyBackCostMerchantAll, merchantId)

	if err := h.svc.cacher.Get(key, item); err == nil {
		return item, nil
	}

	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": oid, "is_active": true}
	opts := options.Find().
		SetSort(bson.M{"name": 1, "payout_currency": 1, "region": 1, "country": 1, "mcc_code": 1})
	cursor, err := h.svc.db.Collection(collectionMoneyBackCostMerchant).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionMoneyBackCostMerchant),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostMerchant)
	}

	err = cursor.All(ctx, &item.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = h.svc.cacher.Set(key, item, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, item),
		)
	}

	return item, nil
}

func (h MoneyBackCostMerchant) updateCaches(obj *billingpb.MoneyBackCostMerchant) (err error) {
	groupKeys := []string{
		fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage, obj.MccCode),
		fmt.Sprintf(cacheMoneyBackCostMerchantKey, obj.MerchantId, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, "", obj.PaymentStage, obj.MccCode),
		fmt.Sprintf(cacheMoneyBackCostMerchantAll, obj.MerchantId),
	}

	for _, key := range groupKeys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}

	keys := []string{
		fmt.Sprintf(cacheMoneyBackCostMerchantKeyId, obj.Id),
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
