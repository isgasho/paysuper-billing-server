package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"sort"
)

const (
	cacheMoneyBackCostSystemKey   = "pucs:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d:mcc:%s:oc:%s"
	cacheMoneyBackCostSystemKeyId = "pucs:id:%s"
	cacheMoneyBackCostSystemAll   = "pucs:all"

	collectionMoneyBackCostSystem = "money_back_cost_system"
)

var (
	errorMoneybackSystemGetAll                    = newBillingServerErrorMsg("mbs000001", "can't get list of money back setting for system")
	errorMoneybackSystemGet                       = newBillingServerErrorMsg("mbs000002", "can't get money back setting for system")
	errorMoneybackSystemSetFailed                 = newBillingServerErrorMsg("mbs000003", "can't set money back setting for system")
	errorMoneybackSystemDelete                    = newBillingServerErrorMsg("mbs000004", "can't delete money back setting for system")
	errorMoneybackSystemCurrency                  = newBillingServerErrorMsg("mbs000005", "currency not supported")
	errorMoneybackCostAlreadyExist                = newBillingServerErrorMsg("mbs000006", "cost with specified parameters already exist")
	errorMoneybackSystemMccCode                   = newBillingServerErrorMsg("mbs000007", "mcc code not supported")
	errorMoneybackSystemOperatingCompanyNotExists = newBillingServerErrorMsg("mbs000008", "operating company not exists")
	errorMoneybackSystemDaysMatchedNotFound       = newBillingServerErrorMsg("mbs000009", "days matched not found")
)

type MoneyBackCostSystemInterface interface {
	MultipleInsert(obj []*billing.MoneyBackCostSystem) error
	Update(obj *billing.MoneyBackCostSystem) error
	Get(name, payoutCurrency, undoReason, region, country, mccCode, operatingCompanyId string, paymentStage int32) (c []*internalPkg.MoneyBackCostSystemSet, err error)
	GetById(id string) (*billing.MoneyBackCostSystem, error)
	Delete(obj *billing.MoneyBackCostSystem) error
	GetAll() (*billing.MoneyBackCostSystemList, error)
}

func (s *Service) GetAllMoneyBackCostSystem(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.MoneyBackCostSystemListResponse,
) error {
	val, err := s.moneyBackCostSystem.GetAll(ctx)
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
	val, err := s.getMoneyBackCostSystem(ctx, req)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
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
		country, err := s.country.GetByIsoCodeA2(ctx, req.Country)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorCountryNotFound
			return nil
		}
		req.Region = country.PayerTariffRegion
	} else {
		exists := s.country.IsTariffRegionSupported(req.Region)
		if !exists {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorCountryRegionNotExists
			return nil
		}
	}

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currencies.EmptyRequest{})
	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemMccCode
		return nil
	}
	if !s.operatingCompany.Exists(ctx, req.OperatingCompanyId) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemOperatingCompanyNotExists
		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostSystem.GetById(ctx, req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorMoneybackSystemSetFailed
			return nil
		}
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostSystem.Update(ctx, req)
	} else {
		req.Id = primitive.NewObjectID().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostSystem.Insert(ctx, req)
	}
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorMoneybackCostAlreadyExist
		}

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
	pc, err := s.moneyBackCostSystem.GetById(ctx, req.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.moneyBackCostSystem.Delete(ctx, pc)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemDelete
		return nil
	}
	res.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostSystem(
	ctx context.Context,
	req *billing.MoneyBackCostSystemRequest,
) (*billing.MoneyBackCostSystem, error) {
	val, err := s.moneyBackCostSystem.Get(
		ctx,
		req.Name,
		req.PayoutCurrency,
		req.UndoReason,
		req.Region,
		req.Country,
		req.MccCode,
		req.OperatingCompanyId,
		req.PaymentStage,
	)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorMoneybackSystemDaysMatchedNotFound
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

	return nil, errorMoneybackSystemDaysMatchedNotFound
}

func newMoneyBackCostSystemService(svc *Service) *MoneyBackCostSystem {
	s := &MoneyBackCostSystem{svc: svc}
	return s
}

func (h *MoneyBackCostSystem) Insert(ctx context.Context, obj *billing.MoneyBackCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	_, err := h.svc.db.Collection(collectionMoneyBackCostSystem).InsertOne(ctx, obj)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)

		return err
	}

	return h.updateCaches(obj)
}

func (h MoneyBackCostSystem) MultipleInsert(ctx context.Context, obj []*billing.MoneyBackCostSystem) error {
	c := make([]interface{}, len(obj))

	for i, v := range obj {
		if v.Id == "" {
			v.Id = primitive.NewObjectID().Hex()
		}

		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.ToPrecise(v.Percent)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v
	}

	_, err := h.svc.db.Collection(collectionMoneyBackCostSystem).InsertMany(ctx, c)

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

func (h MoneyBackCostSystem) Update(ctx context.Context, obj *billing.MoneyBackCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMoneyBackCostSystem).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h MoneyBackCostSystem) Get(
	ctx context.Context,
	name string,
	payoutCurrency string,
	undoReason string,
	region string,
	country string,
	mccCode string,
	operatingCompanyId string,
	paymentStage int32,
) (c []*internalPkg.MoneyBackCostSystemSet, err error) {
	key := fmt.Sprintf(
		cacheMoneyBackCostSystemKey,
		name,
		payoutCurrency,
		undoReason,
		region,
		country,
		paymentStage,
		mccCode,
		operatingCompanyId,
	)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	matchQuery := bson.M{
		"name":                 primitive.Regex{Pattern: "^" + name + "$", Options: "i"},
		"payout_currency":      payoutCurrency,
		"undo_reason":          undoReason,
		"payment_stage":        paymentStage,
		"is_active":            true,
		"mcc_code":             mccCode,
		"operating_company_id": operatingCompanyId,
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

	cursor, err := h.svc.db.Collection(collectionMoneyBackCostSystem).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionMoneyBackCostSystem),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostSystem)
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMoneyBackCostSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostSystem)
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

func (h MoneyBackCostSystem) GetById(ctx context.Context, id string) (*billing.MoneyBackCostSystem, error) {
	var c billing.MoneyBackCostSystem
	key := fmt.Sprintf(cacheMoneyBackCostSystemKeyId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "is_active": true}
	err := h.svc.db.Collection(collectionMoneyBackCostSystem).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionMoneyBackCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h MoneyBackCostSystem) Delete(ctx context.Context, obj *billing.MoneyBackCostSystem) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMoneyBackCostSystem).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h MoneyBackCostSystem) GetAll(ctx context.Context) (*billing.MoneyBackCostSystemList, error) {
	var c = &billing.MoneyBackCostSystemList{}
	key := cacheMoneyBackCostSystemAll
	err := h.svc.cacher.Get(key, c)

	if err != nil {
		filter := bson.M{"is_active": true}
		opts := options.Find().
			SetSort(bson.M{"name": 1, "payout_currency": 1, "undo_reason": 1, "region": 1, "country": 1, "payment_stage": 1})
		cursor, err := h.svc.db.Collection(collectionMoneyBackCostSystem).Find(ctx, filter, opts)

		if err != nil {
			return nil, err
		}

		err = cursor.All(ctx, &c.Items)

		if err != nil {
			return nil, err
		}

		_ = h.svc.cacher.Set(key, c, 0)
	}

	return c, nil
}

func (h MoneyBackCostSystem) updateCaches(obj *billing.MoneyBackCostSystem) (err error) {
	groupKeys := []string{
		fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage, obj.MccCode, obj.OperatingCompanyId),
		fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, "", obj.PaymentStage, obj.MccCode, obj.OperatingCompanyId),
		cacheMoneyBackCostSystemAll,
	}
	for _, key := range groupKeys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}

	keys := []string{
		fmt.Sprintf(cacheMoneyBackCostSystemKeyId, obj.Id),
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
