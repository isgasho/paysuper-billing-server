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
)

const (
	cachePaymentChannelCostSystemKey   = "pccs:n:%s:r:%s:c:%s:mcc:%s:oc:%s"
	cachePaymentChannelCostSystemKeyId = "pccs:id:%s"
	cachePaymentChannelCostSystemAll   = "pccs:all"

	collectionPaymentChannelCostSystem = "payment_channel_cost_system"
)

var (
	errorPaymentChannelSystemGetAll                    = newBillingServerErrorMsg("pcs000001", "can't get list of payment channel setting for system")
	errorPaymentChannelSystemGet                       = newBillingServerErrorMsg("pcs000002", "can't get payment channel setting for system")
	errorPaymentChannelSystemSetFailed                 = newBillingServerErrorMsg("pcs000003", "can't set payment channel setting for system")
	errorPaymentChannelSystemDelete                    = newBillingServerErrorMsg("pcs000004", "can't delete payment channel setting for system")
	errorPaymentChannelSystemCurrency                  = newBillingServerErrorMsg("pcs000005", "currency not supported")
	errorPaymentChannelSystemCostAlreadyExist          = newBillingServerErrorMsg("pcs000006", "cost with specified parameters already exist")
	errorPaymentChannelSystemMccCode                   = newBillingServerErrorMsg("pcs000007", "mcc code not supported")
	errorPaymentChannelSystemOperatingCompanyNotExists = newBillingServerErrorMsg("pcs000008", "operating company not exists")
)

type PaymentChannelCostSystemInterface interface {
	MultipleInsert(ctx context.Context, obj []*billingpb.PaymentChannelCostSystem) error
	Update(ctx context.Context, obj *billingpb.PaymentChannelCostSystem) error
	GetById(ctx context.Context, id string) (*billingpb.PaymentChannelCostSystem, error)
	Get(ctx context.Context, name, region, country, mccCode, operatingCompanyId string) (*billingpb.PaymentChannelCostSystem, error)
	Delete(ctx context.Context, obj *billingpb.PaymentChannelCostSystem) error
	GetAll(ctx context.Context) (*billingpb.PaymentChannelCostSystemList, error)
}

func (s *Service) GetAllPaymentChannelCostSystem(
	ctx context.Context,
	req *billingpb.EmptyRequest,
	res *billingpb.PaymentChannelCostSystemListResponse,
) error {
	val, err := s.paymentChannelCostSystem.GetAll(ctx)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemGetAll
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetPaymentChannelCostSystem(
	ctx context.Context,
	req *billingpb.PaymentChannelCostSystemRequest,
	res *billingpb.PaymentChannelCostSystemResponse,
) error {
	val, err := s.paymentChannelCostSystem.Get(ctx, req.Name, req.Region, req.Country, req.MccCode, req.OperatingCompanyId)

	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorPaymentChannelSystemGet
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetPaymentChannelCostSystem(
	ctx context.Context,
	req *billingpb.PaymentChannelCostSystem,
	res *billingpb.PaymentChannelCostSystemResponse,
) error {
	val, err := s.paymentChannelCostSystem.Get(
		ctx,
		req.Name,
		req.Region,
		req.Country,
		req.MccCode,
		req.OperatingCompanyId,
	)

	if err != nil && err.Error() != fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem) {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemSetFailed
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

	req.IsActive = true

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currenciespb.EmptyRequest{})
	if err != nil {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemMccCode
		return nil
	}
	if !s.operatingCompany.Exists(ctx, req.OperatingCompanyId) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemOperatingCompanyNotExists
		return nil
	}

	if val == nil {
		req.Id = primitive.NewObjectID().Hex()
		err = s.paymentChannelCostSystem.Insert(ctx, req)
	} else {
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.paymentChannelCostSystem.Update(ctx, req)
	}
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = billingpb.ResponseStatusBadData
			res.Message = errorPaymentChannelSystemCostAlreadyExist
		}

		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeletePaymentChannelCostSystem(
	ctx context.Context,
	req *billingpb.PaymentCostDeleteRequest,
	res *billingpb.ResponseError,
) error {
	pc, err := s.paymentChannelCostSystem.GetById(ctx, req.Id)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.paymentChannelCostSystem.Delete(ctx, pc)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemDelete
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	return nil
}

func newPaymentChannelCostSystemService(svc *Service) *PaymentChannelCostSystem {
	s := &PaymentChannelCostSystem{svc: svc}
	return s
}

func (h *PaymentChannelCostSystem) Insert(ctx context.Context, obj *billingpb.PaymentChannelCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	_, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).InsertOne(ctx, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h PaymentChannelCostSystem) MultipleInsert(ctx context.Context, obj []*billingpb.PaymentChannelCostSystem) error {
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

	_, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).InsertMany(ctx, c)

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

func (h PaymentChannelCostSystem) Update(ctx context.Context, obj *billingpb.PaymentChannelCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h PaymentChannelCostSystem) GetById(ctx context.Context, id string) (*billingpb.PaymentChannelCostSystem, error) {
	var c billingpb.PaymentChannelCostSystem
	key := fmt.Sprintf(cachePaymentChannelCostSystemKeyId, id)
	err := h.svc.cacher.Get(key, c)

	if err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "is_active": true}
	err = h.svc.db.Collection(collectionPaymentChannelCostSystem).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}

func (h PaymentChannelCostSystem) Get(
	ctx context.Context,
	name, region, country, mccCode, operatingCompanyId string,
) (*billingpb.PaymentChannelCostSystem, error) {
	var c *billingpb.PaymentChannelCostSystem
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, name, region, country, mccCode, operatingCompanyId)
	err := h.svc.cacher.Get(key, c)

	if err == nil {
		return c, nil
	}

	matchQuery := bson.M{
		"name":                 primitive.Regex{Pattern: "^" + name + "$", Options: "i"},
		"mcc_code":             mccCode,
		"operating_company_id": operatingCompanyId,
		"is_active":            true,
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
		{
			"$limit": 1,
		},
	}

	set := &internalPkg.PaymentChannelCostSystemSet{}
	cursor, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		err = cursor.Decode(&set)
		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostSystem),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
			return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
		}
	}

	if len(set.Set) == 0 {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	c = set.Set[0]

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

func (h PaymentChannelCostSystem) Delete(ctx context.Context, obj *billingpb.PaymentChannelCostSystem) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false

	oid, _ := primitive.ObjectIDFromHex(obj.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).ReplaceOne(ctx, filter, obj)

	if err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h PaymentChannelCostSystem) GetAll(ctx context.Context) (*billingpb.PaymentChannelCostSystemList, error) {
	var c = &billingpb.PaymentChannelCostSystemList{
		Items: []*billingpb.PaymentChannelCostSystem{},
	}
	key := cachePaymentChannelCostSystemAll
	err := h.svc.cacher.Get(key, c)

	if err == nil {
		return c, nil
	}

	query := bson.M{"is_active": true}
	opts := options.Find().SetSort(bson.M{"name": 1, "region": 1, "country": 1})
	cursor, err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostSystem),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	err = cursor.All(ctx, &c.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentChannelCostSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)

	return c, nil
}

func (h PaymentChannelCostSystem) updateCaches(obj *billingpb.PaymentChannelCostSystem) (err error) {
	groupKeys := []string{
		fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, obj.Country, obj.MccCode, obj.OperatingCompanyId),
		fmt.Sprintf(cachePaymentChannelCostSystemKey, obj.Name, obj.Region, "", obj.MccCode, obj.OperatingCompanyId),
		cachePaymentChannelCostSystemAll,
	}
	for _, key := range groupKeys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}

	keys := []string{
		fmt.Sprintf(cachePaymentChannelCostSystemKeyId, obj.Id),
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
