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
	MultipleInsert(obj []*billing.PaymentChannelCostSystem) error
	Update(obj *billing.PaymentChannelCostSystem) error
	GetById(id string) (*billing.PaymentChannelCostSystem, error)
	Get(name, region, country, mccCode, operatingCompanyId string) (*billing.PaymentChannelCostSystem, error)
	Delete(obj *billing.PaymentChannelCostSystem) error
	GetAll() (*billing.PaymentChannelCostSystemList, error)
}

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
	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country, req.MccCode, req.OperatingCompanyId)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
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

	val, err := s.paymentChannelCostSystem.Get(req.Name, req.Region, req.Country, req.MccCode, req.OperatingCompanyId)
	if err != nil && err.Error() != fmt.Sprintf(errorNotFound, collectionPaymentChannelCostSystem) {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemSetFailed
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

	req.IsActive = true

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currencies.EmptyRequest{})
	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemCurrency
		return nil
	}
	if !contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemCurrency
		return nil
	}
	if !contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemMccCode
		return nil
	}
	if !s.operatingCompany.Exists(req.OperatingCompanyId) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentChannelSystemOperatingCompanyNotExists
		return nil
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
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorPaymentChannelSystemSetFailed

		if mgo.IsDup(err) {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaymentChannelSystemCostAlreadyExist
		}

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
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
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
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Insert(obj); err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h PaymentChannelCostSystem) MultipleInsert(obj []*billing.PaymentChannelCostSystem) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		if v.Id == "" {
			v.Id = bson.NewObjectId().Hex()
		}
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.ToPrecise(v.Percent)
		v.CreatedAt = ptypes.TimestampNow()
		v.UpdatedAt = ptypes.TimestampNow()
		v.IsActive = true
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Insert(c...); err != nil {
		return err
	}

	for _, v := range obj {
		if err := h.updateCaches(v); err != nil {
			return err
		}
	}

	return nil
}

func (h PaymentChannelCostSystem) Update(obj *billing.PaymentChannelCostSystem) error {
	obj.FixAmount = tools.FormatAmount(obj.FixAmount)
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	return h.updateCaches(obj)
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

func (h PaymentChannelCostSystem) Get(name, region, country, mccCode, operatingCompanyId string) (*billing.PaymentChannelCostSystem, error) {
	var c *billing.PaymentChannelCostSystem
	key := fmt.Sprintf(cachePaymentChannelCostSystemKey, name, region, country, mccCode, operatingCompanyId)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	matchQuery := bson.M{
		"name":      name,
		"is_active": true,
		"$or": []bson.M{
			{
				"country": country,
				"region":  region,
			},
			{
				"$or": []bson.M{
					{"country": ""},
					{"country": bson.M{"exists": false}},
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

	var set = &internalPkg.PaymentChannelCostSystemSet{}

	err := h.svc.db.Collection(collectionPaymentChannelCostSystem).Pipe(query).One(&set)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostSystem),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
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

func (h PaymentChannelCostSystem) Delete(obj *billing.PaymentChannelCostSystem) error {
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = false
	if err := h.svc.db.Collection(collectionPaymentChannelCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	return h.updateCaches(obj)
}

func (h PaymentChannelCostSystem) GetAll() (*billing.PaymentChannelCostSystemList, error) {
	var c = &billing.PaymentChannelCostSystemList{
		Items: []*billing.PaymentChannelCostSystem{},
	}
	key := cachePaymentChannelCostSystemAll

	if err := h.svc.cacher.Get(key, c); err == nil {
		return c, nil
	}

	query := bson.M{
		"is_active": true,
	}

	err := h.svc.db.Collection(collectionPaymentChannelCostSystem).
		Find(query).
		Sort("name", "region", "country").
		All(&c.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPaymentChannelCostSystem),
			zap.Any("query", query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionPaymentChannelCostSystem)
	}

	_ = h.svc.cacher.Set(key, c, 0)

	return c, nil
}

func (h PaymentChannelCostSystem) updateCaches(obj *billing.PaymentChannelCostSystem) (err error) {
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

	for _, key := range keys {
		err = h.svc.cacher.Delete(key)
		if err != nil {
			return
		}
	}
	return
}
