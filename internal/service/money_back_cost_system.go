package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"sort"
)

const (
	cacheMoneyBackCostSystemKey   = "pucs:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d:mcc:%s:oc:%s"
	cacheMoneyBackCostSystemKeyId = "pucs:id:%s"
	cacheMoneyBackCostSystemAll   = "pucs:all"

	collectionMoneyBackCostSystem = "money_back_cost_system"

	errorDaysMatchedNotFound = "days matched not found"
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

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currencies.EmptyRequest{})
	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemMccCode
		return nil
	}
	if !s.operatingCompany.Exists(req.OperatingCompanyId) {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorMoneybackSystemOperatingCompanyNotExists
		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostSystem.GetById(req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorMoneybackSystemSetFailed
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

		if mgo.IsDup(err) {
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
	pc, err := s.moneyBackCostSystem.GetById(req.Id)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.moneyBackCostSystem.Delete(pc)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorMoneybackSystemDelete
		return nil
	}
	res.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostSystem(req *billing.MoneyBackCostSystemRequest) (*billing.MoneyBackCostSystem, error) {
	val, err := s.moneyBackCostSystem.Get(req.Name, req.PayoutCurrency, req.UndoReason, req.Region, req.Country, req.PaymentStage, req.MccCode, req.OperatingCompanyId)
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
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.CreatedAt = ptypes.TimestampNow()
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true

	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).Insert(obj); err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionMoneyBackCostSystem),
			zap.Any("query", obj),
		)

		return err
	}

	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage, obj.MccCode, obj.OperatingCompanyId)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)

		return err
	}

	key = fmt.Sprintf(cacheMoneyBackCostSystemKeyId, obj.Id)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)

		return err
	}

	if err := h.svc.cacher.Delete(cacheMoneyBackCostSystemAll); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, key),
		)

		return err
	}

	return nil
}

func (h MoneyBackCostSystem) MultipleInsert(obj []*billing.MoneyBackCostSystem) error {
	c := make([]interface{}, len(obj))
	for i, v := range obj {
		v.FixAmount = tools.FormatAmount(v.FixAmount)
		v.Percent = tools.ToPrecise(v.Percent)
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
	obj.Percent = tools.ToPrecise(obj.Percent)
	obj.UpdatedAt = ptypes.TimestampNow()
	obj.IsActive = true
	if err := h.svc.db.Collection(collectionMoneyBackCostSystem).UpdateId(bson.ObjectIdHex(obj.Id), obj); err != nil {
		return err
	}
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage, obj.MccCode, obj.OperatingCompanyId)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)
		return err
	}
	key = fmt.Sprintf(cacheMoneyBackCostSystemKeyId, obj.Id)
	if err := h.svc.cacher.Set(key, obj, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, obj),
		)

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
	mcc_code string,
	operating_company_id string,
) (*moneyBackCostSystems, error) {
	var c moneyBackCostSystems
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, name, payout_currency, undo_reason, region, country, payment_stage, mcc_code, operating_company_id)

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
	key := fmt.Sprintf(cacheMoneyBackCostSystemKey, obj.Name, obj.PayoutCurrency, obj.UndoReason, obj.Region, obj.Country, obj.PaymentStage, obj.MccCode, obj.OperatingCompanyId)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}
	key = fmt.Sprintf(cacheMoneyBackCostSystemKeyId, obj.Id)
	if err := h.svc.cacher.Delete(key); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, key),
		)
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
