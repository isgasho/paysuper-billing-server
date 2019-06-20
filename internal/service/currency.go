package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	cacheCurrencyA3  = "currency:code_a3:%s"
	cacheCurrencyInt = "currency:code_int:%d"
	cacheCurrencyAll = "currency:all"

	collectionCurrency = "currency"
)

func (s *Service) GetCurrencyList(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *billing.CurrencyList,
) error {
	currency, err := s.currency.GetAll()
	if err != nil {
		zap.S().Errorf("Query to get all currency failed", "err", err.Error(), "data", req)
		return err
	}

	res.Currency = currency

	return nil
}

func (s *Service) GetCurrency(
	ctx context.Context,
	req *billing.GetCurrencyRequest,
	res *billing.Currency,
) error {
	var err error

	if req.Int != 0 {
		res, err = s.currency.GetByCodeInt(int(req.Int))
	}

	if req.A3 != "" {
		res, err = s.currency.GetByCodeA3(req.A3)
	}

	if err != nil {
		zap.S().Errorf("Query to find currency failed", "err", err.Error(), "data", req)
		return err
	}

	return nil
}

type CurrencyServiceInterface interface {
	Insert(*billing.Currency) error
	MultipleInsert([]*billing.Currency) error
	GetByCodeA3(string) (*billing.Currency, error)
	GetByCodeInt(int) (*billing.Currency, error)
	GetAll() ([]*billing.Currency, error)
}

func newCurrencyService(svc *Service) *Currency {
	s := &Currency{svc: svc}
	return s
}

func (h *Currency) Insert(currency *billing.Currency) error {
	err := h.svc.db.Collection(collectionCurrency).Insert(currency)
	if err != nil {
		return err
	}

	if err := h.updateCache(currency); err != nil {
		return err
	}

	return nil
}

func (h Currency) MultipleInsert(currency []*billing.Currency) error {
	c := make([]interface{}, len(currency))
	for i, v := range currency {
		c[i] = v
	}

	err := h.svc.db.Collection(collectionCurrency).Insert(c...)
	if err != nil {
		return err
	}

	return nil
}

func (h Currency) GetByCodeA3(code string) (*billing.Currency, error) {
	var c billing.Currency
	key := fmt.Sprintf(cacheCurrencyA3, code)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionCurrency).
		Find(bson.M{"is_active": true, "code_a3": code}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionCurrency)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h Currency) GetByCodeInt(code int) (*billing.Currency, error) {
	var c billing.Currency
	key := fmt.Sprintf(cacheCurrencyInt, code)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionCurrency).
		Find(bson.M{"is_active": true, "code_int": code}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionCurrency)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h Currency) GetAll() ([]*billing.Currency, error) {
	var c []*billing.Currency

	if err := h.svc.cacher.Get(cacheCurrencyAll, c); err == nil {
		return c, nil
	}

	err := h.svc.db.Collection(collectionCurrency).Find(bson.M{"is_active": true}).All(&c)
	if err != nil {
		return nil, err
	}

	err = h.svc.cacher.Set(cacheCurrencyAll, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", cacheCurrencyAll, "data", c)
	}

	return c, nil
}

func (h *Currency) updateCache(currency *billing.Currency) error {
	err := h.svc.cacher.Set(fmt.Sprintf(cacheCurrencyA3, currency.CodeA3), currency, 0)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheCurrencyInt, currency.CodeInt), currency, 0)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Delete(cacheCurrencyAll)
	if err != nil {
		return err
	}

	return nil
}
