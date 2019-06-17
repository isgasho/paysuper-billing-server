package service

import (
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	cacheZipCodeByZipAndCountry = "zip_code:zip_country:%s_%s"
	collectionZipCode           = "zip_code"
)

func newZipCodeService(svc *Service) *ZipCode {
	return &ZipCode{svc: svc}
}

func (h *ZipCode) getByZipAndCountry(zip, country string) (*billing.ZipCode, error) {
	data := new(billing.ZipCode)
	key := fmt.Sprintf(cacheZipCodeByZipAndCountry, zip, country)

	err := h.svc.cacher.Get(key, data)

	if err == nil {
		return data, nil
	}

	query := bson.M{"zip": zip, "country": country}
	err = h.svc.db.Collection(collectionZipCode).Find(query).One(&data)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.L().Error(
				pkg.ErrorDatadaseQueryFailed,
				zap.Error(err),
				zap.String("collection", collectionZipCode),
				zap.Any("query", query),
			)
		}

		return nil, fmt.Errorf(errorNotFound, collectionZipCode)
	}

	err = h.svc.cacher.Set(key, data, 0)

	if err != nil {
		zap.L().Error("Save zip codes data to cache failed", zap.Error(err))
	}

	return data, nil
}
