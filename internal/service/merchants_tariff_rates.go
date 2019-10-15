package service

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	collectionMerchantsTariffRates     = "merchants_tariff_rates"
	onboardingTariffRatesCacheKeyGetBy = "onboarding_tariff_rates:%x"
)

type TariffRates struct {
	Items []*billing.MerchantTariffRatesPayment `json:"items"`
}

type MerchantTariffRatesInterface interface {
	GetBy(*grpc.GetMerchantTariffRatesRequest) ([]*billing.MerchantTariffRatesPayment, error)
	GetCacheKeyForGetBy(*grpc.GetMerchantTariffRatesRequest) (string, error)
}

func newMerchantsTariffRatesRepository(s *Service) *MerchantsTariffRatesRepository {
	return &MerchantsTariffRatesRepository{svc: s}
}

func (h *MerchantsTariffRatesRepository) GetBy(
	in *grpc.GetMerchantTariffRatesRequest,
) ([]*billing.MerchantTariffRatesPayment, error) {
	var item *TariffRates
	key, err := h.GetCacheKeyForGetBy(in)

	if err != nil {
		return nil, err
	}

	err = h.svc.cacher.Get(key, &item)

	if err == nil {
		return item.Items, nil
	}

	query := bson.M{"merchant_home_region": in.HomeRegion}

	if in.PayerRegion != "" {
		query["payer_regions"] = bson.M{"$in": []string{in.HomeRegion}}
	}

	if in.MinAmount >= 0 && in.MaxAmount > in.MinAmount {
		query["min_amount"] = in.MinAmount
		query["max_amount"] = in.MaxAmount
	}

	err = h.svc.db.Collection(collectionMerchantsTariffRates).Find(query).All(&item.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantsTariffRates),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, fmt.Errorf(errorNotFound, collectionMerchantsTariffRates)
	}

	err = h.svc.cacher.Set(key, item, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, item),
		)

		return nil, fmt.Errorf(errorNotFound, collectionMerchantsTariffRates)
	}

	return item.Items, nil
}

func (h *MerchantsTariffRatesRepository) GetCacheKeyForGetBy(req *grpc.GetMerchantTariffRatesRequest) (string, error) {
	b, err := json.Marshal(req)

	if err != nil {
		zap.L().Error(
			"Marshaling failed",
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, req),
		)

		return "", err
	}

	return fmt.Sprintf(onboardingTariffRatesCacheKeyGetBy, md5.Sum(b)), nil
}
