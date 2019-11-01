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
	collectionMerchantsPaymentTariffs = "merchants_payment_tariffs"
	collectionMerchantTariffsSettings = "merchant_tariffs_settings"

	merchantTariffsSettingsKey         = "merchant_tariffs_settings:mcc:%s"
	onboardingTariffRatesCacheKeyGetBy = "onboarding_tariff_rates:%x"
)

type TariffRates struct {
	Items []*billing.MerchantTariffRatesPayment `json:"items"`
}

type MerchantTariffRatesInterface interface {
	GetPaymentTariffsBy(*grpc.GetMerchantTariffRatesRequest) ([]*billing.MerchantTariffRatesPayment, error)
	GetTariffsSettings(in *grpc.GetMerchantTariffRatesRequest) (*billing.MerchantTariffRatesSettings, error)
	GetBy(in *grpc.GetMerchantTariffRatesRequest) (*grpc.GetMerchantTariffRatesResponseItems, error)
	GetCacheKeyForGetBy(*grpc.GetMerchantTariffRatesRequest) (string, error)
}

func newMerchantsTariffRatesRepository(s *Service) *MerchantsTariffRatesRepository {
	return &MerchantsTariffRatesRepository{svc: s}
}

func (h *MerchantsTariffRatesRepository) GetBy(
	in *grpc.GetMerchantTariffRatesRequest,
) (*grpc.GetMerchantTariffRatesResponseItems, error) {
	payment, err := h.GetPaymentTariffsBy(in)

	if err != nil {
		return nil, err
	}

	settings, err := h.GetTariffsSettings(in)

	if err != nil {
		return nil, err
	}

	tariffs := &grpc.GetMerchantTariffRatesResponseItems{
		Payment:    payment,
		Refund:     settings.Refund,
		Chargeback: settings.Chargeback,
		Payout:     settings.Payout,
	}

	return tariffs, nil
}

func (h *MerchantsTariffRatesRepository) GetTariffsSettings(in *grpc.GetMerchantTariffRatesRequest) (*billing.MerchantTariffRatesSettings, error) {
	mccCode, err := getMccByOperationsType(in.MerchantOperationsType)
	if err != nil {
		return nil, err
	}

	item := new(billing.MerchantTariffRatesSettings)
	key := fmt.Sprintf(merchantTariffsSettingsKey, mccCode)
	err = h.svc.cacher.Get(key, &item)

	if err == nil {
		return item, nil
	}

	query := bson.M{}
	err = h.svc.db.Collection(collectionMerchantTariffsSettings).Find(query).One(item)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantTariffsSettings),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, merchantTariffsNotFound
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
	}

	return item, nil
}

func (h *MerchantsTariffRatesRepository) GetPaymentTariffsBy(
	in *grpc.GetMerchantTariffRatesRequest,
) ([]*billing.MerchantTariffRatesPayment, error) {
	item := new(TariffRates)
	key, err := h.GetCacheKeyForGetBy(in)

	if err != nil {
		return nil, err
	}

	err = h.svc.cacher.Get(key, &item)

	if err == nil {
		return item.Items, nil
	}

	mccCode, err := getMccByOperationsType(in.MerchantOperationsType)
	if err != nil {
		return nil, err
	}

	query := bson.M{"merchant_home_region": in.HomeRegion, "mcc_code": mccCode}

	if in.PayerRegion == "" {
		query["payer_region"] = in.HomeRegion
	} else {
		query["payer_region"] = in.PayerRegion
	}

	if in.MinAmount >= 0 && in.MaxAmount > in.MinAmount {
		query["min_amount"] = in.MinAmount
		query["max_amount"] = in.MaxAmount
	}

	err = h.svc.db.Collection(collectionMerchantsPaymentTariffs).Find(query).
		Sort("min_amount", "max_amount", "payer_region", "position").All(&item.Items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantsPaymentTariffs),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, merchantErrorUnknown
	}

	if len(item.Items) <= 0 {
		return nil, merchantTariffsNotFound
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

		return nil, fmt.Errorf(errorNotFound, collectionMerchantsPaymentTariffs)
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
