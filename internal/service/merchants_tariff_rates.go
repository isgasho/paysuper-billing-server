package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	collectionMerchantsPaymentTariffs = "merchants_payment_tariffs"
	collectionMerchantTariffsSettings = "merchant_tariffs_settings"

	merchantTariffsSettingsKey         = "merchant_tariffs_settings:mcc:%s"
	onboardingTariffRatesCacheKeyGetBy = "onboarding_tariff_rates:%x"
)

type TariffRates struct {
	Items []*billingpb.MerchantTariffRatesPayment `json:"items"`
}

type MerchantTariffRatesInterface interface {
	GetPaymentTariffsBy(context.Context, *billingpb.GetMerchantTariffRatesRequest) ([]*billingpb.MerchantTariffRatesPayment, error)
	GetTariffsSettings(ctx context.Context, in *billingpb.GetMerchantTariffRatesRequest) (*billingpb.MerchantTariffRatesSettings, error)
	GetBy(ctx context.Context, in *billingpb.GetMerchantTariffRatesRequest) (*billingpb.GetMerchantTariffRatesResponseItems, error)
	GetCacheKeyForGetBy(*billingpb.GetMerchantTariffRatesRequest) (string, error)
}

func newMerchantsTariffRatesRepository(s *Service) *MerchantsTariffRatesRepository {
	return &MerchantsTariffRatesRepository{svc: s}
}

func (h *MerchantsTariffRatesRepository) GetBy(
	ctx context.Context,
	in *billingpb.GetMerchantTariffRatesRequest,
) (*billingpb.GetMerchantTariffRatesResponseItems, error) {
	payment, err := h.GetPaymentTariffsBy(ctx, in)

	if err != nil {
		return nil, err
	}

	settings, err := h.GetTariffsSettings(ctx, in)

	if err != nil {
		return nil, err
	}

	tariffs := &billingpb.GetMerchantTariffRatesResponseItems{
		Payment:       payment,
		Refund:        settings.Refund,
		Chargeback:    settings.Chargeback,
		Payout:        settings.Payout,
		MinimalPayout: settings.MinimalPayout,
		MccCode:       settings.MccCode,
	}

	return tariffs, nil
}

func (h *MerchantsTariffRatesRepository) GetTariffsSettings(
	ctx context.Context,
	in *billingpb.GetMerchantTariffRatesRequest,
) (*billingpb.MerchantTariffRatesSettings, error) {
	mccCode, err := getMccByOperationsType(in.MerchantOperationsType)
	if err != nil {
		return nil, err
	}

	item := new(billingpb.MerchantTariffRatesSettings)
	key := fmt.Sprintf(merchantTariffsSettingsKey, mccCode)
	err = h.svc.cacher.Get(key, &item)

	if err == nil {
		return item, nil
	}

	query := bson.M{"mcc_code": mccCode}
	err = h.svc.db.Collection(collectionMerchantTariffsSettings).FindOne(ctx, query).Decode(item)

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
	ctx context.Context,
	in *billingpb.GetMerchantTariffRatesRequest,
) ([]*billingpb.MerchantTariffRatesPayment, error) {
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

	query := bson.M{"merchant_home_region": in.HomeRegion, "mcc_code": mccCode, "is_active": true}

	if in.PayerRegion != "" {
		query["payer_region"] = in.PayerRegion
	}

	if in.MinAmount >= 0 && in.MaxAmount > in.MinAmount {
		query["min_amount"] = in.MinAmount
		query["max_amount"] = in.MaxAmount
	}

	opts := options.Find().
		SetSort(bson.M{"min_amount": 1, "max_amount": 1, "payer_region": 1, "position": 1})
	cursor, err := h.svc.db.Collection(collectionMerchantsPaymentTariffs).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantsPaymentTariffs),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, merchantErrorUnknown
	}

	err = cursor.All(ctx, &item.Items)
	_ = cursor.Close(ctx)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
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

func (h *MerchantsTariffRatesRepository) GetCacheKeyForGetBy(req *billingpb.GetMerchantTariffRatesRequest) (string, error) {
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
