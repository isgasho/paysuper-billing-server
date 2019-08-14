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

type MerchantTariffRatesInterface interface {
	GetBy(*grpc.GetMerchantTariffRatesRequest) (*billing.MerchantTariffRates, error)
	GetCacheKeyForGetBy(*grpc.GetMerchantTariffRatesRequest) (string, error)
}

type MerchantsTariffRates struct {
	Payments []struct {
		Payment []*billing.MerchantTariffRatesPayments `bson:"payment"`
	} `bson:"payments"`
	MoneyBack []struct {
		MoneyBack []*billing.MerchantTariffRatesMoneyBack `bson:"money_back"`
	} `bson:"money_back"`
	Payout []struct {
		Payout *billing.TariffRatesItem `bson:"payout"`
	} `bson:"payout"`
	Chargeback []struct {
		Chargeback *billing.TariffRatesItem `bson:"chargeback"`
	} `bson:"chargeback"`
}

func newMerchantsTariffRatesRepository(s *Service) *MerchantsTariffRatesRepository {
	return &MerchantsTariffRatesRepository{svc: s}
}

func (h *MerchantsTariffRatesRepository) GetBy(
	in *grpc.GetMerchantTariffRatesRequest,
) (*billing.MerchantTariffRates, error) {
	var item *billing.MerchantTariffRates
	key, err := h.GetCacheKeyForGetBy(in)

	if err != nil {
		return nil, err
	}

	err = h.svc.cacher.Get(key, &item)

	if err == nil {
		return item, nil
	}

	var mtr *MerchantsTariffRates
	query := []bson.M{
		{"$match": bson.M{"region": in.Region}},
		{
			"$facet": bson.M{
				"payments": []bson.M{
					{
						"$project": bson.M{
							"payment": bson.M{
								"$filter": bson.M{
									"input": "$payment",
									"as":    "item",
									"cond": bson.M{
										"$and": []bson.M{
											{"$eq": []interface{}{"$$item.payout_currency", in.PayoutCurrency}},
											{"$eq": []interface{}{"$$item.amount_range.from", in.AmountFrom}},
											{"$eq": []interface{}{"$$item.amount_range.to", in.AmountTo}},
										},
									},
								},
							},
						},
					},
				},
				"money_back": []bson.M{
					{
						"$project": bson.M{
							"money_back": bson.M{
								"$filter": bson.M{
									"input": "$money_back",
									"as":    "item",
									"cond":  []bson.M{{"$eq": []interface{}{"$$item.payout_currency", in.PayoutCurrency}}},
								},
							},
						},
					},
				},
				"payout":     []bson.M{{"$project": bson.M{"payout": true}}},
				"chargeback": []bson.M{{"$project": bson.M{"chargeback": true}}},
			},
		},
	}

	err = h.svc.db.Collection(collectionMerchantsTariffRates).Pipe(query).One(&mtr)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantsTariffRates),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, fmt.Errorf(errorNotFound, collectionMerchantsTariffRates)
	}

	item = h.transformToBillingMerchantTariffRates(mtr)
	item.Region = in.Region
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

	return item, nil
}

func (h *MerchantsTariffRatesRepository) transformToBillingMerchantTariffRates(
	in *MerchantsTariffRates,
) *billing.MerchantTariffRates {
	result := &billing.MerchantTariffRates{}

	if len(in.Payments) > 0 {
		result.Payment = in.Payments[0].Payment
	}

	if len(in.MoneyBack) > 0 {
		result.MoneyBack = in.MoneyBack[0].MoneyBack
	}

	if len(in.Payout) > 0 {
		result.Payout = in.Payout[0].Payout
	}

	if len(in.Chargeback) > 0 {
		result.Chargeback = in.Chargeback[0].Chargeback
	}

	return result
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
