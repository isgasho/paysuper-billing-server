package mock

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/micro/go-micro/client"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"math"
)

const (
	SomeError          = "some error"
	precision          = 10
	eurPriceinRub      = float64(72)
	eurPriceInRubCb    = float64(72.5)
	eurPriceInRubStock = float64(71)
	usdPriceInRub      = float64(65)
	usdPriceInRubCb    = float64(65.5)
	usdPriceInRubStock = float64(64)
)

func toPrecise(val float64) float64 {
	p := math.Pow(10, precision)
	return math.Round(val*p) / p
}

var (
	MerchantIdMock = bson.NewObjectId().Hex()
)

type CurrencyServiceMockOk struct{}
type CurrencyServiceMockError struct{}

func NewCurrencyServiceMockOk() currencies.CurrencyratesService {
	return &CurrencyServiceMockOk{}
}

func NewCurrencyServiceMockError() currencies.CurrencyratesService {
	return &CurrencyServiceMockError{}
}

func (s *CurrencyServiceMockOk) GetRateCurrentCommon(
	ctx context.Context,
	in *currencies.GetRateCurrentCommonRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return &currencies.RateData{}, nil
}

func (s *CurrencyServiceMockOk) GetRateByDateCommon(
	ctx context.Context,
	in *currencies.GetRateByDateCommonRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return &currencies.RateData{
		Id:        bson.NewObjectId().Hex(),
		CreatedAt: ptypes.TimestampNow(),
		Pair:      in.From + in.To,
		Rate:      3,
		Source:    bson.NewObjectId().Hex(),
		Volume:    3,
	}, nil
}

func (s *CurrencyServiceMockOk) GetRateCurrentForMerchant(
	ctx context.Context,
	in *currencies.GetRateCurrentForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return &currencies.RateData{
		Id:        bson.NewObjectId().Hex(),
		CreatedAt: ptypes.TimestampNow(),
		Pair:      in.From + in.To,
		Rate:      3,
		Source:    bson.NewObjectId().Hex(),
		Volume:    3,
	}, nil
}

func (s *CurrencyServiceMockOk) GetRateByDateForMerchant(
	ctx context.Context,
	in *currencies.GetRateByDateForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return &currencies.RateData{}, nil
}

func (s *CurrencyServiceMockOk) ExchangeCurrencyCurrentCommon(
	ctx context.Context,
	in *currencies.ExchangeCurrencyCurrentCommonRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	if in.From == "EUR" && in.To == "RUB" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * eurPriceinRub),
			ExchangeRate:    eurPriceinRub,
			Correction:      0,
			OriginalRate:    eurPriceinRub,
		}, nil
	}
	if in.From == "RUB" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount / eurPriceinRub),
			ExchangeRate:    toPrecise(1 / eurPriceinRub),
			Correction:      0,
			OriginalRate:    toPrecise(1 / eurPriceinRub),
		}, nil
	}

	if in.From == "USD" && in.To == "EUR" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * (usdPriceInRubStock / eurPriceInRubStock)),
				ExchangeRate:    toPrecise(usdPriceInRubStock / eurPriceInRubStock),
				Correction:      0,
				OriginalRate:    toPrecise(usdPriceInRubStock / eurPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * (usdPriceInRubCb / eurPriceInRubCb)),
				ExchangeRate:    toPrecise(usdPriceInRubCb / eurPriceInRubCb),
				Correction:      0,
				OriginalRate:    toPrecise(usdPriceInRubCb / eurPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * (usdPriceInRub / eurPriceinRub)),
			ExchangeRate:    toPrecise(usdPriceInRub / eurPriceinRub),
			Correction:      0,
			OriginalRate:    toPrecise(usdPriceInRub / eurPriceinRub),
		}, nil
	}
	if in.From == "EUR" && in.To == "USD" {

		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * (eurPriceInRubStock / usdPriceInRubStock)),
				ExchangeRate:    toPrecise(eurPriceInRubStock / usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    toPrecise(eurPriceInRubStock / usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * (eurPriceInRubCb / usdPriceInRubCb)),
				ExchangeRate:    toPrecise(eurPriceInRubCb / usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    toPrecise(eurPriceInRubCb / usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * (eurPriceinRub / usdPriceInRub)),
			ExchangeRate:    toPrecise(eurPriceinRub / usdPriceInRub),
			Correction:      0,
			OriginalRate:    toPrecise(eurPriceinRub / usdPriceInRub),
		}, nil
	}
	if in.From == "USD" && in.To == "RUB" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * usdPriceInRubStock),
				ExchangeRate:    toPrecise(usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    toPrecise(usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * usdPriceInRubCb),
				ExchangeRate:    toPrecise(usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    toPrecise(usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * usdPriceInRub),
			ExchangeRate:    usdPriceInRub,
			Correction:      0,
			OriginalRate:    usdPriceInRub,
		}, nil
	}
	if in.From == "RUB" && in.To == "USD" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount / usdPriceInRubStock),
				ExchangeRate:    toPrecise(1 / usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    toPrecise(1 / usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			a := in.Amount / usdPriceInRubCb
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(a),
				ExchangeRate:    toPrecise(1 / usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    toPrecise(1 / usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount / usdPriceInRub),
			ExchangeRate:    toPrecise(1 / usdPriceInRub),
			Correction:      0,
			OriginalRate:    toPrecise(1 / usdPriceInRub),
		}, nil
	}

	return &currencies.ExchangeCurrencyResponse{
		ExchangedAmount: 10,
		ExchangeRate:    0.25,
		Correction:      2,
		OriginalRate:    0.5,
	}, nil
}

func (s *CurrencyServiceMockOk) ExchangeCurrencyCurrentForMerchant(
	ctx context.Context,
	in *currencies.ExchangeCurrencyCurrentForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	if in.From == "EUR" && in.To == "RUB" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * eurPriceinRub * 1.02),
			ExchangeRate:    toPrecise(eurPriceinRub * 1.02),
			Correction:      0,
			OriginalRate:    toPrecise(eurPriceinRub * 1.02),
		}, nil
	}
	if in.From == "RUB" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount / eurPriceinRub * 1.02),
			ExchangeRate:    toPrecise((1 / eurPriceinRub) * 1.02),
			Correction:      0,
			OriginalRate:    toPrecise((1 / eurPriceinRub) * 1.02),
		}, nil
	}
	if in.From == "USD" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * (usdPriceInRub / eurPriceinRub) * 0.98),
			ExchangeRate:    toPrecise((usdPriceInRub / eurPriceinRub) * 0.98),
			Correction:      0,
			OriginalRate:    toPrecise((usdPriceInRub / eurPriceinRub) * 0.98),
		}, nil
	}
	if in.From == "EUR" && in.To == "USD" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * (eurPriceinRub / usdPriceInRub) * 1.02),
			ExchangeRate:    toPrecise((eurPriceinRub / usdPriceInRub) * 1.02),
			Correction:      0,
			OriginalRate:    toPrecise((eurPriceinRub / usdPriceInRub) * 1.02),
		}, nil
	}

	if in.From == "USD" && in.To == "RUB" {
		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(in.Amount * usdPriceInRubCb * 0.98),
				ExchangeRate:    toPrecise(usdPriceInRubCb * 0.98),
				Correction:      0,
				OriginalRate:    toPrecise(usdPriceInRubCb * 0.98),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(in.Amount * usdPriceInRub * 0.98),
			ExchangeRate:    toPrecise(usdPriceInRub * 0.98),
			Correction:      0,
			OriginalRate:    toPrecise(usdPriceInRub * 0.98),
		}, nil
	}
	if in.From == "RUB" && in.To == "USD" {
		if in.RateType == curPkg.RateTypeCentralbanks {
			a := (in.Amount / usdPriceInRubCb) * 1.02
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: toPrecise(a),
				ExchangeRate:    toPrecise(1 / usdPriceInRubCb * 1.02),
				Correction:      0,
				OriginalRate:    toPrecise(1 / usdPriceInRubCb * 1.02),
			}, nil
		}
		a := (in.Amount / usdPriceInRub) * 1.02
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: toPrecise(a),
			ExchangeRate:    toPrecise(1 / usdPriceInRub * 1.02),
			Correction:      0,
			OriginalRate:    toPrecise(1 / usdPriceInRub * 1.02),
		}, nil
	}

	return &currencies.ExchangeCurrencyResponse{
		ExchangedAmount: 10,
		ExchangeRate:    0.25,
		Correction:      2,
		OriginalRate:    0.5,
	}, nil
}

func (s *CurrencyServiceMockOk) ExchangeCurrencyByDateCommon(
	ctx context.Context,
	in *currencies.ExchangeCurrencyByDateCommonRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	if in.From == "TRY" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * 6,
		}, nil
	}
	if in.From == "TRY" && in.To == "RUB" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * 10,
		}, nil
	}
	if in.From == "EUR" && in.To == "RUB" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * 70,
		}, nil
	}

	return &currencies.ExchangeCurrencyResponse{}, nil
}

func (s *CurrencyServiceMockOk) ExchangeCurrencyByDateForMerchant(
	ctx context.Context,
	in *currencies.ExchangeCurrencyByDateForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	return &currencies.ExchangeCurrencyResponse{}, nil
}

func (s *CurrencyServiceMockOk) GetCommonRateCorrectionRule(
	ctx context.Context,
	in *currencies.CommonCorrectionRuleRequest,
	opts ...client.CallOption,
) (*currencies.CorrectionRule, error) {
	return &currencies.CorrectionRule{}, nil
}

func (s *CurrencyServiceMockOk) GetMerchantRateCorrectionRule(
	ctx context.Context,
	in *currencies.MerchantCorrectionRuleRequest,
	opts ...client.CallOption,
) (*currencies.CorrectionRule, error) {
	return &currencies.CorrectionRule{}, nil
}

func (s *CurrencyServiceMockOk) AddCommonRateCorrectionRule(
	ctx context.Context,
	in *currencies.CommonCorrectionRule,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return &currencies.EmptyResponse{}, nil
}

func (s *CurrencyServiceMockOk) AddMerchantRateCorrectionRule(
	ctx context.Context,
	in *currencies.CorrectionRule,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return &currencies.EmptyResponse{}, nil
}

func (s *CurrencyServiceMockOk) SetPaysuperCorrectionCorridor(
	ctx context.Context,
	in *currencies.CorrectionCorridor,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return &currencies.EmptyResponse{}, nil
}

func (s *CurrencyServiceMockOk) GetSupportedCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return &currencies.CurrenciesList{
		Currencies: []string{"USD", "EUR", "RUB", "GBP"},
	}, nil
}

func (s *CurrencyServiceMockOk) GetSettlementCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return &currencies.CurrenciesList{Currencies: []string{"USD", "EUR"}}, nil
}

func (s *CurrencyServiceMockOk) GetPriceCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return &currencies.CurrenciesList{}, nil
}

func (s *CurrencyServiceMockOk) GetVatCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return &currencies.CurrenciesList{}, nil
}

func (s *CurrencyServiceMockOk) GetAccountingCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return &currencies.CurrenciesList{}, nil
}

func (s *CurrencyServiceMockError) GetRateCurrentCommon(
	ctx context.Context,
	in *currencies.GetRateCurrentCommonRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetRateByDateCommon(
	ctx context.Context,
	in *currencies.GetRateByDateCommonRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetRateCurrentForMerchant(
	ctx context.Context,
	in *currencies.GetRateCurrentForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	if in.MerchantId == MerchantIdMock {
		return &currencies.RateData{Rate: 10}, nil
	}

	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetRateByDateForMerchant(
	ctx context.Context,
	in *currencies.GetRateByDateForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.RateData, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) ExchangeCurrencyCurrentCommon(
	ctx context.Context,
	in *currencies.ExchangeCurrencyCurrentCommonRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) ExchangeCurrencyCurrentForMerchant(
	ctx context.Context,
	in *currencies.ExchangeCurrencyCurrentForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) ExchangeCurrencyByDateCommon(
	ctx context.Context,
	in *currencies.ExchangeCurrencyByDateCommonRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) ExchangeCurrencyByDateForMerchant(
	ctx context.Context,
	in *currencies.ExchangeCurrencyByDateForMerchantRequest,
	opts ...client.CallOption,
) (*currencies.ExchangeCurrencyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetCommonRateCorrectionRule(
	ctx context.Context,
	in *currencies.CommonCorrectionRuleRequest,
	opts ...client.CallOption,
) (*currencies.CorrectionRule, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetMerchantRateCorrectionRule(
	ctx context.Context,
	in *currencies.MerchantCorrectionRuleRequest,
	opts ...client.CallOption,
) (*currencies.CorrectionRule, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) AddCommonRateCorrectionRule(
	ctx context.Context,
	in *currencies.CommonCorrectionRule,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) AddMerchantRateCorrectionRule(
	ctx context.Context,
	in *currencies.CorrectionRule,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) SetPaysuperCorrectionCorridor(
	ctx context.Context,
	in *currencies.CorrectionCorridor,
	opts ...client.CallOption,
) (*currencies.EmptyResponse, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetSupportedCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetSettlementCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetPriceCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetVatCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return nil, errors.New(SomeError)
}

func (s *CurrencyServiceMockError) GetAccountingCurrencies(
	ctx context.Context,
	in *currencies.EmptyRequest,
	opts ...client.CallOption,
) (*currencies.CurrenciesList, error) {
	return nil, errors.New(SomeError)
}
