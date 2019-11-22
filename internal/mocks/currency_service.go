package mocks

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/micro/go-micro/client"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

const (
	eurPriceinRub         = float64(72)
	eurPriceInRubCb       = float64(72.5)
	eurPriceInRubCbOnDate = float64(70)
	eurPriceInRubStock    = float64(71)

	usdPriceInRub         = float64(65)
	usdPriceInRubCb       = float64(65.5)
	usdPriceInRubCbOnDate = float64(63)
	usdPriceInRubStock    = float64(64)
)

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
			ExchangedAmount: tools.ToPrecise(in.Amount * eurPriceinRub),
			ExchangeRate:    eurPriceinRub,
			Correction:      0,
			OriginalRate:    eurPriceinRub,
		}, nil
	}
	if in.From == "RUB" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount / eurPriceinRub),
			ExchangeRate:    tools.ToPrecise(1 / eurPriceinRub),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(1 / eurPriceinRub),
		}, nil
	}

	if in.From == "USD" && in.To == "EUR" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * (usdPriceInRubStock / eurPriceInRubStock)),
				ExchangeRate:    tools.ToPrecise(usdPriceInRubStock / eurPriceInRubStock),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(usdPriceInRubStock / eurPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * (usdPriceInRubCb / eurPriceInRubCb)),
				ExchangeRate:    tools.ToPrecise(usdPriceInRubCb / eurPriceInRubCb),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(usdPriceInRubCb / eurPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * (usdPriceInRub / eurPriceinRub)),
			ExchangeRate:    tools.ToPrecise(usdPriceInRub / eurPriceinRub),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(usdPriceInRub / eurPriceinRub),
		}, nil
	}
	if in.From == "EUR" && in.To == "USD" {

		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * (eurPriceInRubStock / usdPriceInRubStock)),
				ExchangeRate:    tools.ToPrecise(eurPriceInRubStock / usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(eurPriceInRubStock / usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * (eurPriceInRubCb / usdPriceInRubCb)),
				ExchangeRate:    tools.ToPrecise(eurPriceInRubCb / usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(eurPriceInRubCb / usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * (eurPriceinRub / usdPriceInRub)),
			ExchangeRate:    tools.ToPrecise(eurPriceinRub / usdPriceInRub),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(eurPriceinRub / usdPriceInRub),
		}, nil
	}
	if in.From == "USD" && in.To == "RUB" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * usdPriceInRubStock),
				ExchangeRate:    tools.ToPrecise(usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * usdPriceInRubCb),
				ExchangeRate:    tools.ToPrecise(usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * usdPriceInRub),
			ExchangeRate:    usdPriceInRub,
			Correction:      0,
			OriginalRate:    usdPriceInRub,
		}, nil
	}
	if in.From == "RUB" && in.To == "USD" {
		if in.RateType == curPkg.RateTypeStock {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount / usdPriceInRubStock),
				ExchangeRate:    tools.ToPrecise(1 / usdPriceInRubStock),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(1 / usdPriceInRubStock),
			}, nil
		}

		if in.RateType == curPkg.RateTypeCentralbanks {
			a := in.Amount / usdPriceInRubCb
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(a),
				ExchangeRate:    tools.ToPrecise(1 / usdPriceInRubCb),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(1 / usdPriceInRubCb),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount / usdPriceInRub),
			ExchangeRate:    tools.ToPrecise(1 / usdPriceInRub),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(1 / usdPriceInRub),
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
			ExchangedAmount: tools.ToPrecise(in.Amount * eurPriceinRub * 1.02),
			ExchangeRate:    tools.ToPrecise(eurPriceinRub * 1.02),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(eurPriceinRub * 1.02),
		}, nil
	}
	if in.From == "RUB" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount / eurPriceinRub * 1.02),
			ExchangeRate:    tools.ToPrecise((1 / eurPriceinRub) * 1.02),
			Correction:      0,
			OriginalRate:    tools.ToPrecise((1 / eurPriceinRub) * 1.02),
		}, nil
	}
	if in.From == "USD" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * (usdPriceInRub / eurPriceinRub) * 0.98),
			ExchangeRate:    tools.ToPrecise((usdPriceInRub / eurPriceinRub) * 0.98),
			Correction:      0,
			OriginalRate:    tools.ToPrecise((usdPriceInRub / eurPriceinRub) * 0.98),
		}, nil
	}
	if in.From == "EUR" && in.To == "USD" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * (eurPriceinRub / usdPriceInRub) * 1.02),
			ExchangeRate:    tools.ToPrecise((eurPriceinRub / usdPriceInRub) * 1.02),
			Correction:      0,
			OriginalRate:    tools.ToPrecise((eurPriceinRub / usdPriceInRub) * 1.02),
		}, nil
	}

	if in.From == "USD" && in.To == "RUB" {
		if in.RateType == curPkg.RateTypeCentralbanks {
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(in.Amount * usdPriceInRubCb * 0.98),
				ExchangeRate:    tools.ToPrecise(usdPriceInRubCb * 0.98),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(usdPriceInRubCb * 0.98),
			}, nil
		}

		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(in.Amount * usdPriceInRub * 0.98),
			ExchangeRate:    tools.ToPrecise(usdPriceInRub * 0.98),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(usdPriceInRub * 0.98),
		}, nil
	}
	if in.From == "RUB" && in.To == "USD" {
		if in.RateType == curPkg.RateTypeCentralbanks {
			a := (in.Amount / usdPriceInRubCb) * 1.02
			return &currencies.ExchangeCurrencyResponse{
				ExchangedAmount: tools.ToPrecise(a),
				ExchangeRate:    tools.ToPrecise(1 / usdPriceInRubCb * 1.02),
				Correction:      0,
				OriginalRate:    tools.ToPrecise(1 / usdPriceInRubCb * 1.02),
			}, nil
		}
		a := (in.Amount / usdPriceInRub) * 1.02
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: tools.ToPrecise(a),
			ExchangeRate:    tools.ToPrecise(1 / usdPriceInRub * 1.02),
			Correction:      0,
			OriginalRate:    tools.ToPrecise(1 / usdPriceInRub * 1.02),
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
			ExchangedAmount: in.Amount * eurPriceInRubCbOnDate,
		}, nil
	}
	if in.From == "RUB" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * (1 / eurPriceInRubCbOnDate),
		}, nil
	}
	if in.From == "USD" && in.To == "RUB" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * usdPriceInRubCbOnDate,
		}, nil
	}
	if in.From == "RUB" && in.To == "USD" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * (1 / usdPriceInRubCbOnDate),
		}, nil
	}
	if in.From == "USD" && in.To == "EUR" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * (usdPriceInRubCbOnDate / eurPriceInRubCbOnDate),
		}, nil
	}
	if in.From == "EUR" && in.To == "USD" {
		return &currencies.ExchangeCurrencyResponse{
			ExchangedAmount: in.Amount * (eurPriceInRubCbOnDate / usdPriceInRubCbOnDate),
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
	return &currencies.CurrenciesList{
		Currencies: []string{"AED", "ARS", "AUD", "BHD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CRC", "CZK", "DKK",
			"EGP", "EUR", "GBP", "HKD", "HRK", "HUF", "IDR", "ILS", "INR", "JPY", "KRW", "KZT", "MXN",
			"MYR", "NOK", "NZD", "PEN", "PHP", "PLN", "QAR", "RON", "RSD", "RUB", "SAR", "SEK", "SGD",
			"THB", "TRY", "TWD", "USD", "VND", "ZAR"},
	}, nil
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
