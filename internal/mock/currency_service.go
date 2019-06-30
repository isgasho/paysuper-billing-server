package mock

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
)

const (
	SomeError = "some error"
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
	return &currencies.ExchangeCurrencyResponse{
		ExchangedAmount: 30,
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
	return &currencies.CurrenciesList{}, nil
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
