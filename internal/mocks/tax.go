package mocks

import (
	"context"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-proto/go/taxpb"
)

type TaxServiceOkMock struct{}

func NewTaxServiceOkMock() taxpb.TaxService {
	return &TaxServiceOkMock{}
}

func (m *TaxServiceOkMock) GetRate(
	ctx context.Context,
	in *taxpb.GeoIdentity,
	opts ...client.CallOption,
) (*taxpb.TaxRate, error) {
	if in.Country == "US" {
		return &taxpb.TaxRate{
			Id:      1,
			Zip:     "98001",
			Country: "US",
			State:   "NY",
			City:    "Washington",
			Rate:    0.19,
		}, nil
	}
	return &taxpb.TaxRate{
		Id:      0,
		Zip:     "190000",
		Country: "RU",
		State:   "SPE",
		City:    "St.Petersburg",
		Rate:    0.20,
	}, nil

}

func (m *TaxServiceOkMock) GetRates(
	ctx context.Context,
	in *taxpb.GetRatesRequest,
	opts ...client.CallOption,
) (*taxpb.GetRatesResponse, error) {
	return &taxpb.GetRatesResponse{}, nil
}

func (m *TaxServiceOkMock) CreateOrUpdate(
	ctx context.Context,
	in *taxpb.TaxRate,
	opts ...client.CallOption,
) (*taxpb.TaxRate, error) {
	return &taxpb.TaxRate{}, nil
}

func (m *TaxServiceOkMock) DeleteRateById(
	ctx context.Context,
	in *taxpb.DeleteRateRequest,
	opts ...client.CallOption,
) (*taxpb.DeleteRateResponse, error) {
	return &taxpb.DeleteRateResponse{}, nil
}
