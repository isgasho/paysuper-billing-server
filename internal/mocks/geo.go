package mocks

import (
	"context"
	"errors"
	"github.com/ProtocolONE/geoip-service/pkg/proto"
	"github.com/micro/go-micro/client"
)

type GeoIpServiceTestOk struct{}
type GeoIpServiceTestOkWithoutSubdivision struct{}
type GeoIpServiceTestError struct{}

func NewGeoIpServiceTestOkWithoutSubdivision() proto.GeoIpService {
	return &GeoIpServiceTestOkWithoutSubdivision{}
}
func NewGeoIpServiceTestOk() proto.GeoIpService {
	return &GeoIpServiceTestOk{}
}

func NewGeoIpServiceTestError() proto.GeoIpService {
	return &GeoIpServiceTestError{}
}

func (s *GeoIpServiceTestOk) GetIpData(
	ctx context.Context,
	in *proto.GeoIpDataRequest,
	opts ...client.CallOption,
) (*proto.GeoIpDataResponse, error) {
	var data = &proto.GeoIpDataResponse{}

	switch in.IP {
	case "127.0.0.1":
		data = &proto.GeoIpDataResponse{
			Country: &proto.GeoIpCountry{
				IsoCode: "RU",
				Names:   map[string]string{"en": "Russia", "ru": "Россия"},
			},
			City: &proto.GeoIpCity{
				Names: map[string]string{"en": "St.Petersburg", "ru": "Санкт-Петербург"},
			},
			Location: &proto.GeoIpLocation{
				TimeZone: "Europe/Moscow",
			},
			Subdivisions: []*proto.GeoIpSubdivision{
				{
					GeoNameID: uint32(1),
					IsoCode:   "SPE",
					Names:     map[string]string{"en": "St.Petersburg", "ru": "Санкт-Петербург"},
				},
			},
		}
		break

	case "127.0.0.2":
		data = &proto.GeoIpDataResponse{
			Country: &proto.GeoIpCountry{
				IsoCode: "US",
				Names:   map[string]string{"en": "United States of America", "ru": "США"},
			},
			City: &proto.GeoIpCity{
				Names: map[string]string{"en": "New York", "ru": "Нью-Йорк"},
			},
			Location: &proto.GeoIpLocation{
				TimeZone: "America/New_York",
			},
			Subdivisions: []*proto.GeoIpSubdivision{
				{
					GeoNameID: uint32(1),
					IsoCode:   "NY",
					Names:     map[string]string{"en": "New York", "ru": "Нью-Йорк"},
				},
			},
		}
		break
	case "127.0.0.3":
		data = &proto.GeoIpDataResponse{
			Country: &proto.GeoIpCountry{
				IsoCode: "",
				Names:   map[string]string{"en": "", "ru": ""},
			},
			City: &proto.GeoIpCity{
				Names: map[string]string{"en": "", "ru": ""},
			},
			Location: &proto.GeoIpLocation{
				TimeZone: "",
			},
			Subdivisions: []*proto.GeoIpSubdivision{
				{
					GeoNameID: 0,
					IsoCode:   "",
					Names:     map[string]string{"en": "", "ru": ""},
				},
			},
		}
		break
	default:
		data = &proto.GeoIpDataResponse{
			Country: &proto.GeoIpCountry{
				IsoCode: "UA",
				Names:   map[string]string{"en": "Ukraina", "ru": "Украина"},
			},
			City: &proto.GeoIpCity{
				Names: map[string]string{"en": "Kiev", "ru": "Киев"},
			},
			Location: &proto.GeoIpLocation{
				TimeZone: "Europe/Kiev",
			},
			Subdivisions: []*proto.GeoIpSubdivision{
				{
					GeoNameID: uint32(2),
					IsoCode:   "30",
					Names:     map[string]string{"en": "Kiev", "ru": "Киев"},
				},
			},
		}
	}

	return data, nil
}

func (s *GeoIpServiceTestOkWithoutSubdivision) GetIpData(
	ctx context.Context,
	in *proto.GeoIpDataRequest,
	opts ...client.CallOption,
) (*proto.GeoIpDataResponse, error) {
	data := &proto.GeoIpDataResponse{
		Country: &proto.GeoIpCountry{
			IsoCode: "RU",
			Names:   map[string]string{"en": "Russia", "ru": "Россия"},
		},
		City: &proto.GeoIpCity{
			Names: map[string]string{"en": "St.Petersburg", "ru": "Санкт-Петербург"},
		},
		Location: &proto.GeoIpLocation{
			TimeZone: "Europe/Moscow",
		},
	}

	return data, nil
}

func (s *GeoIpServiceTestError) GetIpData(
	ctx context.Context,
	in *proto.GeoIpDataRequest,
	opts ...client.CallOption,
) (*proto.GeoIpDataResponse, error) {
	return &proto.GeoIpDataResponse{}, errors.New("some error")
}
