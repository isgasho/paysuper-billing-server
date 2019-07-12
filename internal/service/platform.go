package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

func (s *Service) GetPlatforms(context.Context, *grpc.ListPlatformsRequest, *grpc.ListPlatformsResponse) error {
	panic("implement me")
}

