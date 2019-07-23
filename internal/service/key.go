package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

func (s *Service) GetAvailableKeysCount(context.Context, *grpc.GetPlatformKeyCountRequest, *grpc.GetPlatformKeyCountResponse) error {
	panic("implement me")
}

func (s *Service) UploadKeysFile(context.Context, *grpc.PlatformKeysFileRequest, *grpc.PlatformKeysFileResponse) error {
	panic("implement me")
}

func (s *Service) GetKeyByID(context.Context, *grpc.KeyForOrderRequest, *grpc.GetKeyForOrderRequestResponse) error {
	panic("implement me")
}

func (s *Service) ReserveKeyForOrder(context.Context, *grpc.PlatformKeyReserveRequest, *grpc.PlatformKeyReserveResponse) error {
	panic("implement me")
}

func (s *Service) FinishRedeemKeyForOrder(context.Context, *grpc.KeyForOrderRequest, *grpc.GetKeyForOrderRequestResponse) error {
	panic("implement me")
}

func (s *Service) CancelRedeemKeyForOrder(context.Context, *grpc.KeyForOrderRequest, *grpc.EmptyResponseWithStatus) error {
	panic("implement me")
}
