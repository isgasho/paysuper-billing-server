package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

var usersDbInternalError = newBillingServerErrorMsg("uu000001", "unknown database error")

func (s *Service) GetMerchantUsers(ctx context.Context, req *grpc.GetMerchantUsersRequest, res *grpc.GetMerchantUsersResponse) error {
	_, err := s.merchant.GetById(req.MerchantId)
	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = merchantErrorNotFound
		return nil
	}

	users, err := s.userRoleRepository.GetUsersForMerchant(req.MerchantId)

	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = usersDbInternalError
		res.Message.Details = err.Error()

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Users = users

	return nil
}

func (s *Service) GetAdminUsers(ctx context.Context, _ *grpc.EmptyRequest, res *grpc.GetAdminUsersResponse) error {
	users, err := s.userRoleRepository.GetUsersForAdmin()

	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = usersDbInternalError
		res.Message.Details = err.Error()

		return nil
	}
	res.Status = pkg.ResponseStatusOk
	res.Users = users

	return nil
}
