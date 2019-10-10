package service

import (
	"context"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	defaultCompanyName = "PaySuper"
)

var (
	usersDbInternalError           = newBillingServerErrorMsg("uu000001", "unknown database error")
	errorUserAlreadyExist          = newBillingServerErrorMsg("uu000002", "user already exist.")
	errorUserUnableToAdd           = newBillingServerErrorMsg("uu000003", "unable to add user.")
	errorUserNotFound              = newBillingServerErrorMsg("uu000004", "user not found.")
	errorUserInviteAlreadyAccepted = newBillingServerErrorMsg("uu000005", "user already accepted invite.")
	errorUserMerchantNotFound      = newBillingServerErrorMsg("uu000006", "merchant not found.")
	errorUserUnableToSendInvite    = newBillingServerErrorMsg("uu000007", "unable to send invite email.")
	errorUserNoHavePermission      = newBillingServerErrorMsg("uu000008", "user not have permission to invite users.")
)

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

func (s *Service) InviteUserMerchant(
	ctx context.Context,
	req *grpc.InviteUserMerchantRequest,
	res *grpc.InviteUserMerchantResponse,
) error {
	owner, err := s.merchant.GetByUserId(req.UserId)

	if err != nil {
		zap.L().Error(errorUserMerchantNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserMerchantNotFound

		return nil
	}

	user, err := s.userRoleRepository.GetMerchantUserByEmail(owner.Id, req.Item.User.Email)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	req.Item.Id = bson.NewObjectId().Hex()
	req.Item.User.Status = pkg.UserRoleStatusInvited

	if err = s.userRoleRepository.AddMerchantUser(req.Item); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd

		return nil
	}

	name := owner.User.FirstName + " " + owner.User.LastName
	if err = s.sendInviteEmail(req.Item.User.Email, owner.User.Email, name, owner.Company.Name); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Item.User.Email),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderName", name),
			zap.String("senderCompany", owner.Company.Name),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req.Item

	return nil
}

func (s *Service) InviteUserAdmin(
	ctx context.Context,
	req *grpc.InviteUserAdminRequest,
	res *grpc.InviteUserAdminResponse,
) error {
	owner, err := s.userRoleRepository.GetAdminUserByUserId(req.UserId)

	if err != nil || owner.Role != pkg.AdminUserRole {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	user, err := s.userRoleRepository.GetAdminUserByEmail(req.Item.User.Email)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	req.Item.Id = bson.NewObjectId().Hex()
	req.Item.User.Status = pkg.UserRoleStatusInvited

	if err = s.userRoleRepository.AddAdminUser(req.Item); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd

		return nil
	}

	name := owner.User.FirstName + " " + owner.User.LastName
	if err = s.sendInviteEmail(req.Item.User.Email, owner.User.Email, name, defaultCompanyName); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Item.User.Email),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderName", name),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = req.Item

	return nil
}

func (s *Service) ResendInviteMerchant(
	ctx context.Context,
	req *grpc.ResendInviteMerchantRequest,
	res *grpc.EmptyResponseWithStatus,
) error {
	owner, err := s.merchant.GetByUserId(req.UserId)

	if err != nil {
		zap.L().Error(errorUserMerchantNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserMerchantNotFound

		return nil
	}

	_, err = s.userRoleRepository.GetMerchantUserByEmail(owner.Id, req.ReceiverEmail)

	if err != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	name := owner.User.FirstName + " " + owner.User.LastName
	if err = s.sendInviteEmail(req.ReceiverEmail, owner.User.Email, name, owner.Company.Name); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.ReceiverEmail),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderName", name),
			zap.String("senderCompany", owner.Company.Name),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) ResendInviteAdmin(
	ctx context.Context,
	req *grpc.ResendInviteAdminRequest,
	res *grpc.EmptyResponseWithStatus,
) error {
	owner, err := s.userRoleRepository.GetAdminUserByUserId(req.UserId)

	if err != nil || owner.Role != pkg.AdminUserRole {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	user, err := s.userRoleRepository.GetAdminUserByEmail(req.ReceiverEmail)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	name := owner.User.FirstName + " " + owner.User.LastName
	if err = s.sendInviteEmail(req.ReceiverEmail, owner.User.Email, name, defaultCompanyName); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.ReceiverEmail),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderName", name),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) AcceptMerchantInvite(
	ctx context.Context,
	req *billing.UserRoleMerchant,
	res *grpc.EmptyResponseWithStatus,
) error {
	user, err := s.userRoleRepository.GetMerchantUserById(req.Id)

	if err != nil {
		zap.L().Error(errorUserNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNotFound
		return nil
	}

	if user.User.Status != pkg.UserRoleStatusInvited {
		zap.L().Error(errorUserInviteAlreadyAccepted.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInviteAlreadyAccepted
		return nil
	}

	user.User.Status = pkg.UserRoleStatusAccepted

	if err = s.userRoleRepository.UpdateMerchantUser(user); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd
		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) AcceptAdminInvite(
	ctx context.Context,
	req *billing.UserRoleAdmin,
	res *grpc.EmptyResponseWithStatus,
) error {
	user, err := s.userRoleRepository.GetAdminUserById(req.Id)

	if err != nil {
		zap.L().Error(errorUserNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNotFound
		return nil
	}

	if user.User.Status != pkg.UserRoleStatusInvited {
		zap.L().Error(errorUserInviteAlreadyAccepted.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInviteAlreadyAccepted
		return nil
	}

	user.User.Status = pkg.UserRoleStatusAccepted

	if err = s.userRoleRepository.UpdateAdminUser(user); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd
		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetMerchantUser(
	ctx context.Context,
	req *grpc.GetMerchantUserRequest,
	res *grpc.GetMerchantUserResponse,
) error {
	user, err := s.userRoleRepository.GetMerchantUserById(req.Id)

	if err != nil {
		zap.L().Error(errorUserNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNotFound

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = user

	return nil
}

func (s *Service) GetAdminUser(
	ctx context.Context,
	req *grpc.GetAdminUserRequest,
	res *grpc.GetAdminUserResponse,
) error {
	user, err := s.userRoleRepository.GetAdminUserById(req.Id)

	if err != nil {
		zap.L().Error(errorUserNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNotFound

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Item = user

	return nil
}

func (s *Service) sendInviteEmail(receiverEmail string, senderEmail string, senderName string, senderCompany string) error {
	// TODO: Send invite mail over postmark
	return nil
}
