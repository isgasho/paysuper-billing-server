package service

import (
	"context"
	"errors"
	"github.com/dgrijalva/jwt-go"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"time"
)

const (
	defaultCompanyName = "PaySuper"

	claimType   = "type"
	claimEmail  = "email"
	claimRoleId = "role_id"
	claimExpire = "exp"

	typeMerchant = "merchant"
	typeAdmin    = "admin"
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
	errorUserUnableToCreateToken   = newBillingServerErrorMsg("uu000009", "unable to create invite token.")
	errorUserInvalidToken          = newBillingServerErrorMsg("uu000010", "invalid token string.")
	errorUserInvalidInviteEmail    = newBillingServerErrorMsg("uu000011", "invalid invite email.")
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
	owner, err := s.userRoleRepository.GetMerchantUserByUserId(req.Role.MerchantId, req.UserId)

	if err != nil || !owner.IsOwner() {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	merchant, err := s.merchant.GetById(owner.MerchantId)

	if err != nil {
		zap.L().Error(errorUserMerchantNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserMerchantNotFound

		return nil
	}

	user, err := s.userRoleRepository.GetMerchantUserByEmail(owner.MerchantId, req.Role.User.Email)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	req.Role.Id = bson.NewObjectId().Hex()
	req.Role.User.Status = pkg.UserRoleStatusInvited

	if err = s.userRoleRepository.AddMerchantUser(req.Role); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd

		return nil
	}

	expire := time.Now().Add(time.Hour * time.Duration(s.cfg.UserInviteTokenTimeout)).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		claimType:   typeMerchant,
		claimEmail:  req.Role.User.Email,
		claimRoleId: req.Role.Id,
		claimExpire: expire,
	})
	tokenString, err := token.SignedString([]byte(s.cfg.UserInviteTokenSecret))

	if err != nil {
		zap.L().Error(errorUserUnableToCreateToken.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToCreateToken

		return nil
	}

	if err = s.sendInviteEmail(req.Role.User.Email, owner.User.Email, owner.User.FirstName, owner.User.LastName, merchant.Company.Name, tokenString); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Role.User.Email),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderFirstName", owner.User.FirstName),
			zap.String("senderLastName", owner.User.LastName),
			zap.String("senderCompany", merchant.Company.Name),
			zap.String("token", tokenString),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Role = req.Role

	return nil
}

func (s *Service) InviteUserAdmin(
	ctx context.Context,
	req *grpc.InviteUserAdminRequest,
	res *grpc.InviteUserAdminResponse,
) error {
	owner, err := s.userRoleRepository.GetAdminUserByUserId(req.UserId)

	if err != nil || !owner.IsAdmin() {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	user, err := s.userRoleRepository.GetAdminUserByEmail(req.Role.User.Email)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	req.Role.Id = bson.NewObjectId().Hex()
	req.Role.User.Status = pkg.UserRoleStatusInvited

	if err = s.userRoleRepository.AddAdminUser(req.Role); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd

		return nil
	}

	expire := time.Now().Add(time.Hour * time.Duration(s.cfg.UserInviteTokenTimeout)).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		claimType:   typeAdmin,
		claimEmail:  req.Role.User.Email,
		claimRoleId: req.Role.Id,
		claimExpire: expire,
	})
	tokenString, err := token.SignedString([]byte(s.cfg.UserInviteTokenSecret))

	if err != nil {
		zap.L().Error(errorUserUnableToCreateToken.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToCreateToken

		return nil
	}

	if err = s.sendInviteEmail(req.Role.User.Email, owner.User.Email, owner.User.FirstName, owner.User.LastName, defaultCompanyName, tokenString); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Role.User.Email),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderFirstName", owner.User.FirstName),
			zap.String("senderLastName", owner.User.LastName),
			zap.String("token", tokenString),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToSendInvite

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Role = req.Role

	return nil
}

func (s *Service) ResendInviteMerchant(
	ctx context.Context,
	req *grpc.ResendInviteMerchantRequest,
	res *grpc.EmptyResponseWithStatus,
) error {
	owner, err := s.userRoleRepository.GetMerchantUserByUserId(req.Role.MerchantId, req.UserId)

	if err != nil || !owner.IsOwner() {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	merchant, err := s.merchant.GetById(owner.MerchantId)

	if err != nil {
		zap.L().Error(errorUserMerchantNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserMerchantNotFound

		return nil
	}

	_, err = s.userRoleRepository.GetMerchantUserByEmail(merchant.Id, req.Role.User.Email)

	if err != nil {
		zap.L().Error(errorUserNotFound.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNotFound

		return nil
	}

	expire := time.Now().Add(time.Hour * time.Duration(s.cfg.UserInviteTokenTimeout)).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		claimType:   typeMerchant,
		claimEmail:  req.Role.User.Email,
		claimRoleId: req.Role.Id,
		claimExpire: expire,
	})
	tokenString, err := token.SignedString([]byte(s.cfg.UserInviteTokenSecret))

	if err != nil {
		zap.L().Error(errorUserUnableToCreateToken.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToCreateToken

		return nil
	}

	if err = s.sendInviteEmail(req.Role.User.Email, owner.User.Email, owner.User.FirstName, owner.User.LastName, merchant.Company.Name, tokenString); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Role.User.Email),
			zap.String("senderEmail", merchant.User.Email),
			zap.String("senderFirstName", merchant.User.FirstName),
			zap.String("senderLastName", merchant.User.LastName),
			zap.String("senderCompany", merchant.Company.Name),
			zap.String("tokenString", tokenString),
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

	if err != nil || owner.IsAdmin() {
		zap.L().Error(errorUserNoHavePermission.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserNoHavePermission

		return nil
	}

	user, err := s.userRoleRepository.GetAdminUserByEmail(req.Role.User.Email)

	if (err != nil && err != mgo.ErrNotFound) || user != nil {
		zap.L().Error(errorUserAlreadyExist.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserAlreadyExist

		return nil
	}

	expire := time.Now().Add(time.Hour * time.Duration(s.cfg.UserInviteTokenTimeout)).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		claimType:   typeAdmin,
		claimEmail:  req.Role.User.Email,
		claimRoleId: req.Role.Id,
		claimExpire: expire,
	})
	tokenString, err := token.SignedString([]byte(s.cfg.UserInviteTokenSecret))

	if err != nil {
		zap.L().Error(errorUserUnableToCreateToken.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToCreateToken

		return nil
	}

	if err = s.sendInviteEmail(req.Role.User.Email, owner.User.Email, owner.User.FirstName, owner.User.LastName, defaultCompanyName, tokenString); err != nil {
		zap.L().Error(
			errorUserUnableToSendInvite.Message,
			zap.Error(err),
			zap.String("receiverEmail", req.Role.User.Email),
			zap.String("senderEmail", owner.User.Email),
			zap.String("senderFirstName", owner.User.FirstName),
			zap.String("senderLastName", owner.User.LastName),
			zap.String("tokenString", tokenString),
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
	req *grpc.AcceptMerchantInviteRequest,
	res *grpc.AcceptMerchantInviteResponse,
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
	user.User.UserId = req.UserId
	user.User.FirstName = req.FirstName
	user.User.LastName = req.LastName

	if err = s.userRoleRepository.UpdateMerchantUser(user); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd
		return nil
	}

	// TODO: Add user to casbin

	res.Status = pkg.ResponseStatusOk
	res.Role = user

	return nil
}

func (s *Service) AcceptAdminInvite(
	ctx context.Context,
	req *grpc.AcceptAdminInviteRequest,
	res *grpc.AcceptAdminInviteResponse,
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
	user.User.UserId = req.UserId
	user.User.FirstName = req.FirstName
	user.User.LastName = req.LastName

	if err = s.userRoleRepository.UpdateAdminUser(user); err != nil {
		zap.L().Error(errorUserUnableToAdd.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserUnableToAdd
		return nil
	}

	// TODO: Add user to casbin

	res.Status = pkg.ResponseStatusOk
	res.Role = user

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
	res.Role = user

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
	res.Role = user

	return nil
}

func (s *Service) CheckInviteToken(
	ctx context.Context,
	req *grpc.CheckInviteTokenRequest,
	res *grpc.CheckInviteTokenResponse,
) error {
	token, err := jwt.Parse(req.Token, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New(errorUserInvalidToken.Message)
		}

		return []byte(s.cfg.UserInviteTokenSecret), nil
	})

	if err != nil {
		zap.L().Error(errorUserInvalidToken.Message, zap.Error(err), zap.Any("req", req))
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInvalidToken

		return nil
	}

	if !token.Valid {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInvalidToken

		return nil
	}

	claims, ok := token.Claims.(jwt.MapClaims)

	if !ok {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInvalidToken

		return nil
	}

	if claims[claimEmail] != req.Email {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorUserInvalidInviteEmail

		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.RoleId = claims[claimRoleId].(string)
	res.RoleType = claims[claimType].(string)

	return nil
}

func (s *Service) sendInviteEmail(receiverEmail, senderEmail, senderFirstName, senderLastName, senderCompany, token string) error {
	// TODO: What will be the address of the links? How do we transfer the parameters (to the public or to the JWT key)?
	inviteLink := "/user/invite/member?key=" + token

	if senderCompany != defaultCompanyName {
		inviteLink = "/user/invite/merchant?key=" + token
	}

	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: s.cfg.EmailInviteTemplate,
		TemplateModel: map[string]string{
			"sender_first_name": senderFirstName,
			"sender_last_name":  senderLastName,
			"sender_email":      senderEmail,
			"sender_company":    senderCompany,
			"invite_link":       inviteLink,
		},
		To: receiverEmail,
	}
	err := s.broker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

	if err != nil {
		return err
	}

	return nil
}

func (s *Service) GetMerchantsForUser(ctx context.Context, req *grpc.GetMerchantsForUserRequest, res *grpc.GetMerchantsForUserResponse) error {
	users, err := s.userRoleRepository.GetMerchantsForUser(req.UserId)

	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = usersDbInternalError
		res.Message.Details = err.Error()

		return nil
	}

	merchants := make([]*grpc.MerchantForUserInfo, len(users))

	for i, user := range users {
		merchant, err := s.merchant.GetById(user.MerchantId)
		if err != nil {
			zap.L().Error(
				"Can't get merchant by id",
				zap.Error(err),
				zap.String("merchant_id", user.MerchantId),
			)

			res.Status = pkg.ResponseStatusSystemError
			res.Message = usersDbInternalError
			res.Message.Details = err.Error()

			return nil
		}

		merchants[i].Id = user.MerchantId
		merchants[i].Name = merchant.Company.Name
	}

	res.Status = pkg.ResponseStatusOk
	res.Merchants = merchants
	return nil
}
