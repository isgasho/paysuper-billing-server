package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"strings"
	"time"
)

const (
	collectionOPageReview        = "feedback"
	userEmailConfirmTokenStorage = "email_confirm:token:%s"
)

var (
	userProfileErrorNotFound                  = newBillingServerErrorMsg("op000001", "user profile not found")
	userProfileErrorUnknown                   = newBillingServerErrorMsg("op000002", "unknown error. try request later")
	userProfileEmailConfirmationTokenNotFound = newBillingServerErrorMsg("op000003", "user email confirmation token not found")
)

type EmailConfirmToken struct {
	Token     string
	ProfileId string
	CreatedAt time.Time
}

func (s *Service) CreateOrUpdateUserProfile(
	ctx context.Context,
	req *grpc.UserProfile,
	rsp *grpc.GetUserProfileResponse,
) error {
	var err error

	profile, err := s.userProfileRepository.GetByUserId(ctx, req.UserId)

	if profile == nil {
		profile = req
		profile.Id = primitive.NewObjectID().Hex()
		profile.CreatedAt = ptypes.TimestampNow()
	} else {
		profile = s.updateOnboardingProfile(profile, req)
	}

	profile.UpdatedAt = ptypes.TimestampNow()
	expire := time.Now().Add(time.Minute * 30).Unix()
	profile.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(profile.Id, expire)

	if err = s.userProfileRepository.Upsert(ctx, profile); err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	if profile.NeedConfirmEmail() {
		profile.Email.ConfirmationUrl, err = s.setUserEmailConfirmationToken(profile)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = userProfileErrorUnknown

			return nil
		}

		err = s.sendUserEmailConfirmationToken(ctx, profile)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = userProfileErrorUnknown

			return nil
		}
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) GetUserProfile(
	ctx context.Context,
	req *grpc.GetUserProfileRequest,
	rsp *grpc.GetUserProfileResponse,
) error {
	var err error
	var profile *grpc.UserProfile

	if req.ProfileId != "" {
		profile, err = s.userProfileRepository.GetById(ctx, req.ProfileId)
	} else {
		profile, err = s.userProfileRepository.GetByUserId(ctx, req.UserId)
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	expire := time.Now().Add(time.Minute * 30).Unix()
	centrifugoToken := s.centrifugoDashboard.GetChannelToken(profile.Id, expire)

	profile.CentrifugoToken = centrifugoToken

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) updateOnboardingProfile(profile, profileReq *grpc.UserProfile) *grpc.UserProfile {
	if profileReq.HasPersonChanges(profile) {
		if profile.Personal == nil {
			profile.Personal = &grpc.UserProfilePersonal{}
		}

		if profile.Personal.FirstName != profileReq.Personal.FirstName {
			profile.Personal.FirstName = profileReq.Personal.FirstName
		}

		if profile.Personal.LastName != profileReq.Personal.LastName {
			profile.Personal.LastName = profileReq.Personal.LastName
		}

		if profile.Personal.Position != profileReq.Personal.Position {
			profile.Personal.Position = profileReq.Personal.Position
		}
	}

	if profileReq.HasHelpChanges(profile) {
		if profile.Help == nil {
			profile.Help = &grpc.UserProfileHelp{}
		}

		if profile.Help.ProductPromotionAndDevelopment != profileReq.Help.ProductPromotionAndDevelopment {
			profile.Help.ProductPromotionAndDevelopment = profileReq.Help.ProductPromotionAndDevelopment
		}

		if profile.Help.ReleasedGamePromotion != profileReq.Help.ReleasedGamePromotion {
			profile.Help.ReleasedGamePromotion = profileReq.Help.ReleasedGamePromotion
		}

		if profile.Help.InternationalSales != profileReq.Help.InternationalSales {
			profile.Help.InternationalSales = profileReq.Help.InternationalSales
		}

		if profile.Help.Other != profileReq.Help.Other {
			profile.Help.Other = profileReq.Help.Other
		}
	}

	if profileReq.HasCompanyChanges(profile) {
		if profile.Company == nil {
			profile.Company = &grpc.UserProfileCompany{}
		}

		if profile.Company.CompanyName != profileReq.Company.CompanyName {
			profile.Company.CompanyName = profileReq.Company.CompanyName
		}

		if profile.Company.Website != profileReq.Company.Website {
			profile.Company.Website = profileReq.Company.Website
		}

		if profileReq.HasCompanyAnnualIncomeChanges(profile) {
			if profile.Company.AnnualIncome == nil {
				profile.Company.AnnualIncome = &billing.RangeInt{}
			}

			if profile.Company.AnnualIncome.From != profileReq.Company.AnnualIncome.From {
				profile.Company.AnnualIncome.From = profileReq.Company.AnnualIncome.From
			}

			if profile.Company.AnnualIncome.To != profileReq.Company.AnnualIncome.To {
				profile.Company.AnnualIncome.To = profileReq.Company.AnnualIncome.To
			}
		}

		if profileReq.HasCompanyNumberOfEmployeesChanges(profile) {
			if profile.Company.NumberOfEmployees == nil {
				profile.Company.NumberOfEmployees = &billing.RangeInt{}
			}

			if profile.Company.NumberOfEmployees.From != profileReq.Company.NumberOfEmployees.From {
				profile.Company.NumberOfEmployees.From = profileReq.Company.NumberOfEmployees.From
			}

			if profile.Company.NumberOfEmployees.To != profileReq.Company.NumberOfEmployees.To {
				profile.Company.NumberOfEmployees.To = profileReq.Company.NumberOfEmployees.To
			}
		}

		if profile.Company.KindOfActivity != profileReq.Company.KindOfActivity {
			profile.Company.KindOfActivity = profileReq.Company.KindOfActivity
		}

		if profileReq.HasCompanyMonetizationChanges(profile) {
			if profile.Company.Monetization == nil {
				profile.Company.Monetization = &grpc.UserProfileCompanyMonetization{}
			}

			if profile.Company.Monetization.PaidSubscription != profileReq.Company.Monetization.PaidSubscription {
				profile.Company.Monetization.PaidSubscription = profileReq.Company.Monetization.PaidSubscription
			}

			if profile.Company.Monetization.InGameAdvertising != profileReq.Company.Monetization.InGameAdvertising {
				profile.Company.Monetization.InGameAdvertising = profileReq.Company.Monetization.InGameAdvertising
			}

			if profile.Company.Monetization.InGamePurchases != profileReq.Company.Monetization.InGamePurchases {
				profile.Company.Monetization.InGamePurchases = profileReq.Company.Monetization.InGamePurchases
			}

			if profile.Company.Monetization.PremiumAccess != profileReq.Company.Monetization.PremiumAccess {
				profile.Company.Monetization.PremiumAccess = profileReq.Company.Monetization.PremiumAccess
			}

			if profile.Company.Monetization.Other != profileReq.Company.Monetization.Other {
				profile.Company.Monetization.Other = profileReq.Company.Monetization.Other
			}
		}

		if profileReq.HasCompanyPlatformsChanges(profile) {
			if profile.Company.Platforms == nil {
				profile.Company.Platforms = &grpc.UserProfileCompanyPlatforms{}
			}

			if profile.Company.Platforms.PcMac != profileReq.Company.Platforms.PcMac {
				profile.Company.Platforms.PcMac = profileReq.Company.Platforms.PcMac
			}

			if profile.Company.Platforms.GameConsole != profileReq.Company.Platforms.GameConsole {
				profile.Company.Platforms.GameConsole = profileReq.Company.Platforms.GameConsole
			}

			if profile.Company.Platforms.MobileDevice != profileReq.Company.Platforms.MobileDevice {
				profile.Company.Platforms.MobileDevice = profileReq.Company.Platforms.MobileDevice
			}

			if profile.Company.Platforms.WebBrowser != profileReq.Company.Platforms.WebBrowser {
				profile.Company.Platforms.WebBrowser = profileReq.Company.Platforms.WebBrowser
			}

			if profile.Company.Platforms.Other != profileReq.Company.Platforms.Other {
				profile.Company.Platforms.Other = profileReq.Company.Platforms.Other
			}
		}
	}

	if profile.LastStep != profileReq.LastStep {
		profile.LastStep = profileReq.LastStep
	}

	return profile
}

func (s *Service) ConfirmUserEmail(
	ctx context.Context,
	req *grpc.ConfirmUserEmailRequest,
	rsp *grpc.ConfirmUserEmailResponse,
) error {
	userId, err := s.getUserEmailConfirmationToken(req.Token)

	if err != nil || userId == "" {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileEmailConfirmationTokenNotFound

		return nil
	}

	rsp.Profile, err = s.userProfileRepository.GetByUserId(ctx, userId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	if rsp.Profile.IsEmailVerified() {
		rsp.Status = pkg.ResponseStatusOk

		return nil
	}

	err = s.emailConfirmedSuccessfully(ctx, rsp.Profile)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	return nil
}

func (s *Service) setUserEmailConfirmationToken(profile *grpc.UserProfile) (string, error) {
	stToken := &EmailConfirmToken{
		Token:     s.getTokenString(s.cfg.Length),
		ProfileId: profile.Id,
		CreatedAt: time.Now(),
	}

	b, err := json.Marshal(stToken)

	if err != nil {
		zap.S().Error(
			"Confirm email token marshaling failed",
			zap.Error(err),
			zap.Any("profile", profile),
		)

		return "", err
	}

	hash := sha512.New()
	hash.Write(b)

	token := strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
	err = s.redis.Set(s.getConfirmEmailStorageKey(token), profile.UserId, s.cfg.GetEmailConfirmTokenLifetime()).Err()

	if err != nil {
		zap.S().Error(
			"Save confirm email token to Redis failed",
			zap.Error(err),
			zap.Any("profile", profile),
		)

		return "", err
	}

	return s.cfg.GetUserConfirmEmailUrl(map[string]string{"token": token}), nil
}

func (s *Service) getUserEmailConfirmationToken(token string) (string, error) {
	data, err := s.redis.Get(s.getConfirmEmailStorageKey(token)).Result()

	if err != nil {
		zap.S().Error(
			"Getting user email confirmation token failed",
			zap.Error(err),
			zap.String("token", token),
		)
	}

	return data, err
}

func (s *Service) sendUserEmailConfirmationToken(ctx context.Context, profile *grpc.UserProfile) error {
	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: s.cfg.EmailTemplates.ConfirmAccount,
		TemplateModel: map[string]string{
			"confirm_url": profile.Email.ConfirmationUrl,
		},
		To: profile.Email.Email,
	}

	err := s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

	if err != nil {
		zap.S().Error(
			"Publication message to user email confirmation to queue failed",
			zap.Error(err),
			zap.Any("profile", profile),
		)

		return err
	}

	profile.Email.IsConfirmationEmailSent = true

	if err = s.userProfileRepository.Update(ctx, profile); err != nil {
		return err
	}

	return nil
}

func (s *Service) getConfirmEmailStorageKey(token string) string {
	return fmt.Sprintf(userEmailConfirmTokenStorage, token)
}

func (s *Service) emailConfirmedSuccessfully(ctx context.Context, profile *grpc.UserProfile) error {
	profile.Email.Confirmed = true
	profile.Email.ConfirmedAt = ptypes.TimestampNow()

	if err := s.userProfileRepository.Update(ctx, profile); err != nil {
		return err
	}

	msg := map[string]string{"code": "op000005", "message": "user email confirmed successfully"}
	ch := fmt.Sprintf(s.cfg.CentrifugoUserChannel, profile.Id)

	return s.centrifugoDashboard.Publish(ctx, ch, msg)
}

func (s *Service) emailConfirmedTruncate(ctx context.Context, profile *grpc.UserProfile) error {
	profile.Email.Confirmed = false
	profile.Email.ConfirmedAt = nil

	if err := s.userProfileRepository.Update(ctx, profile); err != nil {
		return err
	}

	msg := map[string]string{"code": "op000005", "message": "user email confirmed successfully"}
	ch := fmt.Sprintf(s.cfg.CentrifugoUserChannel, profile.Id)

	return s.centrifugoDashboard.Publish(ctx, ch, msg)
}

func (s *Service) CreatePageReview(
	ctx context.Context,
	req *grpc.CreatePageReviewRequest,
	rsp *grpc.CheckProjectRequestSignatureResponse,
) error {
	review := &grpc.PageReview{
		Id:        primitive.NewObjectID().Hex(),
		UserId:    req.UserId,
		Review:    req.Review,
		Url:       req.Url,
		CreatedAt: ptypes.TimestampNow(),
	}

	_, err := s.db.Collection(collectionOPageReview).InsertOne(ctx, review)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOPageReview),
			zap.Any("data", review),
		)

		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetCommonUserProfile(
	ctx context.Context,
	req *grpc.CommonUserProfileRequest,
	rsp *grpc.CommonUserProfileResponse,
) error {
	profile, err := s.userProfileRepository.GetByUserId(ctx, req.UserId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	rsp.Profile = &grpc.CommonUserProfile{
		Profile: profile,
	}

	expire := time.Now().Add(time.Minute * 30).Unix()
	rsp.Profile.Profile.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(profile.Id, expire)

	role := s.findRoleForUser(ctx, req.MerchantId, req.UserId)

	if role != nil {
		rsp.Profile.Role = role
		rsp.Profile.Merchant, _ = s.merchant.GetById(ctx, role.MerchantId)
		rsp.Profile.Merchant.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(
			rsp.Profile.Merchant.Id,
			time.Now().Add(time.Hour*3).Unix(),
		)
		rsp.Profile.Merchant.HasProjects = s.getProjectsCountByMerchant(ctx, rsp.Profile.Merchant.Id) > 0

		if role.Role != pkg.RoleMerchantOwner &&
			role.Role != pkg.RoleMerchantAccounting &&
			role.Role != pkg.RoleMerchantDeveloper {
			merchant := &billing.Merchant{
				Id:          rsp.Profile.Merchant.Id,
				Company:     &billing.MerchantCompanyInfo{Name: rsp.Profile.Merchant.Company.Name},
				Banking:     &billing.MerchantBanking{Currency: rsp.Profile.Merchant.Banking.Currency},
				Status:      rsp.Profile.Merchant.Status,
				HasProjects: rsp.Profile.Merchant.HasProjects,
			}
			rsp.Profile.Merchant = merchant
		}
	} else {
		rsp.Profile.Role, _ = s.userRoleRepository.GetAdminUserByUserId(ctx, req.UserId)
	}

	if rsp.Profile.Role != nil {
		rsp.Profile.Permissions, err = s.getUserPermissions(ctx, req.UserId, rsp.Profile.Role.MerchantId)

		if err != nil {
			zap.S().Error(
				"unable to get user permissions",
				zap.Error(err),
				zap.Any("req", req),
			)
		}
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) findRoleForUser(ctx context.Context, merchantId string, userId string) *billing.UserRole {
	if merchantId != "" {
		role, _ := s.userRoleRepository.GetMerchantUserByUserId(ctx, merchantId, userId)
		return role
	}

	roles, _ := s.userRoleRepository.GetMerchantsForUser(ctx, userId)
	if len(roles) > 0 {
		return roles[0]
	}

	return nil
}
