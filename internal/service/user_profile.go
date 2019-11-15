package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"strings"
	"time"
)

const (
	collectionUserProfile        = "user_profile"
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

	profile, err := s.userProfileRepository.GetByUserId(req.UserId)

	if profile == nil {
		profile = req
		profile.Id = bson.NewObjectId().Hex()
		profile.CreatedAt = ptypes.TimestampNow()
	} else {
		profile = s.updateOnboardingProfile(profile, req)
	}

	profile.UpdatedAt = ptypes.TimestampNow()
	profile.CentrifugoToken, err = s.getUserCentrifugoToken(profile)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	_, err = s.db.Collection(collectionUserProfile).UpsertId(bson.ObjectIdHex(profile.Id), profile)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionUserProfile),
			zap.Any(pkg.ErrorDatabaseFieldQuery, profile),
		)

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

		err = s.sendUserEmailConfirmationToken(profile)

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
		profile, err = s.userProfileRepository.GetById(req.ProfileId)
	} else {
		profile, err = s.userProfileRepository.GetByUserId(req.UserId)
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	centrifugoToken, err := s.getUserCentrifugoToken(profile)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

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

func (s *Service) getUserCentrifugoToken(profile *grpc.UserProfile) (string, error) {
	expire := time.Now().Add(time.Minute * 30).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": profile.Id, "exp": expire})
	centrifugoToken, err := token.SignedString([]byte(s.cfg.CentrifugoSecret))

	if err != nil {
		zap.S().Error(
			"Signing centrifugo token string failed",
			zap.Error(err),
			zap.Any("profile", profile),
		)
	}

	return centrifugoToken, err
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

	rsp.Profile, err = s.userProfileRepository.GetByUserId(userId)

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

func (s *Service) sendUserEmailConfirmationToken(profile *grpc.UserProfile) error {
	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: s.cfg.EmailConfirmTemplate,
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

	query := bson.M{"_id": bson.ObjectIdHex(profile.Id)}
	set := bson.M{"$set": bson.M{"email.is_confirmation_email_sent": true}}
	err = s.db.Collection(collectionUserProfile).Update(query, set)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionUserProfile),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSet, set),
		)

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

	err := s.db.Collection(collectionUserProfile).UpdateId(bson.ObjectIdHex(profile.Id), profile)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionUserProfile),
			zap.Any("query", profile),
		)

		return err
	}

	msg := map[string]string{"code": "op000005", "message": "user email confirmed successfully"}
	ch := fmt.Sprintf(s.cfg.CentrifugoUserChannel, profile.Id)

	return s.centrifugo.Publish(ctx, ch, msg)
}

func (s *Service) CreatePageReview(
	ctx context.Context,
	req *grpc.CreatePageReviewRequest,
	rsp *grpc.CheckProjectRequestSignatureResponse,
) error {
	review := &grpc.PageReview{
		Id:        bson.NewObjectId().Hex(),
		UserId:    req.UserId,
		Review:    req.Review,
		Url:       req.Url,
		CreatedAt: ptypes.TimestampNow(),
	}

	err := s.db.Collection(collectionOPageReview).Insert(review)

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
	profile, err := s.userProfileRepository.GetByUserId(req.UserId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	rsp.Profile = &grpc.CommonUserProfile{
		Profile: profile,
	}

	if req.MerchantId != "" {
		rsp.Profile.Role, _ = s.userRoleRepository.GetMerchantUserByUserId(req.MerchantId, req.UserId)
		rsp.Profile.Merchant, _ = s.merchant.GetCommonById(req.MerchantId)
	} else {
		rsp.Profile.Role, _ = s.userRoleRepository.GetAdminUserByUserId(req.UserId)
	}

	if rsp.Profile.Role != nil {
		rsp.Profile.Permissions, err = s.getUserPermissions(ctx, req.UserId, req.MerchantId)

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

type UserProfileRepositoryInterface interface {
	GetById(string) (*grpc.UserProfile, error)
	GetByUserId(string) (*grpc.UserProfile, error)
}

func newUserProfileRepository(svc *Service) UserProfileRepositoryInterface {
	s := &UserProfileRepository{svc: svc}
	return s
}

func (r *UserProfileRepository) GetById(id string) (*grpc.UserProfile, error) {
	var c *grpc.UserProfile

	err := r.svc.db.Collection(collectionUserProfile).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionUserProfile),
		)
		return nil, fmt.Errorf(errorNotFound, collectionUserProfile)
	}

	return c, nil
}

func (r *UserProfileRepository) GetByUserId(userId string) (*grpc.UserProfile, error) {
	var c *grpc.UserProfile

	err := r.svc.db.Collection(collectionUserProfile).Find(bson.M{"user_id": userId}).One(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionUserProfile),
		)
		return nil, fmt.Errorf(errorNotFound, collectionUserProfile)
	}

	return c, nil
}
