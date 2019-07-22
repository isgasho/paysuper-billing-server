package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"strings"
	"time"
)

const (
	collectionUserProfile        = "user_profile"
	collectionOPageReview        = "page_review"
	userEmailConfirmTokenStorage = "email_confirm:token:%s"
)

var (
	userProfileErrorNotFound                  = newBillingServerErrorMsg("op000001", "user profile not found")
	userProfileErrorUnknown                   = newBillingServerErrorMsg("op000002", "unknown error. try request later")
	userProfileEmailConfirmed                 = newBillingServerErrorMsg("op000003", "user email already confirmed")
	userProfileEmailConfirmationTokenNotFound = newBillingServerErrorMsg("op000004", "user email confirmation token not found")
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

	profile := s.getOnboardingProfileByUser(req.UserId)

	if profile == nil {
		profile = req
		profile.Id = bson.NewObjectId().Hex()
		profile.CreatedAt = ptypes.TimestampNow()
		profile.UpdatedAt = ptypes.TimestampNow()

		err = s.db.Collection(collectionUserProfile).Insert(profile)
	} else {
		profile, err = s.updateOnboardingProfile(profile, req)
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	s.getUserCentrifugoToken(profile)

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) GetUserProfile(
	ctx context.Context,
	req *grpc.GetUserProfileRequest,
	rsp *grpc.GetUserProfileResponse,
) error {
	profile := s.getOnboardingProfileByUser(req.UserId)

	if profile == nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	s.getUserCentrifugoToken(profile)

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) getOnboardingProfileByUser(userId string) (profile *grpc.UserProfile) {
	query := bson.M{"user_id": userId}
	err := s.db.Collection(collectionUserProfile).Find(query).One(&profile)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionUserProfile),
			zap.Any("query", query),
		)
	}

	return profile
}

func (s *Service) updateOnboardingProfile(
	profile, profileReq *grpc.UserProfile,
) (*grpc.UserProfile, error) {
	if profileReq.HasPersonChanges(profile) == true {
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

	if profileReq.HasHelpChanges(profile) == true {
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

	if profileReq.HasCompanyChanges(profile) == true {
		if profile.Company == nil {
			profile.Company = &grpc.UserProfileCompany{}
		}

		if profile.Company.CompanyName != profileReq.Company.CompanyName {
			profile.Company.CompanyName = profileReq.Company.CompanyName
		}

		if profile.Company.Website != profileReq.Company.Website {
			profile.Company.Website = profileReq.Company.Website
		}

		if profileReq.HasCompanyAnnualIncomeChanges(profile) == true {
			if profile.Company.AnnualIncome == nil {
				profile.Company.AnnualIncome = &grpc.RangeInt{}
			}

			if profile.Company.AnnualIncome.From != profileReq.Company.AnnualIncome.From {
				profile.Company.AnnualIncome.From = profileReq.Company.AnnualIncome.From
			}

			if profile.Company.AnnualIncome.To != profileReq.Company.AnnualIncome.To {
				profile.Company.AnnualIncome.To = profileReq.Company.AnnualIncome.To
			}
		}

		if profileReq.HasCompanyNumberOfEmployeesChanges(profile) == true {
			if profile.Company.NumberOfEmployees == nil {
				profile.Company.NumberOfEmployees = &grpc.RangeInt{}
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

		if profileReq.HasCompanyMonetizationChanges(profile) == true {
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

		if profileReq.HasCompanyPlatformsChanges(profile) == true {
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

	profile.UpdatedAt = ptypes.TimestampNow()

	err := s.db.Collection(collectionUserProfile).UpdateId(bson.ObjectIdHex(profile.Id), profile)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionUserProfile),
			zap.Any("profile", profile),
		)

		return nil, err
	}

	return profile, nil
}

func (s *Service) getUserCentrifugoToken(profile *grpc.UserProfile) {
	expire := time.Now().Add(time.Minute * 30).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": profile.Id, "exp": expire})
	profile.CentrifugoToken, _ = token.SignedString([]byte(s.cfg.CentrifugoSecret))
}

func (s *Service) SendConfirmEmailToUser(
	ctx context.Context,
	req *grpc.SendConfirmEmailToUserRequest,
	rsp *grpc.GetUserProfileResponse,
) error {
	var err error

	profile := s.getOnboardingProfileByUser(req.UserId)

	if profile == nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileErrorNotFound

		return nil
	}

	if profile.IsEmailVerified() == true {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = userProfileEmailConfirmed

		return nil
	}

	profile.Email.ConfirmationUrl, err = s.setUserEmailConfirmationToken(req.Url, profile)

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

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) ConfirmUserEmail(
	ctx context.Context,
	req *grpc.ConfirmUserEmailRequest,
	rsp *grpc.CheckProjectRequestSignatureResponse,
) error {
	userId, err := s.getUserEmailConfirmationToken(req.Token)

	if err != nil || userId == "" {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = userProfileEmailConfirmationTokenNotFound

		return nil
	}

	profile := s.getOnboardingProfileByUser(userId)

	if profile == nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	err = s.emailConfirmedSuccessfully(ctx, profile)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = userProfileErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) setUserEmailConfirmationToken(url string, profile *grpc.UserProfile) (string, error) {
	stToken := &EmailConfirmToken{
		Token:     s.getTokenString(s.cfg.Length),
		ProfileId: profile.Id,
		CreatedAt: time.Now(),
	}

	b, _ := json.Marshal(stToken)
	hash := sha512.New()
	hash.Write(b)

	token := strings.ToUpper(hex.EncodeToString(hash.Sum(nil)))
	err := s.redis.Set(s.getConfirmEmailStorageKey(token), profile.UserId, s.cfg.GetEmailConfirmTokenLifetime()).Err()

	if err != nil {
		zap.L().Error(
			"Save confirm email token to Redis failed",
			zap.Error(err),
			zap.Any("profile", profile),
		)

		return "", err
	}

	return url + "?token=" + token, nil
}

func (s *Service) getUserEmailConfirmationToken(token string) (string, error) {
	data, err := s.redis.Get(s.getConfirmEmailStorageKey(token)).Result()

	if err != nil {
		zap.L().Error(
			"Getting user email confirmation token failed",
			zap.Error(err),
			zap.String("token", token),
		)
	}

	return data, err
}

func (s *Service) sendUserEmailConfirmationToken(profile *grpc.UserProfile) error {
	err := s.broker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, profile, amqp.Table{})

	if err != nil {
		zap.L().Error(
			"Publication message to user email confirmation to queue failed",
			zap.Error(err),
			zap.Any("profile", profile),
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
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionUserProfile),
			zap.Any("query", profile),
		)

		return err
	}

	msg := map[string]interface{}{"code": "op000005", "message": "user email confirmed successfully"}
	b, _ := json.Marshal(msg)

	ch := fmt.Sprintf(s.cfg.CentrifugoUserChannel, profile.Id)
	err = s.centrifugoClient.Publish(ctx, ch, b)

	if err != nil {
		zap.L().Error(
			"Send message to centrifugo failed",
			zap.Error(err),
			zap.String("channel", ch),
			zap.Any("message", msg),
		)
	}

	return err
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
		PageId:    req.PageId,
		CreatedAt: ptypes.TimestampNow(),
	}

	err := s.db.Collection(collectionOPageReview).Insert(review)

	if err != nil {
		zap.L().Error(
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
