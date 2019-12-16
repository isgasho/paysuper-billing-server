package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/divan/num2words"
	"github.com/golang/protobuf/ptypes"
	"github.com/micro/go-micro/client"
	casbinProto "github.com/paysuper/casbin-server/pkg/generated/api/proto/casbinpb"
	documentSignerConst "github.com/paysuper/document-signer/pkg/constant"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	reporterConst "github.com/paysuper/paysuper-reporter/pkg"
	reporterProto "github.com/paysuper/paysuper-reporter/pkg/proto"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"strings"
	"time"
)

const (
	collectionNotification = "notification"

	merchantStatusSigningMessage  = "We've got your license agreement signing request. If we will need your further assistance, processing this request, our onboarding manager will contact you directly."
	merchantStatusSignedMessage   = "Your license agreement signing request is confirmed and document is signed by Pay Super. Let us have productive cooperation!"
	merchantStatusRejectedMessage = "Your license agreement signing request was confirmed as SPAM and will be no longer processed."
	merchantStatusDeletedMessage  = "Sorry, but while processing your license agreement signing request we encountered insuperable obstacles which lead to termination of our deal."
	merchantStatusPendingMessage  = "We've got your onboarding request. We will check information about your company and return with resolution."
	merchantStatusAcceptedMessage = "Preliminary verification is complete and now you are welcome to sign the license agreement."
)

var (
	merchantErrorChangeNotAllowed             = newBillingServerErrorMsg("mr000001", "merchant data changing not allowed")
	merchantErrorCountryNotFound              = newBillingServerErrorMsg("mr000002", "merchant country not found")
	merchantErrorCurrencyNotFound             = newBillingServerErrorMsg("mr000003", "merchant bank accounting currency not found")
	merchantErrorUnknown                      = newBillingServerErrorMsg("mr000008", "request processing failed. try request later")
	merchantErrorNotFound                     = newBillingServerErrorMsg("mr000009", "merchant with specified identifier not found")
	merchantErrorBadData                      = newBillingServerErrorMsg("mr000010", "request data is incorrect")
	notificationErrorMerchantIdIncorrect      = newBillingServerErrorMsg("mr000012", "merchant identifier incorrect, notification can't be saved")
	notificationErrorMessageIsEmpty           = newBillingServerErrorMsg("mr000014", "notification message can't be empty")
	notificationErrorNotFound                 = newBillingServerErrorMsg("mr000015", "notification not found")
	merchantErrorOnboardingNotComplete        = newBillingServerErrorMsg("mr000019", "merchant onboarding not complete")
	merchantErrorOnboardingTariffAlreadyExist = newBillingServerErrorMsg("mr000020", "merchant tariffs already sets")
	merchantStatusChangeNotPossible           = newBillingServerErrorMsg("mr000021", "change status not possible by merchant flow")
	merchantNotificationSettingNotFound       = newBillingServerErrorMsg("mr000022", "setting for create notification for status change not found")
	merchantTariffsNotFound                   = newBillingServerErrorMsg("mr000023", "tariffs for merchant not found")
	merchantPayoutCurrencyMissed              = newBillingServerErrorMsg("mr000024", "merchant don't have payout currency")
	merchantErrorOperationsTypeNotSupported   = newBillingServerErrorMsg("mr000025", "merchant operations type not supported")
	merchantErrorCurrencyNotSet               = newBillingServerErrorMsg("mr000027", "merchant payout currency not set")
	merchantErrorNoTariffsInPayoutCurrency    = newBillingServerErrorMsg("mr000028", "no tariffs found for merchant payout currency")
	merchantUnableToAddMerchantUserRole       = newBillingServerErrorMsg("mr000029", "unable to add user role to merchant")

	merchantSignAgreementMessage        = map[string]string{"code": "mr000017", "message": "license agreement was signed by merchant"}
	merchantAgreementReadyToSignMessage = map[string]interface{}{"code": "mr000025", "generated": true, "message": "merchant license agreement ready to sign"}

	merchantStatusChangesMessages = map[int32]string{
		pkg.MerchantStatusAgreementSigning: merchantStatusSigningMessage,
		pkg.MerchantStatusAgreementSigned:  merchantStatusSignedMessage,
		pkg.MerchantStatusDeleted:          merchantStatusDeletedMessage,
		pkg.MerchantStatusRejected:         merchantStatusRejectedMessage,
		pkg.MerchantStatusPending:          merchantStatusPendingMessage,
		pkg.MerchantStatusAccepted:         merchantStatusAcceptedMessage,
	}
)

func (s *Service) GetMerchantBy(
	ctx context.Context,
	req *grpc.GetMerchantByRequest,
	rsp *grpc.GetMerchantResponse,
) error {
	if req.MerchantId == "" && req.UserId == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorBadData

		return nil
	}

	query := make(bson.M)

	if req.MerchantId != "" {
		query["_id"], _ = primitive.ObjectIDFromHex(req.MerchantId)
	}

	if req.UserId != "" {
		query["user.id"] = req.UserId
	}

	merchant, err := s.getMerchantBy(ctx, query)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = e

			if err != merchantErrorNotFound {
				rsp.Status = pkg.ResponseStatusBadData
			}

			return nil
		}
		return err
	}

	merchant.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(
		s.cfg.CentrifugoDashboard.Secret,
		merchant.Id,
		time.Now().Add(time.Hour*3).Unix(),
	)
	merchant.HasProjects = s.getProjectsCountByMerchant(ctx, merchant.Id) > 0

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ListMerchants(
	ctx context.Context,
	req *grpc.MerchantListingRequest,
	rsp *grpc.MerchantListingResponse,
) error {
	var merchants []*billing.Merchant
	query := make(bson.M)

	if req.QuickSearch != "" {
		query["$or"] = []bson.M{
			{"company.name": primitive.Regex{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
			{"user.email": primitive.Regex{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
			{"user.first_name": primitive.Regex{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
			{"user.last_name": primitive.Regex{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
		}
	} else {
		if req.Name != "" {
			query["company.name"] = primitive.Regex{Pattern: ".*" + req.Name + ".*", Options: "i"}
		}

		if req.LastPayoutDateFrom > 0 || req.LastPayoutDateTo > 0 {
			payoutDates := make(bson.M)

			if req.LastPayoutDateFrom > 0 {
				payoutDates["$gte"] = time.Unix(req.LastPayoutDateFrom, 0)
			}

			if req.LastPayoutDateTo > 0 {
				payoutDates["$lte"] = time.Unix(req.LastPayoutDateTo, 0)
			}

			query["last_payout.date"] = payoutDates
		}

		if req.IsSigned > 0 {
			if req.IsSigned == 1 {
				query["is_signed"] = false
			} else {
				query["is_signed"] = true
			}
		}

		if req.LastPayoutAmount > 0 {
			query["last_payout.amount"] = req.LastPayoutAmount
		}

		if req.RegistrationDateFrom > 0 || req.RegistrationDateTo > 0 {
			regDates := bson.M{}

			if req.RegistrationDateFrom > 0 {
				regDates["$gte"] = time.Unix(req.RegistrationDateFrom, 0)
			}

			if req.RegistrationDateTo > 0 {
				regDates["$lte"] = time.Unix(req.RegistrationDateTo, 0)
			}

			query["user.registration_date"] = regDates
		}

		if req.ReceivedDateFrom > 0 || req.ReceivedDateTo > 0 {
			dates := bson.M{}

			if req.ReceivedDateFrom > 0 {
				dates["$gte"] = time.Unix(req.ReceivedDateFrom, 0)
			}

			if req.ReceivedDateTo > 0 {
				dates["$lte"] = time.Unix(req.ReceivedDateTo, 0)
			}

			query["received_date"] = dates
		}
	}

	if len(req.Statuses) > 0 {
		query["status"] = bson.M{"$in": req.Statuses}
	}

	count, err := s.db.Collection(collectionMerchant).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return merchantErrorUnknown
	}

	opts := options.Find().
		SetSort(mongodb.ToSortOption(req.Sort)).
		SetLimit(req.Limit).
		SetSkip(req.Offset)
	cursor, err := s.db.Collection(collectionMerchant).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return merchantErrorUnknown
	}

	err = cursor.All(ctx, &merchants)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return merchantErrorUnknown
	}

	rsp.Count = count
	rsp.Items = []*billing.Merchant{}

	if len(merchants) > 0 {
		rsp.Items = merchants
	}

	return nil
}

func (s *Service) ChangeMerchant(
	ctx context.Context,
	req *grpc.OnboardingRequest,
	rsp *grpc.ChangeMerchantResponse,
) error {
	var (
		merchant      *billing.Merchant
		err           error
		isNewMerchant bool
	)

	if req.HasIdentificationFields() {
		query := make(bson.M)

		if req.Id != "" && req.User != nil && req.User.Id != "" {
			oid, _ := primitive.ObjectIDFromHex(req.Id)
			query["$or"] = []bson.M{{"_id": oid}, {"user.id": req.User.Id}}
		} else {
			if req.Id != "" {
				query["_id"], _ = primitive.ObjectIDFromHex(req.Id)
			}

			if req.User != nil && req.User.Id != "" {
				query["user.id"] = req.User.Id
			}
		}

		merchant, err = s.getMerchantBy(ctx, query)

		if err != nil && err != merchantErrorNotFound {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = err.(*grpc.ResponseErrorMessage)

			return nil
		}
	}

	if merchant == nil {
		merchant = &billing.Merchant{
			Id:                 primitive.NewObjectID().Hex(),
			User:               req.User,
			MinimalPayoutLimit: pkg.MerchantMinimalPayoutLimit,
			Status:             pkg.MerchantStatusDraft,
			CreatedAt:          ptypes.TimestampNow(),
		}
		merchant.AgreementNumber = s.getMerchantAgreementNumber(merchant.Id)
		isNewMerchant = true
	}

	if !s.IsChangeDataAllow(merchant, req) {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = merchantErrorChangeNotAllowed

		return nil
	}

	if req.Company != nil {
		_, err := s.country.GetByIsoCodeA2(ctx, req.Company.Country)

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = merchantErrorCountryNotFound

			return nil
		}

		merchant.Company = req.Company
	}

	if req.Banking != nil {
		if req.Banking.Currency != "" {
			if !contains(s.supportedCurrencies, req.Banking.Currency) {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = merchantErrorCurrencyNotFound
				return nil
			}
		}

		if req.Banking.AccountNumber != "" {
			req.Banking.AccountNumber = strings.Join(strings.Fields(req.Banking.AccountNumber), "")
		}

		merchant.Banking = req.Banking
	}

	if req.Contacts != nil {
		merchant.Contacts = req.Contacts
	}

	if merchant.IsDataComplete() {
		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationMerchant, merchant.GetAuthorizedEmail())
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationAdmin, s.cfg.EmailOnboardingAdminRecipient)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	merchant.UpdatedAt = ptypes.TimestampNow()

	if merchant.Steps == nil {
		merchant.Steps = &billing.MerchantCompletedSteps{}
	}

	merchant.Steps.Company = merchant.IsCompanyComplete()
	merchant.Steps.Contacts = merchant.IsContactsComplete()
	merchant.Steps.Banking = merchant.IsBankingComplete()

	if !merchant.HasPrimaryOnboardingUserName() {
		profile, _ := s.userProfileRepository.GetByUserId(ctx, req.User.Id)

		if profile != nil {
			merchant.User.ProfileId = profile.Id

			if profile.IsPersonalComplete() {
				merchant.User.FirstName = profile.Personal.FirstName
				merchant.User.LastName = profile.Personal.LastName
			}

			if profile.IsEmailVerified() {
				merchant.User.RegistrationDate = profile.Email.ConfirmedAt
			}
		}
	}

	err = s.merchant.Upsert(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	if isNewMerchant == true {
		err = s.userRoleRepository.AddMerchantUser(
			ctx,
			&billing.UserRole{
				Id:         primitive.NewObjectID().Hex(),
				MerchantId: merchant.Id,
				Status:     pkg.UserRoleStatusAccepted,
				Role:       pkg.RoleMerchantOwner,
				UserId:     merchant.User.Id,
				Email:      merchant.User.Email,
				FirstName:  merchant.User.FirstName,
				LastName:   merchant.User.LastName,
			},
		)

		if err == nil {
			_, err = s.casbinService.AddRoleForUser(ctx, &casbinProto.UserRoleRequest{
				User: fmt.Sprintf(pkg.CasbinMerchantUserMask, merchant.Id, merchant.User.Id),
				Role: pkg.RoleMerchantOwner,
			})
		}
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantUnableToAddMerchantUserRole

		return nil
	}

	merchant.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(
		s.cfg.CentrifugoDashboard.Secret,
		merchant.Id,
		time.Now().Add(time.Hour*3).Unix(),
	)

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantStatus(
	ctx context.Context,
	req *grpc.MerchantChangeStatusRequest,
	rsp *grpc.ChangeMerchantStatusResponse,
) error {
	merchant, err := s.merchant.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	if !merchant.CanChangeStatusTo(req.Status) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantStatusChangeNotPossible

		return nil
	}

	statusChange := &billing.SystemNotificationStatuses{From: merchant.Status, To: req.Status}
	message, ok := merchantStatusChangesMessages[req.Status]

	if !ok {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound

		return nil
	}

	merchant.Status = req.Status
	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	err = s.merchant.Update(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantData(
	ctx context.Context,
	req *grpc.ChangeMerchantDataRequest,
	rsp *grpc.ChangeMerchantDataResponse,
) error {
	merchant, err := s.merchant.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	if merchant.IsSigned && merchant.HasMerchantSignature && merchant.HasPspSignature {
		rsp.Status = pkg.ResponseStatusOk
		rsp.Item = merchant
		return nil
	}

	if !merchant.HasPspSignature && req.HasPspSignature {
		merchant.HasPspSignature = req.HasPspSignature
	}

	if !merchant.HasMerchantSignature && req.HasMerchantSignature {
		merchant.ReceivedDate = ptypes.TimestampNow()
		merchant.HasMerchantSignature = req.HasMerchantSignature

		_ = s.centrifugoDashboard.Publish(ctx, s.cfg.CentrifugoAdminChannel, merchantSignAgreementMessage)
	}

	merchant.IsSigned = merchant.HasPspSignature == true && merchant.HasMerchantSignature == true
	statusChange := &billing.SystemNotificationStatuses{}

	if merchant.HasMerchantSignature {
		statusChange.From = merchant.Status
		statusChange.To = pkg.MerchantStatusAgreementSigning

		merchant.Status = pkg.MerchantStatusAgreementSigning
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	}

	if merchant.IsSigned {
		statusChange.From = merchant.Status
		statusChange.To = pkg.MerchantStatusAgreementSigned

		merchant.Status = pkg.MerchantStatusAgreementSigned
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	}

	message, ok := merchantStatusChangesMessages[merchant.Status]

	if !ok {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound
		return nil
	}

	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	err = s.merchant.Update(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant
	return nil
}

func (s *Service) SetMerchantOperatingCompany(
	ctx context.Context,
	req *grpc.SetMerchantOperatingCompanyRequest,
	rsp *grpc.SetMerchantOperatingCompanyResponse,
) error {
	merchant, err := s.merchant.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	oc, err := s.operatingCompany.GetById(ctx, req.OperatingCompanyId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	if !merchant.IsDataComplete() {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorOnboardingNotComplete
		return nil
	}

	statusChange := &billing.SystemNotificationStatuses{From: merchant.Status, To: pkg.MerchantStatusAccepted}

	merchant.OperatingCompanyId = oc.Id
	merchant.Status = pkg.MerchantStatusAccepted
	merchant.StatusLastUpdatedAt = ptypes.TimestampNow()

	message, ok := merchantStatusChangesMessages[merchant.Status]

	if !ok {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound

		return nil
	}

	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	err = s.merchant.Update(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	err = s.generateMerchantAgreement(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	err = s.sendOnboardingLetter(merchant, oc, s.cfg.EmailTemplates.OnboardingCompleted, merchant.GetAuthorizedEmail())
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantManualPayouts(
	ctx context.Context,
	req *grpc.ChangeMerchantManualPayoutsRequest,
	rsp *grpc.ChangeMerchantManualPayoutsResponse,
) error {
	merchant, err := s.merchant.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	if merchant.ManualPayoutsEnabled == req.ManualPayoutsEnabled {
		rsp.Status = pkg.ResponseStatusNotModified
		return nil
	}

	merchant.ManualPayoutsEnabled = req.ManualPayoutsEnabled

	err = s.merchant.Update(ctx, merchant)
	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) SetMerchantS3Agreement(
	ctx context.Context,
	req *grpc.SetMerchantS3AgreementRequest,
	rsp *grpc.ChangeMerchantDataResponse,
) error {
	merchant, err := s.merchant.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	merchant.S3AgreementName = req.S3AgreementName

	if merchant.AgreementSignatureData == nil {
		merchant.AgreementSignatureData, err = s.getMerchantAgreementSignature(ctx, merchant)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = err.(*grpc.ResponseErrorMessage)

			return nil
		}
	}

	err = s.merchant.Update(ctx, merchant)

	if err != nil {
		return merchantErrorUnknown
	}

	channel := s.getMerchantCentrifugoChannel(merchant.Id)
	err = s.centrifugoDashboard.Publish(ctx, channel, merchantAgreementReadyToSignMessage)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) CreateNotification(
	ctx context.Context,
	req *grpc.NotificationRequest,
	rsp *grpc.CreateNotificationResponse,
) error {
	rsp.Status = pkg.ResponseStatusOk

	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	_, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	if req.Message == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = notificationErrorMessageIsEmpty
		return nil
	}

	n, err := s.addNotification(ctx, req.Message, req.MerchantId, req.UserId, nil)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Item = n

	return nil
}

func (s *Service) GetNotification(
	ctx context.Context,
	req *grpc.GetNotificationRequest,
	rsp *billing.Notification,
) error {
	notification, err := s.getNotificationById(ctx, req.MerchantId, req.NotificationId)

	if err != nil {
		return err
	}

	s.mapNotificationData(rsp, notification)

	return nil
}

func (s *Service) ListNotifications(
	ctx context.Context,
	req *grpc.ListingNotificationRequest,
	rsp *grpc.Notifications,
) error {
	var notifications []*billing.Notification

	query := make(bson.M)

	if req.MerchantId != "" {
		merchantOid, err := primitive.ObjectIDFromHex(req.MerchantId)

		if err == nil {
			query["merchant_id"] = merchantOid
		}
	}

	if req.UserId != "" {
		query["user_id"] = req.UserId
	}

	if req.IsSystem > 0 {
		if req.IsSystem == 1 {
			query["is_system"] = false
		} else {
			query["is_system"] = true
		}
	}

	count, err := s.db.Collection(collectionNotification).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to count merchant notifications failed", "err", err.Error(), "query", query)
		return merchantErrorUnknown
	}

	opts := options.Find().
		SetSort(mongodb.ToSortOption(req.Sort)).
		SetLimit(req.Limit).
		SetSkip(req.Offset)
	cursor, err := s.db.Collection(collectionNotification).Find(ctx, query, opts)

	if err != nil {
		if err != mongo.ErrNoDocuments {
			zap.L().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionNotification),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
			return merchantErrorUnknown
		}

		return nil
	}

	err = cursor.All(ctx, &notifications)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionNotification),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return merchantErrorUnknown
	}

	rsp.Count = count
	rsp.Items = []*billing.Notification{}

	if len(notifications) > 0 {
		rsp.Items = notifications
	}

	return nil
}

func (s *Service) MarkNotificationAsRead(
	ctx context.Context,
	req *grpc.GetNotificationRequest,
	rsp *billing.Notification,
) error {
	notification, err := s.getNotificationById(ctx, req.MerchantId, req.NotificationId)

	if err != nil {
		return err
	}

	notification.IsRead = true

	oid, _ := primitive.ObjectIDFromHex(notification.Id)
	filter := bson.M{"_id": oid}
	_, err = s.db.Collection(collectionNotification).ReplaceOne(ctx, filter, notification)

	if err != nil {
		zap.S().Errorf("Update notification failed", "err", err.Error(), "query", notification)
		return merchantErrorUnknown
	}

	s.mapNotificationData(rsp, notification)

	return nil
}

func (s *Service) GetMerchantPaymentMethod(
	ctx context.Context,
	req *grpc.GetMerchantPaymentMethodRequest,
	rsp *grpc.GetMerchantPaymentMethodResponse,
) error {
	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	_, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	pms, err := s.merchant.GetPaymentMethod(ctx, req.MerchantId, req.PaymentMethodId)
	if err == nil {
		rsp.Item = pms

		return nil
	}

	pm, err := s.paymentMethod.GetById(ctx, req.PaymentMethodId)

	if err != nil {
		s.logError(
			"Payment method with specified id not found in cache",
			[]interface{}{
				"error", err.Error(),
				"id", req.PaymentMethodId,
			},
		)

		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}

	rsp.Item = &billing.MerchantPaymentMethod{
		PaymentMethod: &billing.MerchantPaymentMethodIdentification{
			Id:   pm.Id,
			Name: pm.Name,
		},
		Commission:  s.getDefaultPaymentMethodCommissions(),
		Integration: &billing.MerchantPaymentMethodIntegration{},
		IsActive:    true,
	}

	return nil
}

func (s *Service) ListMerchantPaymentMethods(
	ctx context.Context,
	req *grpc.ListMerchantPaymentMethodsRequest,
	rsp *grpc.ListingMerchantPaymentMethod,
) error {
	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	_, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		return nil
	}

	var pms []*billing.PaymentMethod

	query := bson.M{"is_active": true}

	if req.PaymentMethodName != "" {
		query["name"] = primitive.Regex{Pattern: ".*" + req.PaymentMethodName + ".*", Options: "i"}
	}

	opts := options.Find().SetSort(mongodb.ToSortOption(req.Sort))
	cursor, err := s.db.Collection(collectionPaymentMethod).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMethod),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil
	}

	err = cursor.All(ctx, &pms)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMethod),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil
	}

	if len(pms) <= 0 {
		return nil
	}

	for _, pm := range pms {
		mPm, err := s.merchant.GetPaymentMethod(ctx, req.MerchantId, pm.Id)

		paymentMethod := &billing.MerchantPaymentMethod{
			PaymentMethod: &billing.MerchantPaymentMethodIdentification{
				Id:   pm.Id,
				Name: pm.Name,
			},
			Commission:  s.getDefaultPaymentMethodCommissions(),
			Integration: &billing.MerchantPaymentMethodIntegration{},
			IsActive:    true,
		}

		if err == nil {
			paymentMethod.Commission = mPm.Commission
			paymentMethod.Integration = mPm.Integration
			paymentMethod.IsActive = mPm.IsActive
		}

		rsp.PaymentMethods = append(rsp.PaymentMethods, paymentMethod)
	}

	return nil
}

func (s *Service) ChangeMerchantPaymentMethod(
	ctx context.Context,
	req *grpc.MerchantPaymentMethodRequest,
	rsp *grpc.MerchantPaymentMethodResponse,
) (err error) {
	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	merchant, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	pm, e := s.paymentMethod.GetById(ctx, req.PaymentMethod.Id)
	if e != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}
	req.Integration.Integrated = req.HasIntegration()

	if req.HasPerTransactionCurrency() {
		if !contains(s.supportedCurrencies, req.GetPerTransactionCurrency()) {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCurrencyNotFound

			return nil
		}
	}

	if len(merchant.PaymentMethods) <= 0 {
		merchant.PaymentMethods = make(map[string]*billing.MerchantPaymentMethod)
	}

	mpm := &billing.MerchantPaymentMethod{
		PaymentMethod: req.PaymentMethod,
		Commission:    req.Commission,
		Integration:   req.Integration,
		IsActive:      req.IsActive,
	}

	merchant.PaymentMethods[pm.Id] = mpm

	// insert in history collection first than really update merchant
	history := &billing.MerchantPaymentMethodHistory{
		Id:            primitive.NewObjectID().Hex(),
		MerchantId:    merchant.Id,
		UserId:        req.UserId,
		CreatedAt:     ptypes.TimestampNow(),
		PaymentMethod: mpm,
	}
	_, err = s.db.Collection(collectionMerchantPaymentMethodHistory).InsertOne(ctx, history)
	if err != nil {
		zap.S().Errorf("Query to update merchant payment methods history", "err", err.Error(), "data", merchant)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	if err := s.merchant.Update(ctx, merchant); err != nil {
		zap.S().Errorf("Query to update merchant payment methods failed", "err", err.Error(), "data", merchant)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant.PaymentMethods[pm.Id]

	return nil
}

func (s *Service) getMerchantBy(ctx context.Context, query bson.M) (*billing.Merchant, error) {
	var merchant *billing.Merchant
	err := s.db.Collection(collectionMerchant).FindOne(ctx, query).Decode(&merchant)

	if err != nil && err != mongo.ErrNoDocuments {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return merchant, merchantErrorUnknown
	}

	if merchant == nil {
		return merchant, merchantErrorNotFound
	}

	return merchant, nil
}

func (s *Service) addNotification(
	ctx context.Context,
	msg, merchantId, userId string,
	nStatuses *billing.SystemNotificationStatuses,
) (*billing.Notification, error) {
	if merchantId == "" {
		return nil, notificationErrorMerchantIdIncorrect
	}

	_, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		return nil, notificationErrorMerchantIdIncorrect
	}

	notification := &billing.Notification{
		Id:         primitive.NewObjectID().Hex(),
		Message:    msg,
		MerchantId: merchantId,
		UserId:     userId,
		IsRead:     false,
		IsSystem:   userId == "",
		Statuses:   nStatuses,
	}

	_, err = s.db.Collection(collectionNotification).InsertOne(ctx, notification)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, notification),
		)
		return nil, merchantErrorUnknown
	}

	channel := s.getMerchantCentrifugoChannel(merchantId)
	err = s.centrifugoDashboard.Publish(ctx, channel, notification)

	if err != nil {
		return nil, merchantErrorUnknown
	}

	return notification, nil
}

func (s *Service) getNotificationById(
	ctx context.Context,
	merchantId, notificationId string,
) (notification *billing.Notification, err error) {
	oid, _ := primitive.ObjectIDFromHex(notificationId)
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": merchantOid, "_id": oid}
	err = s.db.Collection(collectionNotification).FindOne(ctx, query).Decode(&notification)

	if err != nil {
		if err != mongo.ErrNoDocuments {
			zap.S().Errorf("Query to find notification by id failed", "err", err.Error(), "query", query)
		}

		return notification, notificationErrorNotFound
	}

	if notification == nil {
		return notification, notificationErrorNotFound
	}

	return
}

func (s *Service) mapNotificationData(rsp *billing.Notification, notification *billing.Notification) {
	rsp.Id = notification.Id
	rsp.UserId = notification.UserId
	rsp.MerchantId = notification.MerchantId
	rsp.Message = notification.Message
	rsp.IsSystem = notification.IsSystem
	rsp.IsRead = notification.IsRead
	rsp.CreatedAt = notification.CreatedAt
	rsp.UpdatedAt = notification.UpdatedAt
	rsp.Statuses = notification.Statuses
}

func (s *Service) IsChangeDataAllow(merchant *billing.Merchant, data *grpc.OnboardingRequest) bool {
	if merchant.Status != pkg.MerchantStatusDraft &&
		(data.Company != nil || data.Contacts != nil || data.Banking != nil || merchant.HasTariff()) {
		return false
	}

	return true
}

func (s *Service) GetMerchantOnboardingCompleteData(
	ctx context.Context,
	req *grpc.SetMerchantS3AgreementRequest,
	rsp *grpc.GetMerchantOnboardingCompleteDataResponse,
) error {
	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	merchant, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.GetMerchantOnboardingCompleteDataResponseItem{
		Steps:              merchant.Steps,
		Status:             merchant.GetPrintableStatus(),
		CompleteStepsCount: merchant.GetCompleteStepsCount(),
	}

	return nil
}

func (s *Service) getMerchantAgreementSignature(
	ctx context.Context,
	merchant *billing.Merchant,
) (*billing.MerchantAgreementSignatureData, error) {
	op, err := s.operatingCompany.GetById(ctx, merchant.OperatingCompanyId)

	if err != nil {
		return nil, err
	}

	message := "License Agreement signing procedure between your company and PaySuper was initiated.\r\n" +
		"This service provides legally binding e-signatures, so please check carefully this signing request origin " +
		"and then click the big blue button to review and then sign the document.\r\n" +
		"After signing you will get a both sides signed PDF-copy in next email."

	req := &proto.CreateSignatureRequest{
		RequestType: documentSignerConst.RequestTypeCreateWebsite,
		Subject:     "PaySuper and " + merchant.Company.Name + " License Agreement signing request",
		Title:       "License Agreement #" + merchant.AgreementNumber,
		Message:     message,
		ClientId:    s.cfg.HelloSignAgreementClientId,
		Ccs: []*proto.CreateSignatureRequestCcs{
			{EmailAddress: merchant.User.Email, RoleName: "Merchant Owner"},
			{EmailAddress: s.cfg.EmailOnboardingAdminRecipient, RoleName: "PaySuper Verifier"},
		},
		Signers: []*proto.CreateSignatureRequestSigner{
			{
				Email:    merchant.GetAuthorizedEmail(),
				Name:     merchant.GetAuthorizedName(),
				RoleName: documentSignerConst.SignerRoleNameMerchant,
			},
			{
				Email:    op.Email,
				Name:     op.SignatoryName,
				RoleName: documentSignerConst.SignerRoleNamePaysuper,
			},
		},
		Metadata: map[string]string{
			documentSignerConst.MetadataFieldMerchantId: merchant.Id,
		},
		FileUrl: []*proto.CreateSignatureRequestFileUrl{
			{
				Name:    merchant.S3AgreementName,
				Storage: documentSignerConst.StorageTypeAgreement,
			},
		},
	}

	ctx, _ = context.WithTimeout(context.Background(), time.Minute*2)
	opts := []client.CallOption{
		client.WithRequestTimeout(time.Minute * 2),
	}
	rsp, err := s.documentSigner.CreateSignature(ctx, req, opts...)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "DocumentSignerService"),
			zap.String(errorFieldMethod, "CreateSignature"),
			zap.Any(errorFieldRequest, req),
		)

		return nil, merchantErrorUnknown
	}

	if rsp.Status != pkg.ResponseStatusOk {
		err = &grpc.ResponseErrorMessage{
			Code:    rsp.Message.Code,
			Message: rsp.Message.Message,
			Details: rsp.Message.Details,
		}

		return nil, err
	}

	data := &billing.MerchantAgreementSignatureData{
		DetailsUrl:          rsp.Item.DetailsUrl,
		FilesUrl:            rsp.Item.FilesUrl,
		SignatureRequestId:  rsp.Item.SignatureRequestId,
		MerchantSignatureId: rsp.Item.MerchantSignatureId,
		PsSignatureId:       rsp.Item.PsSignatureId,
	}

	return data, nil
}

func (s *Service) GetMerchantTariffRates(
	ctx context.Context,
	req *grpc.GetMerchantTariffRatesRequest,
	rsp *grpc.GetMerchantTariffRatesResponse,
) error {
	if req.PayerRegion == "" {
		req.PayerRegion = req.HomeRegion
	}

	tariffs, err := s.merchantTariffRates.GetBy(ctx, req)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantTariffsNotFound {
			rsp.Status = pkg.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Items = tariffs

	return nil
}

func (s *Service) SetMerchantTariffRates(
	ctx context.Context,
	req *grpc.SetMerchantTariffRatesRequest,
	rsp *grpc.CheckProjectRequestSignatureResponse,
) error {
	mccCode, err := getMccByOperationsType(req.MerchantOperationsType)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	oid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	merchant, err := s.getMerchantBy(ctx, bson.M{"_id": oid})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantErrorUnknown {
			rsp.Status = pkg.ResponseStatusSystemError
		}

		return nil
	}

	if merchant.HasTariff() {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorOnboardingTariffAlreadyExist

		return nil
	}

	if merchant.IsAgreementSigningStarted() {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorChangeNotAllowed

		return nil
	}

	merchantPayoutCurrency := merchant.GetPayoutCurrency()
	if merchantPayoutCurrency == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorCurrencyNotSet

		return nil
	}

	query := &grpc.GetMerchantTariffRatesRequest{
		HomeRegion:             req.HomeRegion,
		MerchantOperationsType: req.MerchantOperationsType,
	}
	tariffs, err := s.merchantTariffRates.GetBy(ctx, query)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	payoutTariff, ok := tariffs.Payout[merchantPayoutCurrency]
	if !ok {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorNoTariffsInPayoutCurrency

		return nil
	}

	minimalPayoutLimit, ok := tariffs.MinimalPayout[merchantPayoutCurrency]
	if !ok {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorNoTariffsInPayoutCurrency

		return nil
	}

	timestampNow := ptypes.TimestampNow()

	merchant.MerchantOperationsType = req.MerchantOperationsType
	merchant.Tariff = &billing.MerchantTariff{
		Payment:    tariffs.Payment,
		Payout:     payoutTariff,
		HomeRegion: req.HomeRegion,
	}

	merchant.MinimalPayoutLimit = minimalPayoutLimit

	if len(tariffs.Payment) > 0 {
		var costs []*billing.PaymentChannelCostMerchant

		for _, v := range tariffs.Payment {
			cost := &billing.PaymentChannelCostMerchant{
				Id:                      primitive.NewObjectID().Hex(),
				MerchantId:              req.MerchantId,
				Name:                    strings.ToUpper(v.MethodName),
				PayoutCurrency:          merchantPayoutCurrency,
				MinAmount:               v.MinAmount,
				Region:                  v.PayerRegion,
				MethodPercent:           v.MethodPercentFee,
				MethodFixAmount:         v.MethodFixedFee,
				MethodFixAmountCurrency: v.MethodFixedFeeCurrency,
				PsPercent:               v.PsPercentFee,
				PsFixedFee:              v.PsFixedFee,
				PsFixedFeeCurrency:      v.PsFixedFeeCurrency,
				CreatedAt:               timestampNow,
				UpdatedAt:               timestampNow,
				IsActive:                true,
				MccCode:                 mccCode,
			}

			costs = append(costs, cost)
		}

		if len(costs) <= 0 {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}

		err = s.paymentChannelCostMerchant.MultipleInsert(ctx, costs)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}
	}

	regions, err := s.country.GetAll(ctx)

	if err != nil || len(regions.Countries) <= 0 {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	var (
		cost  *billing.MoneyBackCostMerchant
		costs []*billing.MoneyBackCostMerchant
	)

	for _, tariffRegion := range pkg.SupportedTariffRegions {
		for _, v := range tariffs.Refund {
			cost = &billing.MoneyBackCostMerchant{
				Id:                primitive.NewObjectID().Hex(),
				MerchantId:        req.MerchantId,
				Name:              strings.ToUpper(v.MethodName),
				PayoutCurrency:    merchantPayoutCurrency,
				UndoReason:        pkg.UndoReasonReversal,
				Region:            tariffRegion,
				Country:           "",
				DaysFrom:          0,
				PaymentStage:      1,
				Percent:           v.MethodPercentFee,
				FixAmount:         v.MethodFixedFee,
				FixAmountCurrency: v.MethodFixedFeeCurrency,
				IsPaidByMerchant:  v.IsPaidByMerchant,
				CreatedAt:         timestampNow,
				UpdatedAt:         timestampNow,
				IsActive:          true,
				MccCode:           mccCode,
			}
			costs = append(costs, cost)
		}
		for _, v := range tariffs.Chargeback {
			cost = &billing.MoneyBackCostMerchant{
				Id:                primitive.NewObjectID().Hex(),
				MerchantId:        req.MerchantId,
				Name:              strings.ToUpper(v.MethodName),
				PayoutCurrency:    merchantPayoutCurrency,
				UndoReason:        pkg.UndoReasonChargeback,
				Region:            tariffRegion,
				Country:           "",
				DaysFrom:          0,
				PaymentStage:      1,
				Percent:           v.MethodPercentFee,
				FixAmount:         v.MethodFixedFee,
				FixAmountCurrency: v.MethodFixedFeeCurrency,
				IsPaidByMerchant:  v.IsPaidByMerchant,
				CreatedAt:         timestampNow,
				UpdatedAt:         timestampNow,
				IsActive:          true,
				MccCode:           mccCode,
			}
			costs = append(costs, cost)
		}
	}

	if len(costs) > 0 {
		err = s.moneyBackCostMerchant.MultipleInsert(ctx, costs)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}
	}

	merchant.MccCode = mccCode

	if merchant.Steps == nil {
		merchant.Steps = &billing.MerchantCompletedSteps{}
	}

	merchant.Steps.Tariff = true

	if merchant.IsDataComplete() {
		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationMerchant, merchant.GetAuthorizedEmail())
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationAdmin, s.cfg.EmailOnboardingAdminRecipient)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		statusChange := &billing.SystemNotificationStatuses{From: pkg.MerchantStatusDraft, To: pkg.MerchantStatusPending}

		merchant.Status = pkg.MerchantStatusPending
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
		message, ok := merchantStatusChangesMessages[merchant.Status]

		if !ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = merchantNotificationSettingNotFound

			return nil
		}

		_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

		if err != nil {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = err.(*grpc.ResponseErrorMessage)

			return nil
		}
	}

	err = s.merchant.Update(ctx, merchant)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	return nil
}

func (s *Service) generateMerchantAgreement(ctx context.Context, merchant *billing.Merchant) error {
	payoutCostInt := int(merchant.Tariff.Payout.MethodFixedFee)
	payoutCostWord := num2words.Convert(payoutCostInt)
	minPayoutLimitInt := int(merchant.MinimalPayoutLimit)
	minPayoutLimitWord := num2words.Convert(minPayoutLimitInt)

	payoutCost := fmt.Sprintf("%s (%d) %s", payoutCostWord, payoutCostInt, merchant.Tariff.Payout.MethodFixedFeeCurrency)
	minPayoutLimit := fmt.Sprintf("%s (%d) %s", minPayoutLimitWord, minPayoutLimitInt, merchant.GetPayoutCurrency())

	operatingCompany, err := s.operatingCompany.GetById(ctx, merchant.OperatingCompanyId)
	if err != nil {
		zap.L().Error("Operating company not found", zap.Error(err), zap.String("operating_company_id", merchant.OperatingCompanyId))
		return err
	}

	params := map[string]interface{}{
		reporterConst.RequestParameterAgreementNumber:                             merchant.AgreementNumber,
		reporterConst.RequestParameterAgreementLegalName:                          merchant.Company.Name,
		reporterConst.RequestParameterAgreementAddress:                            merchant.GetAddress(),
		reporterConst.RequestParameterAgreementRegistrationNumber:                 merchant.Company.RegistrationNumber,
		reporterConst.RequestParameterAgreementPayoutCost:                         payoutCost,
		reporterConst.RequestParameterAgreementMinimalPayoutLimit:                 minPayoutLimit,
		reporterConst.RequestParameterAgreementPayoutCurrency:                     merchant.GetPayoutCurrency(),
		reporterConst.RequestParameterAgreementPSRate:                             merchant.Tariff.Payment,
		reporterConst.RequestParameterAgreementHomeRegion:                         pkg.HomeRegions[merchant.Tariff.HomeRegion],
		reporterConst.RequestParameterAgreementMerchantAuthorizedName:             merchant.Contacts.Authorized.Name,
		reporterConst.RequestParameterAgreementMerchantAuthorizedPosition:         merchant.Contacts.Authorized.Position,
		reporterConst.RequestParameterAgreementOperatingCompanyLegalName:          operatingCompany.Name,
		reporterConst.RequestParameterAgreementOperatingCompanyAddress:            operatingCompany.Address,
		reporterConst.RequestParameterAgreementOperatingCompanyRegistrationNumber: operatingCompany.RegistrationNumber,
		reporterConst.RequestParameterAgreementOperatingCompanyAuthorizedName:     operatingCompany.SignatoryName,
		reporterConst.RequestParameterAgreementOperatingCompanyAuthorizedPosition: operatingCompany.SignatoryPosition,
	}

	b, err := json.Marshal(params)

	if err != nil {
		zap.L().Error(
			"Marshal params to json for generate agreement failed",
			zap.Error(err),
			zap.Any("parameters", params),
		)
		return merchantErrorUnknown
	}

	req := &reporterProto.ReportFile{
		UserId:           merchant.User.Id,
		MerchantId:       merchant.Id,
		ReportType:       reporterConst.ReportTypeAgreement,
		FileType:         reporterConst.OutputExtensionPdf,
		Params:           b,
		SendNotification: false,
	}
	rsp, err := s.reporterService.CreateFile(ctx, req, client.WithRequestTimeout(time.Minute*10))

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, reporterConst.ServiceName),
			zap.String(errorFieldMethod, "CreateFile"),
			zap.Any(errorFieldRequest, req),
		)
		return merchantErrorUnknown
	}

	if rsp.Status != pkg.ResponseStatusOk {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Any("error", rsp.Message),
			zap.String(errorFieldService, reporterConst.ServiceName),
			zap.String(errorFieldMethod, "CreateFile"),
			zap.Any(errorFieldRequest, req),
		)
		return &grpc.ResponseErrorMessage{Code: rsp.Message.Code, Message: rsp.Message.Message}
	}

	return nil
}

func (s *Service) getMerchantAgreementNumber(merchantId string) string {
	now := time.Now()
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	return fmt.Sprintf("%s%s-%03d", now.Format("01"), now.Format("02"), mongodb.GetObjectIDCounter(merchantOid))
}

func (s *Service) sendOnboardingLetter(merchant *billing.Merchant, oc *billing.OperatingCompany, template, recipientEmail string) (err error) {
	ocName := ""
	if oc != nil {
		ocName = oc.Name
	}

	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: template,
		TemplateModel: map[string]string{
			"merchant_id":                   merchant.Id,
			"operating_name":                ocName,
			"merchant_legal_name":           merchant.GetCompanyName(),
			"merchant_agreement_sign_url":   s.cfg.GetMerchantCompanyUrl(),
			"admin_company_url":             s.cfg.GetAdminCompanyUrl(merchant.Id),
			"admin_onboarding_requests_url": s.cfg.GetAdminOnboardingRequestsUrl(),
		},
		To: recipientEmail,
	}

	err = s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})

	if err != nil {
		zap.L().Error(
			"Publication message during merchant onboarding failed",
			zap.Error(err),
			zap.String("template", template),
			zap.String("recipientEmail", recipientEmail),
			zap.Any("merchant", merchant),
		)
	}

	return
}
