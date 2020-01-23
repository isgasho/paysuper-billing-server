package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/divan/num2words"
	"github.com/golang/protobuf/ptypes"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinProto "github.com/paysuper/paysuper-proto/go/casbinpb"
	"github.com/paysuper/paysuper-proto/go/document_signerpb"
	"github.com/paysuper/paysuper-proto/go/postmarkpb"
	"github.com/paysuper/paysuper-proto/go/reporterpb"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"strings"
	"time"
)

const (
	collectionNotification                 = "notification"
	collectionMerchantPaymentMethodHistory = "payment_method_history"

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
		billingpb.MerchantStatusAgreementSigning: merchantStatusSigningMessage,
		billingpb.MerchantStatusAgreementSigned:  merchantStatusSignedMessage,
		billingpb.MerchantStatusDeleted:          merchantStatusDeletedMessage,
		billingpb.MerchantStatusRejected:         merchantStatusRejectedMessage,
		billingpb.MerchantStatusPending:          merchantStatusPendingMessage,
		billingpb.MerchantStatusAccepted:         merchantStatusAcceptedMessage,
	}
)

func (s *Service) GetMerchantBy(
	ctx context.Context,
	req *billingpb.GetMerchantByRequest,
	rsp *billingpb.GetMerchantResponse,
) error {
	if req.MerchantId == "" && req.UserId == "" {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorBadData

		return nil
	}

	var (
		merchant *billingpb.Merchant
		err      error
	)

	if req.UserId != "" {
		merchant, err = s.merchantRepository.GetByUserId(ctx, req.UserId)
	} else {
		merchant, err = s.merchantRepository.GetById(ctx, req.MerchantId)
	}

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)

		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		if err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusBadData
		}

		return nil
	}

	merchant.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(merchant.Id, time.Now().Add(time.Hour*3).Unix())
	merchant.HasProjects = s.getProjectsCountByMerchant(ctx, merchant.Id) > 0

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ListMerchants(
	ctx context.Context,
	req *billingpb.MerchantListingRequest,
	rsp *billingpb.MerchantListingResponse,
) error {
	var err error
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

	rsp.Count, err = s.merchantRepository.FindCount(ctx, query)

	if err != nil {
		return merchantErrorUnknown
	}

	rsp.Items, err = s.merchantRepository.Find(ctx, query, req.Sort, req.Offset, req.Limit)

	return nil
}

func (s *Service) ChangeMerchant(
	ctx context.Context,
	req *billingpb.OnboardingRequest,
	rsp *billingpb.ChangeMerchantResponse,
) error {
	var (
		merchant      *billingpb.Merchant
		err           error
		isNewMerchant bool
	)

	if req.HasIdentificationFields() {
		if req.User != nil && req.User.Id != "" {
			merchant, err = s.merchantRepository.GetByUserId(ctx, req.User.Id)
		}

		if err != nil && req.Id != "" {
			merchant, err = s.merchantRepository.GetById(ctx, req.Id)
		}

		if err != nil && err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown

			return nil
		}
	}

	if merchant == nil {
		merchant = &billingpb.Merchant{
			Id:                 primitive.NewObjectID().Hex(),
			User:               req.User,
			MinimalPayoutLimit: pkg.MerchantMinimalPayoutLimit,
			Status:             billingpb.MerchantStatusDraft,
			CreatedAt:          ptypes.TimestampNow(),
		}
		merchant.AgreementNumber = s.getMerchantAgreementNumber(merchant.Id)
		isNewMerchant = true
	}

	if !s.IsChangeDataAllow(merchant, req) {
		rsp.Status = billingpb.ResponseStatusForbidden
		rsp.Message = merchantErrorChangeNotAllowed

		return nil
	}

	if req.Company != nil {
		_, err := s.country.GetByIsoCodeA2(ctx, req.Company.Country)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = merchantErrorCountryNotFound

			return nil
		}

		merchant.Company = req.Company
	}

	if req.Banking != nil {
		if req.Banking.Currency != "" {
			if !helper.Contains(s.supportedCurrencies, req.Banking.Currency) {
				rsp.Status = billingpb.ResponseStatusBadData
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
			if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
				rsp.Status = billingpb.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationAdmin, s.cfg.EmailOnboardingAdminRecipient)
		if err != nil {
			if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
				rsp.Status = billingpb.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	merchant.UpdatedAt = ptypes.TimestampNow()

	if merchant.Steps == nil {
		merchant.Steps = &billingpb.MerchantCompletedSteps{}
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

	err = s.merchantRepository.Upsert(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	if isNewMerchant == true {
		err = s.userRoleRepository.AddMerchantUser(
			ctx,
			&billingpb.UserRole{
				Id:         primitive.NewObjectID().Hex(),
				MerchantId: merchant.Id,
				Status:     pkg.UserRoleStatusAccepted,
				Role:       billingpb.RoleMerchantOwner,
				UserId:     merchant.User.Id,
				Email:      merchant.User.Email,
				FirstName:  merchant.User.FirstName,
				LastName:   merchant.User.LastName,
			},
		)

		if err == nil {
			_, err = s.casbinService.AddRoleForUser(ctx, &casbinProto.UserRoleRequest{
				User: fmt.Sprintf(pkg.CasbinMerchantUserMask, merchant.Id, merchant.User.Id),
				Role: billingpb.RoleMerchantOwner,
			})
		}
	}

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantUnableToAddMerchantUserRole

		return nil
	}

	merchant.CentrifugoToken = s.centrifugoDashboard.GetChannelToken(merchant.Id, time.Now().Add(time.Hour*3).Unix())

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantStatus(
	ctx context.Context,
	req *billingpb.MerchantChangeStatusRequest,
	rsp *billingpb.ChangeMerchantStatusResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorNotFound

		return nil
	}

	if !merchant.CanChangeStatusTo(req.Status) {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantStatusChangeNotPossible

		return nil
	}

	statusChange := &billingpb.SystemNotificationStatuses{From: merchant.Status, To: req.Status}
	message, ok := merchantStatusChangesMessages[req.Status]

	if !ok {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound

		return nil
	}

	merchant.Status = req.Status
	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	err = s.merchantRepository.Update(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantData(
	ctx context.Context,
	req *billingpb.ChangeMerchantDataRequest,
	rsp *billingpb.ChangeMerchantDataResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound
		return nil
	}

	if merchant.IsSigned && merchant.HasMerchantSignature && merchant.HasPspSignature {
		rsp.Status = billingpb.ResponseStatusOk
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
	statusChange := &billingpb.SystemNotificationStatuses{}

	if merchant.HasMerchantSignature {
		statusChange.From = merchant.Status
		statusChange.To = billingpb.MerchantStatusAgreementSigning

		merchant.Status = billingpb.MerchantStatusAgreementSigning
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	}

	if merchant.IsSigned {
		statusChange.From = merchant.Status
		statusChange.To = billingpb.MerchantStatusAgreementSigned

		merchant.Status = billingpb.MerchantStatusAgreementSigned
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
	}

	message, ok := merchantStatusChangesMessages[merchant.Status]

	if !ok {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound
		return nil
	}

	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)
		return nil
	}

	err = s.merchantRepository.Update(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant
	return nil
}

func (s *Service) SetMerchantOperatingCompany(
	ctx context.Context,
	req *billingpb.SetMerchantOperatingCompanyRequest,
	rsp *billingpb.SetMerchantOperatingCompanyResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	oc, err := s.operatingCompany.GetById(ctx, req.OperatingCompanyId)

	if err != nil {
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	if !merchant.IsDataComplete() {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorOnboardingNotComplete
		return nil
	}

	statusChange := &billingpb.SystemNotificationStatuses{From: merchant.Status, To: billingpb.MerchantStatusAccepted}

	merchant.OperatingCompanyId = oc.Id
	merchant.DontChargeVat = req.DontChargeVat
	merchant.Status = billingpb.MerchantStatusAccepted
	merchant.StatusLastUpdatedAt = ptypes.TimestampNow()

	message, ok := merchantStatusChangesMessages[merchant.Status]

	if !ok {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantNotificationSettingNotFound

		return nil
	}

	_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	err = s.merchantRepository.Update(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	err = s.generateMerchantAgreement(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	err = s.sendOnboardingLetter(merchant, oc, s.cfg.EmailTemplates.OnboardingCompleted, merchant.GetAuthorizedEmail())
	if err != nil {
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantManualPayouts(
	ctx context.Context,
	req *billingpb.ChangeMerchantManualPayoutsRequest,
	rsp *billingpb.ChangeMerchantManualPayoutsResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	if merchant.ManualPayoutsEnabled == req.ManualPayoutsEnabled {
		rsp.Status = billingpb.ResponseStatusNotModified
		return nil
	}

	merchant.ManualPayoutsEnabled = req.ManualPayoutsEnabled

	err = s.merchantRepository.Update(ctx, merchant)
	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) SetMerchantS3Agreement(
	ctx context.Context,
	req *billingpb.SetMerchantS3AgreementRequest,
	rsp *billingpb.ChangeMerchantDataResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	merchant.S3AgreementName = req.S3AgreementName

	if merchant.AgreementSignatureData == nil {
		merchant.AgreementSignatureData, err = s.getMerchantAgreementSignature(ctx, merchant)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = err.(*billingpb.ResponseErrorMessage)

			return nil
		}
	}

	err = s.merchantRepository.Update(ctx, merchant)

	if err != nil {
		return merchantErrorUnknown
	}

	channel := s.getMerchantCentrifugoChannel(merchant.Id)
	err = s.centrifugoDashboard.Publish(ctx, channel, merchantAgreementReadyToSignMessage)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant

	return nil
}

func (s *Service) CreateNotification(
	ctx context.Context,
	req *billingpb.NotificationRequest,
	rsp *billingpb.CreateNotificationResponse,
) error {
	rsp.Status = billingpb.ResponseStatusOk

	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorNotFound
		return nil
	}

	if req.Message == "" {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = notificationErrorMessageIsEmpty
		return nil
	}

	n, err := s.addNotification(ctx, req.Message, req.MerchantId, req.UserId, nil)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusBadData
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
	req *billingpb.GetNotificationRequest,
	rsp *billingpb.Notification,
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
	req *billingpb.ListingNotificationRequest,
	rsp *billingpb.Notifications,
) error {
	var notifications []*billingpb.Notification

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
	rsp.Items = []*billingpb.Notification{}

	if len(notifications) > 0 {
		rsp.Items = notifications
	}

	return nil
}

func (s *Service) MarkNotificationAsRead(
	ctx context.Context,
	req *billingpb.GetNotificationRequest,
	rsp *billingpb.Notification,
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
	req *billingpb.GetMerchantPaymentMethodRequest,
	rsp *billingpb.GetMerchantPaymentMethodResponse,
) error {
	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	pms, err := s.getMerchantPaymentMethod(ctx, req.MerchantId, req.PaymentMethodId)
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

		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}

	rsp.Item = &billingpb.MerchantPaymentMethod{
		PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
			Id:   pm.Id,
			Name: pm.Name,
		},
		Commission:  s.getDefaultPaymentMethodCommissions(),
		Integration: &billingpb.MerchantPaymentMethodIntegration{},
		IsActive:    true,
	}

	return nil
}

func (s *Service) ListMerchantPaymentMethods(
	ctx context.Context,
	req *billingpb.ListMerchantPaymentMethodsRequest,
	rsp *billingpb.ListingMerchantPaymentMethod,
) error {
	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		return nil
	}

	var pms []*billingpb.PaymentMethod

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
		mPm, err := s.getMerchantPaymentMethod(ctx, req.MerchantId, pm.Id)

		paymentMethod := &billingpb.MerchantPaymentMethod{
			PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
				Id:   pm.Id,
				Name: pm.Name,
			},
			Commission:  s.getDefaultPaymentMethodCommissions(),
			Integration: &billingpb.MerchantPaymentMethodIntegration{},
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
	req *billingpb.MerchantPaymentMethodRequest,
	rsp *billingpb.MerchantPaymentMethodResponse,
) (err error) {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	pm, e := s.paymentMethod.GetById(ctx, req.PaymentMethod.Id)
	if e != nil {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}
	req.Integration.Integrated = req.HasIntegration()

	if req.HasPerTransactionCurrency() {
		if !helper.Contains(s.supportedCurrencies, req.GetPerTransactionCurrency()) {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = orderErrorCurrencyNotFound

			return nil
		}
	}

	if len(merchant.PaymentMethods) <= 0 {
		merchant.PaymentMethods = make(map[string]*billingpb.MerchantPaymentMethod)
	}

	mpm := &billingpb.MerchantPaymentMethod{
		PaymentMethod: req.PaymentMethod,
		Commission:    req.Commission,
		Integration:   req.Integration,
		IsActive:      req.IsActive,
	}

	merchant.PaymentMethods[pm.Id] = mpm

	// insert in history collection first than really update merchant
	history := &billingpb.MerchantPaymentMethodHistory{
		Id:            primitive.NewObjectID().Hex(),
		MerchantId:    merchant.Id,
		UserId:        req.UserId,
		CreatedAt:     ptypes.TimestampNow(),
		PaymentMethod: mpm,
	}
	_, err = s.db.Collection(collectionMerchantPaymentMethodHistory).InsertOne(ctx, history)
	if err != nil {
		zap.S().Errorf("Query to update merchant payment methods history", "err", err.Error(), "data", merchant)

		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	if err := s.merchantRepository.Update(ctx, merchant); err != nil {
		zap.S().Errorf("Query to update merchant payment methods failed", "err", err.Error(), "data", merchant)

		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = merchant.PaymentMethods[pm.Id]

	return nil
}

func (s *Service) addNotification(
	ctx context.Context,
	msg, merchantId, userId string,
	nStatuses *billingpb.SystemNotificationStatuses,
) (*billingpb.Notification, error) {
	if merchantId == "" {
		return nil, notificationErrorMerchantIdIncorrect
	}

	_, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		return nil, notificationErrorMerchantIdIncorrect
	}

	notification := &billingpb.Notification{
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
) (notification *billingpb.Notification, err error) {
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

func (s *Service) mapNotificationData(rsp *billingpb.Notification, notification *billingpb.Notification) {
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

func (s *Service) IsChangeDataAllow(merchant *billingpb.Merchant, data *billingpb.OnboardingRequest) bool {
	if merchant.Status != billingpb.MerchantStatusDraft &&
		(data.Company != nil || data.Contacts != nil || data.Banking != nil || merchant.HasTariff()) {
		return false
	}

	return true
}

func (s *Service) GetMerchantOnboardingCompleteData(
	ctx context.Context,
	req *billingpb.SetMerchantS3AgreementRequest,
	rsp *billingpb.GetMerchantOnboardingCompleteDataResponse,
) error {
	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = &billingpb.GetMerchantOnboardingCompleteDataResponseItem{
		Steps:              merchant.Steps,
		Status:             merchant.GetPrintableStatus(),
		CompleteStepsCount: merchant.GetCompleteStepsCount(),
	}

	return nil
}

func (s *Service) getMerchantAgreementSignature(
	ctx context.Context,
	merchant *billingpb.Merchant,
) (*billingpb.MerchantAgreementSignatureData, error) {
	op, err := s.operatingCompany.GetById(ctx, merchant.OperatingCompanyId)

	if err != nil {
		return nil, err
	}

	message := "License Agreement signing procedure between your company and PaySuper was initiated.\r\n" +
		"This service provides legally binding e-signatures, so please check carefully this signing request origin " +
		"and then click the big blue button to review and then sign the document.\r\n" +
		"After signing you will get a both sides signed PDF-copy in next email."

	req := &document_signerpb.CreateSignatureRequest{
		RequestType: document_signerpb.RequestTypeCreateWebsite,
		Subject:     "PaySuper and " + merchant.Company.Name + " License Agreement signing request",
		Title:       "License Agreement #" + merchant.AgreementNumber,
		Message:     message,
		ClientId:    s.cfg.HelloSignAgreementClientId,
		Ccs: []*document_signerpb.CreateSignatureRequestCcs{
			{EmailAddress: merchant.User.Email, RoleName: "Merchant Owner"},
			{EmailAddress: s.cfg.EmailOnboardingAdminRecipient, RoleName: "PaySuper Verifier"},
		},
		Signers: []*document_signerpb.CreateSignatureRequestSigner{
			{
				Email:    merchant.GetAuthorizedEmail(),
				Name:     merchant.GetAuthorizedName(),
				RoleName: document_signerpb.SignerRoleNameMerchant,
			},
			{
				Email:    op.Email,
				Name:     op.SignatoryName,
				RoleName: document_signerpb.SignerRoleNamePaysuper,
			},
		},
		Metadata: map[string]string{
			document_signerpb.MetadataFieldMerchantId: merchant.Id,
		},
		FileUrl: []*document_signerpb.CreateSignatureRequestFileUrl{
			{
				Name:    merchant.S3AgreementName,
				Storage: document_signerpb.StorageTypeAgreement,
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

	if rsp.Status != billingpb.ResponseStatusOk {
		err = &billingpb.ResponseErrorMessage{
			Code:    rsp.Message.Code,
			Message: rsp.Message.Message,
			Details: rsp.Message.Details,
		}

		return nil, err
	}

	data := &billingpb.MerchantAgreementSignatureData{
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
	req *billingpb.GetMerchantTariffRatesRequest,
	rsp *billingpb.GetMerchantTariffRatesResponse,
) error {
	if req.PayerRegion == "" {
		req.PayerRegion = req.HomeRegion
	}

	tariffs, err := s.merchantTariffRates.GetBy(ctx, req)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		if err == merchantTariffsNotFound {
			rsp.Status = billingpb.ResponseStatusNotFound
		}

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Items = tariffs

	return nil
}

func (s *Service) SetMerchantTariffRates(
	ctx context.Context,
	req *billingpb.SetMerchantTariffRatesRequest,
	rsp *billingpb.CheckProjectRequestSignatureResponse,
) error {
	mccCode, err := getMccByOperationsType(req.MerchantOperationsType)
	if err != nil {
		if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	merchant, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		if err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
		}

		return nil
	}

	if merchant.HasTariff() {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorOnboardingTariffAlreadyExist

		return nil
	}

	if merchant.IsAgreementSigningStarted() {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorChangeNotAllowed

		return nil
	}

	merchantPayoutCurrency := merchant.GetPayoutCurrency()
	if merchantPayoutCurrency == "" {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = merchantErrorCurrencyNotSet

		return nil
	}

	query := &billingpb.GetMerchantTariffRatesRequest{
		HomeRegion:             req.HomeRegion,
		MerchantOperationsType: req.MerchantOperationsType,
	}
	tariffs, err := s.merchantTariffRates.GetBy(ctx, query)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown

		return nil
	}

	payoutTariff, ok := tariffs.Payout[merchantPayoutCurrency]
	if !ok {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorNoTariffsInPayoutCurrency

		return nil
	}

	minimalPayoutLimit, ok := tariffs.MinimalPayout[merchantPayoutCurrency]
	if !ok {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorNoTariffsInPayoutCurrency

		return nil
	}

	timestampNow := ptypes.TimestampNow()

	merchant.MerchantOperationsType = req.MerchantOperationsType
	merchant.Tariff = &billingpb.MerchantTariff{
		Payment:    tariffs.Payment,
		Payout:     payoutTariff,
		HomeRegion: req.HomeRegion,
	}

	merchant.MinimalPayoutLimit = minimalPayoutLimit

	if len(tariffs.Payment) > 0 {
		var costs []*billingpb.PaymentChannelCostMerchant

		for _, v := range tariffs.Payment {
			cost := &billingpb.PaymentChannelCostMerchant{
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
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}

		err = s.paymentChannelCostMerchant.MultipleInsert(ctx, costs)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}
	}

	regions, err := s.country.GetAll(ctx)

	if err != nil || len(regions.Countries) <= 0 {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	var (
		cost  *billingpb.MoneyBackCostMerchant
		costs []*billingpb.MoneyBackCostMerchant
	)

	for _, tariffRegion := range pkg.SupportedTariffRegions {
		for _, v := range tariffs.Refund {
			cost = &billingpb.MoneyBackCostMerchant{
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
			cost = &billingpb.MoneyBackCostMerchant{
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
		err = s.moneyBackCostMerchantRepository.MultipleInsert(ctx, costs)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantErrorUnknown
			return nil
		}
	}

	merchant.MccCode = mccCode

	if merchant.Steps == nil {
		merchant.Steps = &billingpb.MerchantCompletedSteps{}
	}

	merchant.Steps.Tariff = true
	/*fmt.Println(merchant.Company)
	  fmt.Println(merchant.IsCompanyComplete())
	  fmt.Println(merchant.IsContactsComplete())
	  fmt.Println(merchant.IsBankingComplete())
	  fmt.Println(merchant.HasTariff())*/
	if merchant.IsDataComplete() {
		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationMerchant, merchant.GetAuthorizedEmail())
		if err != nil {
			if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
				rsp.Status = billingpb.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		err = s.sendOnboardingLetter(merchant, nil, s.cfg.EmailTemplates.OnboardingVerificationAdmin, s.cfg.EmailOnboardingAdminRecipient)
		if err != nil {
			if e, ok := err.(*billingpb.ResponseErrorMessage); ok {
				rsp.Status = billingpb.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		statusChange := &billingpb.SystemNotificationStatuses{From: billingpb.MerchantStatusDraft, To: billingpb.MerchantStatusPending}

		merchant.Status = billingpb.MerchantStatusPending
		merchant.StatusLastUpdatedAt = ptypes.TimestampNow()
		message, ok := merchantStatusChangesMessages[merchant.Status]

		if !ok {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = merchantNotificationSettingNotFound

			return nil
		}

		_, err = s.addNotification(ctx, message, merchant.Id, "", statusChange)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusSystemError
			rsp.Message = err.(*billingpb.ResponseErrorMessage)

			return nil
		}
	}

	err = s.merchantRepository.Update(ctx, merchant)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	return nil
}

func (s *Service) generateMerchantAgreement(ctx context.Context, merchant *billingpb.Merchant) error {
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
		reporterpb.RequestParameterAgreementNumber:                             merchant.AgreementNumber,
		reporterpb.RequestParameterAgreementLegalName:                          merchant.Company.Name,
		reporterpb.RequestParameterAgreementAddress:                            merchant.GetAddress(),
		reporterpb.RequestParameterAgreementRegistrationNumber:                 merchant.Company.RegistrationNumber,
		reporterpb.RequestParameterAgreementPayoutCost:                         payoutCost,
		reporterpb.RequestParameterAgreementMinimalPayoutLimit:                 minPayoutLimit,
		reporterpb.RequestParameterAgreementPayoutCurrency:                     merchant.GetPayoutCurrency(),
		reporterpb.RequestParameterAgreementPSRate:                             merchant.Tariff.Payment,
		reporterpb.RequestParameterAgreementHomeRegion:                         billingpb.HomeRegions[merchant.Tariff.HomeRegion],
		reporterpb.RequestParameterAgreementMerchantAuthorizedName:             merchant.Contacts.Authorized.Name,
		reporterpb.RequestParameterAgreementMerchantAuthorizedPosition:         merchant.Contacts.Authorized.Position,
		reporterpb.RequestParameterAgreementOperatingCompanyLegalName:          operatingCompany.Name,
		reporterpb.RequestParameterAgreementOperatingCompanyAddress:            operatingCompany.Address,
		reporterpb.RequestParameterAgreementOperatingCompanyRegistrationNumber: operatingCompany.RegistrationNumber,
		reporterpb.RequestParameterAgreementOperatingCompanyAuthorizedName:     operatingCompany.SignatoryName,
		reporterpb.RequestParameterAgreementOperatingCompanyAuthorizedPosition: operatingCompany.SignatoryPosition,
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

	req := &reporterpb.ReportFile{
		UserId:           merchant.User.Id,
		MerchantId:       merchant.Id,
		ReportType:       reporterpb.ReportTypeAgreement,
		FileType:         reporterpb.OutputExtensionPdf,
		Params:           b,
		SendNotification: false,
	}
	rsp, err := s.reporterService.CreateFile(ctx, req, client.WithRequestTimeout(time.Minute*10))

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, reporterpb.ServiceName),
			zap.String(errorFieldMethod, "CreateFile"),
			zap.Any(errorFieldRequest, req),
		)
		return merchantErrorUnknown
	}

	if rsp.Status != billingpb.ResponseStatusOk {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Any("error", rsp.Message),
			zap.String(errorFieldService, reporterpb.ServiceName),
			zap.String(errorFieldMethod, "CreateFile"),
			zap.Any(errorFieldRequest, req),
		)
		return &billingpb.ResponseErrorMessage{Code: rsp.Message.Code, Message: rsp.Message.Message}
	}

	return nil
}

func (s *Service) getMerchantAgreementNumber(merchantId string) string {
	now := time.Now()
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	return fmt.Sprintf("%s%s-%03d", now.Format("01"), now.Format("02"), mongodb.GetObjectIDCounter(merchantOid))
}

func (s *Service) sendOnboardingLetter(merchant *billingpb.Merchant, oc *billingpb.OperatingCompany, template, recipientEmail string) (err error) {
	ocName := ""
	if oc != nil {
		ocName = oc.Name
	}

	payload := &postmarkpb.Payload{
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

	err = s.postmarkBroker.Publish(postmarkpb.PostmarkSenderTopicName, payload, amqp.Table{})

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
