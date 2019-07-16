package service

import (
	"context"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
	"time"
)

const (
	collectionNotification      = "notification"
	collectionOnboardingProfile = "onboarding_profile"
)

var (
	merchantErrorChangeNotAllowed            = newBillingServerErrorMsg("mr000001", "merchant data changing not allowed")
	merchantErrorCountryNotFound             = newBillingServerErrorMsg("mr000002", "merchant country not found")
	merchantErrorCurrencyNotFound            = newBillingServerErrorMsg("mr000003", "merchant bank accounting currency not found")
	merchantErrorAgreementRequested          = newBillingServerErrorMsg("mr000004", "agreement for merchant can't be requested")
	merchantErrorOnReview                    = newBillingServerErrorMsg("mr000005", "merchant hasn't allowed status for review")
	merchantErrorSigning                     = newBillingServerErrorMsg("mr000006", "signing uncompleted merchant is impossible")
	merchantErrorSigned                      = newBillingServerErrorMsg("mr000007", "document can't be mark as signed")
	merchantErrorUnknown                     = newBillingServerErrorMsg("mr000008", "request processing failed. try request later")
	merchantErrorNotFound                    = newBillingServerErrorMsg("mr000009", "merchant with specified identifier not found")
	merchantErrorBadData                     = newBillingServerErrorMsg("mr000010", "request data is incorrect")
	merchantErrorAgreementTypeSelectNotAllow = newBillingServerErrorMsg("mr000011", "merchant status not allow select agreement type")
	notificationErrorMerchantIdIncorrect     = newBillingServerErrorMsg("mr000012", "merchant identifier incorrect, notification can't be saved")
	notificationErrorUserIdIncorrect         = newBillingServerErrorMsg("mr000013", "user identifier incorrect, notification can't be saved")
	notificationErrorMessageIsEmpty          = newBillingServerErrorMsg("mr000014", "notification message can't be empty")
	notificationErrorNotFound                = newBillingServerErrorMsg("mr000015", "notification not found")
	onboardingProfileErrorNotFound           = newBillingServerErrorMsg("op000001", "user profile not found")
	onboardingProfileErrorUnknown            = newBillingServerErrorMsg("op000002", "unknown error. try request later")

	NotificationStatusChangeTitles = map[int32]string{
		pkg.MerchantStatusDraft:              "New merchant created",
		pkg.MerchantStatusAgreementRequested: "Merchant asked for agreement",
		pkg.MerchantStatusOnReview:           "Merchant on KYC review",
		pkg.MerchantStatusAgreementSigning:   "Agreement signing",
		pkg.MerchantStatusAgreementSigned:    "Agreement signed",
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
		query["_id"] = bson.ObjectIdHex(req.MerchantId)
	}

	if req.UserId != "" {
		query["user.id"] = req.UserId
	}

	merchant, err := s.getMerchantBy(query)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err != merchantErrorNotFound {
			rsp.Status = pkg.ResponseStatusBadData
		}

		return nil
	}

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
			{"name": bson.RegEx{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
			{"user.email": bson.RegEx{Pattern: ".*" + req.QuickSearch + ".*", Options: "i"}},
		}
	} else {
		if req.Name != "" {
			query["name"] = bson.RegEx{Pattern: ".*" + req.Name + ".*", Options: "i"}
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
	}

	if len(req.Statuses) > 0 {
		query["status"] = bson.M{"$in": req.Statuses}
	}

	count, err := s.db.Collection(collectionMerchant).Find(query).Count()

	if err != nil {
		zap.S().Errorf("Query to count merchants failed", "err", err.Error(), "query", query)
		return merchantErrorUnknown
	}

	err = s.db.Collection(collectionMerchant).Find(query).Sort(req.Sort...).Limit(int(req.Limit)).
		Skip(int(req.Offset)).All(&merchants)

	if err != nil {
		zap.S().Errorf("Query to find merchants failed", "err", err.Error(), "query", query)
		return merchantErrorUnknown
	}

	rsp.Count = int32(count)
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
	var merchant *billing.Merchant
	var err error
	var isNew bool

	rsp.Status = pkg.ResponseStatusOk

	if req.Id == "" && (req.User == nil || req.User.Id == "") {
		isNew = true
	} else {
		query := make(bson.M)

		if req.Id != "" && req.User != nil && req.User.Id != "" {
			query["$or"] = []bson.M{{"_id": bson.ObjectIdHex(req.Id)}, {"user.id": req.User.Id}}
		} else {
			if req.Id != "" {
				query["_id"] = bson.ObjectIdHex(req.Id)
			}

			if req.User != nil && req.User.Id != "" {
				query["user.id"] = req.User.Id
			}
		}

		merchant, err = s.getMerchantBy(query)

		if err != nil {
			if err != merchantErrorNotFound {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = err.(*grpc.ResponseErrorMessage)
				return nil
			}

			isNew = true
		}
	}

	if isNew {
		merchant = &billing.Merchant{
			Id:        bson.NewObjectId().Hex(),
			User:      req.User,
			Status:    pkg.MerchantStatusDraft,
			CreatedAt: ptypes.TimestampNow(),
		}
	}

	if merchant == nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorUnknown
		return nil
	}

	if merchant.ChangesAllowed() == false {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = merchantErrorChangeNotAllowed
		return nil
	}

	if req.Country != "" {
		country, err := s.country.GetByIsoCodeA2(req.Country)

		if err != nil {
			zap.S().Errorf("Get country for merchant failed", "err", err.Error(), "request", req)
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = merchantErrorCountryNotFound
			return nil
		}

		merchant.Country = country.IsoCodeA2
	}

	merchant.Banking = &billing.MerchantBanking{}

	if req.Banking != nil && req.Banking.Currency != "" {
		currency, err := s.currency.GetByCodeA3(req.Banking.Currency)

		if err != nil {
			zap.S().Errorf("Get currency for merchant failed", "err", err.Error(), "request", req)
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = merchantErrorCurrencyNotFound
			return nil
		}

		merchant.Banking.Currency = currency
	}

	merchant.Name = req.Name
	merchant.AlternativeName = req.AlternativeName
	merchant.Website = req.Website
	merchant.State = req.State
	merchant.Zip = req.Zip
	merchant.City = req.City
	merchant.Address = req.Address
	merchant.AddressAdditional = req.AddressAdditional
	merchant.RegistrationNumber = req.RegistrationNumber
	merchant.TaxId = req.TaxId
	merchant.Contacts = req.Contacts
	merchant.Banking.Name = req.Banking.Name
	merchant.Banking.Address = req.Banking.Address
	merchant.Banking.AccountNumber = req.Banking.AccountNumber
	merchant.Banking.Swift = req.Banking.Swift
	merchant.Banking.Details = req.Banking.Details
	merchant.UpdatedAt = ptypes.TimestampNow()

	if isNew {
		err = s.merchant.Insert(merchant)
	} else {
		err = s.merchant.Update(merchant)
	}

	if err != nil {
		zap.S().Errorf("Query to change merchant data failed", "err", err.Error(), "data", merchant)
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorUnknown
		return nil
	}

	rsp.Item = merchant
	return nil
}

func (s *Service) ChangeMerchantStatus(
	ctx context.Context,
	req *grpc.MerchantChangeStatusRequest,
	rsp *grpc.ChangeMerchantStatusResponse,
) error {
	rsp.Status = pkg.ResponseStatusOk

	merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	if req.Status == pkg.MerchantStatusAgreementRequested && merchant.Status != pkg.MerchantStatusDraft {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorAgreementRequested
		return nil
	}

	if req.Status == pkg.MerchantStatusOnReview && merchant.Status != pkg.MerchantStatusAgreementRequested {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorOnReview
		return nil

	}

	if req.Status == pkg.MerchantStatusAgreementSigning && merchant.CanChangeStatusToSigning() == false {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorSigning
		return nil
	}

	if req.Status == pkg.MerchantStatusAgreementSigned && (merchant.Status != pkg.MerchantStatusAgreementSigning ||
		merchant.HasMerchantSignature != true || merchant.HasPspSignature != true) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorSigned
		return nil
	}

	nStatuses := &billing.SystemNotificationStatuses{From: merchant.Status, To: req.Status}
	merchant.Status = req.Status

	if req.Status == pkg.MerchantStatusAgreementSigned {
		merchant.IsSigned = true
	}

	if req.Status == pkg.MerchantStatusDraft {
		merchant.AgreementType = 0
		merchant.HasPspSignature = false
		merchant.HasMerchantSignature = false
		merchant.IsSigned = false
	}

	if title, ok := NotificationStatusChangeTitles[req.Status]; ok {
		_, err := s.addNotification(title, req.Message, merchant.Id, "", nStatuses)

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if err := s.merchant.Update(merchant); err != nil {
		zap.S().Errorf("Query to change merchant data failed", "err", err.Error(), "data", rsp)
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = merchantErrorUnknown
		return nil

	}

	rsp.Item = merchant

	return nil
}

func (s *Service) ChangeMerchantData(
	ctx context.Context,
	req *grpc.ChangeMerchantDataRequest,
	rsp *grpc.ChangeMerchantDataResponse,
) error {
	merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	if req.AgreementType > 0 && merchant.AgreementType != req.AgreementType {
		if merchant.ChangesAllowed() == false {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = merchantErrorAgreementTypeSelectNotAllow

			return nil
		}

		nStatuses := &billing.SystemNotificationStatuses{From: merchant.Status, To: pkg.MerchantStatusAgreementRequested}
		_, err := s.addNotification(NotificationStatusChangeTitles[merchant.Status], "", merchant.Id, "", nStatuses)

		if err != nil {
			zap.S().Errorf("Add notification failed", "err", err.Error(), "data", merchant)
		}

		merchant.Status = pkg.MerchantStatusAgreementRequested
		merchant.AgreementType = req.AgreementType
	}

	merchant.HasPspSignature = req.HasPspSignature
	merchant.HasMerchantSignature = req.HasMerchantSignature
	merchant.AgreementSentViaMail = req.AgreementSentViaMail
	merchant.MailTrackingLink = req.MailTrackingLink
	merchant.IsSigned = merchant.HasPspSignature == true && merchant.HasMerchantSignature == true

	if merchant.NeedMarkESignAgreementAsSigned() == true {
		merchant.Status = pkg.MerchantStatusAgreementSigned
	}

	if err := s.merchant.Update(merchant); err != nil {
		zap.S().Errorf("Query to change merchant data failed", "err", err.Error(), "data", merchant)
		return merchantErrorUnknown
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
	merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	merchant.S3AgreementName = req.S3AgreementName

	if err := s.merchant.Update(merchant); err != nil {
		zap.S().Errorf("Query to change merchant data failed", "err", err.Error(), "data", merchant)
		return merchantErrorUnknown
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

	_, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	if req.UserId == "" || bson.IsObjectIdHex(req.UserId) == false {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = notificationErrorUserIdIncorrect
		return nil
	}

	if req.Message == "" {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = notificationErrorMessageIsEmpty
		return nil
	}

	n, err := s.addNotification(req.Title, req.Message, req.MerchantId, req.UserId, nil)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	rsp.Item = n

	return nil
}

func (s *Service) GetNotification(
	ctx context.Context,
	req *grpc.GetNotificationRequest,
	rsp *billing.Notification,
) error {
	notification, err := s.getNotificationById(req.MerchantId, req.NotificationId)

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

	if req.MerchantId != "" && bson.IsObjectIdHex(req.MerchantId) == true {
		query["merchant_id"] = bson.ObjectIdHex(req.MerchantId)
	}

	if req.UserId != "" && bson.IsObjectIdHex(req.UserId) == true {
		query["user_id"] = bson.ObjectIdHex(req.UserId)
	}

	if req.IsSystem > 0 {
		if req.IsSystem == 1 {
			query["is_system"] = false
		} else {
			query["is_system"] = true
		}
	}

	count, err := s.db.Collection(collectionNotification).Find(query).Count()

	if err != nil {
		zap.S().Errorf("Query to count merchant notifications failed", "err", err.Error(), "query", query)
		return merchantErrorUnknown
	}

	err = s.db.Collection(collectionNotification).Find(query).Sort(req.Sort...).
		Limit(int(req.Limit)).Skip(int(req.Offset)).All(&notifications)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.S().Errorf("Query to find notifications failed", "err", err.Error(), "query", query)
			return merchantErrorUnknown
		}

		return nil
	}

	rsp.Count = int32(count)
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
	notification, err := s.getNotificationById(req.MerchantId, req.NotificationId)

	if err != nil {
		return err
	}

	notification.IsRead = true

	err = s.db.Collection(collectionNotification).UpdateId(bson.ObjectIdHex(notification.Id), notification)

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
	_, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	pms, err := s.merchant.GetPaymentMethod(req.MerchantId, req.PaymentMethodId)
	if err == nil {
		rsp.Item = pms

		return nil
	}

	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)

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
	_, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		return nil
	}

	var pms []*billing.PaymentMethod

	query := bson.M{"is_active": true}

	if req.PaymentMethodName != "" {
		query["name"] = bson.RegEx{Pattern: ".*" + req.PaymentMethodName + ".*", Options: "i"}
	}

	err = s.db.Collection(collectionPaymentMethod).Find(query).Sort(req.Sort...).All(&pms)

	if err != nil {
		zap.S().Errorf("Query to find payment methods failed", "err", err.Error(), "query", query)
		return nil
	}

	if len(pms) <= 0 {
		return nil
	}

	for _, pm := range pms {
		mPm, err := s.merchant.GetPaymentMethod(req.MerchantId, pm.Id)

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
	merchant, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	pm, e := s.paymentMethod.GetById(req.PaymentMethod.Id)
	if e != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}
	req.Integration.Integrated = req.HasIntegration()

	if req.HasPerTransactionCurrency() {
		if _, err := s.currency.GetByCodeA3(req.GetPerTransactionCurrency()); err != nil {
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
		Id:            bson.NewObjectId().Hex(),
		MerchantId:    merchant.Id,
		UserId:        req.UserId,
		CreatedAt:     ptypes.TimestampNow(),
		PaymentMethod: mpm,
	}
	err = s.db.Collection(collectionMerchantPaymentMethodHistory).Insert(history)
	if err != nil {
		zap.S().Errorf("Query to update merchant payment methods history", "err", err.Error(), "data", merchant)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	if err := s.merchant.Update(merchant); err != nil {
		zap.S().Errorf("Query to update merchant payment methods failed", "err", err.Error(), "data", merchant)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = merchant.PaymentMethods[pm.Id]

	return nil
}

func (s *Service) getMerchantBy(query bson.M) (*billing.Merchant, error) {
	var merchant *billing.Merchant
	err := s.db.Collection(collectionMerchant).Find(query).One(&merchant)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Errorf("Query to find merchant by id failed", "err", err.Error(), "query", query)

		return merchant, merchantErrorUnknown
	}

	if merchant == nil {
		return merchant, merchantErrorNotFound
	}

	return merchant, nil
}

func (s *Service) addNotification(
	title, msg, merchantId, userId string,
	nStatuses *billing.SystemNotificationStatuses,
) (*billing.Notification, error) {
	if merchantId == "" || bson.IsObjectIdHex(merchantId) == false {
		return nil, notificationErrorMerchantIdIncorrect
	}

	notification := &billing.Notification{
		Id:         bson.NewObjectId().Hex(),
		Title:      title,
		Message:    msg,
		MerchantId: merchantId,
		IsRead:     false,
		Statuses:   nStatuses,
	}

	if userId == "" || bson.IsObjectIdHex(userId) == false {
		notification.UserId = pkg.SystemUserId
		notification.IsSystem = true
	} else {
		notification.UserId = userId
	}

	err := s.db.Collection(collectionNotification).Insert(notification)

	if err != nil {
		zap.S().Errorf("Query to insert notification failed", "err", err.Error(), "query", notification)
		return nil, merchantErrorUnknown
	}

	return notification, nil
}

func (s *Service) getNotificationById(
	merchantId, notificationId string,
) (notification *billing.Notification, err error) {
	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"_id":         bson.ObjectIdHex(notificationId),
	}
	err = s.db.Collection(collectionNotification).Find(query).One(&notification)

	if err != nil {
		if err != mgo.ErrNotFound {
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
	rsp.Title = notification.Title
	rsp.IsSystem = notification.IsSystem
	rsp.IsRead = notification.IsRead
	rsp.CreatedAt = notification.CreatedAt
	rsp.UpdatedAt = notification.UpdatedAt
}

func (s *Service) CreateOrUpdateOnboardingProfile(
	ctx context.Context,
	req *grpc.PrimaryOnboarding,
	rsp *grpc.GetPrimaryOnboardingResponse,
) error {
	var err error

	profile := s.getOnboardingProfileByUser(req.UserId)

	if profile == nil {
		profile = req
		profile.Id = bson.NewObjectId().Hex()
		profile.CreatedAt = ptypes.TimestampNow()
		profile.UpdatedAt = ptypes.TimestampNow()

		err = s.db.Collection(collectionOnboardingProfile).Insert(profile)
	} else {
		profile, err = s.updateOnboardingProfile(profile, req)
	}

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = onboardingProfileErrorUnknown

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) GetOnboardingProfile(
	ctx context.Context,
	req *grpc.GetPrimaryOnboardingRequest,
	rsp *grpc.GetPrimaryOnboardingResponse,
) error {
	profile := s.getOnboardingProfileByUser(req.UserId)

	if profile == nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = onboardingProfileErrorNotFound

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = profile

	return nil
}

func (s *Service) getOnboardingProfileByUser(userId string) (profile *grpc.PrimaryOnboarding) {
	query := bson.M{"user_id": userId}
	err := s.db.Collection(collectionOnboardingProfile).Find(query).One(&profile)

	if err != nil && err != mgo.ErrNotFound {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOnboardingProfile),
			zap.Any("query", query),
		)
	}

	return profile
}

func (s *Service) updateOnboardingProfile(
	profile, profileReq *grpc.PrimaryOnboarding,
) (*grpc.PrimaryOnboarding, error) {
	if profileReq.HasPersonChanges(profile) == true {
		if profile.Personal == nil {
			profile.Personal = &grpc.PrimaryOnboardingPersonal{}
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
			profile.Help = &grpc.PrimaryOnboardingHelp{}
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
			profile.Company = &grpc.PrimaryOnboardingCompany{}
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
				profile.Company.Monetization = &grpc.PrimaryOnboardingCompanyMonetization{}
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
				profile.Company.Platforms = &grpc.PrimaryOnboardingCompanyPlatforms{}
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

	err := s.db.Collection(collectionOnboardingProfile).UpdateId(bson.ObjectIdHex(profile.Id), profile)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOnboardingProfile),
			zap.Any("profile", profile),
		)

		return nil, err
	}

	return profile, nil
}
