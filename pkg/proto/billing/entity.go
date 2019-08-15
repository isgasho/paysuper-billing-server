package billing

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
)

var (
	orderRefundAllowedStatuses = map[int32]bool{
		constant.OrderStatusPaymentSystemComplete: true,
		constant.OrderStatusProjectInProgress:     true,
		constant.OrderStatusProjectComplete:       true,
		constant.OrderStatusProjectPending:        true,
	}

	orderStatusPublicMapping = map[int32]string{
		constant.OrderStatusNew:                         constant.OrderPublicStatusCreated,
		constant.OrderStatusPaymentSystemCreate:         constant.OrderPublicStatusCreated,
		constant.OrderStatusPaymentSystemCanceled:       constant.OrderPublicStatusCanceled,
		constant.OrderStatusPaymentSystemRejectOnCreate: constant.OrderPublicStatusRejected,
		constant.OrderStatusPaymentSystemReject:         constant.OrderPublicStatusRejected,
		constant.OrderStatusProjectReject:               constant.OrderPublicStatusRejected,
		constant.OrderStatusPaymentSystemDeclined:       constant.OrderPublicStatusRejected,
		constant.OrderStatusPaymentSystemComplete:       constant.OrderPublicStatusProcessed,
		constant.OrderStatusProjectComplete:             constant.OrderPublicStatusProcessed,
		constant.OrderStatusRefund:                      constant.OrderPublicStatusRefunded,
		constant.OrderStatusChargeback:                  constant.OrderPublicStatusChargeback,
	}
)

func (m *Merchant) ChangesAllowed() bool {
	return m.Status == pkg.MerchantStatusDraft
}

func (m *Merchant) GetPayoutCurrency() string {
	if m.Banking == nil {
		return ""
	}

	return m.Banking.Currency
}

func (m *Merchant) NeedMarkESignAgreementAsSigned() bool {
	return m.HasMerchantSignature == true && m.HasPspSignature == true &&
		m.Status != pkg.MerchantStatusAgreementSigned
}

func (m *Merchant) CanGenerateAgreement() bool {
	return (m.Status == pkg.MerchantStatusOnReview || m.Status == pkg.MerchantStatusAgreementSigning ||
		m.Status == pkg.MerchantStatusAgreementSigned) && m.Banking != nil && m.Company.Country != "" &&
		m.Contacts != nil && m.Contacts.Authorized != nil
}

func (m *Merchant) CanChangeStatusToSigning() bool {
	return m.Status == pkg.MerchantStatusOnReview && m.Banking != nil && m.Company.Country != "" &&
		m.Contacts != nil && m.Contacts.Authorized != nil
}

func (m *Merchant) IsFullySigned() bool {
	return m.HasMerchantSignature && m.HasPspSignature
}

func (m *Merchant) IsDeleted() bool {
	return m.Status == pkg.MerchantStatusDeleted
}

func (m *Project) IsProduction() bool {
	return m.Status == pkg.ProjectStatusInProduction
}

func (m *Project) IsDeleted() bool {
	return m.Status == pkg.ProjectStatusDeleted
}

func (m *Project) NeedChangeStatusToDraft(req *Project) bool {
	if m.Status != pkg.ProjectStatusTestCompleted &&
		m.Status != pkg.ProjectStatusInProduction {
		return false
	}

	if m.CallbackProtocol == pkg.ProjectCallbackProtocolEmpty &&
		req.CallbackProtocol == pkg.ProjectCallbackProtocolDefault {
		return true
	}

	if req.UrlCheckAccount != "" &&
		req.UrlCheckAccount != m.UrlCheckAccount {
		return true
	}

	if req.UrlProcessPayment != "" &&
		req.UrlProcessPayment != m.UrlProcessPayment {
		return true
	}

	return false
}

func (m *OrderUser) IsIdentified() bool {
	return m.Id != "" && bson.IsObjectIdHex(m.Id) == true
}

func (m *PaymentMethod) IsValid() bool {
	return m.ExternalId != "" &&
		m.Currencies != nil &&
		m.Type != "" &&
		m.Group != "" &&
		m.Name != "" &&
		m.TestSettings != nil &&
		m.ProductionSettings != nil
}

func (m *Merchant) HasAuthorizedEmail() bool {
	return m.Contacts != nil && m.Contacts.Authorized != nil && m.Contacts.Authorized.Email != ""
}

func (m *Merchant) GetAuthorizedEmail() string {
	return m.Contacts.Authorized.Email
}

func (m *Merchant) GetAuthorizedName() string {
	return m.Contacts.Authorized.Name
}

func (m *RoyaltyReport) ChangesAvailable(newStatus string) bool {
	if m.Status == pkg.RoyaltyReportStatusAccepted {
		return false
	}

	if m.Status == pkg.RoyaltyReportStatusNew && newStatus != pkg.RoyaltyReportStatusPending && newStatus != pkg.RoyaltyReportStatusCanceled {
		return false
	}

	if m.Status == pkg.RoyaltyReportStatusPending && newStatus != pkg.RoyaltyReportStatusAccepted &&
		newStatus != pkg.RoyaltyReportStatusDispute {
		return false
	}

	if m.Status == pkg.RoyaltyReportStatusCanceled && newStatus != pkg.RoyaltyReportStatusNew {
		return false
	}

	if m.Status == pkg.RoyaltyReportStatusDispute && newStatus != pkg.RoyaltyReportStatusPending {
		return false
	}

	return true
}

func (m *Merchant) IsAgreementSigningStarted() bool {
	return m.AgreementSignatureData != nil && (!m.HasPspSignature || !m.HasMerchantSignature)
}

func (m *Merchant) IsAgreementSigned() bool {
	return m.HasMerchantSignature && m.HasPspSignature
}

func (m *Merchant) GetPrintableStatus() string {
	status := "draft"

	if m.Status == pkg.MerchantStatusAgreementSigned {
		status = "life"
	}

	return status
}

func (m *Merchant) GetCompleteStepsCount() int32 {
	count := int32(0)

	if m.Steps.Company {
		count++
	}

	if m.Steps.Contacts {
		count++
	}

	if m.Steps.Banking {
		count++
	}

	if m.Steps.Tariff {
		count++
	}

	return count
}

func (m *Merchant) IsDataComplete() bool {
	return m.Company != nil && m.Contacts != nil && m.Banking != nil && m.Tariff != ""
}

func (m *Merchant) GetMerchantSignatureId() string {
	return m.AgreementSignatureData.MerchantSignatureId
}

func (m *Merchant) GetPaysuperSignatureId() string {
	return m.AgreementSignatureData.PsSignatureId
}

func (m *Merchant) GetMerchantSignUrl() *MerchantAgreementSignatureDataSignUrl {
	return m.AgreementSignatureData.MerchantSignUrl
}

func (m *Merchant) GetPaysuperSignUrl() *MerchantAgreementSignatureDataSignUrl {
	return m.AgreementSignatureData.PsSignUrl
}

func (m *Merchant) IsPaysuperSignatureId(signatureId string) bool {
	return m.AgreementSignatureData.PsSignatureId == signatureId
}

func (m *Merchant) IsMerchantSignature(signatureId string) bool {
	return m.AgreementSignatureData.MerchantSignatureId == signatureId
}
