package billing

import (
	"errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"time"
)

const (
	orderBankCardBrandNotFound = "brand for bank card not found"
	orderPaymentMethodNotSet   = "payment method not set"
)

func (m *Order) GetMerchantId() string {
	return m.Project.MerchantId
}

func (m *Order) GetPaymentMethodId() string {
	return m.PaymentMethod.Id
}

func (m *Order) IsDeclined() bool {
	return m.PrivateStatus == constant.OrderStatusPaymentSystemDeclined || m.PrivateStatus == constant.OrderStatusPaymentSystemCanceled
}

func (m *Order) GetDeclineReason() string {
	reason, ok := m.PaymentMethodTxnParams[pkg.TxnParamsFieldDeclineReason]

	if !ok {
		return ""
	}

	return reason
}

func (m *Order) GetPrivateDeclineCode() string {
	code, ok := m.PaymentMethodTxnParams[pkg.TxnParamsFieldDeclineCode]

	if !ok {
		return ""
	}

	return code
}

func (m *Order) GetPublicDeclineCode() string {
	code := m.GetPrivateDeclineCode()

	if code == "" {
		return ""
	}

	code, ok := pkg.DeclineCodeMap[code]

	if !ok {
		return ""
	}

	return code
}

func (m *Order) GetMerchantRoyaltyCurrency() string {
	return m.PaymentMethod.Params.Currency
}

func (m *Order) GetPaymentMethodName() string {
	return m.PaymentMethod.Name
}

func (m *Order) GetBankCardBrand() (string, error) {
	val, ok := m.PaymentRequisites[pkg.PaymentCreateBankCardFieldBrand]

	if !ok {
		return "", errors.New(orderBankCardBrandNotFound)
	}

	return val, nil
}

func (m *Order) GetCountry() string {
	if m.BillingAddress != nil && m.BillingAddress.Country != "" {
		return m.BillingAddress.Country
	}
	if m.User != nil && m.User.Address != nil && m.User.Address.Country != "" {
		return m.User.Address.Country
	}
	return ""
}

func (m *Order) HasEndedStatus() bool {
	return m.PrivateStatus == constant.OrderStatusPaymentSystemReject || m.PrivateStatus == constant.OrderStatusProjectComplete ||
		m.PrivateStatus == constant.OrderStatusProjectReject || m.PrivateStatus == constant.OrderStatusRefund ||
		m.PrivateStatus == constant.OrderStatusChargeback
}

func (m *Order) RefundAllowed() bool {
	v, ok := orderRefundAllowedStatuses[m.PrivateStatus]

	return ok && v == true
}

func (m *Order) FormInputTimeIsEnded() bool {
	t, err := ptypes.Timestamp(m.ExpireDateToFormInput)

	return err != nil || t.Before(time.Now())
}

func (m *Order) GetProjectId() string {
	return m.Project.Id
}

func (m *Order) GetPublicStatus() string {
	st, ok := orderStatusPublicMapping[m.PrivateStatus]
	if !ok {
		return constant.OrderPublicStatusPending
	}
	return st
}

func (m *Order) GetReceiptUserEmail() string {
	if m.User != nil {
		return m.User.Email
	}
	return ""
}

func (m *Order) GetReceiptUserPhone() string {
	if m.User != nil {
		return m.User.Phone
	}
	return ""
}

func (m *Order) GetState() string {
	if m.BillingAddress != nil && m.BillingAddress.State != "" {
		return m.BillingAddress.State
	}
	if m.User != nil && m.User.Address != nil && m.User.Address.State != "" {
		return m.User.Address.State
	}
	return ""
}

func (m *Order) SetNotificationStatus(key string, val bool) {
	if m.IsNotificationsSent == nil {
		m.IsNotificationsSent = make(map[string]bool)
	}
	m.IsNotificationsSent[key] = val
}

func (m *Order) GetNotificationStatus(key string) bool {
	if m.IsNotificationsSent == nil {
		return false
	}
	val, ok := m.IsNotificationsSent[key]
	if !ok {
		return false
	}
	return val
}

func (m *Order) GetCostPaymentMethodName() (string, error) {
	if m.PaymentMethod == nil {
		return "", errors.New(orderPaymentMethodNotSet)
	}
	if m.PaymentMethod.IsBankCard() {
		return m.GetBankCardBrand()
	}
	return m.GetPaymentMethodName(), nil
}
