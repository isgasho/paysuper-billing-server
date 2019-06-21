package billing

import (
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
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
	return m.RoyaltyData.MerchantRoyaltyCurrency
}
