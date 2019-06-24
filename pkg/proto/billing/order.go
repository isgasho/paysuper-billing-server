package billing

import (
	"errors"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
)

const (
	orderBankCardBrandNotFound = "brand for bank card not found"
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

func (m *Order) GetUserCountry() string {
	if m.BillingAddress != nil && m.BillingAddress.Country != "" {
		return m.BillingAddress.Country
	}

	return m.User.Address.Country
}

func (m *Order) GetCostPaymentMethodName() (string, error) {
	if m.PaymentMethod.IsBankCard() == false {
		return m.GetPaymentMethodName(), nil
	}

	return m.GetBankCardBrand()
}
