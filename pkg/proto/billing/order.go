package billing

func (m *Order) GetMerchantId() string {
	return m.Project.MerchantId
}

func (m *Order) GetPaymentMethodId() string {
	return m.PaymentMethod.Id
}
