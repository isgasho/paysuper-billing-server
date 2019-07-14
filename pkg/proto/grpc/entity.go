package grpc

import (
	"errors"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

var (
	productNoPriceInCurrency           = "no price in currency %s"
	productNoNameInLanguage            = "no name in language %s"
	productNoDescriptionInLanguage     = "no description in language %s"
	productNoLongDescriptionInLanguage = "no long description in language %s"
)

func (m *MerchantPaymentMethodRequest) GetPerTransactionCurrency() string {
	return m.Commission.PerTransaction.Currency
}

func (m *MerchantPaymentMethodRequest) GetPerTransactionFee() float64 {
	return m.Commission.PerTransaction.Fee
}

func (m *MerchantPaymentMethodRequest) HasPerTransactionCurrency() bool {
	return m.Commission.PerTransaction.Currency != ""
}

func (m *MerchantPaymentMethodRequest) HasIntegration() bool {
	return m.Integration.TerminalId != "" && m.Integration.TerminalPassword != "" &&
		m.Integration.TerminalCallbackPassword != ""
}

func (p *Product) IsPricesContainDefaultCurrency() bool {
	_, err := p.GetPriceInCurrency(&billing.PriceGroup{Currency: p.DefaultCurrency})
	return err == nil
}

func (p *Product) GetPriceInCurrency(group *billing.PriceGroup) (float64, error) {
	for _, price := range p.Prices {
		if group.Region != "" && price.Region == group.Region {
			return price.Amount, nil
		}

		if group.Region == "" && price.Region == group.Currency {
			return price.Amount, nil
		}
	}
	return 0, errors.New(fmt.Sprintf(productNoPriceInCurrency, group.Region))
}

func (p *Product) GetLocalizedName(lang string) (string, error) {
	v, ok := p.Name[lang]
	if !ok {
		return "", errors.New(fmt.Sprintf(productNoNameInLanguage, lang))
	}
	return v, nil
}

func (p *Product) GetLocalizedDescription(lang string) (string, error) {
	v, ok := p.Description[lang]
	if !ok {
		return "", errors.New(fmt.Sprintf(productNoDescriptionInLanguage, lang))
	}
	return v, nil
}

func (p *Product) GetLocalizedLongDescription(lang string) (string, error) {
	v, ok := p.LongDescription[lang]
	if !ok {
		return "", errors.New(fmt.Sprintf(productNoLongDescriptionInLanguage, lang))
	}
	return v, nil
}

func (r *ResponseErrorMessage) Error() string {
	return r.Message
}

func (r *ResponseError) Error() string {
	return r.Message.Message
}
