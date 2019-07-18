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

func (m *UserProfile) HasPersonChanges(profile *UserProfile) bool {
	return m.Personal != nil && profile.Personal != m.Personal
}

func (m *UserProfile) HasHelpChanges(profile *UserProfile) bool {
	return m.Help != nil && profile.Help != m.Help
}

func (m *UserProfile) HasCompanyChanges(profile *UserProfile) bool {
	return m.Company != nil && profile.Company != m.Company
}

func (m *UserProfile) HasCompanyAnnualIncomeChanges(profile *UserProfile) bool {
	return m.Company.AnnualIncome != nil && profile.Company.AnnualIncome != m.Company.AnnualIncome
}

func (m *UserProfile) HasCompanyNumberOfEmployeesChanges(profile *UserProfile) bool {
	return m.Company.NumberOfEmployees != nil && profile.Company.NumberOfEmployees != m.Company.NumberOfEmployees
}

func (m *UserProfile) HasCompanyMonetizationChanges(profile *UserProfile) bool {
	return m.Company.Monetization != nil && profile.Company.Monetization != m.Company.Monetization
}

func (m *UserProfile) HasCompanyPlatformsChanges(profile *UserProfile) bool {
	return m.Company.Platforms != nil && profile.Company.Platforms != m.Company.Platforms
}
