package mocks

import "github.com/paysuper/paysuper-billing-server/internal/localization"

type localizatorOk struct {

}

func (localizatorOk) FormatCurrency(locale string, amount float64, currency string) (string, error) {
	return "test", nil
}

func NewLocalizatorOK() localization.Localizator {
	return &localizatorOk{
	}
}
