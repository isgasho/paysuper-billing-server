package mocks

import paysuper_i18n "github.com/paysuper/paysuper-i18n"

type formatterOk struct {

}

func (formatterOk) FormatCurrency(locale string, amount float64, currency string) (string, error) {
	return "test", nil
}

func NewFormatterOK() paysuper_i18n.Formatter {
	return &formatterOk{
	}
}
