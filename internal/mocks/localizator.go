package mocks

import (
	paysuper_i18n "github.com/paysuper/paysuper-i18n"
	"time"
)

type formatterOk struct {
}

func (formatterOk) FormatDateTime(locale string, datetime time.Time) (string, error) {
	return "test", nil
}

func (formatterOk) FormatCurrency(locale string, amount float64, currency string) (string, error) {
	return "test", nil
}

func NewFormatterOK() paysuper_i18n.Formatter {
	return &formatterOk{}
}
