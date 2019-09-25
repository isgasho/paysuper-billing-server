package localization

import (
	"github.com/vube/i18n"
	"go.uber.org/zap"
)

type Localizator interface {
	FormatCurrency(locale string, amount float64, currency string) (string, error)
}

type localizatorImpl struct {
	factory *i18n.TranslatorFactory
}

func NewLocalizator() (Localizator, error) {
	f, errs := i18n.NewTranslatorFactory(
		[]string{"data/rules"},
		[]string{"data/messages"},
		"en",
	)

	if len(errs) > 0 {
		zap.S().Errorw("Could not create new factory factory", "err", errs[0])
		return nil, errs[0]
	}

	return &localizatorImpl{
		factory: f,
	}, nil
}

func (t *localizatorImpl) FormatCurrency(locale string, amount float64, currency string) (string, error) {
	translator, errs := t.factory.GetTranslator(locale)
	if len(errs) > 0 {
		zap.S().Errorw("Can't get translator", "err", errs[0])
		return "", errs[0]
	}

	result, err := translator.FormatCurrency(amount, currency)
	if err != nil {
		zap.S().Errorw("Can't format currency", "err", err)
		return result, err
	}

	return result, nil
}