package service

import (
	paysuper_i18n "github.com/paysuper/paysuper-i18n"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_Formatter(t *testing.T) {
	shouldBe := require.New(t)

	formatter, err := paysuper_i18n.NewFormatter([]string{"../../i18n/rules"}, []string{"../../i18n/messages"})
	shouldBe.Nil(err)

	price, err := formatter.FormatCurrency("en", 33.33, "USD")
	shouldBe.Nil(err)
	shouldBe.EqualValues("$33.33", price)
}
