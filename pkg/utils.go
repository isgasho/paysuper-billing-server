package pkg

import (
	"fmt"
	"strings"
)

func GetPaymentMethodKey(currency, mccCode, operatingCompanyId, brand string) string {
	return fmt.Sprintf(PaymentMethodKey, strings.ToUpper(currency), mccCode, strings.ToLower(operatingCompanyId), strings.ToUpper(brand))
}
