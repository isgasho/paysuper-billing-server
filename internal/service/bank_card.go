package service

import (
	"strconv"
	"time"
)

var (
	bankCardPanIsRequired         = newBillingServerErrorMsg("bc000001", "bank card number is required")
	bankCardCvvIsRequired         = newBillingServerErrorMsg("bc000002", "bank card CVV number is required")
	bankCardExpireMonthIsRequired = newBillingServerErrorMsg("bc000003", "bank card expire month is required")
	bankCardExpireYearIsRequired  = newBillingServerErrorMsg("bc000004", "bank card expire year is required")
	bankCardHolderIsRequired      = newBillingServerErrorMsg("bc000005", "bank card holder name is required")
	bankCardMonthIsInvalid        = newBillingServerErrorMsg("bc000006", "invalid month of card expiration")
	bankCardIsExpired             = newBillingServerErrorMsg("bc000007", "bank card is expired")
	bankCardCvvIsInvalid          = newBillingServerErrorMsg("bc000008", "bank card CVV is invalid")
	bankCardPanIsInvalid          = newBillingServerErrorMsg("bc000009", "bank card number is invalid")
)

type bankCardValidator struct {
	Pan    string
	Cvv    string
	Month  string
	Year   string
	Holder string
}

func (v *bankCardValidator) Validate() error {
	if len(v.Pan) <= 0 {
		return bankCardPanIsRequired
	}

	if len(v.Cvv) <= 0 {
		return bankCardCvvIsRequired
	}

	if len(v.Month) <= 0 {
		return bankCardExpireMonthIsRequired
	}

	if len(v.Year) <= 0 {
		return bankCardExpireYearIsRequired
	}

	if len(v.Holder) <= 0 {
		return bankCardHolderIsRequired
	}

	if err := v.validateExpire(); err != nil {
		return err
	}

	if len(v.Cvv) < 3 || len(v.Cvv) > 4 {
		return bankCardCvvIsInvalid
	}

	if len(v.Pan) < 13 {
		return bankCardPanIsInvalid
	}

	if ok := v.validateNumber(); !ok {
		return bankCardPanIsInvalid
	}

	return nil
}

func (v *bankCardValidator) validateExpire() error {
	var year int
	var month int

	if len(v.Year) < 3 {
		year, _ = strconv.Atoi(strconv.Itoa(time.Now().UTC().Year())[:2] + v.Year)
	} else {
		year, _ = strconv.Atoi(v.Year)
	}

	month, _ = strconv.Atoi(v.Month)

	if month < 1 || month > 12 {
		return bankCardMonthIsInvalid
	}

	tn := time.Now().UTC()

	if year < tn.Year() {
		return bankCardIsExpired
	}

	if year == tn.Year() && month < int(tn.Month()) {
		return bankCardIsExpired
	}

	return nil
}

func (v *bankCardValidator) validateNumber() bool {
	var sum int
	var alternate bool

	numberLen := len(v.Pan)

	if numberLen < 13 || numberLen > 19 {
		return false
	}

	for i := numberLen - 1; i > -1; i-- {
		mod, _ := strconv.Atoi(string(v.Pan[i]))

		if alternate {
			mod *= 2

			if mod > 9 {
				mod = (mod % 10) + 1
			}
		}

		alternate = !alternate
		sum += mod
	}

	return sum%10 == 0
}
