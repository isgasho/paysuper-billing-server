package errors

import (
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

func newBillingServerErrorMsg(code, msg string, details ...string) *billingpb.ResponseErrorMessage {
	var det string

	if len(details) > 0 && details[0] != "" {
		det = details[0]
	} else {
		det = ""
	}

	return &billingpb.ResponseErrorMessage{Code: code, Message: msg, Details: det}
}

var (
	KeyErrorFileProcess    = newBillingServerErrorMsg("ks000001", "failed to process file")
	KeyErrorNotFound       = newBillingServerErrorMsg("ks000002", "key not found")
	KeyErrorFailedToInsert = newBillingServerErrorMsg("ks000003", "failed to insert key")
	KeyErrorCanceled       = newBillingServerErrorMsg("ks000004", "unable to cancel key")
	KeyErrorFinish         = newBillingServerErrorMsg("ks000005", "unable to finish key")
	KeyErrorReserve        = newBillingServerErrorMsg("ks000006", "unable to reserve key")
)
