package service

var (
	errorPaymentChannelSystemGetAll    = newBillingServerErrorMsg("pcs000001", "can't get list of payment channel setting for system")
	errorPaymentChannelSystemGet       = newBillingServerErrorMsg("pcs000002", "can't get payment channel setting for system")
	errorPaymentChannelSystemSetFailed = newBillingServerErrorMsg("pcs000003", "can't set payment channel setting for system")
	errorPaymentChannelSystemDelete    = newBillingServerErrorMsg("pcs000004", "can't delete payment channel setting for system")

	errorPaymentChannelMerchantGetAll    = newBillingServerErrorMsg("pcm000001", "can't get list of payment channel setting for merchant")
	errorPaymentChannelMerchantGet       = newBillingServerErrorMsg("pcm000002", "can't get payment channel setting for merchant")
	errorPaymentChannelMerchantSetFailed = newBillingServerErrorMsg("pcm000003", "can't set payment channel setting for merchant")
	errorPaymentChannelMerchantDelete    = newBillingServerErrorMsg("pcm000004", "can't delete payment channel setting for merchant")

	errorMoneybackSystemGetAll    = newBillingServerErrorMsg("mbs000001", "can't get list of money back setting for system")
	errorMoneybackSystemGet       = newBillingServerErrorMsg("mbs000002", "can't get money back setting for system")
	errorMoneybackSystemSetFailed = newBillingServerErrorMsg("mbs000003", "can't set money back setting for system")
	errorMoneybackSystemDelete    = newBillingServerErrorMsg("mbs000004", "can't delete money back setting for system")

	errorMoneybackMerchantGetAll    = newBillingServerErrorMsg("mbs000001", "can't get list of money back setting for merchant")
	errorMoneybackMerchantGet       = newBillingServerErrorMsg("mbs000002", "can't get money back setting for merchant")
	errorMoneybackMerchantSetFailed = newBillingServerErrorMsg("mbs000003", "can't set money back setting for merchant")
	errorMoneybackMerchantDelete    = newBillingServerErrorMsg("mbs000004", "can't delete money back setting for merchant")
)
