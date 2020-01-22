package pkg

import (
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"net/http"
)

type Path struct {
	Path   string
	Method string
}

const (
	LoggerName = "PAYONE_BILLING_SERVER"

	StatusOK              = int32(0)
	StatusErrorValidation = int32(1)
	StatusErrorSystem     = int32(2)
	StatusTemporary       = int32(4)

	MerchantMinimalPayoutLimit = float32(1000)

	RefundStatusCreated               = int32(0)
	RefundStatusRejected              = int32(1)
	RefundStatusInProgress            = int32(2)
	RefundStatusCompleted             = int32(3)
	RefundStatusPaymentSystemDeclined = int32(4)
	RefundStatusPaymentSystemCanceled = int32(5)

	PaymentSystemErrorCreateRefundFailed   = "refund can't be create. try request later"
	PaymentSystemErrorCreateRefundRejected = "refund create request rejected"

	PaymentSystemHandlerCardPay = "cardpay"

	MerchantAgreementTypeESign = 2

	ObjectTypeUser = "user"

	UserIdentityTypeEmail    = "email"
	UserIdentityTypePhone    = "phone"
	UserIdentityTypeExternal = "external"

	TechEmailDomain = "@paysuper.com"

	MigrationSource = "file://./migrations"

	ErrorGrpcServiceCallFailed       = "gRPC call failed"
	ErrorVatReportDateCantBeInFuture = "vat report date cant be in future"
	MethodFinishedWithError          = "method finished with error"
	LogFieldRequest                  = "request"
	LogFieldResponse                 = "response"
	LogFieldHandler                  = "handler"

	ObjectTypeBalanceTransaction = "balance_transaction"

	AccountingEntryTypeRealGrossRevenue                    = "real_gross_revenue"
	AccountingEntryTypeRealTaxFee                          = "real_tax_fee"
	AccountingEntryTypeCentralBankTaxFee                   = "central_bank_tax_fee"
	AccountingEntryTypeRealTaxFeeTotal                     = "real_tax_fee_total"
	AccountingEntryTypePsGrossRevenueFx                    = "ps_gross_revenue_fx"
	AccountingEntryTypePsGrossRevenueFxTaxFee              = "ps_gross_revenue_fx_tax_fee"
	AccountingEntryTypePsGrossRevenueFxProfit              = "ps_gross_revenue_fx_profit"
	AccountingEntryTypeMerchantGrossRevenue                = "merchant_gross_revenue"
	AccountingEntryTypeMerchantTaxFeeCostValue             = "merchant_tax_fee_cost_value"
	AccountingEntryTypeMerchantTaxFeeCentralBankFx         = "merchant_tax_fee_central_bank_fx"
	AccountingEntryTypeMerchantTaxFee                      = "merchant_tax_fee"
	AccountingEntryTypePsMethodFee                         = "ps_method_fee"
	AccountingEntryTypeMerchantMethodFee                   = "merchant_method_fee"
	AccountingEntryTypeMerchantMethodFeeCostValue          = "merchant_method_fee_cost_value"
	AccountingEntryTypePsMarkupMerchantMethodFee           = "ps_markup_merchant_method_fee"
	AccountingEntryTypeMerchantMethodFixedFee              = "merchant_method_fixed_fee"
	AccountingEntryTypeRealMerchantMethodFixedFee          = "real_merchant_method_fixed_fee"
	AccountingEntryTypeMarkupMerchantMethodFixedFeeFx      = "markup_merchant_method_fixed_fee_fx"
	AccountingEntryTypeRealMerchantMethodFixedFeeCostValue = "real_merchant_method_fixed_fee_cost_value"
	AccountingEntryTypePsMethodFixedFeeProfit              = "ps_method_fixed_fee_profit"
	AccountingEntryTypeMerchantPsFixedFee                  = "merchant_ps_fixed_fee"
	AccountingEntryTypeRealMerchantPsFixedFee              = "real_merchant_ps_fixed_fee"
	AccountingEntryTypeMarkupMerchantPsFixedFee            = "markup_merchant_ps_fixed_fee"
	AccountingEntryTypePsMethodProfit                      = "ps_method_profit"
	AccountingEntryTypeMerchantNetRevenue                  = "merchant_net_revenue"
	AccountingEntryTypePsProfitTotal                       = "ps_profit_total"

	AccountingEntryTypeRealRefund                      = "real_refund"
	AccountingEntryTypeRealRefundTaxFee                = "real_refund_tax_fee"
	AccountingEntryTypeRealRefundFee                   = "real_refund_fee"
	AccountingEntryTypeRealRefundFixedFee              = "real_refund_fixed_fee"
	AccountingEntryTypeMerchantRefund                  = "merchant_refund"
	AccountingEntryTypePsMerchantRefundFx              = "ps_merchant_refund_fx"
	AccountingEntryTypeMerchantRefundFee               = "merchant_refund_fee"
	AccountingEntryTypePsMarkupMerchantRefundFee       = "ps_markup_merchant_refund_fee"
	AccountingEntryTypeMerchantRefundFixedFeeCostValue = "merchant_refund_fixed_fee_cost_value"
	AccountingEntryTypeMerchantRefundFixedFee          = "merchant_refund_fixed_fee"
	AccountingEntryTypePsMerchantRefundFixedFeeFx      = "ps_merchant_refund_fixed_fee_fx"
	AccountingEntryTypePsMerchantRefundFixedFeeProfit  = "ps_merchant_refund_fixed_fee_profit"
	AccountingEntryTypeReverseTaxFee                   = "reverse_tax_fee"
	AccountingEntryTypeReverseTaxFeeDelta              = "reverse_tax_fee_delta"
	AccountingEntryTypePsReverseTaxFeeDelta            = "ps_reverse_tax_fee_delta"
	AccountingEntryTypeMerchantReverseTaxFee           = "merchant_reverse_tax_fee"
	AccountingEntryTypeMerchantReverseRevenue          = "merchant_reverse_revenue"
	AccountingEntryTypePsRefundProfit                  = "ps_refund_profit"
	AccountingEntryTypeMerchantRollingReserveCreate    = "merchant_rolling_reserve_create"
	AccountingEntryTypeMerchantRollingReserveRelease   = "merchant_rolling_reserve_release"
	AccountingEntryTypeMerchantRoyaltyCorrection       = "merchant_royalty_correction"

	BalanceTransactionStatusAvailable = "available"

	ErrorTimeConversion       = "Time conversion error"
	ErrorTimeConversionValue  = "value"
	ErrorTimeConversionMethod = "conversion method"

	ErrorDatabaseQueryFailed          = "Query to database collection failed"
	ErrorDatabaseInvalidObjectId      = "String is not a valid ObjectID"
	ErrorQueryCursorExecutionFailed   = "Execute result from query cursor failed"
	ErrorDatabaseFieldCollection      = "collection"
	ErrorDatabaseFieldDocumentId      = "document_id"
	ErrorDatabaseFieldQuery           = "query"
	ErrorDatabaseFieldSet             = "set"
	ErrorDatabaseFieldSorts           = "sorts"
	ErrorDatabaseFieldLimit           = "limit"
	ErrorDatabaseFieldOffset          = "offset"
	ErrorDatabaseFieldOperation       = "operation"
	ErrorDatabaseFieldOperationCount  = "count"
	ErrorDatabaseFieldOperationInsert = "insert"
	ErrorDatabaseFieldOperationUpdate = "update"
	ErrorDatabaseFieldOperationUpsert = "upsert"
	ErrorDatabaseFieldDocument        = "document"

	ErrorJsonMarshallingFailed = "json marshalling failed"

	ErrorCacheQueryFailed = "Query to cache storage failed"
	ErrorCacheFieldKey    = "key"
	ErrorCacheFieldData   = "data"
	ErrorCacheFieldCmd    = "command"

	ErrorRoyaltyReportGenerationFailed = "royalty report generation failed"
	ErrorRoyaltyReportFieldMerchantId  = "merchant_id"
	ErrorRoyaltyReportFieldFrom        = "from"
	ErrorRoyaltyReportFieldTo          = "to"

	OrderTypeOrder  = "order"
	OrderTypeRefund = "refund"

	EmailRoyaltyReportMessage = "Royalty report updated"

	RoyaltyReportChangeSourceAuto     = "auto"
	RoyaltyReportChangeSourceMerchant = "merchant"
	RoyaltyReportChangeSourceAdmin    = "admin"

	VatCurrencyRatesPolicyOnDay    = "on-day"
	VatCurrencyRatesPolicyLastDay  = "last-day"
	VatCurrencyRatesPolicyAvgMonth = "avg-month"

	VatReportStatusThreshold = "threshold"
	VatReportStatusExpired   = "expired"
	VatReportStatusPending   = "pending"
	VatReportStatusNeedToPay = "need_to_pay"
	VatReportStatusPaid      = "paid"
	VatReportStatusOverdue   = "overdue"
	VatReportStatusCanceled  = "canceled"

	UndoReasonReversal   = "reversal"
	UndoReasonChargeback = "chargeback"

	DashboardPeriodCurrentDay      = "current_day"
	DashboardPeriodPreviousDay     = "previous_day"
	DashboardPeriodTwoDaysAgo      = "two_days_ago"
	DashboardPeriodCurrentWeek     = "current_week"
	DashboardPeriodPreviousWeek    = "previous_week"
	DashboardPeriodTwoWeeksAgo     = "two_weeks_ago"
	DashboardPeriodCurrentMonth    = "current_month"
	DashboardPeriodPreviousMonth   = "previous_month"
	DashboardPeriodTwoMonthsAgo    = "two_months_ago"
	DashboardPeriodCurrentQuarter  = "current_quarter"
	DashboardPeriodPreviousQuarter = "previous_quarter"
	DashboardPeriodTwoQuarterAgo   = "two_quarter_ago"
	DashboardPeriodCurrentYear     = "current_year"
	DashboardPeriodPreviousYear    = "previous_year"
	DashboardPeriodTwoYearsAgo     = "two_years_ago"

	PayoutDocumentStatusSkip     = "skip"
	PayoutDocumentStatusPending  = "pending"
	PayoutDocumentStatusPaid     = "paid"
	PayoutDocumentStatusCanceled = "canceled"
	PayoutDocumentStatusFailed   = "failed"

	OrderIssuerReferenceTypePaylink = "paylink"

	PaylinkUrlDefaultMask = "/paylink/%s"

	DatabaseRequestDefaultLimit = int64(100)

	ProjectSellCountTypeFractional = "fractional"
	ProjectSellCountTypeIntegral   = "integral"

	PaymentSystemActionAuthenticate     = "auth"
	PaymentSystemActionRefresh          = "refresh"
	PaymentSystemActionCreatePayment    = "create_payment"
	PaymentSystemActionRecurringPayment = "recurring_payment"
	PaymentSystemActionRefund           = "refund"

	MerchantOperationTypeLowRisk  = "low-risk"
	MerchantOperationTypeHighRisk = "high-risk"

	PaymentMethodKey = "%s:%s:%s:%s" // currency:mcc_code:operating_company_id:brand, for example: "USD:5816:5dc3f70deb494903d835f28a:VISA"

	RoleTypeMerchant = "merchant"
	RoleTypeSystem   = "system"

	UserRoleStatusInvited  = "invited"
	UserRoleStatusAccepted = "accepted"

	// MerchantId_UserId
	CasbinMerchantUserMask = "%s_%s"

	EmailConfirmUrl            = "%s/confirm_email"
	RoyaltyReportsUrl          = "%s/royalty_reports"
	PayoutsUrl                 = "%s/payouts"
	ReceiptPurchaseUrl         = "%s/pay/receipt/purchase/%s/%s"
	ReceiptRefundUrl           = "%s/pay/receipt/refund/%s/%s"
	MerchantCompanyUrl         = "%s/company"
	AdminCompanyUrl            = "%s/merchants/%s/company-info"
	AdminOnboardingRequestsUrl = "%s/agreement-requests"
	UserInviteUrl              = "%s/login?invite_token=%s"

	OrderType_simple         = "simple"
	OrderType_key            = "key"
	OrderType_product        = "product"
	OrderTypeVirtualCurrency = "virtual_currency"

	DefaultPaymentMethodFee               = float64(5)
	DefaultPaymentMethodPerTransactionFee = float64(0)
	DefaultPaymentMethodCurrency          = ""

	ProjectRedirectModeAny        = "any"
	ProjectRedirectModeDisable    = "disable"
	ProjectRedirectModeSuccessful = "successful"
	ProjectRedirectModeFail       = "fail"
	ProjectRedirectUsageAny       = "any"
)

var (
	MerchantOperationsTypesToMccCodes = map[string]string{
		MerchantOperationTypeLowRisk:  billingpb.MccCodeLowRisk,
		MerchantOperationTypeHighRisk: billingpb.MccCodeHighRisk,
	}

	SupportedMccCodes = []string{billingpb.MccCodeLowRisk, billingpb.MccCodeHighRisk}

	SupportedTariffRegions = []string{
		billingpb.TariffRegionRussiaAndCis,
		billingpb.TariffRegionEurope,
		billingpb.TariffRegionAsia,
		billingpb.TariffRegionLatAm,
		billingpb.TariffRegionWorldwide,
	}

	CountryPhoneCodes = map[int32]string{
		7:    "RU",
		375:  "BY",
		994:  "AZ",
		91:   "IN",
		77:   "KZ",
		380:  "UA",
		44:   "GB",
		9955: "GE",
		370:  "LT",
		992:  "TJ",
		66:   "TH",
		998:  "UZ",
		507:  "PA",
		374:  "AM",
		371:  "LV",
		90:   "TR",
		373:  "MD",
		972:  "IL",
		84:   "VN",
		372:  "EE",
		82:   "KR",
		996:  "KG",
	}

	CardPayPaths = map[string]*Path{
		PaymentSystemActionAuthenticate: {
			Path:   "/api/auth/token",
			Method: http.MethodPost,
		},
		PaymentSystemActionRefresh: {
			Path:   "/api/auth/token",
			Method: http.MethodPost,
		},
		PaymentSystemActionCreatePayment: {
			Path:   "/api/payments",
			Method: http.MethodPost,
		},
		PaymentSystemActionRecurringPayment: {
			Path:   "/api/recurrings",
			Method: http.MethodPost,
		},
		PaymentSystemActionRefund: {
			Path:   "/api/refunds",
			Method: http.MethodPost,
		},
	}
)
