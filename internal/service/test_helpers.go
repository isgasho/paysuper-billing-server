package service

// helpers, that used in accounting_entry_test, royalty_report_test, vat_reports_test and order_view_test

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/internal/repository"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math/rand"
	"strconv"
	"time"
)

func helperCreateEntitiesForTests(suite suite.Suite, service *Service) (
	*billingpb.Merchant,
	*billingpb.Project,
	*billingpb.PaymentMethod,
	*billingpb.PaymentSystem,
) {

	paymentMinLimitsSystem := []*billingpb.PaymentMinLimitSystem{
		{
			Currency: "RUB",
			Amount:   0.01,
		},
		{
			Currency: "USD",
			Amount:   0.01,
		},
		{
			Currency: "EUR",
			Amount:   0.01,
		},
	}
	err := service.paymentMinLimitSystem.MultipleInsert(context.TODO(), paymentMinLimitsSystem)
	if err != nil {
		suite.FailNow("Insert PaymentMinLimitSystem test data failed", "%v", err)
	}

	operatingCompany := helperOperatingCompany(suite, service)

	keyRub := billingpb.GetPaymentMethodKey("RUB", billingpb.MccCodeLowRisk, operatingCompany.Id, "")
	keyUsd := billingpb.GetPaymentMethodKey("USD", billingpb.MccCodeLowRisk, operatingCompany.Id, "")
	keyEur := billingpb.GetPaymentMethodKey("EUR", billingpb.MccCodeLowRisk, operatingCompany.Id, "")

	paymentSystem := &billingpb.PaymentSystem{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "CardPay",
		AccountingCurrency: "RUB",
		AccountingPeriod:   "every-day",
		Country:            "",
		IsActive:           true,
		Handler:            paymentSystemHandlerCardPayMock,
	}

	pmBankCard := &billingpb.PaymentMethod{
		Id:               primitive.NewObjectID().Hex(),
		Name:             "Bank card",
		Group:            "BANKCARD",
		MinPaymentAmount: 10,
		MaxPaymentAmount: 15000,
		ExternalId:       "BANKCARD",
		ProductionSettings: map[string]*billingpb.PaymentMethodParams{
			keyRub: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "RUB",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
			keyUsd: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "USD",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
			keyEur: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "EUR",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
		},
		TestSettings: map[string]*billingpb.PaymentMethodParams{
			keyRub: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "RUB",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
			keyUsd: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "USD",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
			keyEur: {
				TerminalId:         "15985",
				Secret:             "A1tph4I6BD0f",
				SecretCallback:     "0V1rJ7t4jCRv",
				Currency:           "EUR",
				MccCode:            billingpb.MccCodeLowRisk,
				OperatingCompanyId: operatingCompany.Id,
				Brand:              []string{"VISA", "MASTERCARD"},
			},
		},
		Type:            "bank_card",
		IsActive:        true,
		AccountRegexp:   "^(?:4[0-9]{12}(?:[0-9]{3})?|[25][1-7][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$",
		PaymentSystemId: paymentSystem.Id,
		RefundAllowed:   true,
	}

	merchant := helperCreateMerchant(suite, service, "USD", "RU", pmBankCard, 0, operatingCompany.Id)

	projectFixedAmount := helperCreateProject(suite, service, merchant.Id, billingpb.VatPayerBuyer)

	bin := &BinData{
		Id:                 primitive.NewObjectID(),
		CardBin:            400000,
		CardBrand:          "MASTERCARD",
		CardType:           "DEBIT",
		CardCategory:       "WORLD",
		BankName:           "ALFA BANK",
		BankCountryName:    "UKRAINE",
		BankCountryIsoCode: "UA",
	}

	_, err = service.db.Collection(collectionBinData).InsertOne(context.TODO(), bin)

	if err != nil {
		suite.FailNow("Insert BIN test data failed", "%v", err)
	}

	pms := []*billingpb.PaymentMethod{pmBankCard}
	if err := service.paymentMethod.MultipleInsert(context.TODO(), pms); err != nil {
		suite.FailNow("Insert payment methods test data failed", "%v", err)
	}

	ps := []*billingpb.PaymentSystem{paymentSystem}
	if err := service.paymentSystem.MultipleInsert(context.TODO(), ps); err != nil {
		suite.FailNow("Insert payment system test data failed", "%v", err)
	}

	sysCost := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "reversal",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost2 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "RUB",
		UndoReason:         "reversal",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost3 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "reversal",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "US",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost4 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "reversal",
		Region:             billingpb.TariffRegionEurope,
		Country:            "FI",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost5 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost6 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "RUB",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost7 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "US",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	sysCost8 := &billingpb.MoneyBackCostSystem{
		Name:               "MASTERCARD",
		PayoutCurrency:     "USD",
		UndoReason:         "chargeback",
		Region:             billingpb.TariffRegionEurope,
		Country:            "FI",
		DaysFrom:           0,
		PaymentStage:       1,
		Percent:            0.10,
		FixAmount:          0.15,
		FixAmountCurrency:  "EUR",
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}

	err = service.moneyBackCostSystem.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostSystem{sysCost, sysCost2, sysCost3, sysCost4, sysCost5, sysCost6, sysCost7, sysCost8})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostSystem test data failed", "%v", err)
	}

	paymentSysCost1 := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		Percent:            0.015,
		FixAmount:          0.01,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	paymentSysCost2 := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "US",
		Percent:            0.015,
		FixAmount:          0.01,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	paymentSysCost3 := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionEurope,
		Country:            "FI",
		Percent:            0.015,
		FixAmount:          0.01,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	paymentSysCost4 := &billingpb.PaymentChannelCostSystem{
		Name:               "VISA",
		Region:             billingpb.TariffRegionRussiaAndCis,
		Country:            "RU",
		Percent:            0.015,
		FixAmount:          0.01,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}
	paymentSysCost5 := &billingpb.PaymentChannelCostSystem{
		Name:               "MASTERCARD",
		Region:             billingpb.TariffRegionWorldwide,
		Country:            "AO",
		Percent:            0.015,
		FixAmount:          0.01,
		FixAmountCurrency:  "USD",
		IsActive:           true,
		MccCode:            billingpb.MccCodeLowRisk,
		OperatingCompanyId: operatingCompany.Id,
	}

	err = service.paymentChannelCostSystem.MultipleInsert(
		context.TODO(),
		[]*billingpb.PaymentChannelCostSystem{
			paymentSysCost1,
			paymentSysCost2,
			paymentSysCost3,
			paymentSysCost4,
			paymentSysCost5,
		},
	)

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostSystem test data failed", "%v", err)
	}

	return merchant, projectFixedAmount, pmBankCard, paymentSystem
}

func helperOperatingCompany(
	suite suite.Suite,
	service *Service,
) *billingpb.OperatingCompany {

	operatingCompany := &billingpb.OperatingCompany{
		Id:                 primitive.NewObjectID().Hex(),
		Name:               "Legal name",
		Country:            "RU",
		RegistrationNumber: "some number",
		VatNumber:          "some vat number",
		Address:            "Home, home 0",
		VatAddress:         "Address for VAT purposes",
		SignatoryName:      "Vassiliy Poupkine",
		SignatoryPosition:  "CEO",
		BankingDetails:     "bank details including bank, bank address, account number, swift/ bic, intermediary bank",
		PaymentCountries:   []string{},
	}

	err := service.operatingCompany.Upsert(context.TODO(), operatingCompany)
	if err != nil {
		suite.FailNow("Insert operatingCompany failed", "%v", err)
	}
	return operatingCompany
}

func helperCreateMerchant(
	suite suite.Suite,
	service *Service,
	currency string,
	country string,
	paymentMethod *billingpb.PaymentMethod,
	minPayoutAmount float64,
	operatingCompanyId string,
) *billingpb.Merchant {
	date, err := ptypes.TimestampProto(time.Now().Add(time.Hour * -360))

	if err != nil {
		suite.FailNow("Generate merchant date failed", "%v", err)
	}

	id := primitive.NewObjectID().Hex()

	merchant := &billingpb.Merchant{
		Id: id,
		User: &billingpb.MerchantUser{
			Id: primitive.NewObjectID().Hex(),
		},
		Company: &billingpb.MerchantCompanyInfo{
			Name:               "Unit test",
			AlternativeName:    "merchant1",
			Website:            "http://localhost",
			Country:            country,
			Zip:                "190000",
			City:               "St.Petersburg",
			Address:            "address",
			AddressAdditional:  "address_additional",
			RegistrationNumber: "registration_number",
		},
		Contacts: &billingpb.MerchantContact{
			Authorized: &billingpb.MerchantContactAuthorized{
				Name:     "Unit Test",
				Email:    "test@unit.test",
				Phone:    "123456789",
				Position: "Unit Test",
			},
			Technical: &billingpb.MerchantContactTechnical{
				Name:  "Unit Test",
				Email: "test@unit.test",
				Phone: "123456789",
			},
		},
		Banking: &billingpb.MerchantBanking{
			Currency:             currency,
			Name:                 "Bank name",
			Address:              "address",
			AccountNumber:        "0000001",
			Swift:                "swift",
			CorrespondentAccount: "correspondent_account",
			Details:              "details",
		},
		Status:                    billingpb.MerchantStatusDraft,
		CreatedAt:                 nil,
		UpdatedAt:                 nil,
		FirstPaymentAt:            nil,
		IsVatEnabled:              true,
		IsCommissionToUserEnabled: true,
		HasMerchantSignature:      false,
		HasPspSignature:           false,
		LastPayout: &billingpb.MerchantLastPayout{
			Date:   date,
			Amount: 999999,
		},
		IsSigned:                true,
		PaymentMethods:          map[string]*billingpb.MerchantPaymentMethod{},
		AgreementType:           0,
		AgreementSentViaMail:    false,
		MailTrackingLink:        "",
		S3AgreementName:         "",
		PayoutCostAmount:        0,
		PayoutCostCurrency:      "",
		MinPayoutAmount:         minPayoutAmount,
		RollingReserveThreshold: 0,
		RollingReserveDays:      0,
		RollingReserveChargebackTransactionsThreshold: 0,
		ItemMinCostAmount:      0,
		ItemMinCostCurrency:    "",
		CentrifugoToken:        "",
		AgreementSignatureData: nil,
		Steps:                  nil,
		AgreementTemplate:      "",
		ReceivedDate:           nil,
		StatusLastUpdatedAt:    nil,
		HasProjects:            false,
		AgreementNumber:        service.getMerchantAgreementNumber(id),
		MinimalPayoutLimit:     0,
		Tariff: &billingpb.MerchantTariff{
			Payment: []*billingpb.MerchantTariffRatesPayment{
				{
					MinAmount:              0,
					MaxAmount:              4.99,
					MethodName:             "VISA",
					MethodPercentFee:       1.8,
					MethodFixedFee:         0.2,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           3.0,
					PsFixedFee:             0.3,
					PsFixedFeeCurrency:     "USD",
					MerchantHomeRegion:     "russia_and_cis",
					PayerRegion:            "europe",
				},
				{
					MinAmount:              5,
					MaxAmount:              999999999.99,
					MethodName:             "MasterCard",
					MethodPercentFee:       1.8,
					MethodFixedFee:         0.2,
					MethodFixedFeeCurrency: "USD",
					PsPercentFee:           3.0,
					PsFixedFee:             0.3,
					PsFixedFeeCurrency:     "USD",
					MerchantHomeRegion:     "russia_and_cis",
					PayerRegion:            "europe",
				},
			},
			Payout: &billingpb.MerchantTariffRatesSettingsItem{
				MethodPercentFee:       0,
				MethodFixedFee:         25.0,
				MethodFixedFeeCurrency: "EUR",
				IsPaidByMerchant:       true,
			},
			HomeRegion: "russia_and_cis",
		},
		ManualPayoutsEnabled:   false,
		MccCode:                billingpb.MccCodeLowRisk,
		OperatingCompanyId:     operatingCompanyId,
		MerchantOperationsType: pkg.MerchantOperationTypeLowRisk,
		DontChargeVat:          false,
	}

	if paymentMethod != nil {
		merchant.PaymentMethods[paymentMethod.Id] = &billingpb.MerchantPaymentMethod{
			PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
				Id:   paymentMethod.Id,
				Name: paymentMethod.Name,
			},
			Commission: &billingpb.MerchantPaymentMethodCommissions{
				Fee: 2.5,
				PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
					Fee:      30,
					Currency: "RUB",
				},
			},
			Integration: &billingpb.MerchantPaymentMethodIntegration{
				TerminalId:               "15985",
				TerminalPassword:         "A1tph4I6BD0f",
				TerminalCallbackPassword: "0V1rJ7t4jCRv",
				Integrated:               true,
			},
			IsActive: true,
		}
	}

	merchants := []*billingpb.Merchant{merchant}
	if err := service.merchant.MultipleInsert(context.TODO(), merchants); err != nil {
		suite.FailNow("Insert merchant test data failed", "%v", err)
	}

	merCost1 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost2 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "RUB",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost3 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionWorldwide,
		Country:           "US",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost4 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "reversal",
		Region:            billingpb.TariffRegionEurope,
		Country:           "FI",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  false,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost5 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost6 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "RUB",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionRussiaAndCis,
		Country:           "RU",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost7 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionWorldwide,
		Country:           "US",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	merCost8 := &billingpb.MoneyBackCostMerchant{
		Id:                primitive.NewObjectID().Hex(),
		MerchantId:        merchant.Id,
		Name:              "MASTERCARD",
		PayoutCurrency:    "USD",
		UndoReason:        "chargeback",
		Region:            billingpb.TariffRegionEurope,
		Country:           "FI",
		DaysFrom:          0,
		PaymentStage:      1,
		Percent:           0.2,
		FixAmount:         0.15,
		FixAmountCurrency: "EUR",
		IsPaidByMerchant:  true,
		MccCode:           billingpb.MccCodeLowRisk,
	}

	err = service.moneyBackCostMerchant.MultipleInsert(context.TODO(), []*billingpb.MoneyBackCostMerchant{merCost1, merCost2, merCost3, merCost4, merCost5, merCost6, merCost7, merCost8})

	if err != nil {
		suite.FailNow("Insert MoneyBackCostMerchant test data failed", "%v", err)
	}

	paymentMerCost1 := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              merchant.Id,
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "RU",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}
	paymentMerCost2 := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              merchant.Id,
		Name:                    "MASTERCARD",
		PayoutCurrency:          "RUB",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionRussiaAndCis,
		Country:                 "RU",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}
	paymentMerCost3 := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              merchant.Id,
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionWorldwide,
		Country:                 "US",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}
	paymentMerCost4 := &billingpb.PaymentChannelCostMerchant{
		MerchantId:              merchant.Id,
		Name:                    "MASTERCARD",
		PayoutCurrency:          "USD",
		MinAmount:               0,
		Region:                  billingpb.TariffRegionEurope,
		Country:                 "FI",
		MethodPercent:           0.025,
		MethodFixAmount:         0.02,
		MethodFixAmountCurrency: "EUR",
		PsPercent:               0.05,
		PsFixedFee:              0.05,
		PsFixedFeeCurrency:      "EUR",
		MccCode:                 billingpb.MccCodeLowRisk,
	}

	err = service.paymentChannelCostMerchant.MultipleInsert(context.TODO(), []*billingpb.PaymentChannelCostMerchant{paymentMerCost1, paymentMerCost2, paymentMerCost3, paymentMerCost4})

	if err != nil {
		suite.FailNow("Insert PaymentChannelCostMerchant test data failed", "%v", err)
	}

	return merchant
}

func helperCreateProject(
	suite suite.Suite,
	service *Service,
	merchantId string,
	vatPayer string,
) *billingpb.Project {
	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		CallbackCurrency:         "RUB",
		CallbackProtocol:         "default",
		LimitsCurrency:           "USD",
		MaxPaymentAmount:         15000,
		MinPaymentAmount:         1,
		Name:                     map[string]string{"en": "test project 1"},
		IsProductsCheckout:       false,
		AllowDynamicRedirectUrls: true,
		SecretKey:                "test project 1 secret key",
		Status:                   billingpb.ProjectStatusDraft,
		MerchantId:               merchantId,
		VatPayer:                 vatPayer,
	}

	if err := service.project.Insert(context.TODO(), project); err != nil {
		suite.FailNow("Insert project test data failed", "%v", err)
	}

	return project
}

func helperCreateAndPayPaylinkOrder(
	suite suite.Suite,
	service *Service,
	paylinkId, country string,
	paymentMethod *billingpb.PaymentMethod,
	issuer *billingpb.OrderIssuer,
) *billingpb.Order {
	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock.Anything, mock.Anything).Return("token")
	centrifugoMock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	service.centrifugoDashboard = centrifugoMock
	service.centrifugoPaymentForm = centrifugoMock

	req := &billingpb.OrderCreateByPaylink{
		PaylinkId: paylinkId,
		PayerIp:   "127.0.0.1",
	}

	if issuer != nil {
		req.IssuerUrl = issuer.Url
		req.IsEmbedded = issuer.Embedded
		req.UtmSource = issuer.UtmSource
		req.UtmMedium = issuer.UtmMedium
		req.UtmCampaign = issuer.UtmCampaign
	}

	rsp := &billingpb.OrderCreateProcessResponse{}
	err := service.OrderCreateByPaylink(context.TODO(), req, rsp)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	req1 := &billingpb.ProcessBillingAddressRequest{
		OrderId: rsp.Item.Uuid,
		Country: country,
		Zip:     "123345",
	}
	rsp1 := &billingpb.ProcessBillingAddressResponse{}
	err = service.ProcessBillingAddress(context.TODO(), req1, rsp1)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), rsp1.Status, billingpb.ResponseStatusOk)

	order, err := service.orderRepository.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billingpb.Order{}, order)

	return helperPayOrder(suite, service, order, paymentMethod, country)
}

func helperCreateAndPayOrder(
	suite suite.Suite,
	service *Service,
	amount float64,
	currency, country string,
	project *billingpb.Project,
	paymentMethod *billingpb.PaymentMethod,
) *billingpb.Order {
	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock.Anything, mock.Anything).Return("token")
	centrifugoMock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	service.centrifugoDashboard = centrifugoMock
	service.centrifugoPaymentForm = centrifugoMock

	zip := ""
	if country == CountryCodeUSA {
		zip = "98001"
	}

	req := &billingpb.OrderCreateRequest{
		Type:        pkg.OrderType_simple,
		ProjectId:   project.Id,
		Amount:      amount,
		Currency:    currency,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Id:    primitive.NewObjectID().Hex(),
			Email: "test@unit.unit",
			Ip:    "127.0.0.1",
			Address: &billingpb.OrderBillingAddress{
				Country:    country,
				PostalCode: zip,
			},
		},
	}

	rsp := &billingpb.OrderCreateProcessResponse{}
	err := service.OrderCreateProcess(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	return helperPayOrder(suite, service, rsp.Item, paymentMethod, country)
}

func helperPayOrder(
	suite suite.Suite,
	service *Service,
	order *billingpb.Order,
	paymentMethod *billingpb.PaymentMethod,
	country string,
) *billingpb.Order {
	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock.Anything, mock.Anything).Return("token")
	centrifugoMock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	service.centrifugoDashboard = centrifugoMock
	service.centrifugoPaymentForm = centrifugoMock

	req1 := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         order.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: paymentMethod.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            time.Now().AddDate(1, 0, 0).Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "MR. CARD HOLDER",
		},
		Ip: "127.0.0.1",
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err := service.PaymentCreateProcess(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equalf(suite.T(), billingpb.ResponseStatusOk, rsp1.Status, "%v", rsp1.Message)

	order, err = service.orderRepository.GetById(context.TODO(), order.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billingpb.Order{}, order)

	callbackRequest := &billingpb.CardPayPaymentCallback{
		PaymentMethod: paymentMethod.ExternalId,
		CallbackTime:  time.Now().Format("2006-01-02T15:04:05Z"),
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id:          order.Id,
			Description: order.Description,
		},
		CardAccount: &billingpb.CallbackCardPayBankCardAccount{
			Holder:             order.PaymentRequisites[billingpb.PaymentCreateFieldHolder],
			IssuingCountryCode: country,
			MaskedPan:          order.PaymentRequisites[billingpb.PaymentCreateFieldPan],
			Token:              primitive.NewObjectID().Hex(),
		},
		Customer: &billingpb.CardPayCustomer{
			Email:  order.User.Email,
			Ip:     order.User.Ip,
			Id:     order.ProjectAccount,
			Locale: "Europe/Moscow",
		},
		PaymentData: &billingpb.CallbackCardPayPaymentData{
			Id:          primitive.NewObjectID().Hex(),
			Amount:      order.ChargeAmount,
			Currency:    order.ChargeCurrency,
			Description: order.Description,
			Is_3D:       true,
			Rrn:         primitive.NewObjectID().Hex(),
			Status:      billingpb.CardPayPaymentResponseStatusCompleted,
		},
	}

	buf, err := json.Marshal(callbackRequest)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(buf) + order.PaymentMethod.Params.SecretCallback))

	callbackData := &billingpb.PaymentNotifyRequest{
		OrderId:   order.Id,
		Request:   buf,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}

	callbackResponse := &billingpb.PaymentNotifyResponse{}
	err = service.PaymentCallbackProcess(context.TODO(), callbackData, callbackResponse)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, callbackResponse.Status)

	order, err = service.orderRepository.GetById(context.TODO(), order.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billingpb.Order{}, order)
	assert.Equal(suite.T(), int32(recurringpb.OrderStatusPaymentSystemComplete), order.PrivateStatus)

	return order
}

func helperMakeRefund(suite suite.Suite, service *Service, order *billingpb.Order, amount float64, isChargeback bool) *billingpb.Refund {
	req2 := &billingpb.CreateRefundRequest{
		OrderId:      order.Uuid,
		Amount:       amount,
		CreatorId:    primitive.NewObjectID().Hex(),
		Reason:       "unit test",
		IsChargeback: isChargeback,
		MerchantId:   order.GetMerchantId(),
	}
	rsp2 := &billingpb.CreateRefundResponse{}
	err := service.CreateRefund(context.TODO(), req2, rsp2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp2.Status)
	assert.Empty(suite.T(), rsp2.Message)

	err = service.updateOrder(context.TODO(), order)
	assert.NoError(suite.T(), err)

	refundReq := &billingpb.CardPayRefundCallback{
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id: rsp2.Item.Id,
		},
		PaymentMethod: order.PaymentMethod.Group,
		PaymentData: &billingpb.CardPayRefundCallbackPaymentData{
			Id:              rsp2.Item.Id,
			RemainingAmount: 90,
		},
		RefundData: &billingpb.CardPayRefundCallbackRefundData{
			Amount:   10,
			Created:  time.Now().Format(cardPayDateFormat),
			Id:       primitive.NewObjectID().Hex(),
			Currency: rsp2.Item.Currency,
			Status:   billingpb.CardPayPaymentResponseStatusCompleted,
			AuthCode: primitive.NewObjectID().Hex(),
			Is_3D:    true,
			Rrn:      primitive.NewObjectID().Hex(),
		},
		CallbackTime: time.Now().Format(cardPayDateFormat),
		Customer: &billingpb.CardPayCustomer{
			Email: order.User.Email,
			Id:    order.User.Email,
		},
	}

	b, err := json.Marshal(refundReq)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(b) + order.PaymentMethod.Params.SecretCallback))

	req3 := &billingpb.CallbackRequest{
		Handler:   pkg.PaymentSystemHandlerCardPay,
		Body:      b,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}
	rsp3 := &billingpb.PaymentNotifyResponse{}
	err = service.ProcessRefundCallback(context.TODO(), req3, rsp3)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp3.Status)
	assert.Empty(suite.T(), rsp3.Error)

	var refund *billingpb.Refund
	oid, _ := primitive.ObjectIDFromHex(rsp2.Item.Id)
	filter := bson.M{"_id": oid}
	err = service.db.Collection(repository.CollectionRefund).FindOne(context.TODO(), filter).Decode(&refund)
	assert.NotNil(suite.T(), refund)
	assert.Equal(suite.T(), pkg.RefundStatusCompleted, refund.Status)

	return refund
}

func createProductsForProject(
	suite suite.Suite,
	service *Service,
	project *billingpb.Project,
	productsCount int,
) []*billingpb.Product {
	products := make([]*billingpb.Product, productsCount)

	for i := 0; i < productsCount; i++ {
		name := "ru_test_product_" + strconv.Itoa(i)
		req := &billingpb.Product{
			Object:          "product",
			Type:            "simple_product",
			Sku:             name,
			Name:            map[string]string{"en": name},
			DefaultCurrency: "USD",
			Enabled:         true,
			Description:     map[string]string{"en": name + " description"},
			MerchantId:      project.MerchantId,
			ProjectId:       project.Id,
		}

		baseAmount := 37.00 * float64(i+1)

		req.Prices = append(req.Prices, &billingpb.ProductPrice{
			Currency: "USD",
			Region:   "USD",
			Amount:   baseAmount,
		})
		req.Prices = append(req.Prices, &billingpb.ProductPrice{
			Currency: "EUR",
			Region:   "EUR",
			Amount:   baseAmount * 0.9,
		})
		req.Prices = append(req.Prices, &billingpb.ProductPrice{
			Currency: "RUB",
			Region:   "RUB",
			Amount:   baseAmount * 65.13,
		})

		prod := &billingpb.Product{}
		err := service.CreateOrUpdateProduct(context.TODO(), req, prod)

		if err != nil {
			suite.FailNow("Add products for project failed", "%v", err)
		}

		products[i] = prod
	}

	return products
}

func createKeyProductsForProject(
	suite suite.Suite,
	service *Service,
	project *billingpb.Project,
	productsCount int,
) []*billingpb.KeyProduct {
	products := make([]*billingpb.KeyProduct, 0)

	for i := 0; i < productsCount; i++ {
		baseAmount := 37.00 * float64(i+1)

		name := "ru_test_key_product_" + strconv.Itoa(i)
		req := &billingpb.CreateOrUpdateKeyProductRequest{
			Object:          "key_product",
			Sku:             name,
			Name:            map[string]string{"en": name},
			DefaultCurrency: "USD",
			Description:     map[string]string{"en": name + " description"},
			MerchantId:      project.MerchantId,
			ProjectId:       project.Id,
			Platforms: []*billingpb.PlatformPrice{
				{
					Id: "steam",
					Prices: []*billingpb.ProductPrice{
						{
							Currency: "USD",
							Region:   "USD",
							Amount:   baseAmount,
						},
						{
							Currency: "RUB",
							Region:   "RUB",
							Amount:   baseAmount * 65.13,
						},
					},
				},
			},
		}
		rsp := &billingpb.KeyProductResponse{}
		err := service.CreateOrUpdateKeyProduct(context.TODO(), req, rsp)

		if err != nil {
			suite.FailNow("Add key products for project failed", "%v", err)
		}

		req2 := &billingpb.PublishKeyProductRequest{
			MerchantId:   project.MerchantId,
			KeyProductId: rsp.Product.Id,
		}
		rsp2 := &billingpb.KeyProductResponse{}
		err = service.PublishKeyProduct(context.TODO(), req2, rsp2)

		if err != nil {
			suite.FailNow("Publishing key product for project failed", "%v", err)
		}

		fileContent := fmt.Sprintf("%s-%s-%s-%s", RandomString(4), RandomString(4), RandomString(4), RandomString(4))
		file := []byte(fileContent)

		req3 := &billingpb.PlatformKeysFileRequest{
			KeyProductId: rsp.Product.Id,
			PlatformId:   "steam",
			MerchantId:   project.MerchantId,
			File:         file,
		}
		rsp3 := &billingpb.PlatformKeysFileResponse{}
		err = service.UploadKeysFile(context.TODO(), req3, rsp3)

		if err != nil {
			suite.FailNow("Upload keys to key product for project failed", "%v", err)
		}

		products = append(products, rsp.Product)
	}

	return products
}

func helperCreateAndPayOrder2(
	suite suite.Suite,
	service *Service,
	amount float64,
	currency, country string,
	project *billingpb.Project,
	paymentMethod *billingpb.PaymentMethod,
	paymentMethodClosedAt time.Time,
	product *billingpb.Product,
	keyProduct *billingpb.KeyProduct,
	issuerUrl string,
) *billingpb.Order {
	centrifugoMock := &mocks.CentrifugoInterface{}
	centrifugoMock.On("GetChannelToken", mock.Anything, mock.Anything).Return("token")
	centrifugoMock.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	service.centrifugoDashboard = centrifugoMock
	service.centrifugoPaymentForm = centrifugoMock

	req := &billingpb.OrderCreateRequest{
		ProjectId:   project.Id,
		Account:     "unit test",
		Description: "unit test",
		OrderId:     primitive.NewObjectID().Hex(),
		User: &billingpb.OrderUser{
			Email: "test@unit.unit",
			Ip:    "127.0.0.1",
			Address: &billingpb.OrderBillingAddress{
				Country: country,
			},
		},
		IssuerUrl: issuerUrl,
	}

	if product != nil {
		req.Type = pkg.OrderType_product
		req.Products = []string{product.Id}
	} else if keyProduct != nil {
		req.Type = pkg.OrderType_key
		req.Products = []string{keyProduct.Id}
	} else {
		if amount <= 0 || currency == "" {
			suite.FailNow("Order creation failed because request hasn't required fields", "%v")
		}

		req.Type = pkg.OrderType_simple
		req.Amount = amount
		req.Currency = currency
	}

	rsp := &billingpb.OrderCreateProcessResponse{}
	err := service.OrderCreateProcess(context.TODO(), req, rsp)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), rsp.Status, billingpb.ResponseStatusOk)

	req1 := &billingpb.PaymentCreateRequest{
		Data: map[string]string{
			billingpb.PaymentCreateFieldOrderId:         rsp.Item.Uuid,
			billingpb.PaymentCreateFieldPaymentMethodId: paymentMethod.Id,
			billingpb.PaymentCreateFieldEmail:           "test@unit.unit",
			billingpb.PaymentCreateFieldPan:             "4000000000000002",
			billingpb.PaymentCreateFieldCvv:             "123",
			billingpb.PaymentCreateFieldMonth:           "02",
			billingpb.PaymentCreateFieldYear:            time.Now().AddDate(1, 0, 0).Format("2006"),
			billingpb.PaymentCreateFieldHolder:          "MR. CARD HOLDER",
		},
		Ip: "127.0.0.1",
	}

	rsp1 := &billingpb.PaymentCreateResponse{}
	err = service.PaymentCreateProcess(context.TODO(), req1, rsp1)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), billingpb.ResponseStatusOk, rsp1.Status)

	order, err := service.orderRepository.GetById(context.TODO(), rsp.Item.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billingpb.Order{}, order)

	callbackRequest := &billingpb.CardPayPaymentCallback{
		PaymentMethod: paymentMethod.ExternalId,
		CallbackTime:  paymentMethodClosedAt.Format("2006-01-02T15:04:05Z"),
		MerchantOrder: &billingpb.CardPayMerchantOrder{
			Id:          rsp.Item.Id,
			Description: rsp.Item.Description,
		},
		CardAccount: &billingpb.CallbackCardPayBankCardAccount{
			Holder:             order.PaymentRequisites[billingpb.PaymentCreateFieldHolder],
			IssuingCountryCode: country,
			MaskedPan:          order.PaymentRequisites[billingpb.PaymentCreateFieldPan],
			Token:              primitive.NewObjectID().Hex(),
		},
		Customer: &billingpb.CardPayCustomer{
			Email:  rsp.Item.User.Email,
			Ip:     rsp.Item.User.Ip,
			Id:     rsp.Item.ProjectAccount,
			Locale: "Europe/Moscow",
		},
		PaymentData: &billingpb.CallbackCardPayPaymentData{
			Id:          primitive.NewObjectID().Hex(),
			Amount:      order.ChargeAmount,
			Currency:    order.ChargeCurrency,
			Description: order.Description,
			Is_3D:       true,
			Rrn:         primitive.NewObjectID().Hex(),
			Status:      billingpb.CardPayPaymentResponseStatusCompleted,
		},
	}

	buf, err := json.Marshal(callbackRequest)
	assert.NoError(suite.T(), err)

	hash := sha512.New()
	hash.Write([]byte(string(buf) + order.PaymentMethod.Params.SecretCallback))

	callbackData := &billingpb.PaymentNotifyRequest{
		OrderId:   order.Id,
		Request:   buf,
		Signature: hex.EncodeToString(hash.Sum(nil)),
	}

	callbackResponse := &billingpb.PaymentNotifyResponse{}
	err = service.PaymentCallbackProcess(context.TODO(), callbackData, callbackResponse)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.StatusOK, callbackResponse.Status)

	order, err = service.orderRepository.GetById(context.TODO(), order.Id)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), order)
	assert.IsType(suite.T(), &billingpb.Order{}, order)
	assert.Equal(suite.T(), int32(recurringpb.OrderStatusPaymentSystemComplete), order.PrivateStatus)

	return order
}

func RandomString(n int) string {
	var letter = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}
