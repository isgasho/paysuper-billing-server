package service

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	geoip "github.com/ProtocolONE/geoip-service/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/google/uuid"
	"github.com/jinzhu/copier"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/entity"
	repo "github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"github.com/paysuper/paysuper-tax-service/proto"
	postmarkSdrPkg "github.com/paysuper/postmark-sender/pkg"
	"github.com/streadway/amqp"
	"github.com/ttacon/libphonenumber"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	paymentRequestIncorrect             = "payment request has incorrect format"
	callbackRequestIncorrect            = "callback request has incorrect format"
	callbackHandlerIncorrect            = "unknown callback type"
	orderErrorPublishNotificationFailed = "publish order notification failed"
	orderErrorUpdateOrderDataFailed     = "update order data failed"

	orderDefaultDescription = "Payment by order # %s"

	defaultExpireDateToFormInput = 30
	cookieCounterUpdateTime      = 1800

	taxTypeVat      = "vat"
	taxTypeSalesTax = "sales_tax"

	collectionOrder           = "order"
	collectionBinData         = "bank_bin"
	collectionNotifySales     = "notify_sales"
	collectionNotifyNewRegion = "notify_new_region"
)

var (
	orderErrorProjectIdIncorrect                              = newBillingServerErrorMsg("fm000001", "project identifier is incorrect")
	orderErrorProjectNotFound                                 = newBillingServerErrorMsg("fm000002", "project with specified identifier not found")
	orderErrorProjectInactive                                 = newBillingServerErrorMsg("fm000003", "project with specified identifier is inactive")
	orderErrorProjectMerchantInactive                         = newBillingServerErrorMsg("fm000004", "merchant for project with specified identifier is inactive")
	orderErrorPaymentMethodNotAllowed                         = newBillingServerErrorMsg("fm000005", "payment Method not available for project")
	orderErrorPaymentMethodNotFound                           = newBillingServerErrorMsg("fm000006", "payment Method with specified identifier not found")
	orderErrorPaymentMethodInactive                           = newBillingServerErrorMsg("fm000007", "payment Method with specified identifier is inactive")
	orderErrorConvertionCurrency                              = newBillingServerErrorMsg("fm000008", "currency convertion error")
	orderErrorPaymentMethodEmptySettings                      = newBillingServerErrorMsg("fm000009", "payment Method setting for project is empty")
	orderErrorPaymentSystemInactive                           = newBillingServerErrorMsg("fm000010", "payment system for specified payment Method is inactive")
	orderErrorPayerRegionUnknown                              = newBillingServerErrorMsg("fm000011", "payer region can't be found")
	orderErrorProjectOrderIdIsDuplicate                       = newBillingServerErrorMsg("fm000012", "request with specified project order identifier processed early")
	orderErrorDynamicNotifyUrlsNotAllowed                     = newBillingServerErrorMsg("fm000013", "dynamic verify url or notify url not allowed for project")
	orderErrorDynamicRedirectUrlsNotAllowed                   = newBillingServerErrorMsg("fm000014", "dynamic payer redirect urls not allowed for project")
	orderErrorCurrencyNotFound                                = newBillingServerErrorMsg("fm000015", "currency received from request not found")
	orderErrorAmountLowerThanMinAllowed                       = newBillingServerErrorMsg("fm000016", "order amount is lower than min allowed payment amount for project")
	orderErrorAmountGreaterThanMaxAllowed                     = newBillingServerErrorMsg("fm000017", "order amount is greater than max allowed payment amount for project")
	orderErrorAmountLowerThanMinAllowedPaymentMethod          = newBillingServerErrorMsg("fm000018", "order amount is lower than min allowed payment amount for payment Method")
	orderErrorAmountGreaterThanMaxAllowedPaymentMethod        = newBillingServerErrorMsg("fm000019", "order amount is greater than max allowed payment amount for payment Method")
	orderErrorCanNotCreate                                    = newBillingServerErrorMsg("fm000020", "order can't create. try request later")
	orderErrorNotFound                                        = newBillingServerErrorMsg("fm000021", "order with specified identifier not found")
	orderErrorOrderCreatedAnotherProject                      = newBillingServerErrorMsg("fm000022", "order created for another project")
	orderErrorFormInputTimeExpired                            = newBillingServerErrorMsg("fm000023", "time to enter date on payment form expired")
	orderErrorCurrencyIsRequired                              = newBillingServerErrorMsg("fm000024", "parameter currency in create order request is required")
	orderErrorUnknown                                         = newBillingServerErrorMsg("fm000025", "unknown error. try request later")
	orderCountryPaymentRestrictedError                        = newBillingServerErrorMsg("fm000027", "payments from your country are not allowed")
	orderGetSavedCardError                                    = newBillingServerErrorMsg("fm000028", "saved card data with specified identifier not found")
	orderErrorCountryByPaymentAccountNotFound                 = newBillingServerErrorMsg("fm000029", "information about user country can't be found")
	orderErrorPaymentAccountIncorrect                         = newBillingServerErrorMsg("fm000030", "account in payment system is incorrect")
	orderErrorProductsEmpty                                   = newBillingServerErrorMsg("fm000031", "products set is empty")
	orderErrorProductsInvalid                                 = newBillingServerErrorMsg("fm000032", "some products in set are invalid or inactive")
	orderErrorNoProductsCommonCurrency                        = newBillingServerErrorMsg("fm000033", "no common prices neither in requested currency nor in default currency")
	orderErrorNoNameInDefaultLanguage                         = newBillingServerErrorMsg("fm000034", "no name in default language %s")
	orderErrorNoNameInRequiredLanguage                        = newBillingServerErrorMsg("fm000035", "no name in required language %s")
	orderErrorNoDescriptionInDefaultLanguage                  = newBillingServerErrorMsg("fm000036", "no description in default language %s")
	orderErrorNoDescriptionInRequiredLanguage                 = newBillingServerErrorMsg("fm000037", "no description in required language %s")
	orderErrorProjectMerchantNotFound                         = newBillingServerErrorMsg("fm000038", "merchant for project with specified identifier not found")
	orderErrorRecurringCardNotOwnToUser                       = newBillingServerErrorMsg("fm000039", "you can't use not own bank card for payment")
	orderErrorNotRestricted                                   = newBillingServerErrorMsg("fm000040", "order country not restricted")
	orderErrorEmailRequired                                   = newBillingServerErrorMsg("fm000041", "email is required")
	orderErrorCreatePaymentRequiredFieldIdNotFound            = newBillingServerErrorMsg("fm000042", "required field with order identifier not found")
	orderErrorCreatePaymentRequiredFieldPaymentMethodNotFound = newBillingServerErrorMsg("fm000043", "required field with payment Method identifier not found")
	orderErrorCreatePaymentRequiredFieldEmailNotFound         = newBillingServerErrorMsg("fm000044", "required field \"email\" not found")
	orderErrorCreatePaymentRequiredFieldUserCountryNotFound   = newBillingServerErrorMsg("fm000045", "user country is required")
	orderErrorCreatePaymentRequiredFieldUserZipNotFound       = newBillingServerErrorMsg("fm000046", "user zip is required")
	orderErrorOrderAlreadyComplete                            = newBillingServerErrorMsg("fm000047", "order with specified identifier payed early")
	orderErrorSignatureInvalid                                = newBillingServerErrorMsg("fm000048", "request signature is invalid")
	orderErrorProductsPrice                                   = newBillingServerErrorMsg("fm000051", "can't get product price")
	orderErrorCheckoutWithoutProducts                         = newBillingServerErrorMsg("fm000052", "order products not specified")
	orderErrorCheckoutWithoutAmount                           = newBillingServerErrorMsg("fm000053", "order amount not specified")
	orderErrorUnknownType                                     = newBillingServerErrorMsg("fm000055", "unknown type of order")
	orderErrorMerchantBadTariffs                              = newBillingServerErrorMsg("fm000056", "merchant don't have tariffs")
	orderErrorReceiptNotEquals                                = newBillingServerErrorMsg("fm000057", "receipts not equals")
	orderErrorDuringFormattingCurrency                        = newBillingServerErrorMsg("fm000058", "error during formatting currency")
	orderErrorDuringFormattingDate                            = newBillingServerErrorMsg("fm000059", "error during formatting date")
	orderErrorMerchantForOrderNotFound                        = newBillingServerErrorMsg("fm000060", "merchant for order not found")
	orderErrorPaymentMethodsNotFound                          = newBillingServerErrorMsg("fm000061", "payment methods for payment with specified currency not found")
	orderErrorNoPlatforms                                     = newBillingServerErrorMsg("fm000062", "no available platforms")
	orderCountryPaymentRestricted                             = newBillingServerErrorMsg("fm000063", "payments from your country are not allowed")
	orderErrorCostsRatesNotFound                              = newBillingServerErrorMsg("fm000064", "settings to calculate commissions for order not found")
	orderErrorVirtualCurrencyNotFilled                        = newBillingServerErrorMsg("fm000065", "virtual currency is not filled")
	orderErrorVirtualCurrencyFracNotSupported                 = newBillingServerErrorMsg("fm000066", "fractional numbers is not supported for this virtual currency")
	orderErrorVirtualCurrencyLimits                           = newBillingServerErrorMsg("fm000067", "amount of order is more than max amount or less than minimal amount for virtual currency")
	orderErrorCheckoutWithProducts                            = newBillingServerErrorMsg("fm000069", "request to processing simple payment can't contain products list")
	orderErrorMerchantDoNotHaveCompanyInfo                    = newBillingServerErrorMsg("fm000070", "merchant don't have completed company info")
	orderErrorMerchantDoNotHaveBanking                        = newBillingServerErrorMsg("fm000071", "merchant don't have completed banking info")
	orderErrorAmountLowerThanMinLimitSystem                   = newBillingServerErrorMsg("fm000072", "order amount is lower than min system limit")
	orderErrorAlreadyProcessed                                = newBillingServerErrorMsg("fm000073", "order is already processed")
	orderErrorDontHaveReceiptUrl                              = newBillingServerErrorMsg("fm000074", "processed order don't have receipt url")
	orderErrorWrongPrivateStatus                              = newBillingServerErrorMsg("fm000075", "order has wrong private status and cannot be recreated")
	orderCountryChangeRestrictedError                         = newBillingServerErrorMsg("fm000076", "change country is not allowed")
	orderErrorVatPayerUnknown                                 = newBillingServerErrorMsg("fm000077", "vat payer unknown")

	virtualCurrencyPayoutCurrencyMissed = newBillingServerErrorMsg("vc000001", "virtual currency don't have price in merchant payout currency")

	paymentSystemPaymentProcessingSuccessStatus = "PAYMENT_SYSTEM_PROCESSING_SUCCESS"
)

type orderCreateRequestProcessorChecked struct {
	id                      string
	project                 *billing.Project
	merchant                *billing.Merchant
	currency                string
	amount                  float64
	paymentMethod           *billing.PaymentMethod
	products                []string
	items                   []*billing.OrderItem
	metadata                map[string]string
	privateMetadata         map[string]string
	user                    *billing.OrderUser
	virtualAmount           float64
	mccCode                 string
	operatingCompanyId      string
	priceGroup              *billing.PriceGroup
	isCurrencyPredefined    bool
	isBuyForVirtualCurrency bool
}

type OrderCreateRequestProcessor struct {
	*Service
	checked *orderCreateRequestProcessorChecked
	request *billing.OrderCreateRequest
	ctx     context.Context
}

type PaymentFormProcessor struct {
	service *Service
	order   *billing.Order
	request *grpc.PaymentFormJsonDataRequest
}

type PaymentCreateProcessor struct {
	service        *Service
	data           map[string]string
	ip             string
	acceptLanguage string
	userAgent      string
	checked        struct {
		order         *billing.Order
		project       *billing.Project
		paymentMethod *billing.PaymentMethod
	}
}

type BinData struct {
	Id                 primitive.ObjectID `bson:"_id"`
	CardBin            int32              `bson:"card_bin"`
	CardBrand          string             `bson:"card_brand"`
	CardType           string             `bson:"card_type"`
	CardCategory       string             `bson:"card_category"`
	BankName           string             `bson:"bank_name"`
	BankCountryName    string             `bson:"bank_country_name"`
	BankCountryIsoCode string             `bson:"bank_country_code_a2"`
	BankSite           string             `bson:"bank_site"`
	BankPhone          string             `bson:"bank_phone"`
}

func (s *Service) OrderCreateByPaylink(
	ctx context.Context,
	req *billing.OrderCreateByPaylink,
	rsp *grpc.OrderCreateProcessResponse,
) error {
	pl, err := s.paylinkService.GetById(ctx, req.PaylinkId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	if pl.GetIsExpired() == true {
		rsp.Status = pkg.ResponseStatusGone
		rsp.Message = errorPaylinkExpired
		return nil
	}

	oReq := &billing.OrderCreateRequest{
		ProjectId: pl.ProjectId,
		User: &billing.OrderUser{
			Ip: req.PayerIp,
		},
		Products: pl.Products,
		PrivateMetadata: map[string]string{
			"PaylinkId": pl.Id,
		},
		Type:                pl.ProductsType,
		IssuerUrl:           req.IssuerUrl,
		IsEmbedded:          req.IsEmbedded,
		IssuerReferenceType: pkg.OrderIssuerReferenceTypePaylink,
		IssuerReference:     pl.Id,
		UtmSource:           req.UtmSource,
		UtmMedium:           req.UtmMedium,
		UtmCampaign:         req.UtmCampaign,
		Cookie:              req.Cookie,
	}

	err = s.OrderCreateProcess(ctx, oReq, rsp)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	return nil
}

func (s *Service) OrderCreateProcess(
	ctx context.Context,
	req *billing.OrderCreateRequest,
	rsp *grpc.OrderCreateProcessResponse,
) error {

	rsp.Status = pkg.ResponseStatusOk

	processor := &OrderCreateRequestProcessor{
		Service: s,
		request: req,
		checked: &orderCreateRequestProcessorChecked{},
		ctx:     ctx,
	}

	if req.Token != "" {
		err := processor.processCustomerToken()

		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	} else {
		if req.ProjectId == "" {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorProjectIdIncorrect
			return nil
		}

		_, err := primitive.ObjectIDFromHex(req.ProjectId)

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorProjectIdIncorrect
			return nil
		}
	}

	if err := processor.processProject(); err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	if err := processor.processMerchant(); err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	if processor.checked.mccCode == "" {
		processor.checked.mccCode = processor.checked.merchant.MccCode
	}
	if processor.checked.operatingCompanyId == "" {
		processor.checked.operatingCompanyId = processor.checked.merchant.OperatingCompanyId
	}

	if req.Signature != "" || processor.checked.project.SignatureRequired == true {
		if err := processor.processSignature(); err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	switch req.Type {
	case billing.OrderType_simple, billing.OrderTypeVirtualCurrency:
		if req.Products != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCheckoutWithProducts
			return nil
		}

		if req.Amount <= 0 {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCheckoutWithoutAmount
			return nil
		}
		break
	case billing.OrderType_product, billing.OrderType_key:
		if req.Amount > float64(0) {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCheckoutWithoutProducts
			return nil
		}
		break
	default:
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorUnknownType
		return nil
	}

	if req.User != nil {
		err := processor.processUserData()

		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	if processor.checked.user != nil && processor.checked.user.Ip != "" && !processor.checked.user.HasAddress() {
		err := processor.processPayerIp()

		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}

		// try to restore country change from cookie
		if req.Cookie != "" {
			decryptedBrowserCustomer, err := s.decryptBrowserCookie(req.Cookie)

			if err == nil &&
				processor.checked.user.Address != nil &&
				processor.checked.user.Address.Country == decryptedBrowserCustomer.IpCountry &&
				processor.checked.user.Ip == decryptedBrowserCustomer.Ip &&
				decryptedBrowserCustomer.SelectedCountry != "" {

				processor.checked.user.Address = &billing.OrderBillingAddress{
					Country: decryptedBrowserCustomer.SelectedCountry,
				}
			}
		}
	}

	err := processor.processCurrency(req.Type)
	if err != nil {
		zap.L().Error("process currency failed", zap.Error(err))
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	switch req.Type {
	case billing.OrderType_simple:
		if req.Amount != 0 {
			processor.processAmount()
		}
		break
	case billing.OrderTypeVirtualCurrency:
		err := processor.processVirtualCurrency()
		if err != nil {
			zap.L().Error(
				pkg.MethodFinishedWithError,
				zap.Error(err),
			)

			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
		break
	case billing.OrderType_product:
		if err := processor.processPaylinkProducts(); err != nil {
			if pid := req.PrivateMetadata["PaylinkId"]; pid != "" {
				s.notifyPaylinkError(ctx, pid, err, req, nil)
			}
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}

			if err == grpc.ProductNoPriceInCurrencyError {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = productNoPriceInCurrencyError
				return nil
			}

			return err
		}
		break
	case billing.OrderType_key:
		if err := processor.processPaylinkKeyProducts(); err != nil {
			if pid := req.PrivateMetadata["PaylinkId"]; pid != "" {
				s.notifyPaylinkError(ctx, pid, err, req, nil)
			}
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
		break
	}

	if req.OrderId != "" {
		if err := processor.processProjectOrderId(); err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	if req.PaymentMethod != "" {
		pm, err := s.paymentMethod.GetByGroupAndCurrency(
			ctx,
			processor.checked.project,
			req.PaymentMethod,
			processor.checked.currency,
		)

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorPaymentMethodNotFound
			return nil
		}

		if err := processor.processPaymentMethod(pm); err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	if req.Type == billing.OrderType_simple {
		if err := processor.processLimitAmounts(); err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = e
				return nil
			}
			return err
		}
	}

	processor.processMetadata()
	processor.processPrivateMetadata()

	order, err := processor.prepareOrder()

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	_, err = s.db.Collection(collectionOrder).InsertOne(ctx, order)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrder),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, order),
		)
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorCanNotCreate
		return nil
	}

	rsp.Item = order

	return nil
}

func (s *Service) PaymentFormJsonDataProcess(
	ctx context.Context,
	req *grpc.PaymentFormJsonDataRequest,
	rsp *grpc.PaymentFormJsonDataResponse,
) error {
	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.PaymentFormJsonData{}

	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Item.Type = order.ProductType

	if order.IsDeclinedByCountry() {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderCountryPaymentRestrictedError
		return nil
	}

	if order.PrivateStatus != constant.OrderStatusNew && order.PrivateStatus != constant.OrderStatusPaymentSystemComplete {
		if len(order.ReceiptUrl) == 0 {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorDontHaveReceiptUrl
			return nil
		}
		s.fillPaymentFormJsonData(order, rsp)
		rsp.Item.IsAlreadyProcessed = true
		rsp.Item.ReceiptUrl = order.ReceiptUrl
		return nil
	}

	p := &PaymentFormProcessor{service: s, order: order, request: req}
	p1 := &OrderCreateRequestProcessor{
		Service: s,
		checked: &orderCreateRequestProcessorChecked{
			user: &billing.OrderUser{
				Ip:      req.Ip,
				Address: &billing.OrderBillingAddress{},
			},
		},
		ctx: ctx,
	}

	if !order.User.HasAddress() && p1.checked.user.Ip != "" {
		err = p1.processPayerIp()

		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.ResponseStatusSystemError
				rsp.Message = e
				return nil
			}
			return err
		}

		order.User.Ip = p1.checked.user.Ip
		order.User.Address = &billing.OrderBillingAddress{
			Country:    p1.checked.user.Address.Country,
			City:       p1.checked.user.Address.City,
			PostalCode: p1.checked.user.Address.PostalCode,
			State:      p1.checked.user.Address.State,
		}
	}

	loc, _ := s.getCountryFromAcceptLanguage(req.Locale)
	isIdentified := order.User.IsIdentified()
	browserCustomer := &BrowserCookieCustomer{
		Ip:             req.Ip,
		UserAgent:      req.UserAgent,
		AcceptLanguage: req.Locale,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if isIdentified == true {
		customer, err := s.processCustomerData(ctx, order.User.Id, order, req, browserCustomer, loc)

		if err == nil {
			browserCustomer.CustomerId = customer.Id
		}
	} else {
		if req.Cookie != "" {
			decryptedBrowserCustomer, err := s.decryptBrowserCookie(req.Cookie)

			if err == nil {
				isIdentified = true

				if (time.Now().Unix() - decryptedBrowserCustomer.UpdatedAt.Unix()) <= cookieCounterUpdateTime {
					decryptedBrowserCustomer.SessionCount++
				}

				if decryptedBrowserCustomer.CustomerId != "" {
					customer, err := s.processCustomerData(
						ctx,
						decryptedBrowserCustomer.CustomerId,
						order,
						req,
						decryptedBrowserCustomer,
						loc,
					)

					if err != nil {
						zap.L().Error("Customer by identifier in browser cookie not processed", zap.Error(err))
					}

					if customer != nil {
						browserCustomer = decryptedBrowserCustomer
						order.User.Id = customer.Id
						order.User.TechEmail = customer.TechEmail
					} else {
						browserCustomer.VirtualCustomerId = s.getTokenString(s.cfg.Length)
					}
				} else {
					if decryptedBrowserCustomer.VirtualCustomerId == "" {
						browserCustomer.VirtualCustomerId = s.getTokenString(s.cfg.Length)
					} else {
						browserCustomer.VirtualCustomerId = decryptedBrowserCustomer.VirtualCustomerId
					}
				}

				// restore user address from cookie, if it was changed manually
				if order.User.Address != nil &&
					order.User.Address.Country == decryptedBrowserCustomer.IpCountry &&
					order.User.Ip == decryptedBrowserCustomer.Ip &&
					decryptedBrowserCustomer.SelectedCountry != "" {

					order.User.Address = &billing.OrderBillingAddress{
						Country: decryptedBrowserCustomer.SelectedCountry,
					}
				}

			} else {
				browserCustomer.VirtualCustomerId = s.getTokenString(s.cfg.Length)
			}
		} else {
			browserCustomer.VirtualCustomerId = s.getTokenString(s.cfg.Length)
		}

		if order.User.Id == "" {
			order.User.Id = browserCustomer.VirtualCustomerId
		}

		if order.User.TechEmail == "" {
			order.User.TechEmail = order.User.Id + pkg.TechEmailDomain
		}
	}

	if order.User.Locale == "" && loc != "" && loc != order.User.Locale {
		order.User.Locale = loc
	}

	restricted, err := s.applyCountryRestriction(ctx, order, order.GetCountry())
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}
	if restricted {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = orderCountryPaymentRestricted
		rsp.Item.Id = order.Uuid
		return nil
	}

	switch order.ProductType {
	case billing.OrderType_product:
		err = s.ProcessOrderProducts(ctx, order)
		break
	case billing.OrderType_key:
		rsp.Item.Platforms, err = s.ProcessOrderKeyProducts(ctx, order)
	case billing.OrderTypeVirtualCurrency:
		err = s.ProcessOrderVirtualCurrency(ctx, order)
	}

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(ctx, pid, err, req, order)
		}
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	if order.Issuer == nil {
		order.Issuer = &billing.OrderIssuer{
			Embedded: req.IsEmbedded,
		}
	}
	if order.Issuer.Url == "" {
		order.Issuer.Url = req.Referer
		order.Issuer.ReferrerHost = getHostFromUrl(req.Referer)
	}
	if order.Issuer.ReferenceType == "" {
		order.Issuer.ReferenceType = req.IssuerReferenceType
	}
	if order.Issuer.Reference == "" {
		order.Issuer.Reference = req.IssuerReference
	}
	if order.Issuer.UtmSource == "" {
		order.Issuer.UtmSource = req.UtmSource
	}
	if order.Issuer.UtmCampaign == "" {
		order.Issuer.UtmCampaign = req.UtmCampaign
	}
	if order.Issuer.UtmMedium == "" {
		order.Issuer.UtmMedium = req.UtmMedium
	}
	order.Issuer.ReferrerHost = getHostFromUrl(order.Issuer.Url)

	err = p1.processOrderVat(order)
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	project, err := s.project.GetById(ctx, order.Project.Id)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = orderErrorProjectNotFound
		return nil
	}

	pms, err := p.processRenderFormPaymentMethods(ctx, project)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e

			if e == orderErrorPaymentMethodNotAllowed {
				rsp.Status = pkg.ResponseStatusNotFound
			}

			return nil
		}
		return err
	}

	s.fillPaymentFormJsonData(order, rsp)
	rsp.Item.PaymentMethods = pms

	rsp.Item.VatInChargeCurrency = tools.FormatAmount(order.GetTaxAmountInChargeCurrency())
	rsp.Item.VatRate = tools.ToPrecise(order.Tax.Rate)

	cookie, err := s.generateBrowserCookie(browserCustomer)

	if err == nil {
		rsp.Cookie = cookie
	}

	return nil
}

func (s *Service) fillPaymentFormJsonData(order *billing.Order, rsp *grpc.PaymentFormJsonDataResponse) {
	projectName, ok := order.Project.Name[order.User.Locale]

	if !ok {
		projectName = order.Project.Name[DefaultLanguage]
	}

	expire := time.Now().Add(time.Minute * 30).Unix()

	rsp.Item.Id = order.Uuid
	rsp.Item.Account = order.ProjectAccount
	rsp.Item.Description = order.Description
	rsp.Item.HasVat = order.Tax.Amount > 0
	rsp.Item.Vat = order.Tax.Amount
	rsp.Item.Currency = order.Currency
	rsp.Item.Project = &grpc.PaymentFormJsonDataProject{
		Id:         order.Project.Id,
		Name:       projectName,
		UrlSuccess: order.Project.UrlSuccess,
		UrlFail:    order.Project.UrlFail,
	}
	rsp.Item.Token = s.centrifugoPaymentForm.GetChannelToken(order.Uuid, expire)
	rsp.Item.Amount = order.OrderAmount
	rsp.Item.TotalAmount = order.TotalPaymentAmount
	rsp.Item.ChargeCurrency = order.ChargeCurrency
	rsp.Item.ChargeAmount = order.ChargeAmount
	rsp.Item.Items = order.Items
	rsp.Item.Email = order.User.Email
	rsp.Item.UserAddressDataRequired = order.UserAddressDataRequired

	if order.CountryRestriction != nil {
		rsp.Item.CountryPaymentsAllowed = order.CountryRestriction.PaymentsAllowed
		rsp.Item.CountryChangeAllowed = order.CountryRestriction.ChangeAllowed
	} else {
		rsp.Item.CountryPaymentsAllowed = true
		rsp.Item.CountryChangeAllowed = true
	}

	rsp.Item.UserIpData = &billing.UserIpData{
		Country: order.User.Address.Country,
		City:    order.User.Address.City,
		Zip:     order.User.Address.PostalCode,
	}
	rsp.Item.Lang = order.User.Locale
	rsp.Item.VatPayer = order.VatPayer
}

func (s *Service) PaymentCreateProcess(
	ctx context.Context,
	req *grpc.PaymentCreateRequest,
	rsp *grpc.PaymentCreateResponse,
) error {
	processor := &PaymentCreateProcessor{
		service:        s,
		data:           req.Data,
		ip:             req.Ip,
		acceptLanguage: req.AcceptLanguage,
		userAgent:      req.UserAgent,
	}

	err := processor.processPaymentFormData(ctx)
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	order := processor.checked.order

	if !order.CountryRestriction.PaymentsAllowed {
		rsp.Message = orderCountryPaymentRestrictedError
		rsp.Status = pkg.ResponseStatusForbidden
		return nil
	}

	if order.ProductType == billing.OrderType_product {
		err = s.ProcessOrderProducts(ctx, order)
	} else if order.ProductType == billing.OrderType_key {
		// We should reserve keys only before payment
		if _, err = s.ProcessOrderKeyProducts(ctx, order); err == nil {
			err = processor.reserveKeysForOrder(ctx, order)
		}
	} else if order.ProductType == billing.OrderTypeVirtualCurrency {
		err = s.ProcessOrderVirtualCurrency(ctx, order)
	}

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(ctx, pid, err, req, order)
		}

		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	p1 := &OrderCreateRequestProcessor{Service: s, ctx: ctx}
	err = p1.processOrderVat(order)
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	if req.Ip != "" {
		address, err := s.getAddressByIp(req.Ip)
		if err == nil {
			order.PaymentIpCountry = address.Country
		}
	}

	ps, err := s.paymentSystem.GetById(ctx, processor.checked.paymentMethod.PaymentSystemId)
	if err != nil {
		rsp.Message = orderErrorPaymentSystemInactive
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	order.PaymentMethod = &billing.PaymentMethodOrder{
		Id:              processor.checked.paymentMethod.Id,
		Name:            processor.checked.paymentMethod.Name,
		PaymentSystemId: ps.Id,
		Group:           processor.checked.paymentMethod.Group,
		ExternalId:      processor.checked.paymentMethod.ExternalId,
		Handler:         ps.Handler,
		RefundAllowed:   processor.checked.paymentMethod.RefundAllowed,
	}

	err = s.setOrderChargeAmountAndCurrency(ctx, order)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	methodName, err := order.GetCostPaymentMethodName()
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}

		return err
	}

	order.PaymentMethod.Params, err = s.paymentMethod.GetPaymentSettings(
		processor.checked.paymentMethod,
		order.ChargeCurrency,
		order.MccCode,
		order.OperatingCompanyId,
		methodName,
		processor.checked.project,
	)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}

		return err
	}

	if _, ok := order.PaymentRequisites[pkg.PaymentCreateFieldRecurringId]; ok {
		req.Data[pkg.PaymentCreateFieldRecurringId] = order.PaymentRequisites[pkg.PaymentCreateFieldRecurringId]
		delete(order.PaymentRequisites, pkg.PaymentCreateFieldRecurringId)
	}

	merchant, err := s.merchant.GetById(ctx, order.GetMerchantId())
	if err != nil {
		return err
	}
	order.MccCode = merchant.MccCode
	order.IsHighRisk = merchant.IsHighRisk()

	order.OperatingCompanyId, err = s.getOrderOperatingCompanyId(ctx, order.GetCountry(), merchant)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.L().Error(
			"s.updateOrder Method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		} else {
			rsp.Message = orderErrorUnknown
			rsp.Status = pkg.ResponseStatusSystemError
		}
		return nil
	}

	if !s.hasPaymentCosts(ctx, order) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorCostsRatesNotFound
		return nil
	}

	h, err := s.NewPaymentSystem(ctx, s.cfg.PaymentSystemConfig, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	url, err := h.CreatePayment(order, s.cfg.GetRedirectUrlSuccess(nil), s.cfg.GetRedirectUrlFail(nil), req.Data)

	if err != nil {
		zap.L().Error(
			"h.CreatePayment Method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		} else {
			rsp.Message = orderErrorUnknown
			rsp.Status = pkg.ResponseStatusBadData
		}
		return nil
	}

	err = s.updateOrder(ctx, order)
	if err != nil {
		zap.S().Errorf("Order create in payment system failed", "err", err.Error(), "order", order)

		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.RedirectUrl = url
	rsp.NeedRedirect = true

	if _, ok := req.Data[pkg.PaymentCreateFieldRecurringId]; ok && url == "" {
		rsp.NeedRedirect = false
	}

	return nil
}

func (s *Service) getOrderOperatingCompanyId(
	ctx context.Context,
	orderCountry string,
	merchant *billing.Merchant,
) (string, error) {
	orderOperatingCompany, err := s.operatingCompany.GetByPaymentCountry(ctx, orderCountry)
	if err != nil {
		if err == errorOperatingCompanyNotFound {
			return merchant.OperatingCompanyId, nil
		}

		return "", err
	}
	return orderOperatingCompany.Id, nil
}

func (s *Service) PaymentCallbackProcess(
	ctx context.Context,
	req *grpc.PaymentNotifyRequest,
	rsp *grpc.PaymentNotifyResponse,
) error {
	order, err := s.getOrderById(ctx, req.OrderId)

	if err != nil {
		return orderErrorNotFound
	}

	var data protobuf.Message

	ps, err := s.paymentSystem.GetById(ctx, order.PaymentMethod.PaymentSystemId)
	if err != nil {
		return orderErrorPaymentSystemInactive
	}

	switch ps.Handler {
	case pkg.PaymentSystemHandlerCardPay, paymentSystemHandlerCardPayMock:
		data = &billing.CardPayPaymentCallback{}
		err := json.Unmarshal(req.Request, data)

		if err != nil {
			return errors.New(paymentRequestIncorrect)
		}
		break
	default:
		return orderErrorPaymentMethodNotFound
	}

	h, err := s.NewPaymentSystem(ctx, s.cfg.PaymentSystemConfig, order)

	if err != nil {
		return err
	}

	pErr := h.ProcessPayment(order, data, string(req.Request), req.Signature)

	if pErr != nil {
		pErr, _ := pErr.(*grpc.ResponseError)

		rsp.Error = pErr.Error()
		rsp.Status = pErr.Status

		if pErr.Status == pkg.StatusTemporary {
			return nil
		}
	}

	switch order.PaymentMethod.ExternalId {
	case constant.PaymentSystemGroupAliasBankCard:
		if err := s.fillPaymentDataCard(order); err != nil {
			return err
		}
		break
	case constant.PaymentSystemGroupAliasQiwi,
		constant.PaymentSystemGroupAliasWebMoney,
		constant.PaymentSystemGroupAliasNeteller,
		constant.PaymentSystemGroupAliasAlipay:
		if err := s.fillPaymentDataEwallet(order); err != nil {
			return err
		}
		break
	case constant.PaymentSystemGroupAliasBitcoin:
		if err := s.fillPaymentDataCrypto(order); err != nil {
			return err
		}
		break
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.StatusErrorSystem
			rsp.Error = e.Message
			return nil
		}
		return err
	}

	if pErr == nil {
		if order.PrivateStatus == constant.OrderStatusPaymentSystemComplete {
			err = s.paymentSystemPaymentCallbackComplete(ctx, order)

			if err != nil {
				rsp.Status = pkg.StatusErrorSystem
				rsp.Error = err.Error()
				return nil
			}
		}

		err = s.onPaymentNotify(ctx, order)

		if err != nil {
			zap.L().Error(
				pkg.MethodFinishedWithError,
				zap.String("Method", "onPaymentNotify"),
				zap.Error(err),
				zap.String("orderId", order.Id),
				zap.String("orderUuid", order.Uuid),
			)

			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				rsp.Status = pkg.StatusErrorSystem
				rsp.Error = e.Message
				return nil
			}
			return err
		}

		if order.PrivateStatus == constant.OrderStatusPaymentSystemComplete {
			s.sendMailWithReceipt(ctx, order)
		}

		if h.IsRecurringCallback(data) {
			s.saveRecurringCard(ctx, order, h.GetRecurringId(data))
		}

		rsp.Status = pkg.StatusOK
	}

	return nil
}

func (s *Service) PaymentFormLanguageChanged(
	ctx context.Context,
	req *grpc.PaymentFormUserChangeLangRequest,
	rsp *grpc.PaymentFormDataChangeResponse,
) error {
	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = order.GetPaymentFormDataChangeResult()

	if order.User.Locale == req.Lang {
		return nil
	}

	if order.User.IsIdentified() == true {
		s.updateCustomerFromRequestLocale(ctx, order, req.Ip, req.AcceptLanguage, req.UserAgent, req.Lang)
	}

	order.User.Locale = req.Lang

	if order.ProductType == billing.OrderType_product {
		err = s.ProcessOrderProducts(ctx, order)
	} else if order.ProductType == billing.OrderType_key {
		_, err = s.ProcessOrderKeyProducts(ctx, order)
	}

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(ctx, pid, err, req, order)
		}
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Item = order.GetPaymentFormDataChangeResult()

	return nil
}

func (s *Service) PaymentFormPaymentAccountChanged(
	ctx context.Context,
	req *grpc.PaymentFormUserChangePaymentAccountRequest,
	rsp *grpc.PaymentFormDataChangeResponse,
) error {
	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	project, err := s.project.GetById(ctx, order.Project.Id)
	if err != nil {
		return orderErrorProjectNotFound
	}
	if project.IsDeleted() == true {
		return orderErrorProjectInactive
	}

	pm, err := s.paymentMethod.GetById(ctx, req.MethodId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentMethodNotFound
		return nil
	}

	ps, err := s.paymentSystem.GetById(ctx, pm.PaymentSystemId)
	if err != nil {
		rsp.Message = orderErrorPaymentSystemInactive
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	regex := pm.AccountRegexp

	if pm.ExternalId == constant.PaymentSystemGroupAliasBankCard {
		regex = "^\\d{6}(.*)\\d{4}$"
	}

	match, err := regexp.MatchString(regex, req.Account)

	if match == false || err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentAccountIncorrect
		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	switch pm.ExternalId {
	case constant.PaymentSystemGroupAliasBankCard:
		data := s.getBinData(ctx, req.Account)

		if data == nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCountryByPaymentAccountNotFound
			return nil
		}

		if order.PaymentRequisites == nil {
			order.PaymentRequisites = make(map[string]string)
		}
		order.PaymentRequisites[pkg.PaymentCreateBankCardFieldBrand] = data.CardBrand
		order.PaymentRequisites[pkg.PaymentCreateBankCardFieldIssuerCountryIsoCode] = data.BankCountryIsoCode

		break

	case constant.PaymentSystemGroupAliasQiwi:
		req.Account = "+" + req.Account
		num, err := libphonenumber.Parse(req.Account, CountryCodeUSA)

		if err != nil || num.CountryCode == nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorPaymentAccountIncorrect
			return nil
		}

		_, ok := pkg.CountryPhoneCodes[*num.CountryCode]
		if !ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCountryByPaymentAccountNotFound
			return nil
		}
		break
	}

	order.PaymentMethod = &billing.PaymentMethodOrder{
		Id:              pm.Id,
		Name:            pm.Name,
		PaymentSystemId: ps.Id,
		Group:           pm.Group,
		ExternalId:      pm.ExternalId,
		Handler:         ps.Handler,
		RefundAllowed:   pm.RefundAllowed,
	}

	err = s.setOrderChargeAmountAndCurrency(ctx, order)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	methodName, err := order.GetCostPaymentMethodName()
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}

		return err
	}

	order.PaymentMethod.Params, err = s.paymentMethod.GetPaymentSettings(
		pm,
		order.ChargeCurrency,
		order.MccCode,
		order.OperatingCompanyId,
		methodName,
		project,
	)

	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}

		return err
	}

	if !s.hasPaymentCosts(ctx, order) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorCostsRatesNotFound
		return nil
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Item = order.GetPaymentFormDataChangeResult()
	return nil
}

func (s *Service) ProcessBillingAddress(
	ctx context.Context,
	req *grpc.ProcessBillingAddressRequest,
	rsp *grpc.ProcessBillingAddressResponse,
) error {
	var err error
	var zip *billing.ZipCode

	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	initialCountry := order.GetCountry()

	billingAddress := &billing.OrderBillingAddress{
		Country: req.Country,
	}

	if req.Country == CountryCodeUSA && req.Zip != "" {
		billingAddress.PostalCode = req.Zip

		zip, err = s.zipCode.getByZipAndCountry(ctx, req.Zip, req.Country)

		if err == nil && zip != nil {
			billingAddress.Country = zip.Country
			billingAddress.PostalCode = zip.Zip
			billingAddress.City = zip.City
			billingAddress.State = zip.State.Code
		}
	}

	if !order.CountryChangeAllowed() && initialCountry != billingAddress.Country {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = orderCountryChangeRestrictedError
		return nil
	}

	order.BillingAddress = billingAddress

	restricted, err := s.applyCountryRestriction(ctx, order, billingAddress.Country)
	if err != nil {
		zap.L().Error(
			"s.applyCountryRestriction Method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		} else {
			rsp.Message = orderErrorUnknown
			rsp.Status = pkg.ResponseStatusSystemError
		}
		return nil
	}
	if restricted {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = orderCountryPaymentRestrictedError
		return nil
	}

	// save user replace country rule to cookie - start
	cookie := ""
	customer := &BrowserCookieCustomer{
		CreatedAt: time.Now(),
	}
	if req.Cookie != "" {
		customer, err = s.decryptBrowserCookie(req.Cookie)
		if err != nil || customer == nil {
			customer = &BrowserCookieCustomer{
				CreatedAt: time.Now(),
			}
		}
	}

	address, err := s.getAddressByIp(req.Ip)
	if err == nil {
		customer.Ip = req.Ip
		customer.IpCountry = address.Country
		customer.SelectedCountry = billingAddress.Country
		customer.UpdatedAt = time.Now()

		cookie, err = s.generateBrowserCookie(customer)
		if err != nil {
			cookie = ""
		}
	}
	// save user replace country rule to cookie - end

	if order.ProductType == billing.OrderType_product {
		err = s.ProcessOrderProducts(ctx, order)
	} else if order.ProductType == billing.OrderType_key {
		_, err = s.ProcessOrderKeyProducts(ctx, order)
	}

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(ctx, pid, err, req, order)
		}
		return err
	}

	processor := &OrderCreateRequestProcessor{Service: s, ctx: ctx}
	err = processor.processOrderVat(order)
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.setOrderChargeAmountAndCurrency(ctx, order)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	methodName, _ := order.GetCostPaymentMethodName()
	if methodName != "" && !s.hasPaymentCosts(ctx, order) {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorCostsRatesNotFound
		return nil
	}

	order.BillingCountryChangedByUser = order.BillingCountryChangedByUser == true || initialCountry != order.GetCountry()

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Cookie = cookie
	rsp.Item = &grpc.ProcessBillingAddressResponseItem{
		HasVat:               order.Tax.Rate > 0,
		VatRate:              tools.ToPrecise(order.Tax.Rate),
		Vat:                  order.Tax.Amount,
		VatInChargeCurrency:  s.FormatAmount(order.GetTaxAmountInChargeCurrency(), order.Currency),
		Amount:               order.OrderAmount,
		TotalAmount:          order.TotalPaymentAmount,
		Currency:             order.Currency,
		ChargeCurrency:       order.ChargeCurrency,
		ChargeAmount:         order.ChargeAmount,
		Items:                order.Items,
		CountryChangeAllowed: order.CountryChangeAllowed(),
	}

	return nil
}

func (s *Service) saveRecurringCard(ctx context.Context, order *billing.Order, recurringId string) {
	req := &repo.SavedCardRequest{
		Token:      order.User.Id,
		ProjectId:  order.Project.Id,
		MerchantId: order.Project.MerchantId,
		MaskedPan:  order.PaymentMethodTxnParams[pkg.PaymentCreateFieldPan],
		CardHolder: order.PaymentMethodTxnParams[pkg.PaymentCreateFieldHolder],
		Expire: &entity.CardExpire{
			Month: order.PaymentRequisites[pkg.PaymentCreateFieldMonth],
			Year:  order.PaymentRequisites[pkg.PaymentCreateFieldYear],
		},
		RecurringId: recurringId,
	}

	_, err := s.rep.InsertSavedCard(ctx, req)

	if err != nil {
		s.logError(
			"Call repository service to save recurring card failed",
			[]interface{}{
				"err", err.Error(),
				"request", req,
			},
		)
	} else {
		order.PaymentRequisites["saved"] = "1"
		err = s.updateOrder(ctx, order)
		if err != nil {
			zap.S().Errorf("Failed to update order after save recurruing card", "err", err.Error())
		}
	}
}

func (s *Service) updateOrder(ctx context.Context, order *billing.Order) error {
	ps := order.GetPublicStatus()

	zap.S().Debug("[updateOrder] updating order", "order_id", order.Id, "status", ps)

	originalOrder, _ := s.getOrderById(ctx, order.Id)

	statusChanged := false
	if originalOrder != nil {
		ops := originalOrder.GetPublicStatus()
		zap.S().Debug("[updateOrder] no original order status", "order_id", order.Id, "status", ops)
		statusChanged = ops != ps
	} else {
		zap.S().Debug("[updateOrder] no original order found", "order_id", order.Id)
	}

	needReceipt := statusChanged && (ps == constant.OrderPublicStatusRefunded || ps == constant.OrderPublicStatusProcessed)

	if needReceipt {
		switch order.Type {
		case pkg.OrderTypeRefund:
			order.ReceiptUrl = s.cfg.GetReceiptRefundUrl(order.Uuid, order.ReceiptId)
		case pkg.OrderTypeOrder:
			order.ReceiptUrl = s.cfg.GetReceiptPurchaseUrl(order.Uuid, order.ReceiptId)
		}
	}

	oid, _ := primitive.ObjectIDFromHex(order.Id)
	filter := bson.M{"_id": oid}
	_, err := s.db.Collection(collectionOrder).ReplaceOne(ctx, filter, order)

	if err != nil {
		s.logError(orderErrorUpdateOrderDataFailed, []interface{}{"error", err.Error(), "order", order})
		if err == mongo.ErrNoDocuments {
			return orderErrorNotFound
		}
		return orderErrorUnknown
	}

	zap.S().Debug("[updateOrder] updating order success", "order_id", order.Id, "status_changed", statusChanged, "type", order.ProductType)

	if order.ProductType == billing.OrderType_key {
		s.orderNotifyKeyProducts(context.TODO(), order)
	}

	if statusChanged && order.NeedCallbackNotification() {
		s.orderNotifyMerchant(ctx, order)
	}

	return nil
}

func (s *Service) orderNotifyKeyProducts(ctx context.Context, order *billing.Order) {
	zap.S().Debug("[orderNotifyKeyProducts] called", "order_id", order.Id, "status", order.GetPublicStatus(), "is product notified: ", order.IsKeyProductNotified)

	if order.IsKeyProductNotified {
		return
	}

	keys := order.Keys
	var err error
	switch order.GetPublicStatus() {
	case constant.OrderPublicStatusCanceled, constant.OrderPublicStatusRejected:
		for _, key := range keys {
			zap.S().Infow("[orderNotifyKeyProducts] trying to cancel reserving key", "order_id", order.Id, "key", key)
			rsp := &grpc.EmptyResponseWithStatus{}
			err = s.CancelRedeemKeyForOrder(ctx, &grpc.KeyForOrderRequest{KeyId: key}, rsp)
			if err != nil {
				zap.S().Error("internal error during canceling reservation for key", "err", err, "key", key)
				continue
			}
			if rsp.Status != pkg.ResponseStatusOk {
				zap.S().Error("could not cancel reservation for key", "key", key, "message", rsp.Message)
				continue
			}
		}
		order.IsKeyProductNotified = true
		break
	case constant.OrderPublicStatusProcessed:
		for _, key := range keys {
			zap.S().Infow("[orderNotifyKeyProducts] trying to finish reserving key", "order_id", order.Id, "key", key)
			rsp := &grpc.GetKeyForOrderRequestResponse{}
			err = s.FinishRedeemKeyForOrder(ctx, &grpc.KeyForOrderRequest{KeyId: key}, rsp)
			if err != nil {
				zap.S().Errorw("internal error during finishing reservation for key", "err", err, "key", key)
				continue
			}
			if rsp.Status != pkg.ResponseStatusOk {
				zap.S().Errorw("could not finish reservation for key", "key", key, "message", rsp.Message)
				continue
			}

			s.sendMailWithCode(ctx, order, rsp.Key)
		}
		order.IsKeyProductNotified = true
		break
	}
}

func (s *Service) sendMailWithReceipt(ctx context.Context, order *billing.Order) {
	payload, err := s.getPayloadForReceipt(ctx, order)
	if err != nil {
		zap.L().Error("get order receipt object failed", zap.Error(err))
		return
	}

	zap.S().Infow("sending receipt to broker", "order_id", order.Id, "topic", postmarkSdrPkg.PostmarkSenderTopicName)
	err = s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})
	if err != nil {
		zap.S().Errorw(
			"Publication receipt to user email queue is failed",
			"err", err, "email", order.ReceiptEmail, "order_id", order.Id, "topic", postmarkSdrPkg.PostmarkSenderTopicName)
	}
}

func (s *Service) getPayloadForReceipt(ctx context.Context, order *billing.Order) (*postmarkSdrPkg.Payload, error) {
	template := s.cfg.EmailTemplates.SuccessTransaction
	if order.Type == pkg.OrderTypeRefund {
		template = s.cfg.EmailTemplates.RefundTransaction
	}

	receipt, err := s.getOrderReceiptObject(ctx, order)
	if err != nil {
		return nil, err
	}

	var items []*structpb.Value
	if receipt.Items != nil {
		for _, item := range receipt.Items {
			item := &structpb.Value{
				Kind: &structpb.Value_StructValue{
					StructValue: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"name": {
								Kind: &structpb.Value_StringValue{StringValue: item.Name},
							},
							"price": {
								Kind: &structpb.Value_StringValue{StringValue: item.Price},
							},
						},
					},
				},
			}

			items = append(items, item)
		}
	}

	// set receipt items to nil to omit this field in jsonpb Marshal result
	// otherwise we get an Unmarshal error on attempt to marshal array to string
	receipt.Items = nil

	march := &jsonpb.Marshaler{}
	var buf bytes.Buffer
	err = march.Marshal(&buf, receipt)
	if err != nil {
		return nil, err
	}

	templateModel := make(map[string]string)
	err = json.Unmarshal(buf.Bytes(), &templateModel)
	if err != nil {
		return nil, err
	}

	// removing empty "platform" key (if any), for easy email template condition
	if v, ok := templateModel["platform"]; ok && v == "" {
		delete(templateModel, "platform")
	}

	// add vat_payer value field to template model, for easy template condition
	// for example {{#vat_payer_buyer}} ... {{/vat_payer_buyer}}
	templateModel["vat_payer_"+receipt.VatPayer] = "true"

	// add flag that charge currency differs from order currency to template model, for easy email template condition
	if order.Currency != order.ChargeCurrency {
		templateModel["show_total_charge"] = "true"
	}

	// add order has vat flag to template model, for easy email template condition
	if order.Tax.Amount > 0 {
		templateModel["order_has_vat"] = "true"
	}

	payload := &postmarkSdrPkg.Payload{
		TemplateAlias: template,
		TemplateModel: templateModel,
		To:            order.ReceiptEmail,
	}

	payload.TemplateObjectModel = &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"items": {
				Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{
					Values: items,
				}},
			},
		},
	}

	return payload, nil
}

func (s *Service) sendMailWithCode(_ context.Context, order *billing.Order, key *billing.Key) {
	var platformIconUrl = ""
	if platform, ok := availablePlatforms[order.PlatformId]; ok {
		platformIconUrl = platform.Icon
	}

	for _, item := range order.Items {
		if item.Id == key.KeyProductId {
			item.Code = key.Code
			payload := &postmarkSdrPkg.Payload{
				TemplateAlias: s.cfg.EmailTemplates.ActivationGameKey,
				TemplateModel: map[string]string{
					"code":          key.Code,
					"platform_icon": platformIconUrl,
					"product_name":  item.Name,
				},
				To: order.ReceiptEmail,
			}

			if len(item.Images) > 0 {
				payload.TemplateModel["product_image"] = item.Images[0]
			}

			err := s.postmarkBroker.Publish(postmarkSdrPkg.PostmarkSenderTopicName, payload, amqp.Table{})
			if err != nil {
				zap.S().Errorw(
					"Publication activation code to user email queue is failed",
					"err", err, "email", order.ReceiptEmail, "order_id", order.Id, "key_id", key.Id)

			} else {
				zap.S().Infow("Sent payload to broker", "email", order.ReceiptEmail, "order_id", order.Id, "key_id", key.Id, "topic", postmarkSdrPkg.PostmarkSenderTopicName)
			}
			return
		}
	}

	zap.S().Errorw("Mail not sent because no items found for key", "order_id", order.Id, "key_id", key.Id, "email", order.ReceiptEmail)
}

func (s *Service) orderNotifyMerchant(ctx context.Context, order *billing.Order) {
	zap.S().Debug("[orderNotifyMerchant] try to send notify merchant to rmq", "order_id", order.Id, "status", order.GetPublicStatus())

	err := s.broker.Publish(constant.PayOneTopicNotifyPaymentName, order, amqp.Table{"x-retry-count": int32(0)})
	if err != nil {
		zap.S().Debug("[orderNotifyMerchant] send notify merchant to rmq failed", "order_id", order.Id)
		s.logError(orderErrorPublishNotificationFailed, []interface{}{
			"err", err.Error(), "order", order, "topic", constant.PayOneTopicNotifyPaymentName,
		})
	} else {
		zap.S().Debug("[orderNotifyMerchant] send notify merchant to rmq failed", "order_id", order.Id)
	}
	order.SetNotificationStatus(order.GetPublicStatus(), err == nil)
	oid, _ := primitive.ObjectIDFromHex(order.Id)
	filter := bson.M{"_id": oid}
	_, err = s.db.Collection(collectionOrder).ReplaceOne(ctx, filter, order)
	if err != nil {
		zap.S().Debug("[orderNotifyMerchant] notification status update failed", "order_id", order.Id)
		s.logError(orderErrorUpdateOrderDataFailed, []interface{}{"error", err.Error(), "order", order})
	} else {
		zap.S().Debug("[orderNotifyMerchant] notification status updated succesfully", "order_id", order.Id)
	}
}

func (s *Service) getOrderById(ctx context.Context, id string) (order *billing.Order, err error) {
	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err = s.db.Collection(collectionOrder).FindOne(ctx, filter).Decode(&order)

	if err != nil && err != mongo.ErrNoDocuments {
		zap.S().Errorf("Order not found in payment create process", "err", err.Error(), "order_id", id)
	}

	if order == nil {
		return order, orderErrorNotFound
	}

	return
}

func (s *Service) getOrderByUuid(ctx context.Context, uuid string) (order *billing.Order, err error) {
	order, err = s.orderRepository.GetByUuid(ctx, uuid)

	if err != nil && err != mongo.ErrNoDocuments {
		zap.S().Errorf("Order not found in payment create process", "err", err.Error(), "uuid", uuid)
	}

	if order == nil {
		return order, orderErrorNotFound
	}

	return
}

func (s *Service) getOrderByUuidToForm(ctx context.Context, uuid string) (*billing.Order, error) {
	order, err := s.getOrderByUuid(ctx, uuid)

	if err != nil {
		return nil, orderErrorNotFound
	}

	if order.HasEndedStatus() == true {
		return nil, orderErrorOrderAlreadyComplete
	}

	if order.FormInputTimeIsEnded() == true {
		return nil, orderErrorFormInputTimeExpired
	}

	return order, nil
}

func (s *Service) getBinData(ctx context.Context, pan string) (data *BinData) {
	if len(pan) < 6 {
		zap.S().Errorf("Incorrect PAN to get BIN data", "pan", pan)
		return
	}

	i, err := strconv.ParseInt(pan[:6], 10, 32)

	if err != nil {
		zap.S().Errorf("Parse PAN to int failed", "error", err.Error(), "pan", pan)
		return
	}

	err = s.db.Collection(collectionBinData).FindOne(ctx, bson.M{"card_bin": int32(i)}).Decode(&data)

	if err != nil {
		zap.S().Errorf("Query to get bank card BIN data failed", "error", err.Error(), "pan", pan)
		return
	}

	return
}

func (v *OrderCreateRequestProcessor) prepareOrder() (*billing.Order, error) {
	id := primitive.NewObjectID().Hex()
	amount := v.FormatAmount(v.checked.amount, v.checked.currency)

	if (v.request.UrlVerify != "" || v.request.UrlNotify != "") && v.checked.project.AllowDynamicNotifyUrls == false {
		return nil, orderErrorDynamicNotifyUrlsNotAllowed
	}

	if (v.request.UrlSuccess != "" || v.request.UrlFail != "") && v.checked.project.AllowDynamicRedirectUrls == false {
		return nil, orderErrorDynamicRedirectUrlsNotAllowed
	}

	order := &billing.Order{
		Id:   id,
		Type: pkg.OrderTypeOrder,
		Project: &billing.ProjectOrder{
			Id:                      v.checked.project.Id,
			Name:                    v.checked.project.Name,
			UrlSuccess:              v.checked.project.UrlRedirectSuccess,
			UrlFail:                 v.checked.project.UrlRedirectFail,
			SendNotifyEmail:         v.checked.project.SendNotifyEmail,
			NotifyEmails:            v.checked.project.NotifyEmails,
			SecretKey:               v.checked.project.SecretKey,
			UrlCheckAccount:         v.checked.project.UrlCheckAccount,
			UrlProcessPayment:       v.checked.project.UrlProcessPayment,
			UrlChargebackPayment:    v.checked.project.UrlChargebackPayment,
			UrlCancelPayment:        v.checked.project.UrlCancelPayment,
			UrlRefundPayment:        v.checked.project.UrlRefundPayment,
			UrlFraudPayment:         v.checked.project.UrlFraudPayment,
			CallbackProtocol:        v.checked.project.CallbackProtocol,
			MerchantId:              v.checked.merchant.Id,
			Status:                  v.checked.project.Status,
			MerchantRoyaltyCurrency: v.checked.merchant.GetPayoutCurrency(),
		},
		Description:    fmt.Sprintf(orderDefaultDescription, id),
		ProjectOrderId: v.request.OrderId,
		ProjectAccount: v.request.Account,
		ProjectParams:  v.request.Other,
		PrivateStatus:  constant.OrderStatusNew,
		CreatedAt:      ptypes.TimestampNow(),
		IsJsonRequest:  v.request.IsJson,

		Uuid:               uuid.New().String(),
		ReceiptId:          uuid.New().String(),
		User:               v.checked.user,
		OrderAmount:        amount,
		TotalPaymentAmount: amount,
		ChargeAmount:       amount,
		Currency:           v.checked.currency,
		ChargeCurrency:     v.checked.currency,
		Products:           v.checked.products,
		Items:              v.checked.items,
		Metadata:           v.checked.metadata,
		PrivateMetadata:    v.checked.privateMetadata,
		Issuer: &billing.OrderIssuer{
			Url:           v.request.IssuerUrl,
			Embedded:      v.request.IsEmbedded,
			ReferenceType: v.request.IssuerReferenceType,
			Reference:     v.request.IssuerReference,
			UtmSource:     v.request.UtmSource,
			UtmCampaign:   v.request.UtmCampaign,
			UtmMedium:     v.request.UtmMedium,
			ReferrerHost:  getHostFromUrl(v.request.IssuerUrl),
		},
		CountryRestriction: &billing.CountryRestriction{
			IsoCodeA2:       "",
			PaymentsAllowed: true,
			ChangeAllowed:   true,
		},
		PlatformId:              v.request.PlatformId,
		ProductType:             v.request.Type,
		IsBuyForVirtualCurrency: v.checked.isBuyForVirtualCurrency,
		MccCode:                 v.checked.merchant.MccCode,
		OperatingCompanyId:      v.checked.merchant.OperatingCompanyId,
		IsHighRisk:              v.checked.merchant.IsHighRisk(),
		IsCurrencyPredefined:    v.checked.isCurrencyPredefined,
		VatPayer:                v.checked.project.VatPayer,
	}

	if v.checked.virtualAmount > 0 {
		order.VirtualCurrencyAmount = v.checked.virtualAmount
	}

	if order.User == nil {
		order.User = &billing.OrderUser{
			Object: pkg.ObjectTypeUser,
		}
	} else {
		if order.User.Address != nil {
			err := v.processOrderVat(order)
			if err != nil {
				zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
				return nil, err
			}

			restricted, err := v.applyCountryRestriction(v.ctx, order, order.GetCountry())
			if err != nil {
				return nil, err
			}
			if restricted {
				return nil, orderCountryPaymentRestrictedError
			}
		}
	}

	if v.request.Description != "" {
		order.Description = v.request.Description
	}

	if v.request.UrlSuccess != "" {
		order.Project.UrlSuccess = v.request.UrlSuccess
	}

	if v.request.UrlFail != "" {
		order.Project.UrlFail = v.request.UrlFail
	}

	if v.checked.paymentMethod != nil {
		ps, err := v.paymentSystem.GetById(v.ctx, v.checked.paymentMethod.PaymentSystemId)
		if err != nil {
			return nil, err
		}

		order.PaymentMethod = &billing.PaymentMethodOrder{
			Id:              v.checked.paymentMethod.Id,
			Name:            v.checked.paymentMethod.Name,
			PaymentSystemId: ps.Id,
			Group:           v.checked.paymentMethod.Group,
		}

		methodName, err := order.GetCostPaymentMethodName()
		if err == nil {
			order.PaymentMethod.Params, err = v.paymentMethod.GetPaymentSettings(
				v.checked.paymentMethod,
				v.checked.currency,
				v.checked.mccCode,
				v.checked.operatingCompanyId,
				methodName,
				v.checked.project,
			)

			if err != nil {
				return nil, err
			}
		}
	}

	order.ExpireDateToFormInput, _ = ptypes.TimestampProto(time.Now().Add(time.Minute * defaultExpireDateToFormInput))

	return order, nil
}

func (v *OrderCreateRequestProcessor) processMerchant() error {
	if !v.checked.merchant.IsBankingComplete() {
		return orderErrorMerchantDoNotHaveBanking
	}

	if !v.checked.merchant.IsCompanyComplete() {
		return orderErrorMerchantDoNotHaveCompanyInfo
	}

	if v.checked.merchant.HasTariff() == false {
		return orderErrorMerchantBadTariffs
	}

	return nil
}

func (v *OrderCreateRequestProcessor) processProject() error {
	project, err := v.project.GetById(v.ctx, v.request.ProjectId)

	if err != nil {
		zap.S().Errorw("Order create get project error", "err", err, "request", v.request)
		return orderErrorProjectNotFound
	}

	if project.IsDeleted() == true {
		return orderErrorProjectInactive
	}

	if project.MerchantId == "" {
		return orderErrorProjectMerchantNotFound
	}

	_, err = primitive.ObjectIDFromHex(project.MerchantId)

	if err != nil {
		return orderErrorProjectMerchantNotFound
	}

	merchant, err := v.merchant.GetById(v.ctx, project.MerchantId)
	if err != nil {
		return orderErrorProjectMerchantNotFound
	}

	if merchant.IsDeleted() == true {
		return orderErrorProjectMerchantInactive
	}

	v.checked.project = project
	v.checked.merchant = merchant

	return nil
}

func (v *OrderCreateRequestProcessor) processCurrency(orderType string) error {
	if v.request.Currency != "" {
		if !contains(v.supportedCurrencies, v.request.Currency) {
			return orderErrorCurrencyNotFound
		}

		v.checked.currency = v.request.Currency
		v.checked.isCurrencyPredefined = true

		pricegroup, err := v.priceGroup.GetByRegion(v.ctx, v.checked.currency)
		if err == nil {
			v.checked.priceGroup = pricegroup
		}
		return nil
	}

	if orderType == billing.OrderType_simple {
		return orderErrorCurrencyIsRequired
	}

	v.checked.isCurrencyPredefined = false

	countryCode := v.getCountry()
	if countryCode == "" {
		v.checked.currency = v.checked.merchant.GetPayoutCurrency()
		pricegroup, err := v.priceGroup.GetByRegion(v.ctx, v.checked.currency)
		if err == nil {
			v.checked.priceGroup = pricegroup
		}
		return nil
	}

	country, err := v.country.GetByIsoCodeA2(v.ctx, countryCode)
	if err != nil {
		v.checked.currency = v.checked.merchant.GetPayoutCurrency()
		pricegroup, err := v.priceGroup.GetByRegion(v.ctx, v.checked.currency)
		if err == nil {
			v.checked.priceGroup = pricegroup
		}
		return nil
	}

	pricegroup, err := v.priceGroup.GetById(v.ctx, country.PriceGroupId)
	if err != nil {
		v.checked.currency = v.checked.merchant.GetPayoutCurrency()
		pricegroup, err := v.priceGroup.GetByRegion(v.ctx, v.checked.currency)
		if err == nil {
			v.checked.priceGroup = pricegroup
		}
		return nil
	}

	v.checked.currency = pricegroup.Currency
	v.checked.priceGroup = pricegroup

	return nil
}

func (v *OrderCreateRequestProcessor) processAmount() {
	v.checked.amount = v.request.Amount
}

func (v *OrderCreateRequestProcessor) processMetadata() {
	v.checked.metadata = v.request.Metadata
}

func (v *OrderCreateRequestProcessor) processPrivateMetadata() {
	v.checked.privateMetadata = v.request.PrivateMetadata
}

func (v *OrderCreateRequestProcessor) getCountry() string {
	if v.checked.user == nil {
		return ""
	}
	return v.checked.user.GetCountry()
}

func (v *OrderCreateRequestProcessor) processPayerIp() error {
	address, err := v.getAddressByIp(v.checked.user.Ip)

	if err != nil {
		return err
	}

	// fully replace address, to avoid inconsistence
	v.checked.user.Address = address

	return nil
}

func (v *OrderCreateRequestProcessor) processPaylinkKeyProducts() error {

	amount, priceGroup, items, _, err := v.processKeyProducts(
		v.ctx,
		v.checked.project.Id,
		v.request.Products,
		v.checked.priceGroup,
		DefaultLanguage,
		v.request.PlatformId,
	)

	if err != nil {
		return err
	}

	v.checked.priceGroup = priceGroup

	v.checked.products = v.request.Products
	v.checked.currency = priceGroup.Currency
	v.checked.amount = amount
	v.checked.items = items

	return nil
}

func (v *OrderCreateRequestProcessor) processPaylinkProducts() error {

	amount, priceGroup, items, isBuyForVirtual, err := v.processProducts(
		v.ctx,
		v.checked.project.Id,
		v.request.Products,
		v.checked.priceGroup,
		DefaultLanguage,
	)

	v.checked.isBuyForVirtualCurrency = isBuyForVirtual

	if err != nil {
		return err
	}

	v.checked.priceGroup = priceGroup

	v.checked.products = v.request.Products
	v.checked.currency = priceGroup.Currency
	v.checked.amount = amount
	v.checked.items = items

	return nil
}

func (v *OrderCreateRequestProcessor) processProjectOrderId() error {
	var order *billing.Order

	oid, _ := primitive.ObjectIDFromHex(v.checked.project.Id)
	filter := bson.M{"project._id": oid, "project_order_id": v.request.OrderId}
	err := v.db.Collection(collectionOrder).FindOne(v.ctx, filter).Decode(&order)

	if err != nil && err != mongo.ErrNoDocuments {
		zap.S().Errorw("Order create check project order id unique", "err", err, "filter", filter)
		return orderErrorCanNotCreate
	}

	if order != nil {
		return orderErrorProjectOrderIdIsDuplicate
	}

	return nil
}

func (v *OrderCreateRequestProcessor) processPaymentMethod(pm *billing.PaymentMethod) error {
	if pm.IsActive == false {
		return orderErrorPaymentMethodInactive
	}

	if _, err := v.paymentSystem.GetById(v.ctx, pm.PaymentSystemId); err != nil {
		return orderErrorPaymentSystemInactive
	}

	_, err := v.Service.paymentMethod.GetPaymentSettings(pm, v.checked.currency, v.checked.mccCode, v.checked.operatingCompanyId, "", v.checked.project)

	if err != nil {
		return err
	}

	v.checked.paymentMethod = pm

	return nil
}

func (v *OrderCreateRequestProcessor) processLimitAmounts() (err error) {
	amount := v.checked.amount

	pmls, err := v.paymentMinLimitSystem.GetByCurrency(v.ctx, v.checked.currency)
	if err != nil {
		return err
	}

	if amount < pmls.Amount {
		return orderErrorAmountLowerThanMinLimitSystem
	}

	if v.checked.project.LimitsCurrency != "" && v.checked.project.LimitsCurrency != v.checked.currency {
		if !contains(v.supportedCurrencies, v.checked.project.LimitsCurrency) {
			return orderErrorCurrencyNotFound
		}
		req := &currencies.ExchangeCurrencyCurrentForMerchantRequest{
			From:              v.checked.currency,
			To:                v.checked.project.LimitsCurrency,
			MerchantId:        v.checked.merchant.Id,
			RateType:          curPkg.RateTypeOxr,
			ExchangeDirection: curPkg.ExchangeDirectionSell,
			Amount:            amount,
		}

		rsp, err := v.curService.ExchangeCurrencyCurrentForMerchant(context.TODO(), req)

		if err != nil {
			zap.S().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentForMerchant"),
			)

			return orderErrorConvertionCurrency
		}

		amount = rsp.ExchangedAmount
	}

	if amount < v.checked.project.MinPaymentAmount {
		return orderErrorAmountLowerThanMinAllowed
	}

	if v.checked.project.MaxPaymentAmount > 0 && amount > v.checked.project.MaxPaymentAmount {
		return orderErrorAmountGreaterThanMaxAllowed
	}

	if v.checked.paymentMethod != nil {
		if v.request.Amount < v.checked.paymentMethod.MinPaymentAmount {
			return orderErrorAmountLowerThanMinAllowedPaymentMethod
		}

		if v.checked.paymentMethod.MaxPaymentAmount > 0 && v.request.Amount > v.checked.paymentMethod.MaxPaymentAmount {
			return orderErrorAmountGreaterThanMaxAllowedPaymentMethod
		}
	}

	return
}

func (v *OrderCreateRequestProcessor) processSignature() error {
	var hashString string

	if v.request.IsJson == false {
		var keys []string
		var elements []string

		for k := range v.request.RawParams {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			value := k + "=" + v.request.RawParams[k]
			elements = append(elements, value)
		}

		hashString = strings.Join(elements, "") + v.checked.project.SecretKey
	} else {
		hashString = v.request.RawBody + v.checked.project.SecretKey
	}

	h := sha512.New()
	h.Write([]byte(hashString))

	if hex.EncodeToString(h.Sum(nil)) != v.request.Signature {
		return orderErrorSignatureInvalid
	}

	return nil
}

// Calculate VAT for order
func (v *OrderCreateRequestProcessor) processOrderVat(order *billing.Order) error {
	order.Tax = &billing.OrderTax{
		Amount:   0,
		Rate:     0,
		Type:     taxTypeVat,
		Currency: order.Currency,
	}
	order.TotalPaymentAmount = order.OrderAmount
	order.ChargeAmount = order.TotalPaymentAmount
	order.ChargeCurrency = order.Currency

	countryCode := order.GetCountry()

	if countryCode == "" {
		return nil
	}

	if countryCode == CountryCodeUSA {
		order.Tax.Type = taxTypeSalesTax
	}

	if order.VatPayer == pkg.VatPayerNobody {
		return nil
	}

	if countryCode != "" {
		country, err := v.country.GetByIsoCodeA2(v.ctx, countryCode)
		if err != nil {
			return err
		}
		if country.VatEnabled == false {
			return nil
		}
	}

	req := &tax_service.GeoIdentity{
		Country: countryCode,
	}

	if countryCode == CountryCodeUSA {
		req.Zip = order.GetPostalCode()
	}

	rsp, err := v.tax.GetRate(context.TODO(), req)

	if err != nil {
		v.logError("Tax service return error", []interface{}{"error", err.Error(), "request", req})
		return err
	}

	order.Tax.Rate = rsp.Rate

	switch order.VatPayer {

	case pkg.VatPayerBuyer:
		order.Tax.Amount = v.FormatAmount(order.OrderAmount*order.Tax.Rate, order.Currency)
		order.TotalPaymentAmount = v.FormatAmount(order.OrderAmount+order.Tax.Amount, order.Currency)
		order.ChargeAmount = order.TotalPaymentAmount
		break

	case pkg.VatPayerSeller:
		order.Tax.Amount = v.FormatAmount(tools.GetPercentPartFromAmount(order.TotalPaymentAmount, order.Tax.Rate), order.Currency)
		break

	default:
		return orderErrorVatPayerUnknown
	}

	return nil
}

func (v *OrderCreateRequestProcessor) processCustomerToken() error {
	token, err := v.getTokenBy(v.request.Token)

	if err != nil {
		return err
	}

	customer, err := v.getCustomerById(v.ctx, token.CustomerId)

	if err != nil {
		return err
	}

	v.request.Type = token.Settings.Type
	v.request.PlatformId = token.Settings.PlatformId

	v.request.ProjectId = token.Settings.ProjectId
	v.request.Description = token.Settings.Description
	v.request.Amount = token.Settings.Amount
	v.request.Currency = token.Settings.Currency
	v.request.Products = token.Settings.ProductsIds
	v.request.Metadata = token.Settings.Metadata
	v.request.PaymentMethod = token.Settings.PaymentMethod

	if token.Settings.ReturnUrl != nil {
		v.request.UrlSuccess = token.Settings.ReturnUrl.Success
		v.request.UrlFail = token.Settings.ReturnUrl.Fail
	}

	v.checked.user = &billing.OrderUser{
		ExternalId: token.User.Id,
		Address:    token.User.Address,
		Metadata:   token.User.Metadata,
	}

	if token.User.Name != nil {
		v.checked.user.Name = token.User.Name.Value
	}

	if token.User.Email != nil {
		v.checked.user.Email = token.User.Email.Value
		v.checked.user.EmailVerified = token.User.Email.Verified
	}

	if token.User.Phone != nil {
		v.checked.user.Phone = token.User.Phone.Value
		v.checked.user.PhoneVerified = token.User.Phone.Verified
	}

	if token.User.Ip != nil {
		v.checked.user.Ip = token.User.Ip.Value
	}

	if token.User.Locale != nil {
		v.checked.user.Locale = token.User.Locale.Value
	}

	v.checked.user.Id = customer.Id
	v.checked.user.Object = pkg.ObjectTypeUser
	v.checked.user.TechEmail = customer.TechEmail

	return nil
}

func (v *OrderCreateRequestProcessor) processUserData() (err error) {
	customer := new(billing.Customer)
	tokenReq := v.transformOrderUser2TokenRequest(v.request.User)

	if v.request.Token == "" {
		customer, _ = v.findCustomer(v.ctx, tokenReq, v.checked.project)
	}

	if customer != nil {
		customer, err = v.updateCustomer(v.ctx, tokenReq, v.checked.project, customer)
	} else {
		customer, err = v.createCustomer(v.ctx, tokenReq, v.checked.project)
	}

	if err != nil {
		return err
	}

	v.checked.user = v.request.User
	v.checked.user.Id = customer.Id
	v.checked.user.Object = pkg.ObjectTypeUser
	v.checked.user.TechEmail = customer.TechEmail

	return
}

// GetById payment methods of project for rendering in payment form
func (v *PaymentFormProcessor) processRenderFormPaymentMethods(
	ctx context.Context,
	project *billing.Project,
) ([]*billing.PaymentFormPaymentMethod, error) {
	var projectPms []*billing.PaymentFormPaymentMethod

	paymentMethods, err := v.service.paymentMethod.ListByParams(
		ctx,
		project,
		v.order.Currency,
		v.order.MccCode,
		v.order.OperatingCompanyId,
	)

	if err != nil {
		zap.S().Errorw("ListByParams failed", "error", err, "order_id", v.order.Id, "order_uuid", v.order.Uuid)
		return nil, err
	}

	for _, pm := range paymentMethods {
		if pm.IsActive == false {
			continue
		}

		ps, err := v.service.paymentSystem.GetById(ctx, pm.PaymentSystemId)

		if err != nil {
			zap.S().Errorw("GetById failed", "error", err, "order_id", v.order.Id, "order_uuid", v.order.Uuid)
			continue
		}

		if ps.IsActive == false {
			continue
		}

		if v.order.OrderAmount < pm.MinPaymentAmount ||
			(pm.MaxPaymentAmount > 0 && v.order.OrderAmount > pm.MaxPaymentAmount) {
			continue
		}
		_, err = v.service.paymentMethod.GetPaymentSettings(pm, v.order.Currency, v.order.MccCode, v.order.OperatingCompanyId, "", project)

		if err != nil {
			zap.S().Errorw("GetPaymentSettings failed", "error", err, "order_id", v.order.Id, "order_uuid", v.order.Uuid)
			continue
		}

		formPm := &billing.PaymentFormPaymentMethod{
			Id:            pm.Id,
			Name:          pm.Name,
			Type:          pm.Type,
			Group:         pm.Group,
			AccountRegexp: pm.AccountRegexp,
		}

		err = v.processPaymentMethodsData(formPm)

		if err != nil {
			zap.S().Errorw(
				"Process payment Method data failed",
				"error", err,
				"order_id", v.order.Id,
			)
			continue
		}

		projectPms = append(projectPms, formPm)
	}

	if len(projectPms) <= 0 {
		zap.S().Errorw("Not found any active payment methods", "order_id", v.order.Id, "order_uuid", v.order.Uuid)
		return projectPms, orderErrorPaymentMethodNotAllowed
	}

	return projectPms, nil
}

func (v *PaymentFormProcessor) processPaymentMethodsData(pm *billing.PaymentFormPaymentMethod) error {
	pm.HasSavedCards = false

	if pm.IsBankCard() == true {
		req := &repo.SavedCardRequest{Token: v.order.User.Id}
		rsp, err := v.service.rep.FindSavedCards(context.TODO(), req)

		if err != nil {
			zap.S().Errorw(
				"Get saved cards from repository failed",
				"error", err,
				"token", v.order.User.Id,
				"project_id", v.order.Project.Id,
				"order_id", v.order.Id,
			)
		} else {
			pm.HasSavedCards = len(rsp.SavedCards) > 0
			pm.SavedCards = []*billing.SavedCard{}

			for _, v := range rsp.SavedCards {
				d := &billing.SavedCard{
					Id:         v.Id,
					Pan:        v.MaskedPan,
					CardHolder: v.CardHolder,
					Expire:     &billing.CardExpire{Month: v.Expire.Month, Year: v.Expire.Year},
				}

				pm.SavedCards = append(pm.SavedCards, d)
			}

		}
	}

	return nil
}

func (v *PaymentCreateProcessor) reserveKeysForOrder(ctx context.Context, order *billing.Order) error {
	if len(order.Keys) == 0 {
		zap.S().Infow("[ProcessOrderKeyProducts] reserving keys", "order_id", order.Id)
		keys := make([]string, len(order.Products))
		for i, productId := range order.Products {
			reserveRes := &grpc.PlatformKeyReserveResponse{}
			reserveReq := &grpc.PlatformKeyReserveRequest{
				PlatformId:   order.PlatformId,
				MerchantId:   order.Project.MerchantId,
				OrderId:      order.Id,
				KeyProductId: productId,
				Ttl:          oneDayTtl,
			}

			err := v.service.ReserveKeyForOrder(ctx, reserveReq, reserveRes)
			if err != nil {
				zap.L().Error(
					pkg.ErrorGrpcServiceCallFailed,
					zap.Error(err),
					zap.String(errorFieldService, "KeyService"),
					zap.String(errorFieldMethod, "ReserveKeyForOrder"),
				)
				return err
			}

			if reserveRes.Status != pkg.ResponseStatusOk {
				zap.S().Errorw("[ProcessOrderKeyProducts] can't reserve key. Cancelling reserved before", "message", reserveRes.Message, "order_id", order.Id)

				// we should cancel reservation for keys reserved before
				for _, keyToCancel := range keys {
					if len(keyToCancel) > 0 {
						cancelRes := &grpc.EmptyResponseWithStatus{}
						err := v.service.CancelRedeemKeyForOrder(ctx, &grpc.KeyForOrderRequest{KeyId: keyToCancel}, cancelRes)
						if err != nil {
							zap.L().Error(
								pkg.ErrorGrpcServiceCallFailed,
								zap.Error(err),
								zap.String(errorFieldService, "KeyService"),
								zap.String(errorFieldMethod, "CancelRedeemKeyForOrder"),
							)
						} else if cancelRes.Status != pkg.ResponseStatusOk {
							zap.S().Errorw("[ProcessOrderKeyProducts] error during cancelling reservation", "message", cancelRes.Message, "order_id", order.Id)
						} else {
							zap.S().Infow("[ProcessOrderKeyProducts] successful canceled reservation", "order_id", order.Id, "key_id", keyToCancel)
						}
					}
				}

				return reserveRes.Message
			}
			zap.S().Infow("[ProcessOrderKeyProducts] reserved for product", "product ", productId, "reserveRes ", reserveRes, "order_id", order.Id)
			keys[i] = reserveRes.KeyId
		}

		order.Keys = keys
	}

	return nil
}

// Validate data received from payment form and write validated data to order
func (v *PaymentCreateProcessor) processPaymentFormData(ctx context.Context) error {
	if _, ok := v.data[pkg.PaymentCreateFieldOrderId]; !ok ||
		v.data[pkg.PaymentCreateFieldOrderId] == "" {
		return orderErrorCreatePaymentRequiredFieldIdNotFound
	}

	if _, ok := v.data[pkg.PaymentCreateFieldPaymentMethodId]; !ok ||
		v.data[pkg.PaymentCreateFieldPaymentMethodId] == "" {
		return orderErrorCreatePaymentRequiredFieldPaymentMethodNotFound
	}

	if _, ok := v.data[pkg.PaymentCreateFieldEmail]; !ok ||
		v.data[pkg.PaymentCreateFieldEmail] == "" {
		return orderErrorCreatePaymentRequiredFieldEmailNotFound
	}

	order, err := v.service.getOrderByUuidToForm(ctx, v.data[pkg.PaymentCreateFieldOrderId])

	if err != nil {
		return err
	}

	if order.PrivateStatus != constant.OrderStatusNew && order.PrivateStatus != constant.OrderStatusPaymentSystemComplete {
		return orderErrorAlreadyProcessed
	}

	if order.UserAddressDataRequired == true {
		country, ok := v.data[pkg.PaymentCreateFieldUserCountry]

		if !ok || country == "" {
			return orderErrorCreatePaymentRequiredFieldUserCountryNotFound
		}

		if country == CountryCodeUSA {
			zip, ok := v.data[pkg.PaymentCreateFieldUserZip]

			if !ok || zip == "" {
				return orderErrorCreatePaymentRequiredFieldUserZipNotFound
			}

			zipData, err := v.service.zipCode.getByZipAndCountry(ctx, zip, country)

			if err == nil && zipData != nil {
				v.data[pkg.PaymentCreateFieldUserCity] = zipData.City
				v.data[pkg.PaymentCreateFieldUserState] = zipData.State.Code
			}
		}
	}

	merchant, err := v.service.merchant.GetById(ctx, order.GetMerchantId())
	if err != nil {
		return err
	}

	if order.MccCode == "" {
		order.MccCode = merchant.MccCode
		order.IsHighRisk = merchant.IsHighRisk()
	}

	order.OperatingCompanyId, err = v.service.getOrderOperatingCompanyId(ctx, order.GetCountry(), merchant)
	if err != nil {
		return err
	}

	processor := &OrderCreateRequestProcessor{
		Service: v.service,
		request: &billing.OrderCreateRequest{
			ProjectId: order.Project.Id,
			Amount:    order.OrderAmount,
		},
		checked: &orderCreateRequestProcessorChecked{
			currency:           order.Currency,
			amount:             order.OrderAmount,
			mccCode:            order.MccCode,
			operatingCompanyId: order.OperatingCompanyId,
		},
		ctx: ctx,
	}

	if err := processor.processProject(); err != nil {
		return err
	}

	pm, err := v.service.paymentMethod.GetById(ctx, v.data[pkg.PaymentCreateFieldPaymentMethodId])
	if err != nil {
		return orderErrorPaymentMethodNotFound
	}

	if pm.IsActive == false {
		return orderErrorPaymentMethodInactive
	}

	ps, err := v.service.paymentSystem.GetById(ctx, pm.PaymentSystemId)
	if err != nil {
		return orderErrorPaymentSystemInactive
	}

	if err := processor.processLimitAmounts(); err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			return e
		}
		return err
	}

	if order.User.Ip != v.ip {
		order.User.Ip = v.ip
	}

	updCustomerReq := &grpc.TokenRequest{User: &billing.TokenUser{}}

	if val, ok := v.data[pkg.PaymentCreateFieldEmail]; ok {
		order.User.Email = val
		updCustomerReq.User.Email = &billing.TokenUserEmailValue{Value: val}
	}

	order.PaymentRequisites = make(map[string]string)

	if order.UserAddressDataRequired == true {
		if order.BillingAddress == nil {
			order.BillingAddress = &billing.OrderBillingAddress{}
		}

		if order.BillingAddress.Country != v.data[pkg.PaymentCreateFieldUserCountry] {
			order.BillingAddress.Country = v.data[pkg.PaymentCreateFieldUserCountry]
		}

		if order.BillingAddress.Country == CountryCodeUSA {
			if order.BillingAddress.City != v.data[pkg.PaymentCreateFieldUserCity] {
				order.BillingAddress.City = v.data[pkg.PaymentCreateFieldUserCity]
			}

			if order.BillingAddress.PostalCode != v.data[pkg.PaymentCreateFieldUserZip] {
				order.BillingAddress.PostalCode = v.data[pkg.PaymentCreateFieldUserZip]
			}

			if order.BillingAddress.State != v.data[pkg.PaymentCreateFieldUserState] {
				order.BillingAddress.State = v.data[pkg.PaymentCreateFieldUserState]
			}
		}

		err = processor.processOrderVat(order)
		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
			return err
		}
		updCustomerReq.User.Address = order.BillingAddress
	}

	restricted, err := v.service.applyCountryRestriction(ctx, order, order.GetCountry())
	if err != nil {
		zap.L().Error(
			"v.service.applyCountryRestriction Method failed",
			zap.Error(err),
			zap.Any("order", order),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			return e
		}
		return orderErrorUnknown
	}
	if restricted {
		return orderCountryPaymentRestrictedError
	}

	if order.User.IsIdentified() == true {
		customer, err := v.service.updateCustomerFromRequest(ctx, order, updCustomerReq, v.ip, v.acceptLanguage, v.userAgent)

		if err != nil {
			v.service.logError("Update customer data by request failed", []interface{}{"error", err.Error(), "data", updCustomerReq})
		} else {
			if order.User.Locale == "" && customer.Locale != "" &&
				customer.Locale != order.User.Locale {
				order.User.Locale = customer.Locale
			}
		}
	}

	delete(v.data, pkg.PaymentCreateFieldOrderId)
	delete(v.data, pkg.PaymentCreateFieldPaymentMethodId)
	delete(v.data, pkg.PaymentCreateFieldEmail)

	if pm.IsBankCard() == true {
		if id, ok := v.data[pkg.PaymentCreateFieldStoredCardId]; ok {
			storedCard, err := v.service.rep.FindSavedCardById(context.TODO(), &repo.FindByStringValue{Value: id})

			if err != nil {
				v.service.logError("Get data about stored card failed", []interface{}{"err", err.Error(), "id", id})
			}

			if storedCard == nil {
				v.service.logError("Get data about stored card failed", []interface{}{"id", id})
				return orderGetSavedCardError
			}

			if storedCard.Token != order.User.Id {
				v.service.logError("Alarm: user try use not own bank card for payment", []interface{}{"user_id", order.User.Id, "card_id", id})
				return orderErrorRecurringCardNotOwnToUser
			}

			order.PaymentRequisites[pkg.PaymentCreateFieldPan] = storedCard.MaskedPan
			order.PaymentRequisites[pkg.PaymentCreateFieldMonth] = storedCard.Expire.Month
			order.PaymentRequisites[pkg.PaymentCreateFieldYear] = storedCard.Expire.Year
			order.PaymentRequisites[pkg.PaymentCreateFieldHolder] = storedCard.CardHolder
			order.PaymentRequisites[pkg.PaymentCreateFieldRecurringId] = storedCard.RecurringId
		} else {
			validator := &bankCardValidator{
				Pan:    v.data[pkg.PaymentCreateFieldPan],
				Cvv:    v.data[pkg.PaymentCreateFieldCvv],
				Month:  v.data[pkg.PaymentCreateFieldMonth],
				Year:   v.data[pkg.PaymentCreateFieldYear],
				Holder: v.data[pkg.PaymentCreateFieldHolder],
			}

			if err := validator.Validate(); err != nil {
				return err
			}

			order.PaymentRequisites[pkg.PaymentCreateFieldPan] = tools.MaskBankCardNumber(v.data[pkg.PaymentCreateFieldPan])
			order.PaymentRequisites[pkg.PaymentCreateFieldMonth] = v.data[pkg.PaymentCreateFieldMonth]

			if len(v.data[pkg.PaymentCreateFieldYear]) < 3 {
				v.data[pkg.PaymentCreateFieldYear] = strconv.Itoa(time.Now().UTC().Year())[:2] + v.data[pkg.PaymentCreateFieldYear]
			}

			order.PaymentRequisites[pkg.PaymentCreateFieldYear] = v.data[pkg.PaymentCreateFieldYear]
		}

		bin := v.service.getBinData(ctx, order.PaymentRequisites[pkg.PaymentCreateFieldPan])

		if bin != nil {
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldBrand] = bin.CardBrand
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldType] = bin.CardType
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldCategory] = bin.CardCategory
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldIssuerName] = bin.BankName
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldIssuerCountry] = bin.BankCountryName
			order.PaymentRequisites[pkg.PaymentCreateBankCardFieldIssuerCountryIsoCode] = bin.BankCountryIsoCode
		}
	} else {
		account := ""

		if acc, ok := v.data[pkg.PaymentCreateFieldEWallet]; ok {
			account = acc
		}

		if acc, ok := v.data[pkg.PaymentCreateFieldCrypto]; ok {
			account = acc
		}

		if account == "" {
			return paymentSystemErrorEWalletIdentifierIsInvalid
		}

		order.PaymentRequisites = v.data
	}

	if order.PaymentMethod == nil {
		order.PaymentMethod = &billing.PaymentMethodOrder{
			Id:              pm.Id,
			Name:            pm.Name,
			PaymentSystemId: ps.Id,
			Group:           pm.Group,
			ExternalId:      pm.ExternalId,
			Handler:         ps.Handler,
			RefundAllowed:   pm.RefundAllowed,
		}
	}

	methodName, err := order.GetCostPaymentMethodName()
	if err == nil {
		order.PaymentMethod.Params, err = v.service.paymentMethod.GetPaymentSettings(
			pm,
			processor.checked.currency,
			processor.checked.mccCode,
			processor.checked.operatingCompanyId,
			methodName,
			processor.checked.project,
		)

		if err != nil {
			return err
		}
	}

	v.checked.project = processor.checked.project
	v.checked.paymentMethod = pm
	v.checked.order = order

	if order.ProjectAccount == "" {
		order.ProjectAccount = order.User.Email
	}

	return nil
}

func (s *Service) GetOrderKeyProducts(ctx context.Context, projectId string, productIds []string) ([]*grpc.KeyProduct, error) {
	if len(productIds) == 0 {
		return nil, orderErrorProductsEmpty
	}

	result := grpc.ListKeyProductsResponse{}

	err := s.GetKeyProductsForOrder(ctx, &grpc.GetKeyProductsForOrderRequest{
		ProjectId: projectId,
		Ids:       productIds,
	}, &result)

	if err != nil {
		zap.L().Error(
			"v.GetKeyProductsForOrder Method failed",
			zap.Error(err),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			return nil, e
		}
		return nil, orderErrorUnknown
	}

	if result.Count != int64(len(productIds)) {
		return nil, orderErrorProductsInvalid
	}

	return result.Products, nil
}

func (s *Service) GetOrderKeyProductsAmount(products []*grpc.KeyProduct, group *billing.PriceGroup, platformId string) (float64, error) {
	if len(products) == 0 {
		return 0, orderErrorProductsEmpty
	}

	sum := float64(0)

	for _, p := range products {
		amount, err := p.GetPriceInCurrencyAndPlatform(group, platformId)

		if err != nil {
			return 0, orderErrorNoProductsCommonCurrency
		}

		sum += amount
	}

	return sum, nil
}

func (s *Service) GetOrderProducts(projectId string, productIds []string) ([]*grpc.Product, error) {
	if len(productIds) == 0 {
		return nil, orderErrorProductsEmpty
	}

	result := grpc.ListProductsResponse{}

	err := s.GetProductsForOrder(context.TODO(), &grpc.GetProductsForOrderRequest{
		ProjectId: projectId,
		Ids:       productIds,
	}, &result)

	if err != nil {
		zap.L().Error(
			"v.GetProductsForOrder Method failed",
			zap.Error(err),
		)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			return nil, e
		}
		return nil, orderErrorUnknown
	}

	if result.Total != int64(len(productIds)) {
		return nil, orderErrorProductsInvalid
	}

	return result.Products, nil
}

func (s *Service) GetOrderProductsAmount(products []*grpc.Product, group *billing.PriceGroup) (float64, error) {
	if len(products) == 0 {
		return 0, orderErrorProductsEmpty
	}

	sum := float64(0)

	for _, p := range products {
		amount, err := p.GetPriceInCurrency(group)

		if err != nil {
			return 0, err
		}

		sum += amount
	}

	totalAmount := tools.FormatAmount(sum)

	return totalAmount, nil
}

func (s *Service) GetOrderProductsItems(products []*grpc.Product, language string, group *billing.PriceGroup) ([]*billing.OrderItem, error) {
	var result []*billing.OrderItem

	if len(products) == 0 {
		return nil, orderErrorProductsEmpty
	}

	isDefaultLanguage := language == DefaultLanguage

	for _, p := range products {
		var (
			amount      float64
			name        string
			description string
			err         error
		)

		amount, err = p.GetPriceInCurrency(group)
		if err != nil {
			return nil, orderErrorProductsPrice
		}

		name, err = p.GetLocalizedName(language)
		if err != nil {
			if isDefaultLanguage {
				return nil, orderErrorNoNameInRequiredLanguage
			}
			name, err = p.GetLocalizedName(DefaultLanguage)
			if err != nil {
				return nil, orderErrorNoNameInDefaultLanguage
			}
		}

		description, err = p.GetLocalizedDescription(language)
		if err != nil {
			if isDefaultLanguage {
				return nil, orderErrorNoDescriptionInRequiredLanguage
			}
			description, err = p.GetLocalizedDescription(DefaultLanguage)
			if err != nil {
				return nil, orderErrorNoDescriptionInDefaultLanguage
			}
		}

		item := &billing.OrderItem{
			Id:          p.Id,
			Object:      p.Object,
			Sku:         p.Sku,
			Name:        name,
			Description: description,
			CreatedAt:   p.CreatedAt,
			UpdatedAt:   p.UpdatedAt,
			Images:      p.Images,
			Url:         p.Url,
			Metadata:    p.Metadata,
			Amount:      amount,
			Currency:    group.Currency,
		}
		result = append(result, item)
	}

	return result, nil
}

func (s *Service) GetOrderKeyProductsItems(products []*grpc.KeyProduct, language string, group *billing.PriceGroup, platformId string) ([]*billing.OrderItem, error) {
	var result []*billing.OrderItem

	if len(products) == 0 {
		return nil, orderErrorProductsEmpty
	}

	isDefaultLanguage := language == DefaultLanguage

	for _, p := range products {
		var (
			amount      float64
			name        string
			description string
			err         error
		)

		amount, err = p.GetPriceInCurrencyAndPlatform(group, platformId)
		if err != nil {
			return nil, orderErrorProductsPrice
		}

		name, err = p.GetLocalizedName(language)
		if err != nil {
			if isDefaultLanguage {
				return nil, orderErrorNoNameInRequiredLanguage
			}
			name, err = p.GetLocalizedName(DefaultLanguage)
			if err != nil {
				return nil, orderErrorNoNameInDefaultLanguage
			}
		}

		description, err = p.GetLocalizedDescription(language)
		if err != nil {
			if isDefaultLanguage {
				return nil, orderErrorNoDescriptionInRequiredLanguage
			}
			description, err = p.GetLocalizedDescription(DefaultLanguage)
			if err != nil {
				return nil, orderErrorNoDescriptionInDefaultLanguage
			}
		}

		item := &billing.OrderItem{
			Id:          p.Id,
			Object:      p.Object,
			Sku:         p.Sku,
			Name:        name,
			Description: description,
			CreatedAt:   p.CreatedAt,
			UpdatedAt:   p.UpdatedAt,
			Images:      []string{getImageByLanguage(DefaultLanguage, p.Cover)},
			Url:         p.Url,
			Metadata:    p.Metadata,
			Amount:      amount,
			Currency:    group.Currency,
			PlatformId:  platformId,
		}
		result = append(result, item)
	}

	return result, nil
}

func (s *Service) filterPlatforms(orderProducts []*grpc.KeyProduct) []string {
	// filter available platformIds for all products in request
	var platformIds []string
	for i, product := range orderProducts {
		var platformsToCheck []string
		for _, pl := range product.Platforms {
			platformsToCheck = append(platformsToCheck, pl.Id)
		}

		if i > 0 {
			platformIds = intersect(platformIds, platformsToCheck)
		} else {
			platformIds = platformsToCheck
		}
	}

	return platformIds
}

func (s *Service) ProcessOrderVirtualCurrency(ctx context.Context, order *billing.Order) error {
	var (
		country    string
		currency   string
		priceGroup *billing.PriceGroup
	)

	merchant, _ := s.merchant.GetById(ctx, order.Project.MerchantId)
	defaultCurrency := merchant.GetPayoutCurrency()

	if defaultCurrency == "" {
		zap.S().Infow("merchant payout currency not found", "order.Uuid", order.Uuid)
		return orderErrorNoProductsCommonCurrency
	}

	defaultPriceGroup, err := s.priceGroup.GetByRegion(ctx, defaultCurrency)
	if err != nil {
		zap.S().Errorw("Price group not found", "currency", currency)
		return orderErrorUnknown
	}

	currency = defaultCurrency
	priceGroup = defaultPriceGroup

	country = order.GetCountry()

	if country != "" {
		countryData, err := s.country.GetByIsoCodeA2(ctx, country)
		if err != nil {
			zap.S().Errorw("Country not found", "country", country)
			return orderErrorUnknown
		}

		priceGroup, err = s.priceGroup.GetById(ctx, countryData.PriceGroupId)
		if err != nil {
			zap.S().Errorw("Price group not found", "countryData", countryData)
			return orderErrorUnknown
		}

		currency = priceGroup.Currency
	}

	zap.S().Infow("try to use detected currency for order amount", "currency", currency, "order.Uuid", order.Uuid)

	project, err := s.project.GetById(ctx, order.GetProjectId())

	if project == nil || project.VirtualCurrency == nil {
		return orderErrorVirtualCurrencyNotFilled
	}

	amount, err := s.GetAmountForVirtualCurrency(order.VirtualCurrencyAmount, priceGroup, project.VirtualCurrency.Prices)
	if err != nil {
		if priceGroup.Id == defaultPriceGroup.Id {
			return err
		}

		// try to get order Amount in default currency, if it differs from requested one
		amount, err = s.GetAmountForVirtualCurrency(order.VirtualCurrencyAmount, defaultPriceGroup, project.VirtualCurrency.Prices)
		if err != nil {
			return err
		}
	}

	amount = s.FormatAmount(amount, currency)

	order.Currency = currency
	order.OrderAmount = amount
	order.TotalPaymentAmount = amount
	order.ChargeAmount = amount
	order.ChargeCurrency = currency

	return nil
}

func (s *Service) GetAmountForVirtualCurrency(virtualAmount float64, group *billing.PriceGroup, prices []*billing.ProductPrice) (float64, error) {
	for _, price := range prices {
		if price.Currency == group.Currency {
			return virtualAmount * price.Amount, nil
		}
	}

	return 0, virtualCurrencyPayoutCurrencyMissed
}

func (s *Service) ProcessOrderKeyProducts(ctx context.Context, order *billing.Order) ([]*grpc.Platform, error) {
	if order.ProductType != billing.OrderType_key {
		return nil, nil
	}

	priceGroup, err := s.getOrderPriceGroup(ctx, order)
	if err != nil {
		zap.L().Error(
			"ProcessOrderKeyProducts getOrderPriceGroup failed",
			zap.Error(err),
			zap.String("order.Uuid", order.Uuid),
		)
		return nil, err
	}

	locale := DefaultLanguage
	if order.User != nil && order.User.Locale != "" {
		locale = order.User.Locale
	}

	amount, priceGroup, items, platforms, err := s.processKeyProducts(
		ctx,
		order.Project.Id,
		order.Products,
		priceGroup,
		locale,
		order.PlatformId,
	)

	if err != nil {
		return nil, err
	}

	order.Currency = priceGroup.Currency
	order.OrderAmount = amount
	order.TotalPaymentAmount = amount

	order.ChargeAmount = order.TotalPaymentAmount
	order.ChargeCurrency = order.Currency

	order.Items = items

	return platforms, nil
}

func (s *Service) ProcessOrderProducts(ctx context.Context, order *billing.Order) error {

	if order.ProductType != billing.OrderType_product {
		return nil
	}
	priceGroup, err := s.getOrderPriceGroup(ctx, order)
	if err != nil {
		zap.L().Error(
			"ProcessOrderProducts getOrderPriceGroup failed",
			zap.Error(err),
			zap.String("order.Uuid", order.Uuid),
		)
		return err
	}

	locale := DefaultLanguage
	if order.User != nil && order.User.Locale != "" {
		locale = order.User.Locale
	}

	amount, priceGroup, items, _, err := s.processProducts(
		ctx,
		order.Project.Id,
		order.Products,
		priceGroup,
		locale,
	)

	if err != nil {
		return err
	}

	order.Currency = priceGroup.Currency

	order.OrderAmount = amount
	order.TotalPaymentAmount = amount

	order.ChargeAmount = order.TotalPaymentAmount
	order.ChargeCurrency = order.Currency

	order.Items = items

	return nil
}

func (s *Service) processAmountForFiatCurrency(
	_ context.Context,
	_ *billing.Project,
	orderProducts []*grpc.Product,
	priceGroup *billing.PriceGroup,
	defaultPriceGroup *billing.PriceGroup,
) (float64, *billing.PriceGroup, error) {
	// try to get order Amount in requested currency
	amount, err := s.GetOrderProductsAmount(orderProducts, priceGroup)
	if err != nil {
		if err != grpc.ProductNoPriceInCurrencyError {
			return 0, nil, err
		}

		if priceGroup.Id == defaultPriceGroup.Id {
			return 0, nil, err
		}

		// try to get order Amount in fallback currency
		amount, err = s.GetOrderProductsAmount(orderProducts, defaultPriceGroup)
		if err != nil {
			return 0, nil, err
		}
		return amount, defaultPriceGroup, nil
	}

	return amount, priceGroup, nil
}

func (s *Service) processAmountForVirtualCurrency(
	_ context.Context,
	project *billing.Project,
	orderProducts []*grpc.Product,
	priceGroup *billing.PriceGroup,
	defaultPriceGroup *billing.PriceGroup,
) (float64, *billing.PriceGroup, error) {

	if project.VirtualCurrency == nil || len(project.VirtualCurrency.Prices) == 0 {
		return 0, nil, orderErrorVirtualCurrencyNotFilled
	}

	var amount float64

	usedPriceGroup := priceGroup

	virtualAmount, err := s.GetOrderProductsAmount(orderProducts, &billing.PriceGroup{Currency: grpc.VirtualCurrencyPriceGroup})
	if err != nil {
		zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))
		return 0, nil, err
	}

	amount, err = s.GetAmountForVirtualCurrency(virtualAmount, usedPriceGroup, project.VirtualCurrency.Prices)
	if err != nil {
		zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))
		if priceGroup.Id == defaultPriceGroup.Id {
			return 0, nil, err
		}

		// try to get order Amount in fallback currency
		usedPriceGroup = defaultPriceGroup
		amount, err = s.GetAmountForVirtualCurrency(virtualAmount, usedPriceGroup, project.VirtualCurrency.Prices)
		if err != nil {
			zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))
			return 0, nil, err
		}
	}

	return amount, usedPriceGroup, nil
}

func (s *Service) notifyPaylinkError(ctx context.Context, paylinkId string, err error, req interface{}, order interface{}) {
	msg := map[string]interface{}{
		"event":     "error",
		"paylinkId": paylinkId,
		"message":   "Invalid paylink",
		"error":     err,
		"request":   req,
		"order":     order,
	}
	_ = s.centrifugoDashboard.Publish(ctx, centrifugoChannel, msg)
}

func (v *PaymentCreateProcessor) GetMerchantId() string {
	return v.checked.project.MerchantId
}

func (s *Service) processCustomerData(
	ctx context.Context,
	customerId string,
	order *billing.Order,
	req *grpc.PaymentFormJsonDataRequest,
	browserCustomer *BrowserCookieCustomer,
	locale string,
) (*billing.Customer, error) {
	customer, err := s.getCustomerById(ctx, customerId)

	if err != nil {
		return nil, err
	}

	tokenReq := &grpc.TokenRequest{
		User: &billing.TokenUser{
			Ip:             &billing.TokenUserIpValue{Value: req.Ip},
			Locale:         &billing.TokenUserLocaleValue{Value: locale},
			AcceptLanguage: req.Locale,
			UserAgent:      req.UserAgent,
		},
	}
	project := &billing.Project{
		Id:         order.Project.Id,
		MerchantId: order.Project.MerchantId,
	}

	browserCustomer.CustomerId = customer.Id
	_, err = s.updateCustomer(ctx, tokenReq, project, customer)

	return customer, err
}

func (s *Service) IsOrderCanBePaying(
	ctx context.Context,
	req *grpc.IsOrderCanBePayingRequest,
	rsp *grpc.IsOrderCanBePayingResponse,
) error {
	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)
	rsp.Status = pkg.ResponseStatusBadData

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Message = e
			return nil
		}
		return err
	}

	if order != nil && order.GetProjectId() != req.ProjectId {
		rsp.Message = orderErrorOrderCreatedAnotherProject
		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = order

	return nil
}

func (s *Service) fillPaymentDataCard(order *billing.Order) error {
	first6 := ""
	last4 := ""
	pan, ok := order.PaymentMethodTxnParams[pkg.PaymentCreateFieldPan]
	if !ok || pan == "" {
		pan, ok = order.PaymentRequisites["pan"]
		if !ok {
			pan = ""
		}
	}
	order.PaymentMethodPayerAccount = pan
	if len(pan) >= 6 {
		first6 = pan[0:6]
		last4 = pan[len(pan)-4:]
	}
	cardBrand, ok := order.PaymentRequisites["card_brand"]

	month, ok := order.PaymentRequisites["month"]
	if !ok {
		month = ""
	}
	year, ok := order.PaymentRequisites["year"]
	if !ok {
		year = ""
	}

	order.PaymentMethod.Card = &billing.PaymentMethodCard{
		Masked:      pan,
		First6:      first6,
		Last4:       last4,
		ExpiryMonth: month,
		ExpiryYear:  year,
		Brand:       cardBrand,
		Secure3D:    order.PaymentMethodTxnParams[pkg.TxnParamsFieldBankCardIs3DS] == "1",
	}
	b, err := json.Marshal(order.PaymentMethod.Card)
	if err != nil {
		return err
	}
	fp, err := bcrypt.GenerateFromPassword([]byte(string(b)), bcrypt.MinCost)
	if err == nil {
		order.PaymentMethod.Card.Fingerprint = string(fp)
	}
	return nil
}

func (s *Service) fillPaymentDataEwallet(order *billing.Order) error {
	account := order.PaymentMethodTxnParams[pkg.PaymentCreateFieldEWallet]
	order.PaymentMethodPayerAccount = account
	order.PaymentMethod.Wallet = &billing.PaymentMethodWallet{
		Brand:   order.PaymentMethod.Name,
		Account: account,
	}
	return nil
}

func (s *Service) fillPaymentDataCrypto(order *billing.Order) error {
	address := order.PaymentMethodTxnParams[pkg.PaymentCreateFieldCrypto]
	order.PaymentMethodPayerAccount = address
	order.PaymentMethod.CryptoCurrency = &billing.PaymentMethodCrypto{
		Brand:   order.PaymentMethod.Name,
		Address: address,
	}
	return nil
}

func (s *Service) SetUserNotifySales(
	ctx context.Context,
	req *grpc.SetUserNotifyRequest,
	_ *grpc.EmptyResponse,
) error {

	order, err := s.getOrderByUuid(ctx, req.OrderUuid)

	if err != nil {
		s.logError(orderErrorNotFound.Message, []interface{}{"error", err.Error(), "request", req})
		return orderErrorNotFound
	}

	if req.EnableNotification && req.Email == "" {
		return orderErrorEmailRequired
	}

	order.NotifySale = req.EnableNotification
	order.NotifySaleEmail = req.Email
	err = s.updateOrder(ctx, order)
	if err != nil {
		return err
	}

	if !req.EnableNotification {
		return nil
	}

	data := &grpc.NotifyUserSales{
		Email:   req.Email,
		OrderId: order.Id,
		Date:    time.Now().Format(time.RFC3339),
	}
	if order.User != nil {
		data.UserId = order.User.Id
	}
	_, err = s.db.Collection(collectionNotifySales).InsertOne(ctx, data)
	if err != nil {

		zap.S().Errorf(
			"Save email to collection failed",
			"error", err.Error(),
			"request", req,
			"collection", collectionNotifySales,
		)
		return err
	}

	if order.User.IsIdentified() == true {
		customer, err := s.getCustomerById(ctx, order.User.Id)
		if err != nil {
			return err
		}
		project, err := s.project.GetById(ctx, order.Project.Id)
		if err != nil {
			return err
		}

		customer.NotifySale = req.EnableNotification
		customer.NotifySaleEmail = req.Email

		tokenReq := s.transformOrderUser2TokenRequest(order.User)
		_, err = s.updateCustomer(ctx, tokenReq, project, customer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) SetUserNotifyNewRegion(
	ctx context.Context,
	req *grpc.SetUserNotifyRequest,
	_ *grpc.EmptyResponse,
) error {
	order, err := s.getOrderByUuid(ctx, req.OrderUuid)

	if err != nil {
		s.logError(orderErrorNotFound.Message, []interface{}{"error", err.Error(), "request", req})
		return orderErrorNotFound
	}

	if order.CountryRestriction.PaymentsAllowed {
		s.logError(orderErrorNotRestricted.Message, []interface{}{"request", req})
		return orderErrorNotRestricted
	}

	if req.EnableNotification && req.Email == "" {
		return orderErrorEmailRequired
	}

	if order.User == nil {
		order.User = &billing.OrderUser{}
	}
	order.User.NotifyNewRegion = req.EnableNotification
	order.User.NotifyNewRegionEmail = req.Email
	err = s.updateOrder(ctx, order)
	if err != nil {
		return err
	}

	if !(req.EnableNotification && order.CountryRestriction != nil) {
		return nil
	}

	data := &grpc.NotifyUserNewRegion{
		Email:            req.Email,
		OrderId:          order.Id,
		UserId:           order.User.Id,
		Date:             time.Now().Format(time.RFC3339),
		CountryIsoCodeA2: order.CountryRestriction.IsoCodeA2,
	}
	_, err = s.db.Collection(collectionNotifyNewRegion).InsertOne(ctx, data)
	if err != nil {
		zap.S().Errorf(
			"Save email to collection failed",
			"error", err.Error(),
			"request", req,
			"collection", collectionNotifyNewRegion,
		)
		return err
	}

	if order.User.IsIdentified() == true {
		customer, err := s.getCustomerById(ctx, order.User.Id)
		if err != nil {
			return err
		}
		project, err := s.project.GetById(ctx, order.Project.Id)
		if err != nil {
			return err
		}

		customer.NotifyNewRegion = req.EnableNotification
		customer.NotifyNewRegionEmail = req.Email

		tokenReq := s.transformOrderUser2TokenRequest(order.User)
		_, err = s.updateCustomer(ctx, tokenReq, project, customer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) applyCountryRestriction(
	ctx context.Context,
	order *billing.Order,
	countryCode string,
) (restricted bool, err error) {
	restricted = false
	if countryCode == "" {
		order.UserAddressDataRequired = true
		order.CountryRestriction = &billing.CountryRestriction{
			PaymentsAllowed: false,
			ChangeAllowed:   true,
		}
		return
	}

	country, err := s.country.GetByIsoCodeA2(ctx, countryCode)
	if err != nil {
		return
	}

	merchant, err := s.merchant.GetById(ctx, order.GetMerchantId())
	if err != nil {
		return
	}

	paymentsAllowed, changeAllowed := country.GetPaymentRestrictions(merchant.IsHighRisk())

	order.CountryRestriction = &billing.CountryRestriction{
		IsoCodeA2:       countryCode,
		PaymentsAllowed: paymentsAllowed,
		ChangeAllowed:   changeAllowed,
	}
	if paymentsAllowed {
		return
	}
	if changeAllowed {
		order.UserAddressDataRequired = true
		return
	}
	order.PrivateStatus = constant.OrderStatusPaymentSystemDeclined
	restricted = true
	err = s.updateOrder(ctx, order)
	if err != nil && err.Error() == orderErrorNotFound.Error() {
		err = nil
	}
	return
}

func (s *Service) PaymentFormPlatformChanged(ctx context.Context, req *grpc.PaymentFormUserChangePlatformRequest, rsp *grpc.PaymentFormDataChangeResponse) error {
	order, err := s.getOrderByUuidToForm(ctx, req.OrderId)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk

	order.PlatformId = req.Platform

	if order.ProductType == billing.OrderType_product {
		err = s.ProcessOrderProducts(ctx, order)
	} else if order.ProductType == billing.OrderType_key {
		_, err = s.ProcessOrderKeyProducts(ctx, order)
	}

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(ctx, pid, err, req, order)
		}
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	processor := &OrderCreateRequestProcessor{Service: s, ctx: ctx}
	err = processor.processOrderVat(order)
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error(), "method", "processOrderVat")
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.setOrderChargeAmountAndCurrency(ctx, order)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	err = s.updateOrder(ctx, order)

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusSystemError
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Item = order.GetPaymentFormDataChangeResult()

	return nil
}

func (s *Service) OrderReceipt(
	ctx context.Context,
	req *grpc.OrderReceiptRequest,
	rsp *grpc.OrderReceiptResponse,
) error {
	order, err := s.orderRepository.GetByUuid(ctx, req.OrderId)

	if err != nil {
		zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	if order.ReceiptId != req.ReceiptId {
		zap.L().Error(
			orderErrorReceiptNotEquals.Message,
			zap.String("Requested receipt", req.ReceiptId),
			zap.String("Order receipt", order.ReceiptId),
		)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorReceiptNotEquals

		return nil
	}

	receipt, err := s.getOrderReceiptObject(ctx, order)
	if err != nil {
		zap.L().Error("get order receipt object failed", zap.Error(err))
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Receipt = receipt

	return nil
}

func (s *Service) getOrderReceiptObject(ctx context.Context, order *billing.Order) (*billing.OrderReceipt, error) {
	merchant, err := s.merchant.GetById(ctx, order.GetMerchantId())

	if err != nil {
		zap.L().Error(orderErrorMerchantForOrderNotFound.Message, zap.Error(err))
		return nil, orderErrorMerchantForOrderNotFound
	}

	totalPrice, err := s.formatter.FormatCurrency(DefaultLanguage, order.OrderAmount, order.Currency)
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingCurrency.Message,
			zap.Float64("price", order.OrderAmount),
			zap.String("locale", DefaultLanguage),
			zap.String("currency", order.Currency),
		)
		return nil, orderErrorDuringFormattingCurrency
	}

	totalAmount, err := s.formatter.FormatCurrency(DefaultLanguage, order.TotalPaymentAmount, order.Currency)
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingCurrency.Message,
			zap.Float64("price", order.TotalPaymentAmount),
			zap.String("locale", DefaultLanguage),
			zap.String("currency", order.Currency),
		)
		return nil, orderErrorDuringFormattingCurrency
	}

	vatInOrderCurrency, err := s.formatter.FormatCurrency(DefaultLanguage, order.Tax.Amount, order.Tax.Currency)
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingCurrency.Message,
			zap.Float64("price", order.Tax.Amount),
			zap.String("locale", DefaultLanguage),
			zap.String("currency", order.Tax.Currency),
		)
		return nil, orderErrorDuringFormattingCurrency
	}

	vatInChargeCurrency, err := s.formatter.FormatCurrency(DefaultLanguage, order.GetTaxAmountInChargeCurrency(), order.ChargeCurrency)
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingCurrency.Message,
			zap.Float64("price", order.GetTaxAmountInChargeCurrency()),
			zap.String("locale", DefaultLanguage),
			zap.String("currency", order.ChargeCurrency),
		)
		return nil, orderErrorDuringFormattingCurrency
	}

	totalCharge, err := s.formatter.FormatCurrency(DefaultLanguage, order.ChargeAmount, order.ChargeCurrency)
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingCurrency.Message,
			zap.Float64("price", order.ChargeAmount),
			zap.String("locale", DefaultLanguage),
			zap.String("currency", order.ChargeCurrency),
		)
		return nil, orderErrorDuringFormattingCurrency
	}

	date, err := s.formatter.FormatDateTime(DefaultLanguage, time.Unix(order.CreatedAt.Seconds, 0))
	if err != nil {
		zap.L().Error(
			orderErrorDuringFormattingDate.Message,
			zap.Any("date", order.CreatedAt),
			zap.String("locale", DefaultLanguage),
		)
		return nil, orderErrorDuringFormattingDate
	}

	items := make([]*billing.OrderReceiptItem, len(order.Items))

	currency := order.Currency
	if order.IsBuyForVirtualCurrency {
		project, err := s.project.GetById(ctx, order.GetProjectId())

		if err != nil {
			zap.L().Error(
				projectErrorUnknown.Message,
				zap.Error(err),
				zap.String("order.uuid", order.Uuid),
			)
			return nil, projectErrorUnknown
		}

		var ok = false
		currency, ok = project.VirtualCurrency.Name[DefaultLanguage]

		if !ok {
			zap.L().Error(
				projectErrorVirtualCurrencyNameDefaultLangRequired.Message,
				zap.Error(err),
				zap.String("order.uuid", order.Uuid),
			)
			return nil, projectErrorVirtualCurrencyNameDefaultLangRequired
		}
	}

	for i, item := range order.Items {
		price, err := s.formatter.FormatCurrency(DefaultLanguage, item.Amount, currency)

		if err != nil {
			zap.L().Error(
				orderErrorDuringFormattingCurrency.Message,
				zap.Float64("price", item.Amount),
				zap.String("locale", DefaultLanguage),
				zap.String("currency", item.Currency),
			)
			return nil, orderErrorDuringFormattingCurrency
		}

		items[i] = &billing.OrderReceiptItem{Name: item.Name, Price: price}
	}

	var platformName = ""

	if platform, ok := availablePlatforms[order.PlatformId]; ok {
		platformName = platform.Name
	}

	oc, err := s.operatingCompany.GetById(ctx, order.OperatingCompanyId)

	if err != nil {
		zap.L().Error(pkg.MethodFinishedWithError, zap.Error(err))
		return nil, err
	}

	receipt := &billing.OrderReceipt{
		TotalPrice:          totalPrice,
		TransactionId:       order.Uuid,
		TransactionDate:     date,
		ProjectName:         order.Project.Name[DefaultLanguage],
		MerchantName:        merchant.Company.Name,
		Items:               items,
		OrderType:           order.Type,
		PlatformName:        platformName,
		PaymentPartner:      oc.Name,
		VatPayer:            order.VatPayer,
		VatInOrderCurrency:  vatInOrderCurrency,
		VatInChargeCurrency: vatInChargeCurrency,
		TotalAmount:         totalAmount,
		TotalCharge:         totalCharge,
		ReceiptId:           order.ReceiptId,
		Url:                 order.ReceiptUrl,
	}

	return receipt, nil
}

type OrderRepositoryInterface interface {
	GetByUuid(context.Context, string) (*billing.Order, error)
}

func newOrderRepository(svc *Service) OrderRepositoryInterface {
	s := &OrderRepository{svc: svc}
	return s
}

func (h *OrderRepository) GetByUuid(ctx context.Context, uuid string) (*billing.Order, error) {
	order := &billing.Order{}
	err := h.svc.db.Collection(collectionOrder).FindOne(ctx, bson.M{"uuid": uuid}).Decode(order)

	if err != nil {
		return nil, err
	}

	return order, nil
}

func (v *OrderCreateRequestProcessor) UserCountryExists() bool {
	return v.checked != nil && v.checked.user != nil && v.checked.user.Address != nil &&
		v.checked.user.Address.Country != ""
}

func intersect(a []string, b []string) []string {
	set := make([]string, 0)
	hash := make(map[string]bool)

	for _, v := range a {
		hash[v] = true
	}

	for _, v := range b {
		if _, found := hash[v]; found {
			set = append(set, v)
		}
	}

	return set
}

func (s *Service) hasPaymentCosts(ctx context.Context, order *billing.Order) bool {
	country, err := s.country.GetByIsoCodeA2(ctx, order.GetCountry())

	if err != nil {
		return false
	}

	methodName, err := order.GetCostPaymentMethodName()

	if err != nil {
		return false
	}

	_, err = s.paymentChannelCostSystem.Get(
		ctx,
		methodName,
		country.PayerTariffRegion,
		country.IsoCodeA2,
		order.MccCode,
		order.OperatingCompanyId,
	)

	if err != nil {
		return false
	}

	data := &billing.PaymentChannelCostMerchantRequest{
		MerchantId:     order.GetMerchantId(),
		Name:           methodName,
		PayoutCurrency: order.GetMerchantRoyaltyCurrency(),
		Amount:         order.ChargeAmount,
		Region:         country.PayerTariffRegion,
		Country:        country.IsoCodeA2,
		MccCode:        order.MccCode,
	}
	_, err = s.getPaymentChannelCostMerchant(ctx, data)
	return err == nil
}

func (s *Service) paymentSystemPaymentCallbackComplete(ctx context.Context, order *billing.Order) error {
	ch := s.cfg.GetCentrifugoOrderChannel(order.Uuid)
	message := map[string]string{
		pkg.PaymentCreateFieldOrderId: order.Uuid,
		"status":                      paymentSystemPaymentProcessingSuccessStatus,
	}

	return s.centrifugoPaymentForm.Publish(ctx, ch, message)
}

func (v *OrderCreateRequestProcessor) processVirtualCurrency() error {
	amount := v.request.Amount
	virtualCurrency := v.checked.project.VirtualCurrency

	if virtualCurrency == nil || len(virtualCurrency.Prices) <= 0 {
		return orderErrorVirtualCurrencyNotFilled
	}

	_, frac := math.Modf(amount)

	if virtualCurrency.SellCountType == pkg.ProjectSellCountTypeIntegral && frac > 0 {
		return orderErrorVirtualCurrencyFracNotSupported
	}

	if v.checked.amount < virtualCurrency.MinPurchaseValue ||
		(virtualCurrency.MaxPurchaseValue > 0 && amount > virtualCurrency.MaxPurchaseValue) {
		return orderErrorVirtualCurrencyLimits
	}

	v.checked.virtualAmount = amount
	return nil
}

func (s *Service) OrderReCreateProcess(
	ctx context.Context,
	req *grpc.OrderReCreateProcessRequest,
	res *grpc.OrderCreateProcessResponse,
) error {
	res.Status = pkg.ResponseStatusOk

	order, err := s.orderRepository.GetByUuid(ctx, req.OrderId)
	if err != nil {
		zap.S().Errorw(pkg.ErrorGrpcServiceCallFailed, "err", err.Error(), "data", req)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = orderErrorUnknown
		return nil
	}

	if !order.CanBeRecreated() {
		res.Status = pkg.ResponseStatusBadData
		res.Message = orderErrorWrongPrivateStatus
		return nil
	}

	newOrder := new(billing.Order)
	err = copier.Copy(&newOrder, &order)

	if err != nil {
		zap.S().Error(
			"Copy order to new structure order by refund failed",
			zap.Error(err),
			zap.Any("order", order),
		)

		res.Status = pkg.ResponseStatusSystemError
		res.Message = orderErrorUnknown

		return nil
	}

	newOrder.PrivateStatus = constant.OrderStatusNew
	newOrder.Status = constant.OrderPublicStatusCreated
	newOrder.Id = primitive.NewObjectID().Hex()
	newOrder.Uuid = uuid.New().String()
	newOrder.ReceiptId = uuid.New().String()
	newOrder.CreatedAt = ptypes.TimestampNow()
	newOrder.UpdatedAt = ptypes.TimestampNow()
	newOrder.Canceled = false
	newOrder.CanceledAt = nil
	newOrder.ReceiptUrl = ""
	newOrder.PaymentMethod = nil

	newOrder.User = &billing.OrderUser{
		Id:            order.User.Id,
		Phone:         order.User.Phone,
		PhoneVerified: order.User.PhoneVerified,
		Metadata:      order.Metadata,
		Object:        order.User.Object,
		Name:          order.User.Name,
		ExternalId:    order.User.ExternalId,
	}

	_, err = s.db.Collection(collectionOrder).InsertOne(ctx, newOrder)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrder),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = orderErrorCanNotCreate
		return nil
	}

	res.Item = newOrder

	return nil
}

func (s *Service) getAddressByIp(ip string) (order *billing.OrderBillingAddress, err error) {
	rsp, err := s.geo.GetIpData(context.TODO(), &geoip.GeoIpDataRequest{IP: ip})
	if err != nil {
		zap.L().Error(
			"GetIpData failed",
			zap.Error(err),
			zap.String("ip", ip),
		)

		return nil, orderErrorPayerRegionUnknown
	}

	address := &billing.OrderBillingAddress{
		Country: rsp.Country.IsoCode,
		City:    rsp.City.Names["en"],
	}

	if rsp.Postal != nil {
		address.PostalCode = rsp.Postal.Code
	}

	if len(rsp.Subdivisions) > 0 {
		address.State = rsp.Subdivisions[0].IsoCode
	}

	return address, nil
}

func (s *Service) getOrderPriceGroup(ctx context.Context, order *billing.Order) (priceGroup *billing.PriceGroup, err error) {
	if order.IsCurrencyPredefined {
		priceGroup, err = s.priceGroup.GetByRegion(ctx, order.Currency)
		return
	}

	merchant, err := s.merchant.GetById(ctx, order.GetMerchantId())
	if err != nil {
		return
	}

	defaultPriceGroup, err := s.priceGroup.GetByRegion(ctx, merchant.GetPayoutCurrency())

	countryCode := order.GetCountry()
	if countryCode == "" {
		return defaultPriceGroup, nil
	}

	country, err := s.country.GetByIsoCodeA2(ctx, countryCode)
	if err != nil {
		return defaultPriceGroup, nil
	}

	priceGroup, err = s.priceGroup.GetById(ctx, country.PriceGroupId)
	return
}

func (s *Service) setOrderChargeAmountAndCurrency(ctx context.Context, order *billing.Order) (err error) {
	order.ChargeAmount = order.TotalPaymentAmount
	order.ChargeCurrency = order.Currency

	if order.PaymentRequisites == nil {
		return nil
	}

	if order.PaymentMethod == nil {
		return nil
	}

	binCountryCode, ok := order.PaymentRequisites[pkg.PaymentCreateBankCardFieldIssuerCountryIsoCode]
	if !ok || binCountryCode == "" {
		return nil
	}

	binCardBrand, ok := order.PaymentRequisites[pkg.PaymentCreateBankCardFieldBrand]
	if !ok || binCardBrand == "" {
		return nil
	}

	if order.PaymentIpCountry != "" {
		order.IsIpCountryMismatchBin = order.PaymentIpCountry != binCountryCode
	}

	binCountry, err := s.country.GetByIsoCodeA2(ctx, binCountryCode)
	if err != nil {
		return err
	}
	if binCountry.Currency == order.Currency {
		return nil
	}

	sCurr, err := s.curService.GetPriceCurrencies(ctx, &currencies.EmptyRequest{})
	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetPriceCurrencies"),
			zap.Any(errorFieldEntrySource, order.Id),
		)
		return err
	}
	if !contains(sCurr.Currencies, binCountry.Currency) {
		return nil
	}

	// check that we have terminal in payment method for bin country currency
	project, err := s.project.GetById(ctx, order.Project.Id)
	if err != nil {
		return nil
	}

	pm, err := s.paymentMethod.GetById(ctx, order.PaymentMethod.Id)
	if err != nil {
		return nil
	}

	_, err = s.paymentMethod.GetPaymentSettings(pm, binCountry.Currency, order.MccCode, order.OperatingCompanyId, binCardBrand, project)
	if err != nil {
		return nil
	}

	reqCur := &currencies.ExchangeCurrencyCurrentCommonRequest{
		From:              order.Currency,
		To:                binCountry.Currency,
		RateType:          curPkg.RateTypePaysuper,
		Amount:            order.TotalPaymentAmount,
		ExchangeDirection: curPkg.ExchangeDirectionSell,
	}

	rspCur, err := s.curService.ExchangeCurrencyCurrentCommon(ctx, reqCur)

	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
			zap.Any(errorFieldRequest, reqCur),
			zap.Any(errorFieldEntrySource, order.Id),
		)

		return orderErrorConvertionCurrency
	}

	order.ChargeCurrency = binCountry.Currency
	order.ChargeAmount = s.FormatAmount(rspCur.ExchangedAmount, binCountry.Currency)

	return nil
}

func (s *Service) checkVirtualCurrencyProduct(products []*grpc.Product) bool {
	if len(products) == 0 {
		return false
	}

	for _, product := range products {
		if len(product.Prices) != 1 {
			return false
		}
		if product.Prices[0].IsVirtualCurrency == false {
			return false
		}
	}

	return true
}

func (s *Service) processProducts(
	ctx context.Context,
	projectId string,
	productIds []string,
	priceGroup *billing.PriceGroup,
	locale string,
) (amount float64, usedPriceGroup *billing.PriceGroup, items []*billing.OrderItem, isBuyForVirtualCurrency bool, err error) {
	project, err := s.project.GetById(ctx, projectId)
	if err != nil {
		return
	}
	if project.IsDeleted() == true {
		err = orderErrorProjectInactive
		return
	}

	orderProducts, err := s.GetOrderProducts(project.Id, productIds)
	if err != nil {
		return
	}

	merchant, err := s.merchant.GetById(ctx, project.MerchantId)
	if err != nil {
		return
	}

	defaultPriceGroup, err := s.priceGroup.GetByRegion(ctx, merchant.GetPayoutCurrency())
	if err != nil {
		return
	}

	if priceGroup == nil {
		priceGroup = defaultPriceGroup
	}

	isBuyForVirtualCurrency = s.checkVirtualCurrencyProduct(orderProducts)

	if isBuyForVirtualCurrency {
		amount, usedPriceGroup, err = s.processAmountForVirtualCurrency(ctx, project, orderProducts, priceGroup, defaultPriceGroup)
	} else {
		amount, usedPriceGroup, err = s.processAmountForFiatCurrency(ctx, project, orderProducts, priceGroup, defaultPriceGroup)
	}

	if err != nil {
		return
	}

	amount = s.FormatAmount(amount, usedPriceGroup.Currency)

	if isBuyForVirtualCurrency {
		items, err = s.GetOrderProductsItems(orderProducts, locale, &billing.PriceGroup{Currency: grpc.VirtualCurrencyPriceGroup})
	} else {
		items, err = s.GetOrderProductsItems(orderProducts, locale, usedPriceGroup)
	}

	return
}

func (s *Service) processKeyProducts(
	ctx context.Context,
	projectId string,
	productIds []string,
	priceGroup *billing.PriceGroup,
	locale string,
	platformId string,
) (amount float64, usedPriceGroup *billing.PriceGroup, items []*billing.OrderItem, platforms []*grpc.Platform, err error) {

	project, err := s.project.GetById(ctx, projectId)
	if err != nil {
		return
	}
	if project.IsDeleted() == true {
		err = orderErrorProjectInactive
		return
	}

	orderProducts, err := s.GetOrderKeyProducts(ctx, project.Id, productIds)
	if err != nil {
		return
	}

	platformIds := s.filterPlatforms(orderProducts)
	if len(platformIds) == 0 {
		zap.L().Error("No available platformIds")
		err = orderErrorNoPlatforms
		return
	}

	platforms = make([]*grpc.Platform, len(platformIds))
	for i, v := range platformIds {
		platforms[i] = availablePlatforms[v]
	}
	sort.Slice(platforms, func(i, j int) bool {
		return platforms[i].Order < platforms[j].Order
	})

	if platformId == "" {
		platformId = platforms[0].Id
	}

	merchant, err := s.merchant.GetById(ctx, project.MerchantId)
	if err != nil {
		return
	}

	defaultPriceGroup, err := s.priceGroup.GetByRegion(ctx, merchant.GetPayoutCurrency())
	if err != nil {
		return
	}

	if priceGroup == nil {
		priceGroup = defaultPriceGroup
	}

	usedPriceGroup = priceGroup

	amount, err = s.GetOrderKeyProductsAmount(orderProducts, priceGroup, platformId)
	if err != nil {
		if err != orderErrorNoProductsCommonCurrency {
			return
		} else {

			if priceGroup.Id == defaultPriceGroup.Id {
				return
			}

			usedPriceGroup = defaultPriceGroup

			// try to get order Amount in fallback currency
			amount, err = s.GetOrderKeyProductsAmount(orderProducts, defaultPriceGroup, platformId)
			if err != nil {
				return
			}
		}
	}

	amount = s.FormatAmount(amount, usedPriceGroup.Currency)

	items, err = s.GetOrderKeyProductsItems(orderProducts, locale, usedPriceGroup, platformId)

	return
}
