package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ProtocolONE/geoip-service/pkg/proto"
	"github.com/dgrijalva/jwt-go"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/entity"
	repo "github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"github.com/paysuper/paysuper-tax-service/proto"
	"github.com/streadway/amqp"
	"github.com/ttacon/libphonenumber"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"log"
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

	paymentCreateBankCardFieldBrand         = "card_brand"
	paymentCreateBankCardFieldType          = "card_type"
	paymentCreateBankCardFieldCategory      = "card_category"
	paymentCreateBankCardFieldIssuerName    = "bank_issuer_name"
	paymentCreateBankCardFieldIssuerCountry = "bank_issuer_country"

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
	orderErrorPaymentMethodNotAllowed                         = newBillingServerErrorMsg("fm000005", "payment method not available for project")
	orderErrorPaymentMethodNotFound                           = newBillingServerErrorMsg("fm000006", "payment method with specified identifier not found")
	orderErrorPaymentMethodInactive                           = newBillingServerErrorMsg("fm000007", "payment method with specified identifier is inactive")
	orderErrorCurrencyRateNotFound                            = newBillingServerErrorMsg("fm000008", "currency_rate not found")
	orderErrorPaymentMethodEmptySettings                      = newBillingServerErrorMsg("fm000009", "payment method setting for project is empty")
	orderErrorPaymentSystemInactive                           = newBillingServerErrorMsg("fm000010", "payment system for specified payment method is inactive")
	orderErrorPayerRegionUnknown                              = newBillingServerErrorMsg("fm000011", "payer region can't be found")
	orderErrorProjectOrderIdIsDuplicate                       = newBillingServerErrorMsg("fm000012", "request with specified project order identifier processed early")
	orderErrorDynamicNotifyUrlsNotAllowed                     = newBillingServerErrorMsg("fm000013", "dynamic verify url or notify url not allowed for project")
	orderErrorDynamicRedirectUrlsNotAllowed                   = newBillingServerErrorMsg("fm000014", "dynamic payer redirect urls not allowed for project")
	orderErrorCurrencyNotFound                                = newBillingServerErrorMsg("fm000015", "currency received from request not found")
	orderErrorAmountLowerThanMinAllowed                       = newBillingServerErrorMsg("fm000016", "order amount is lower than min allowed payment amount for project")
	orderErrorAmountGreaterThanMaxAllowed                     = newBillingServerErrorMsg("fm000017", "order amount is greater than max allowed payment amount for project")
	orderErrorAmountLowerThanMinAllowedPaymentMethod          = newBillingServerErrorMsg("fm000018", "order amount is lower than min allowed payment amount for payment method")
	orderErrorAmountGreaterThanMaxAllowedPaymentMethod        = newBillingServerErrorMsg("fm000019", "order amount is greater than max allowed payment amount for payment method")
	orderErrorCanNotCreate                                    = newBillingServerErrorMsg("fm000020", "order can't create. try request later")
	orderErrorNotFound                                        = newBillingServerErrorMsg("fm000021", "order with specified identifier not found")
	orderErrorOrderCreatedAnotherProject                      = newBillingServerErrorMsg("fm000022", "order created for another project")
	orderErrorFormInputTimeExpired                            = newBillingServerErrorMsg("fm000023", "time to enter date on payment form expired")
	orderErrorCurrencyIsRequired                              = newBillingServerErrorMsg("fm000024", "parameter currency in create order request is required")
	orderErrorUnknown                                         = newBillingServerErrorMsg("fm000025", "unknown error. try request later")
	orderCurrencyConvertationError                            = newBillingServerErrorMsg("fm000026", "error in process currency conversion. try request later")
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
	orderErrorCreatePaymentRequiredFieldPaymentMethodNotFound = newBillingServerErrorMsg("fm000043", "required field with payment method identifier not found")
	orderErrorCreatePaymentRequiredFieldEmailNotFound         = newBillingServerErrorMsg("fm000044", "required field \"email\" not found")
	orderErrorCreatePaymentRequiredFieldUserCountryNotFound   = newBillingServerErrorMsg("fm000045", "user country is required")
	orderErrorCreatePaymentRequiredFieldUserZipNotFound       = newBillingServerErrorMsg("fm000046", "user zip is required")
	orderErrorOrderAlreadyComplete                            = newBillingServerErrorMsg("fm000047", "order with specified identifier payed early")
	orderErrorSignatureInvalid                                = newBillingServerErrorMsg("fm000048", "request signature is invalid")
	orderErrorZipCodeNotFound                                 = newBillingServerErrorMsg("fm000050", "zip_code not found")
	orderErrorProductsPrice                                   = newBillingServerErrorMsg("fm000051", "can't get product price")
)

type orderCreateRequestProcessorChecked struct {
	id              string
	project         *billing.Project
	merchant        *billing.Merchant
	currency        *billing.Currency
	amount          float64
	paymentMethod   *billing.PaymentMethod
	products        []string
	items           []*billing.OrderItem
	metadata        map[string]string
	privateMetadata map[string]string
	user            *billing.OrderUser
}

type OrderCreateRequestProcessor struct {
	*Service
	checked *orderCreateRequestProcessorChecked
	request *billing.OrderCreateRequest
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
	Id                 bson.ObjectId `bson:"_id"`
	CardBin            int32         `bson:"card_bin"`
	CardBrand          string        `bson:"card_brand"`
	CardType           string        `bson:"card_type"`
	CardCategory       string        `bson:"card_category"`
	BankName           string        `bson:"bank_name"`
	BankCountryName    string        `bson:"bank_country_name"`
	BankCountryIsoCode string        `bson:"bank_country_code_a2"`
	BankSite           string        `bson:"bank_site"`
	BankPhone          string        `bson:"bank_phone"`
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
	}

	if req.Token != "" {
		err := processor.processCustomerToken()

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	} else {
		if req.ProjectId == "" || bson.IsObjectIdHex(req.ProjectId) == false {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorProjectIdIncorrect
			return nil
		}
	}

	if err := processor.processProject(); err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	if req.Signature != "" || processor.checked.project.SignatureRequired == true {
		if err := processor.processSignature(); err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if req.User != nil {
		err := processor.processUserData()

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if processor.checked.user != nil && processor.checked.user.Ip != "" {
		err := processor.processPayerIp()

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if processor.checked.project.IsProductsCheckout == true {
		if err := processor.processPaylinkProducts(); err != nil {
			if pid := req.PrivateMetadata["PaylinkId"]; pid != "" {
				s.notifyPaylinkError(pid, err, req, nil)
			}
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	} else {
		if req.Currency != "" {
			if err := processor.processCurrency(); err != nil {
				rsp.Status = pkg.ResponseStatusBadData
				rsp.Message = err.(*grpc.ResponseErrorMessage)
				return nil
			}
		}

		if req.Amount != 0 {
			processor.processAmount()
		}
	}

	if processor.checked.currency == nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorCurrencyIsRequired
		return nil
	}

	if req.OrderId != "" {
		if err := processor.processProjectOrderId(); err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if req.PaymentMethod != "" {
		pm, err := s.paymentMethod.GetByGroupAndCurrency(req.PaymentMethod, processor.checked.currency.CodeInt)
		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorPaymentMethodNotFound
			return nil
		}

		if err := processor.processPaymentMethod(pm); err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
	}

	if err := processor.processLimitAmounts(); err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	processor.processMetadata()
	processor.processPrivateMetadata()

	order, err := processor.prepareOrder()

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
	}

	err = s.db.Collection(collectionOrder).Insert(order)

	if err != nil {
		zap.S().Errorw(fmt.Sprintf(errorQueryMask, collectionOrder), "err", err, "inserted_data", order)
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
	order, err := s.getOrderByUuid(req.OrderId)

	if err != nil {
		return err
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
	}

	if req.Ip != "" {
		err = p1.processPayerIp()

		if err != nil {
			return err
		}
	}

	loc, ctr := s.getCountryFromAcceptLanguage(req.Locale)
	isIdentified := order.User.IsIdentified()
	browserCustomer := &BrowserCookieCustomer{
		Ip:             req.Ip,
		UserAgent:      req.UserAgent,
		AcceptLanguage: req.Locale,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if isIdentified == true {
		customer, err := s.processCustomerData(order.User.Id, order, req, browserCustomer, loc)

		if err == nil {
			browserCustomer.CustomerId = customer.Id
		}
	} else {
		if req.Cookie != "" {
			browserCustomer, err = s.decryptBrowserCookie(req.Cookie)

			if err == nil {
				isIdentified = true

				if (time.Now().Unix() - browserCustomer.UpdatedAt.Unix()) <= cookieCounterUpdateTime {
					browserCustomer.SessionCount++
				}

				if browserCustomer.CustomerId != "" {
					customer, err := s.processCustomerData(browserCustomer.CustomerId, order, req, browserCustomer, loc)

					if err != nil {
						zap.S().Errorf("Customer by identifier in browser cookie not processed", "err", err.Error())
					}

					if customer != nil {
						order.User.TechEmail = customer.TechEmail
					} else {
						order.User.Id = s.getTokenString(s.cfg.Length)
					}
				}
			}
		} else {
			order.User.Id = s.getTokenString(s.cfg.Length)
		}

		if order.User.TechEmail == "" {
			order.User.TechEmail = order.User.Id + pkg.TechEmailDomain
		}
	}

	if order.User.Ip == "" || req.Ip != order.User.Ip {
		order.User.Ip = p1.checked.user.Ip
		order.User.Address = &billing.OrderBillingAddress{
			Country:    p1.checked.user.Address.Country,
			City:       p1.checked.user.Address.City,
			PostalCode: p1.checked.user.Address.PostalCode,
			State:      p1.checked.user.Address.State,
		}
	}

	if (order.User.Address != nil && ctr != order.User.Address.Country) || loc != order.User.Locale {
		order.UserAddressDataRequired = true

		rsp.UserAddressDataRequired = order.UserAddressDataRequired
		rsp.UserIpData = &grpc.UserIpData{
			Country: order.User.Address.Country,
			City:    order.User.Address.City,
			Zip:     order.User.Address.PostalCode,
		}

		if loc != order.User.Locale {
			order.User.Locale = loc
		}
	}

	restricted, err := s.applyCountryRestriction(order, order.GetCountry())
	if err != nil {
		return err
	}
	if restricted {
		return orderCountryPaymentRestrictedError
	}

	err = s.ProcessOrderProducts(order)
	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(pid, err, req, order)
		}
		return err
	}

	p1.processOrderVat(order)
	err = s.updateOrder(order)

	if err != nil {
		return err
	}

	pms, err := p.processRenderFormPaymentMethods()

	if err != nil {
		return err
	}

	projectName, ok := order.Project.Name[order.User.Locale]

	if !ok {
		projectName = order.Project.Name[DefaultLanguage]
	}

	expire := time.Now().Add(time.Minute * 30).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": order.Uuid, "exp": expire})

	rsp.Id = order.Uuid
	rsp.Account = order.ProjectAccount
	rsp.Description = order.Description
	rsp.HasVat = order.Tax.Amount > 0
	rsp.Vat = order.Tax.Amount
	rsp.Currency = order.ProjectIncomeCurrency.CodeA3
	rsp.Project = &grpc.PaymentFormJsonDataProject{
		Name:       projectName,
		UrlSuccess: order.Project.UrlSuccess,
		UrlFail:    order.Project.UrlFail,
	}
	rsp.PaymentMethods = pms
	rsp.Token, _ = token.SignedString([]byte(s.cfg.CentrifugoSecret))
	rsp.InlineFormRedirectUrl = fmt.Sprintf(pkg.OrderInlineFormUrlMask, req.Scheme, req.Host, rsp.Id)
	rsp.Amount = order.PaymentMethodOutcomeAmount
	rsp.TotalAmount = order.TotalPaymentAmount
	rsp.Items = order.Items
	rsp.Email = order.User.Email

	if order.CountryRestriction != nil {
		rsp.CountryPaymentsAllowed = order.CountryRestriction.PaymentsAllowed
		rsp.CountryChangeAllowed = order.CountryRestriction.ChangeAllowed
	} else {
		rsp.CountryPaymentsAllowed = true
		rsp.CountryChangeAllowed = true
	}

	cookie, err := s.generateBrowserCookie(browserCustomer)

	if err == nil {
		rsp.Cookie = cookie
	}

	return nil
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
	err := processor.processPaymentFormData()
	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	order := processor.checked.order

	if !order.CountryRestriction.PaymentsAllowed {
		rsp.Message = orderCountryPaymentRestrictedError
		rsp.Status = pkg.ResponseStatusForbidden
		return nil
	}

	err = s.ProcessOrderProducts(order)

	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(pid, err, req, order)
		}

		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	merchant, err := s.merchant.GetById(processor.GetMerchantId())
	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	settings, err := s.paymentMethod.GetPaymentSettings(processor.checked.paymentMethod, merchant, processor.checked.project)
	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	ps, err := s.paymentSystem.GetById(processor.checked.paymentMethod.PaymentSystemId)
	if err != nil {
		rsp.Message = orderErrorPaymentSystemInactive
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	order.PaymentMethod = &billing.PaymentMethodOrder{
		Id:              processor.checked.paymentMethod.Id,
		Name:            processor.checked.paymentMethod.Name,
		Params:          settings,
		PaymentSystemId: ps.Id,
		Group:           processor.checked.paymentMethod.Group,
		ExternalId:      processor.checked.paymentMethod.ExternalId,
	}
	order.PaymentMethod.Params.TerminalId = settings.TerminalId
	order.PaymentMethod.Params.SecretCallback = settings.SecretCallback
	order.PaymentMethod.Params.Secret = settings.Secret

	commissionProcessor := &OrderCreateRequestProcessor{Service: s}
	err = commissionProcessor.processOrderCommissions(order)

	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	err = processor.processPaymentAmounts()

	if err != nil {
		rsp.Message = orderCurrencyConvertationError
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	if _, ok := order.PaymentRequisites[pkg.PaymentCreateFieldRecurringId]; ok {
		req.Data[pkg.PaymentCreateFieldRecurringId] = order.PaymentRequisites[pkg.PaymentCreateFieldRecurringId]
		delete(order.PaymentRequisites, pkg.PaymentCreateFieldRecurringId)
	}

	err = s.updateOrder(order)

	if err != nil {
		rsp.Message = orderErrorUnknown
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	h, err := s.NewPaymentSystem(s.cfg.PaymentSystemConfig, order)

	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	url, err := h.CreatePayment(req.Data)
	if err != nil {
		s.logError("Order create in payment system failed", []interface{}{"err", err.Error(), "order", order})

		rsp.Message = orderErrorUnknown
		rsp.Status = pkg.ResponseStatusBadData

		return nil
	}

	err = s.updateOrder(order)
	if err != nil {
		zap.S().Errorf("Order create in payment system failed", "err", err.Error(), "order", order)

		rsp.Message = err.(*grpc.ResponseErrorMessage)
		rsp.Status = pkg.ResponseStatusSystemError

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.RedirectUrl = url
	rsp.NeedRedirect = true

	if _, ok := req.Data[pkg.PaymentCreateFieldRecurringId]; ok && url == "" {
		rsp.NeedRedirect = false
	}

	return nil
}

func (s *Service) PaymentCallbackProcess(
	ctx context.Context,
	req *grpc.PaymentNotifyRequest,
	rsp *grpc.PaymentNotifyResponse,
) error {
	order, err := s.getOrderById(req.OrderId)

	if err != nil {
		return orderErrorNotFound
	}

	var data protobuf.Message

	ps, err := s.paymentSystem.GetById(order.PaymentMethod.PaymentSystemId)
	if err != nil {
		return orderErrorPaymentSystemInactive
	}

	switch ps.Handler {
	case pkg.PaymentSystemHandlerCardPay:
		data = &billing.CardPayPaymentCallback{}
		err := json.Unmarshal(req.Request, data)

		if err != nil {
			return errors.New(paymentRequestIncorrect)
		}
		break
	default:
		return orderErrorPaymentMethodNotFound
	}

	h, err := s.NewPaymentSystem(s.cfg.PaymentSystemConfig, order)

	if err != nil {
		return err
	}

	pErr := h.ProcessPayment(data, string(req.Request), req.Signature)

	if pErr != nil {
		s.logError(
			"Callback processing failed",
			[]interface{}{
				"err", pErr.Error(),
				"order_id", req.OrderId,
				"request", string(req.Request),
				"signature", req.Signature,
			},
		)

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

	err = s.updateOrder(order)

	if err != nil {
		rsp.Error = err.(*grpc.ResponseErrorMessage).Message
		rsp.Status = pkg.StatusErrorSystem

		return nil
	}

	if pErr == nil {
		if h.IsRecurringCallback(data) {
			s.saveRecurringCard(order, h.GetRecurringId(data))
		}

		err = s.broker.Publish(constant.PayOneTopicNotifyPaymentName, order, amqp.Table{"x-retry-count": int32(0)})

		if err != nil {
			zap.S().Errorf("Publish notify message to queue failed", "err", err.Error(), "order", order)
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
	order, err := s.getOrderByUuidToForm(req.OrderId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.PaymentFormDataChangeResponseItem{
		UserAddressDataRequired: false,
	}

	if order.User.Locale == req.Lang {
		return nil
	}

	if order.User.IsIdentified() == true {
		s.updateCustomerFromRequestLocale(order, req.Ip, req.AcceptLanguage, req.UserAgent, req.Lang)
	}

	order.User.Locale = req.Lang
	order.UserAddressDataRequired = true

	err = s.ProcessOrderProducts(order)
	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(pid, err, req, order)
		}
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	err = s.updateOrder(order)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Item.UserAddressDataRequired = true
	rsp.Item.UserIpData = &grpc.UserIpData{
		Country: order.User.Address.Country,
		City:    order.User.Address.City,
		Zip:     order.User.Address.PostalCode,
	}

	return nil
}

func (s *Service) PaymentFormPaymentAccountChanged(
	ctx context.Context,
	req *grpc.PaymentFormUserChangePaymentAccountRequest,
	rsp *grpc.PaymentFormDataChangeResponse,
) error {
	order, err := s.getOrderByUuidToForm(req.OrderId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	pm, err := s.paymentMethod.GetById(req.MethodId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentMethodNotFound

		return nil
	}

	regex := pm.AccountRegexp

	if pm.ExternalId == constant.PaymentSystemGroupAliasBankCard {
		regex = "^\\d{6,18}$"
	}

	match, err := regexp.MatchString(regex, req.Account)

	if match == false || err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorPaymentAccountIncorrect

		return nil
	}

	brand := ""
	country := ""

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.PaymentFormDataChangeResponseItem{}

	switch pm.ExternalId {
	case constant.PaymentSystemGroupAliasBankCard:
		data := s.getBinData(req.Account)

		if data == nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCountryByPaymentAccountNotFound

			return nil
		}

		brand = data.CardBrand
		country = data.BankCountryIsoCode
		break
	case constant.PaymentSystemGroupAliasQiwi:
		req.Account = "+" + req.Account
		num, err := libphonenumber.Parse(req.Account, CountryCodeUSA)

		if err != nil || num.CountryCode == nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorPaymentAccountIncorrect
			return nil
		}

		ok := false
		country, ok = pkg.CountryPhoneCodes[*num.CountryCode]

		if !ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCountryByPaymentAccountNotFound

			return nil
		}

		break
	default:
		return nil
	}

	if order.User.Address.Country == country {
		return nil
	}

	order.User.Address.Country = country
	order.UserAddressDataRequired = true

	err = s.updateOrder(order)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	restricted, err := s.applyCountryRestriction(order, country)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = orderErrorUnknown

		return nil
	}

	if restricted == true {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = orderCountryPaymentRestrictedError

		return nil
	}

	rsp.Item.UserAddressDataRequired = true
	rsp.Item.UserIpData = &grpc.UserIpData{
		Country: order.User.Address.Country,
		City:    order.User.Address.City,
		Zip:     order.User.Address.PostalCode,
	}
	rsp.Item.Brand = brand

	if order.CountryRestriction != nil {
		rsp.Item.CountryPaymentsAllowed = order.CountryRestriction.PaymentsAllowed
		rsp.Item.CountryChangeAllowed = order.CountryRestriction.ChangeAllowed
	} else {
		rsp.Item.CountryPaymentsAllowed = true
		rsp.Item.CountryChangeAllowed = true
	}

	return nil
}

func (s *Service) ProcessBillingAddress(
	ctx context.Context,
	req *grpc.ProcessBillingAddressRequest,
	rsp *grpc.ProcessBillingAddressResponse,
) error {
	var err error
	var zip *billing.ZipCode

	if req.Country == CountryCodeUSA {
		if req.Zip == "" {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorCreatePaymentRequiredFieldUserZipNotFound

			return nil
		}

		zip, err = s.zipCode.getByZipAndCountry(req.Zip, req.Country)

		if err != nil {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = orderErrorZipCodeNotFound

			return nil
		}
	}

	order, err := s.getOrderByUuidToForm(req.OrderId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	order.BillingAddress = &billing.OrderBillingAddress{
		Country: req.Country,
	}

	if zip != nil {
		log.Println(zip)
		order.BillingAddress.PostalCode = zip.Zip
		order.BillingAddress.City = zip.City
		order.BillingAddress.State = zip.State.Code
	}

	restricted, err := s.applyCountryRestriction(order, req.Country)
	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = orderErrorUnknown
		return nil
	}
	if restricted {
		rsp.Status = pkg.ResponseStatusForbidden
		rsp.Message = orderCountryPaymentRestrictedError
		return nil
	}

	err = s.ProcessOrderProducts(order)
	if err != nil {
		if pid := order.PrivateMetadata["PaylinkId"]; pid != "" {
			s.notifyPaylinkError(pid, err, req, order)
		}
		return err
	}

	processor := &OrderCreateRequestProcessor{Service: s}
	processor.processOrderVat(order)

	err = s.updateOrder(order)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = &grpc.ProcessBillingAddressResponseItem{
		HasVat:      order.Tax.Amount > 0,
		Vat:         order.Tax.Amount,
		Amount:      tools.FormatAmount(order.PaymentMethodOutcomeAmount),
		TotalAmount: tools.FormatAmount(order.TotalPaymentAmount),
	}

	return nil
}

func (s *Service) saveRecurringCard(order *billing.Order, recurringId string) {
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

	_, err := s.rep.InsertSavedCard(context.TODO(), req)

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
		err = s.updateOrder(order)
		if err != nil {
			zap.S().Errorf("Failed to update order after save recurruing card", "err", err.Error())
		}
	}
}

func (s *Service) updateOrder(order *billing.Order) error {

	ps := order.GetPublicStatus()

	zap.S().Debug("[updateOrder] updating order", "order_id", order.Id, "status", ps)

	originalOrder, _ := s.getOrderById(order.Id)

	statusChanged := false
	if originalOrder != nil {
		ops := originalOrder.GetPublicStatus()
		zap.S().Debug("[updateOrder] no original order status", "order_id", order.Id, "status", ops)
		statusChanged = ops != ps
	} else {
		zap.S().Debug("[updateOrder] no original order found", "order_id", order.Id)
	}

	err := s.db.Collection(collectionOrder).UpdateId(bson.ObjectIdHex(order.Id), order)

	if err != nil {
		s.logError(orderErrorUpdateOrderDataFailed, []interface{}{"error", err.Error(), "order", order})
		if err == mgo.ErrNotFound {
			return orderErrorNotFound
		}
		return orderErrorUnknown
	}

	zap.S().Debug("[updateOrder] updating order success", "order_id", order.Id, "status_changed", statusChanged)

	if statusChanged && ps != constant.OrderPublicStatusCreated && ps != constant.OrderPublicStatusPending {
		zap.S().Debug("[updateOrder] notify merchant", "order_id", order.Id)
		s.orderNotifyMerchant(order)
	}

	return nil
}

func (s *Service) orderNotifyMerchant(order *billing.Order) {
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
	err = s.db.Collection(collectionOrder).UpdateId(bson.ObjectIdHex(order.Id), order)
	if err != nil {
		zap.S().Debug("[orderNotifyMerchant] notification status update failed", "order_id", order.Id)
		s.logError(orderErrorUpdateOrderDataFailed, []interface{}{"error", err.Error(), "order", order})
	} else {
		zap.S().Debug("[orderNotifyMerchant] notification status updated succesfully", "order_id", order.Id)
	}
}

func (s *Service) getOrderById(id string) (order *billing.Order, err error) {
	err = s.db.Collection(collectionOrder).FindId(bson.ObjectIdHex(id)).One(&order)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Errorf("Order not found in payment create process", "err", err.Error(), "order_id", id)
	}

	if order == nil {
		return order, orderErrorNotFound
	}

	return
}

func (s *Service) getOrderByUuid(uuid string) (order *billing.Order, err error) {
	err = s.db.Collection(collectionOrder).Find(bson.M{"uuid": uuid}).One(&order)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Errorf("Order not found in payment create process", "err", err.Error(), "uuid", uuid)
	}

	if order == nil {
		return order, orderErrorNotFound
	}

	return
}

func (s *Service) getOrderByUuidToForm(uuid string) (*billing.Order, error) {
	order, err := s.getOrderByUuid(uuid)

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

func (s *Service) getBinData(pan string) (data *BinData) {
	if len(pan) < 6 {
		zap.S().Errorf("Incorrect PAN to get BIN data", "pan", pan)
		return
	}

	i, err := strconv.ParseInt(pan[:6], 10, 32)

	if err != nil {
		zap.S().Errorf("Parse PAN to int failed", "error", err.Error(), "pan", pan)
		return
	}

	err = s.db.Collection(collectionBinData).Find(bson.M{"card_bin": int32(i)}).One(&data)

	if err != nil {
		zap.S().Errorf("Query to get bank card BIN data failed", "error", err.Error(), "pan", pan)
		return
	}

	return
}

func (v *OrderCreateRequestProcessor) prepareOrder() (*billing.Order, error) {
	id := bson.NewObjectId().Hex()
	amount := tools.FormatAmount(v.checked.amount)
	merAccAmount := amount
	merchantPayoutCurrency := v.checked.merchant.GetPayoutCurrency()

	if (v.request.UrlVerify != "" || v.request.UrlNotify != "") && v.checked.project.AllowDynamicNotifyUrls == false {
		return nil, orderErrorDynamicNotifyUrlsNotAllowed
	}

	if (v.request.UrlSuccess != "" || v.request.UrlFail != "") && v.checked.project.AllowDynamicRedirectUrls == false {
		return nil, orderErrorDynamicRedirectUrlsNotAllowed
	}

	if merchantPayoutCurrency != nil && v.checked.currency.CodeInt != merchantPayoutCurrency.CodeInt {
		amnt, err := v.currencyRate.Convert(v.checked.currency.CodeInt, merchantPayoutCurrency.CodeInt, amount)

		if err != nil {
			return nil, err
		}

		merAccAmount = amnt
	}

	order := &billing.Order{
		Id: id,
		Project: &billing.ProjectOrder{
			Id:                   v.checked.project.Id,
			Name:                 v.checked.project.Name,
			UrlSuccess:           v.checked.project.UrlRedirectSuccess,
			UrlFail:              v.checked.project.UrlRedirectFail,
			SendNotifyEmail:      v.checked.project.SendNotifyEmail,
			NotifyEmails:         v.checked.project.NotifyEmails,
			SecretKey:            v.checked.project.SecretKey,
			UrlCheckAccount:      v.checked.project.UrlCheckAccount,
			UrlProcessPayment:    v.checked.project.UrlProcessPayment,
			UrlChargebackPayment: v.checked.project.UrlChargebackPayment,
			UrlCancelPayment:     v.checked.project.UrlCancelPayment,
			UrlRefundPayment:     v.checked.project.UrlRefundPayment,
			UrlFraudPayment:      v.checked.project.UrlFraudPayment,
			CallbackProtocol:     v.checked.project.CallbackProtocol,
			MerchantId:           v.checked.merchant.Id,
			Status:               v.checked.project.Status,
		},
		Description:                        fmt.Sprintf(orderDefaultDescription, id),
		ProjectOrderId:                     v.request.OrderId,
		ProjectAccount:                     v.request.Account,
		ProjectIncomeAmount:                amount,
		ProjectIncomeCurrency:              v.checked.currency,
		ProjectOutcomeAmount:               amount,
		ProjectOutcomeCurrency:             v.checked.currency,
		ProjectParams:                      v.request.Other,
		PrivateStatus:                      constant.OrderStatusNew,
		CreatedAt:                          ptypes.TimestampNow(),
		IsJsonRequest:                      v.request.IsJson,
		AmountInMerchantAccountingCurrency: merAccAmount,
		PaymentMethodOutcomeAmount:         amount,
		PaymentMethodOutcomeCurrency:       v.checked.currency,
		PaymentMethodIncomeAmount:          amount,
		PaymentMethodIncomeCurrency:        v.checked.currency,

		Uuid:            uuid.New().String(),
		User:            v.checked.user,
		OrderAmount:     amount,
		Currency:        v.checked.currency.CodeA3,
		Products:        v.checked.products,
		Items:           v.checked.items,
		Metadata:        v.checked.metadata,
		PrivateMetadata: v.checked.privateMetadata,
		Issuer: &billing.OrderIssuer{
			Url:      v.request.IssuerUrl,
			Embedded: v.request.IsEmbedded,
		},
		CountryRestriction: &billing.CountryRestriction{
			IsoCodeA2:       "",
			PaymentsAllowed: true,
			ChangeAllowed:   true,
		},
	}

	if order.User == nil {
		order.User = &billing.OrderUser{
			Object: pkg.ObjectTypeUser,
		}
	} else {
		if order.User.Address != nil {
			v.processOrderVat(order)

			restricted, err := v.applyCountryRestriction(order, order.GetCountry())
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
		ps, err := v.paymentSystem.GetById(v.checked.paymentMethod.PaymentSystemId)
		if err != nil {
			return nil, err
		}

		settings, err := v.paymentMethod.GetPaymentSettings(v.checked.paymentMethod, v.checked.merchant, v.checked.project)
		if err != nil {
			return nil, err
		}

		order.PaymentMethod = &billing.PaymentMethodOrder{
			Id:              v.checked.paymentMethod.Id,
			Name:            v.checked.paymentMethod.Name,
			Params:          settings,
			PaymentSystemId: ps.Id,
			Group:           v.checked.paymentMethod.Group,
		}

		if err := v.processOrderCommissions(order); err != nil {
			return nil, err
		}
	}

	order.ExpireDateToFormInput, _ = ptypes.TimestampProto(time.Now().Add(time.Minute * defaultExpireDateToFormInput))

	return order, nil
}

func (v *OrderCreateRequestProcessor) processProject() error {
	project, err := v.project.GetById(v.request.ProjectId)

	if err != nil {
		zap.S().Errorw("Order create get project error", "err", err, "request", v.request)
		return orderErrorProjectNotFound
	}

	if project.IsDeleted() == true {
		return orderErrorProjectInactive
	}

	merchant, err := v.merchant.GetById(project.MerchantId)
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

func (v *OrderCreateRequestProcessor) processCurrency() error {
	currency, err := v.currency.GetByCodeA3(v.request.Currency)

	if err != nil {
		zap.S().Errorw("Order create get currency error", "err", err, "request", v.request)
		return orderErrorCurrencyNotFound
	}

	v.checked.currency = currency

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

func (v *OrderCreateRequestProcessor) processPayerIp() error {
	rsp, err := v.geo.GetIpData(context.TODO(), &proto.GeoIpDataRequest{IP: v.checked.user.Ip})

	if err != nil {
		zap.S().Errorw("Order create get payer data error", "err", err, "ip", v.checked.user.Ip)
		return orderErrorPayerRegionUnknown
	}

	if v.checked.user.Address == nil {
		v.checked.user.Address = &billing.OrderBillingAddress{}
	}

	if v.checked.user.Address.Country == "" {
		v.checked.user.Address.Country = rsp.Country.IsoCode
	}

	if v.checked.user.Address.City == "" {
		v.checked.user.Address.City = rsp.City.Names["en"]
	}

	if v.checked.user.Address.PostalCode == "" && rsp.Postal != nil {
		v.checked.user.Address.PostalCode = rsp.Postal.Code
	}

	if v.checked.user.Address.State == "" && len(rsp.Subdivisions) > 0 {
		v.checked.user.Address.State = rsp.Subdivisions[0].IsoCode
	}

	return nil
}

func (v *OrderCreateRequestProcessor) processPaylinkProducts() error {
	if len(v.request.Products) == 0 {
		return nil
	}

	orderProducts, err := v.GetOrderProducts(v.checked.project.Id, v.request.Products)
	if err != nil {
		return err
	}

	pid := v.request.PrivateMetadata["PaylinkId"]

	logInfo := "[processPaylinkProducts] %s"

	currency := v.accountingCurrency
	zap.S().Infow(fmt.Sprintf(logInfo, "accountingCurrency"), "currency", currency.CodeA3, "paylink", pid)

	merchantPayoutCurrency := v.checked.merchant.GetPayoutCurrency()

	if merchantPayoutCurrency != nil {
		currency = merchantPayoutCurrency
		zap.S().Infow(fmt.Sprintf(logInfo, "merchant payout currency"), "currency", currency.CodeA3, "paylink", pid)
	} else {
		zap.S().Infow(fmt.Sprintf(logInfo, "no merchant payout currency set"), "paylink", pid)
	}

	zap.S().Infow(fmt.Sprintf(logInfo, "use currency"), "currency", currency.CodeA3, "paylink", pid)

	amount, err := v.GetOrderProductsAmount(orderProducts, currency.CodeA3)
	if err != nil {
		return err
	}

	items, err := v.GetOrderProductsItems(orderProducts, DefaultLanguage, currency.CodeA3)
	if err != nil {
		return err
	}

	v.checked.products = v.request.Products
	v.checked.currency = currency
	v.checked.amount = amount
	v.checked.items = items

	return nil
}

func (v *OrderCreateRequestProcessor) processProjectOrderId() error {
	var order *billing.Order

	filter := bson.M{
		"project._id":      bson.ObjectIdHex(v.checked.project.Id),
		"project_order_id": v.request.OrderId,
	}

	err := v.db.Collection(collectionOrder).Find(filter).One(&order)

	if err != nil && err != mgo.ErrNotFound {
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

	if _, err := v.paymentSystem.GetById(pm.PaymentSystemId); err != nil {
		return orderErrorPaymentSystemInactive
	}

	if _, err := v.paymentMethod.GetPaymentSettings(pm, v.checked.merchant, v.checked.project); err != nil {
		return orderErrorPaymentMethodEmptySettings
	}

	v.checked.paymentMethod = pm

	return nil
}

func (v *OrderCreateRequestProcessor) processLimitAmounts() (err error) {
	amount := v.checked.amount

	if v.checked.project.LimitsCurrency != "" && v.checked.project.LimitsCurrency != v.checked.currency.CodeA3 {
		currency, err := v.currency.GetByCodeA3(v.checked.project.LimitsCurrency)

		if err != nil {
			return err
		}

		amount, err = v.currencyRate.Convert(v.checked.currency.CodeInt, currency.CodeInt, amount)

		if err != nil {
			return err
		}
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
func (v *OrderCreateRequestProcessor) processOrderVat(order *billing.Order) {
	order.TotalPaymentAmount = order.PaymentMethodOutcomeAmount

	order.Tax = &billing.OrderTax{
		Type:     taxTypeVat,
		Currency: order.PaymentMethodOutcomeCurrency.CodeA3,
	}
	req := &tax_service.GetRateRequest{
		IpData: &tax_service.GeoIdentity{
			Country: order.User.Address.Country,
			City:    order.User.Address.City,
		},
		UserData: &tax_service.GeoIdentity{},
	}

	if order.BillingAddress != nil {
		req.UserData.Country = order.BillingAddress.Country
		req.UserData.City = order.BillingAddress.City
		req.UserData.State = order.BillingAddress.State
	}

	if order.User.Address.Country == CountryCodeUSA {
		order.Tax.Type = taxTypeSalesTax

		req.IpData.Zip = order.User.Address.PostalCode
		req.IpData.State = order.User.Address.State

		if order.BillingAddress != nil {
			req.UserData.Zip = order.BillingAddress.PostalCode
		}
	}

	rsp, err := v.tax.GetRate(context.TODO(), req)

	if err != nil {
		v.logError("Tax service return error", []interface{}{"error", err.Error(), "request", req})
		return
	}

	if order.BillingAddress != nil {
		req.UserData.State = rsp.Rate.State
	}

	order.Tax.Rate = tools.FormatAmount(float64(rsp.Rate.Rate))
	order.Tax.Amount = tools.FormatAmount(order.PaymentMethodOutcomeAmount * float64(rsp.Rate.Rate))
	order.TotalPaymentAmount = tools.FormatAmount(order.TotalPaymentAmount + float64(order.Tax.Amount))

	return
}

// Calculate all possible commissions for order, i.e. payment system fee amount, PSP (P1) fee amount,
// commission shifted from project to user and VAT
func (v *OrderCreateRequestProcessor) processOrderCommissions(o *billing.Order) error {
	merchant, _ := v.merchant.GetById(o.Project.MerchantId)

	mAccCur := merchant.GetPayoutCurrency()
	pmOutCur := o.PaymentMethodOutcomeCurrency.CodeInt
	amount := float64(0)

	// calculate commissions to selected payment method
	commission, err := v.Service.commission.CalculatePmCommission(o.Project.Id, o.PaymentMethod.Id, o.PaymentMethodOutcomeAmount)

	if err != nil {
		return orderErrorUnknown
	}

	// save information about payment system commission
	o.PaymentSystemFeeAmount = &billing.OrderFeePaymentSystem{
		AmountPaymentMethodCurrency: tools.FormatAmount(commission),
	}

	ps, err := v.paymentSystem.GetById(o.PaymentMethod.PaymentSystemId)
	if err != nil {
		return err
	}

	// convert payment system amount of fee to accounting currency of payment system
	amount, err = v.Service.currencyRate.Convert(pmOutCur, ps.AccountingCurrency.CodeInt, commission)

	if err != nil {
		zap.S().Errorw("currency convert error", "from", pmOutCur, "to", ps.AccountingCurrency.CodeInt, "amount", commission)
		return orderErrorCurrencyRateNotFound
	}

	o.PaymentSystemFeeAmount.AmountPaymentSystemCurrency = amount

	if mAccCur != nil {
		// convert payment system amount of fee to accounting currency of merchant
		amount, _ = v.Service.currencyRate.Convert(pmOutCur, mAccCur.CodeInt, commission)
		o.PaymentSystemFeeAmount.AmountMerchantCurrency = amount
	}

	return nil
}

func (v *OrderCreateRequestProcessor) processCustomerToken() error {
	token, err := v.getTokenBy(v.request.Token)

	if err != nil {
		return err
	}

	customer, err := v.getCustomerById(token.CustomerId)

	if err != nil {
		return err
	}

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
		customer, _ = v.findCustomer(tokenReq, v.checked.project)
	}

	if customer != nil {
		customer, err = v.updateCustomer(tokenReq, v.checked.project, customer)
	} else {
		customer, err = v.createCustomer(tokenReq, v.checked.project)
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
func (v *PaymentFormProcessor) processRenderFormPaymentMethods() ([]*billing.PaymentFormPaymentMethod, error) {
	var projectPms []*billing.PaymentFormPaymentMethod

	pmg, err := v.service.paymentMethod.Groups()
	if err != nil {
		return nil, err
	}
	for _, val := range pmg {
		pm, ok := val[v.order.PaymentMethodOutcomeCurrency.CodeInt]

		if !ok || pm.IsActive == false {
			continue
		}

		if ps, err := v.service.paymentSystem.GetById(pm.PaymentSystemId); err != nil || ps.IsActive == false {
			continue
		}

		if v.order.OrderAmount < pm.MinPaymentAmount ||
			(pm.MaxPaymentAmount > 0 && v.order.OrderAmount > pm.MaxPaymentAmount) {
			continue
		}

		formPm := &billing.PaymentFormPaymentMethod{
			Id:            pm.Id,
			Name:          pm.Name,
			Type:          pm.Type,
			Group:         pm.Group,
			AccountRegexp: pm.AccountRegexp,
		}

		err := v.processPaymentMethodsData(formPm)

		if err != nil {
			zap.S().Errorw(
				"Process payment method data failed",
				"error", err,
				"order_id", v.order.Id,
			)
			continue
		}

		projectPms = append(projectPms, formPm)
	}

	if len(projectPms) <= 0 {
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

// Validate data received from payment form and write validated data to order
func (v *PaymentCreateProcessor) processPaymentFormData() error {
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

	order, err := v.service.getOrderByUuidToForm(v.data[pkg.PaymentCreateFieldOrderId])

	if err != nil {
		return err
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

			zipData, err := v.service.zipCode.getByZipAndCountry(zip, country)

			if err != nil {
				return orderErrorZipCodeNotFound
			}

			v.data[pkg.PaymentCreateFieldUserCity] = zipData.City
			v.data[pkg.PaymentCreateFieldUserState] = zipData.State.Code
		}
	}

	processor := &OrderCreateRequestProcessor{
		Service: v.service,
		request: &billing.OrderCreateRequest{
			ProjectId: order.Project.Id,
			Amount:    order.ProjectIncomeAmount,
		},
		checked: &orderCreateRequestProcessorChecked{
			currency: order.ProjectIncomeCurrency,
			amount:   order.ProjectIncomeAmount,
		},
	}

	if err := processor.processProject(); err != nil {
		return err
	}

	pm, err := v.service.paymentMethod.GetById(v.data[pkg.PaymentCreateFieldPaymentMethodId])
	if err != nil {
		return orderErrorPaymentMethodNotFound
	}

	if err = processor.processPaymentMethod(pm); err != nil {
		return err.(*grpc.ResponseErrorMessage)
	}

	if err := processor.processLimitAmounts(); err != nil {
		return err.(*grpc.ResponseErrorMessage)
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

		processor.processOrderVat(order)
		updCustomerReq.User.Address = order.BillingAddress
	}

	restricted, err := v.service.applyCountryRestriction(order, order.GetCountry())
	if err != nil {
		return orderErrorUnknown
	}
	if restricted {
		return orderCountryPaymentRestrictedError
	}

	if order.User.IsIdentified() == true {
		customer, err := v.service.updateCustomerFromRequest(order, updCustomerReq, v.ip, v.acceptLanguage, v.userAgent)

		if err != nil {
			v.service.logError("Update customer data by request failed", []interface{}{"error", err.Error(), "data", updCustomerReq})
		} else {
			if customer.Locale != order.User.Locale {
				order.User.Locale = customer.Locale
			}
		}
	}

	delete(v.data, pkg.PaymentCreateFieldOrderId)
	delete(v.data, pkg.PaymentCreateFieldPaymentMethodId)
	delete(v.data, pkg.PaymentCreateFieldEmail)

	if processor.checked.paymentMethod.IsBankCard() == true {
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

		bin := v.service.getBinData(order.PaymentRequisites[pkg.PaymentCreateFieldPan])

		if bin != nil {
			order.PaymentRequisites[paymentCreateBankCardFieldBrand] = bin.CardBrand
			order.PaymentRequisites[paymentCreateBankCardFieldType] = bin.CardType
			order.PaymentRequisites[paymentCreateBankCardFieldCategory] = bin.CardCategory
			order.PaymentRequisites[paymentCreateBankCardFieldIssuerName] = bin.BankName
			order.PaymentRequisites[paymentCreateBankCardFieldIssuerCountry] = bin.BankCountryName
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

	v.checked.project = processor.checked.project
	v.checked.paymentMethod = processor.checked.paymentMethod
	v.checked.order = order

	if order.ProjectAccount == "" {
		order.ProjectAccount = order.User.Email
	}

	return nil
}

func (v *PaymentCreateProcessor) processPaymentAmounts() (err error) {
	order := v.checked.order

	order.ProjectOutcomeAmount, err = v.service.currencyRate.Convert(
		order.PaymentMethodIncomeCurrency.CodeInt,
		order.ProjectOutcomeCurrency.CodeInt,
		order.PaymentMethodOutcomeAmount,
	)

	if err != nil {
		v.service.logError(
			"Convert to project outcome currency failed",
			[]interface{}{
				"error", err.Error(),
				"from", order.PaymentMethodIncomeCurrency.CodeInt,
				"to", order.ProjectOutcomeCurrency.CodeInt,
				"order_id", order.Id,
			},
		)

		return
	}

	order.AmountInPspAccountingCurrency, err = v.service.currencyRate.Convert(
		order.PaymentMethodIncomeCurrency.CodeInt,
		v.service.accountingCurrency.CodeInt,
		order.PaymentMethodOutcomeAmount,
	)

	if err != nil {
		v.service.logError(
			"Convert to PSP accounting currency failed",
			[]interface{}{
				"error", err.Error(),
				"from", order.PaymentMethodIncomeCurrency.CodeInt,
				"to", v.service.accountingCurrency.CodeInt,
				"order_id", order.Id,
			},
		)

		return
	}

	merchant, _ := v.service.merchant.GetById(order.Project.MerchantId)
	merchantPayoutCurrency := merchant.GetPayoutCurrency()

	if merchantPayoutCurrency != nil {
		order.AmountOutMerchantAccountingCurrency, err = v.service.currencyRate.Convert(
			order.PaymentMethodIncomeCurrency.CodeInt,
			merchantPayoutCurrency.CodeInt,
			order.PaymentMethodOutcomeAmount,
		)

		if err != nil {
			v.service.logError(
				"Convert to merchant accounting currency failed",
				[]interface{}{
					"error", err.Error(),
					"from", order.PaymentMethodIncomeCurrency.CodeInt,
					"to", merchantPayoutCurrency.CodeInt,
					"order_id", order.Id,
				},
			)

			return
		}
	}

	ps, err := v.service.paymentSystem.GetById(order.PaymentMethod.PaymentSystemId)
	if err != nil {
		v.service.logError(
			"Resolve payment system failed",
			[]interface{}{
				"error", err.Error(),
				"from", order.PaymentMethodIncomeCurrency.CodeInt,
				"ps", order.PaymentMethod.PaymentSystemId,
				"order_id", order.Id,
			},
		)

		return
	}
	to := ps.AccountingCurrency.CodeInt

	order.AmountInPaymentSystemAccountingCurrency, err = v.service.currencyRate.Convert(
		order.PaymentMethodIncomeCurrency.CodeInt,
		to,
		order.PaymentMethodOutcomeAmount,
	)

	if err != nil {
		v.service.logError(
			"Convert to payment system accounting currency failed",
			[]interface{}{
				"error", err.Error(),
				"from", order.PaymentMethodIncomeCurrency.CodeInt,
				"to", to,
				"order_id", order.Id,
			},
		)
	}

	return
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
		return nil, orderErrorUnknown
	}

	if result.Total != int32(len(productIds)) {
		return nil, orderErrorProductsInvalid
	}

	return result.Products, nil
}

func (s *Service) GetOrderProductsAmount(products []*grpc.Product, currency string) (float64, error) {
	if len(products) == 0 {
		return 0, orderErrorProductsEmpty
	}

	sum := float64(0)

	for _, p := range products {
		amount, err := p.GetPriceInCurrency(currency)

		if err != nil {
			return 0, orderErrorNoProductsCommonCurrency
		}

		sum += amount
	}

	totalAmount := float64(tools.FormatAmount(sum))

	return totalAmount, nil
}

func (s *Service) GetOrderProductsItems(products []*grpc.Product, language string, currency string) ([]*billing.OrderItem, error) {
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

		amount, err = p.GetPriceInCurrency(currency)
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
			Currency:    currency,
		}
		result = append(result, item)
	}

	return result, nil
}

func (s *Service) ProcessOrderProducts(order *billing.Order) error {
	project, err := s.project.GetById(order.Project.Id)
	if err != nil {
		return orderErrorProjectNotFound
	}
	if project.IsDeleted() == true {
		return orderErrorProjectInactive
	}

	if project.IsProductsCheckout == false {
		return nil
	}

	orderProducts, err := s.GetOrderProducts(project.Id, order.Products)
	if err != nil {
		return err
	}

	var (
		country       string
		currency      *billing.Currency
		itemsCurrency string
		locale        string
		logInfo       = "[ProcessOrderProducts] %s"
	)

	if order.BillingAddress != nil && order.BillingAddress.Country != "" {
		country = order.BillingAddress.Country
	} else if order.User.Address != nil && order.User.Address.Country != "" {
		country = order.User.Address.Country
	}

	defaultCurrency := s.accountingCurrency
	zap.S().Infow(fmt.Sprintf(logInfo, "accountingCurrency"), "currency", defaultCurrency.CodeA3, "order.Uuid", order.Uuid)

	merchant, _ := s.merchant.GetById(order.Project.MerchantId)
	merchantPayoutCurrency := merchant.GetPayoutCurrency()

	if merchantPayoutCurrency != nil {
		defaultCurrency = merchantPayoutCurrency
		zap.S().Infow(fmt.Sprintf(logInfo, "merchant payout currency"), "currency", defaultCurrency.CodeA3, "order.Uuid", order.Uuid)
	} else {
		zap.S().Infow(fmt.Sprintf(logInfo, "no merchant payout currency set"))
	}

	currency = defaultCurrency

	if country != "" {
		countryData, err := s.country.GetByIsoCodeA2(country)
		if err != nil {
			zap.S().Errorw("Country not found", "country", country)
			return orderErrorUnknown
		}
		// todo: change here to priceGroup support instead of country's currency
		curr := countryData.Currency
		currency, err = s.currency.GetByCodeA3(curr)
		if err == nil {
			zap.S().Infow(fmt.Sprintf(logInfo, "currency by country"), "currency", currency.CodeA3, "country", country, "order.Uuid", order.Uuid)
		} else {
			currency = defaultCurrency
		}
	}

	zap.S().Infow(fmt.Sprintf(logInfo, "try to use detected currency for order amount"), "currency", currency.CodeA3, "order.Uuid", order.Uuid)

	itemsCurrency = currency.CodeA3

	// try to get order Amount in requested currency
	amount, err := s.GetOrderProductsAmount(orderProducts, currency.CodeA3)
	if err != nil {
		if currency.CodeA3 == defaultCurrency.CodeA3 {
			return err
		}
		// try to get order Amount in default currency, if it differs from requested one
		amount, err = s.GetOrderProductsAmount(orderProducts, defaultCurrency.CodeA3)
		if err != nil {
			return err
		}
		zap.S().Infow(fmt.Sprintf(logInfo, "try to use default currency for order amount"), "currency", defaultCurrency.CodeA3, "order.Uuid", order.Uuid)

		itemsCurrency = defaultCurrency.CodeA3
		// converting Amount from default currency to requested
		amount, err = s.currencyRate.Convert(defaultCurrency.CodeInt, currency.CodeInt, amount)
		if err != nil {
			zap.S().Errorw("currency convert error", "from", defaultCurrency.CodeInt, "to", currency.CodeInt, "amount", amount)
			return orderErrorUnknown
		}
	}

	if order.User != nil && order.User.Locale != "" {
		locale = order.User.Locale
	} else {
		locale = DefaultLanguage
	}

	items, err := s.GetOrderProductsItems(orderProducts, locale, itemsCurrency)
	if err != nil {
		return err
	}

	merAccAmount := amount
	projectOutcomeCurrency := currency
	if merchantPayoutCurrency != nil && currency.CodeInt != merchantPayoutCurrency.CodeInt {
		amount, err := s.currencyRate.Convert(currency.CodeInt, merchantPayoutCurrency.CodeInt, amount)
		if err != nil {
			zap.S().Errorw("currency convert error", "from", currency.CodeInt, "to", merchantPayoutCurrency.CodeInt, "amount", amount)
			return orderErrorUnknown
		}
		merAccAmount = amount
		projectOutcomeCurrency = merchantPayoutCurrency
	}

	order.Currency = currency.CodeA3
	order.ProjectOutcomeCurrency = projectOutcomeCurrency
	order.ProjectIncomeCurrency = currency
	order.PaymentMethodOutcomeCurrency = currency
	order.PaymentMethodIncomeCurrency = currency

	order.OrderAmount = amount
	order.ProjectIncomeAmount = amount
	order.ProjectOutcomeAmount = merAccAmount
	order.PaymentMethodOutcomeAmount = amount
	order.PaymentMethodIncomeAmount = amount

	order.Items = items

	return nil
}

func (s *Service) notifyPaylinkError(PaylinkId string, err error, req interface{}, order interface{}) {
	msg := map[string]interface{}{
		"event":     "error",
		"paylinkId": PaylinkId,
		"message":   "Invalid paylink",
		"error":     err,
		"request":   req,
		"order":     order,
	}
	sErr := s.sendCentrifugoMessage(msg)
	if sErr != nil {
		zap.S().Errorf(
			"Cannot send centrifugo message about Paylink Error",
			"error", sErr.Error(),
			"PaylinkId", PaylinkId,
			"originalError", err.Error(),
			"request", req,
			"order", order,
		)
	}
}

func (v *PaymentCreateProcessor) GetMerchantId() string {
	return v.checked.project.MerchantId
}

func (s *Service) processCustomerData(
	customerId string,
	order *billing.Order,
	req *grpc.PaymentFormJsonDataRequest,
	browserCustomer *BrowserCookieCustomer,
	locale string,
) (*billing.Customer, error) {
	customer, err := s.getCustomerById(customerId)

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
	_, err = s.updateCustomer(tokenReq, project, customer)

	return customer, err
}

func (s *Service) IsOrderCanBePaying(
	ctx context.Context,
	req *grpc.IsOrderCanBePayingRequest,
	rsp *grpc.IsOrderCanBePayingResponse,
) error {
	order, err := s.getOrderByUuidToForm(req.OrderId)
	rsp.Status = pkg.ResponseStatusBadData

	if err != nil {
		rsp.Message = err.(*grpc.ResponseErrorMessage)
		return nil
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
		first6 = string(pan[0:6])
		last4 = string(pan[len(pan)-4:])
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
	rsp *grpc.EmptyResponse,
) error {

	order, err := s.getOrderByUuid(req.OrderUuid)

	if err != nil {
		s.logError(orderErrorNotFound.Message, []interface{}{"error", err.Error(), "request", req})
		return orderErrorNotFound
	}

	if req.EnableNotification && req.Email == "" {
		return orderErrorEmailRequired
	}

	order.NotifySale = req.EnableNotification
	order.NotifySaleEmail = req.Email
	err = s.updateOrder(order)
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
	err = s.db.Collection(collectionNotifySales).Insert(data)
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
		customer, err := s.getCustomerById(order.User.Id)
		if err != nil {
			return err
		}
		project, err := s.project.GetById(order.Project.Id)
		if err != nil {
			return err
		}

		customer.NotifySale = req.EnableNotification
		customer.NotifySaleEmail = req.Email

		tokenReq := s.transformOrderUser2TokenRequest(order.User)
		_, err = s.updateCustomer(tokenReq, project, customer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) SetUserNotifyNewRegion(
	ctx context.Context,
	req *grpc.SetUserNotifyRequest,
	rsp *grpc.EmptyResponse,
) error {

	order, err := s.getOrderByUuid(req.OrderUuid)

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
	err = s.updateOrder(order)
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
	err = s.db.Collection(collectionNotifyNewRegion).Insert(data)
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
		customer, err := s.getCustomerById(order.User.Id)
		if err != nil {
			return err
		}
		project, err := s.project.GetById(order.Project.Id)
		if err != nil {
			return err
		}

		customer.NotifyNewRegion = req.EnableNotification
		customer.NotifyNewRegionEmail = req.Email

		tokenReq := s.transformOrderUser2TokenRequest(order.User)
		_, err = s.updateCustomer(tokenReq, project, customer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) applyCountryRestriction(order *billing.Order, countryCode string) (restricted bool, err error) {
	restricted = false
	if countryCode == "" {
		return
	}
	country, err := s.country.GetByIsoCodeA2(countryCode)
	if err != nil {
		return
	}
	order.CountryRestriction = &billing.CountryRestriction{
		IsoCodeA2:       countryCode,
		PaymentsAllowed: country.PaymentsAllowed,
		ChangeAllowed:   country.ChangeAllowed,
	}
	if country.PaymentsAllowed {
		return
	}
	if country.ChangeAllowed {
		order.UserAddressDataRequired = true
		return
	}
	order.PrivateStatus = constant.OrderStatusPaymentSystemDeclined
	restricted = true
	err = s.updateOrder(order)
	if err != nil && err.Error() == orderErrorNotFound.Error() {
		err = nil
	}
	return
}
