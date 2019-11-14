package service

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cardPayRequestFieldGrantType    = "grant_type"
	cardPayRequestFieldTerminalCode = "terminal_code"
	cardPayRequestFieldPassword     = "password"
	cardPayRequestFieldRefreshToken = "refresh_token"

	cardPayGrantTypePassword     = "password"
	cardPayGrantTypeRefreshToken = "refresh_token"

	cardPayDateFormat          = "2006-01-02T15:04:05Z"
	cardPayInitiatorCardholder = "cit"

	cardPayMaxItemNameLength = 50
	cardPayMaxItemDescriptionLength = 200
)

var (
	cardPayTokens = map[string]*cardPayToken{}

	successRefundResponseStatuses = map[string]bool{
		pkg.CardPayPaymentResponseStatusAuthorized: true,
		pkg.CardPayPaymentResponseStatusInProgress: true,
		pkg.CardPayPaymentResponseStatusPending:    true,
		pkg.CardPayPaymentResponseStatusRefunded:   true,
		pkg.CardPayPaymentResponseStatusCompleted:  true,
	}
)

type cardPay struct {
	mu         sync.Mutex
	httpClient *http.Client
}

type cardPayTransport struct {
	Transport http.RoundTripper
}

type cardPayContextKey struct {
	name string
}

type cardPayToken struct {
	TokenType              string `json:"token_type"`
	AccessToken            string `json:"access_token"`
	RefreshToken           string `json:"refresh_token"`
	AccessTokenExpire      int    `json:"expires_in"`
	RefreshTokenExpire     int    `json:"refresh_expires_in"`
	AccessTokenExpireTime  time.Time
	RefreshTokenExpireTime time.Time
}

type CardPayBankCardAccount struct {
	Pan        string `json:"pan"`
	HolderName string `json:"holder"`
	Cvv        string `json:"security_code"`
	Expire     string `json:"expiration"`
}

type CardPayEWalletAccount struct {
	Id string `json:"id"`
}

type CardPayRecurringDataFiling struct {
	Id string `json:"id"`
}

type CardPayPaymentData struct {
	Currency   string  `json:"currency"`
	Amount     float64 `json:"amount"`
	Descriptor string  `json:"dynamic_descriptor"`
	Note       string  `json:"note"`
}

type CardPayRecurringData struct {
	Currency   string                      `json:"currency"`
	Amount     float64                     `json:"amount"`
	Filing     *CardPayRecurringDataFiling `json:"filing,omitempty"`
	Descriptor string                      `json:"dynamic_descriptor"`
	Note       string                      `json:"note"`
	Initiator  string                      `json:"initiator"`
}

type CardPayCustomer struct {
	Email   string `json:"email"`
	Ip      string `json:"ip"`
	Account string `json:"id"`
}

type CardPayItem struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Count       int     `json:"count"`
	Price       float64 `json:"price"`
}

type CardPayRequest struct {
	Id   string `json:"id"`
	Time string `json:"time"`
}

type CardPayAddress struct {
	Country string `json:"country"`
	City    string `json:"city,omitempty"`
	Phone   string `json:"phone,omitempty"`
	State   string `json:"state,omitempty"`
	Street  string `json:",omitempty"`
	Zip     string `json:"zip,omitempty"`
}

type CardPayMerchantOrder struct {
	Id              string          `json:"id" validate:"required,hexadecimal"`
	Description     string          `json:"description,omitempty"`
	Items           []*CardPayItem  `json:"items,omitempty"`
	ShippingAddress *CardPayAddress `json:"shipping_address,omitempty"`
}

type CardPayCardAccount struct {
	BillingAddress *CardPayAddress         `json:"billing_address,omitempty"`
	Card           *CardPayBankCardAccount `json:"card"`
	Token          string                  `json:"token,omitempty"`
}

type CardPayCryptoCurrencyAccount struct {
	RollbackAddress string `json:"rollback_address"`
}

type CardPayReturnUrls struct {
	CancelUrl  string `json:"cancel_url,omitempty"`
	DeclineUrl string `json:"decline_url,omitempty"`
	SuccessUrl string `json:"success_url,omitempty"`
}

type CardPayOrder struct {
	Request               *CardPayRequest               `json:"request"`
	MerchantOrder         *CardPayMerchantOrder         `json:"merchant_order"`
	Description           string                        `json:"description"`
	PaymentMethod         string                        `json:"payment_method"`
	PaymentData           *CardPayPaymentData           `json:"payment_data,omitempty"`
	RecurringData         *CardPayRecurringData         `json:"recurring_data,omitempty"`
	CardAccount           *CardPayCardAccount           `json:"card_account,omitempty"`
	Customer              *CardPayCustomer              `json:"customer"`
	EWalletAccount        *CardPayEWalletAccount        `json:"ewallet_account,omitempty"`
	CryptoCurrencyAccount *CardPayCryptoCurrencyAccount `json:"cryptocurrency_account,omitempty"`
	ReturnUrls            *CardPayReturnUrls            `json:"return_urls,omitempty"`
}

type CardPayOrderResponse struct {
	RedirectUrl string `json:"redirect_url"`
}

type CardPayOrderRecurringResponse struct {
	RecurringData *CardPayOrderRecurringResponseRecurringData `json:"recurring_data"`
}

type CardPayOrderRecurringResponseRecurringData struct {
	Id       string                      `json:"id"`
	Filing   *CardPayRecurringDataFiling `json:"filing"`
	Status   string                      `json:"status"`
	Amount   float64                     `json:"amount"`
	Currency string                      `json:"currency"`
	Created  string                      `json:"created"`
	Note     string                      `json:"note"`
	Rrn      string                      `json:"rrn"`
	Is3D     bool                        `json:"is_3d"`
}

type CardPayRefundData struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type CardPayRefundRequest struct {
	Request       *CardPayRequest             `json:"request"`
	MerchantOrder *CardPayMerchantOrder       `json:"merchant_order"`
	PaymentData   *CardPayRecurringDataFiling `json:"payment_data"`
	RefundData    *CardPayRefundData          `json:"refund_data"`
}

type CardPayRefundResponseRefundData struct {
	Id       string  `json:"id"`
	Created  string  `json:"created"`
	Status   string  `json:"status"`
	AuthCode string  `json:"auth_code"`
	Is3d     bool    `json:"is_3d"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type CardPayRefundResponsePaymentData struct {
	Id              string  `json:"id"`
	RemainingAmount float64 `json:"remaining_amount"`
}

type CardPayRefundResponseCustomer struct {
	Id    string `json:"id"`
	Email string `json:"email"`
}

type CardPayRefundResponse struct {
	PaymentMethod  string                            `json:"payment_method"`
	MerchantOrder  *CardPayMerchantOrder             `json:"merchant_order"`
	RefundData     *CardPayRefundResponseRefundData  `json:"refund_data"`
	PaymentData    *CardPayRefundResponsePaymentData `json:"payment_data"`
	Customer       *CardPayRefundResponseCustomer    `json:"customer"`
	CardAccount    interface{}                       `json:"card_account,omitempty"`
	EwalletAccount interface{}                       `json:"ewallet_account,omitempty"`
}

func (m *CardPayRefundResponse) IsSuccessStatus() bool {
	v, ok := successRefundResponseStatuses[m.RefundData.Status]
	return ok && v == true
}

func newCardPayHandler() PaymentSystem {
	return &cardPay{
		httpClient: &http.Client{
			Transport: &cardPayTransport{},
			Timeout:   defaultHttpClientTimeout * time.Second,
		},
	}
}

func (h *cardPay) CreatePayment(
	order *billing.Order,
	successUrl, failUrl string,
	requisites map[string]string,
) (string, error) {
	err := h.auth(order)

	if err != nil {
		return "", err
	}

	request, err := h.getCardPayOrder(order, successUrl, failUrl, requisites)

	if err != nil {
		return "", nil
	}

	action := pkg.PaymentSystemActionCreatePayment

	if request.RecurringData != nil {
		action = pkg.PaymentSystemActionRecurringPayment
	}

	u, err := h.getUrl(order.GetPaymentSystemApiUrl(), action)

	if err != nil {
		return "", err
	}

	order.PrivateStatus = constant.OrderStatusPaymentSystemRejectOnCreate

	b, _ := json.Marshal(request)
	req, err := http.NewRequest(pkg.CardPayPaths[action].Method, u, bytes.NewBuffer(b))

	if err != nil {
		zap.L().Error(
			"cardpay API: create payment request failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any("order", order),
			zap.ByteString(pkg.LogFieldRequest, b),
		)
		return "", err
	}

	token := h.getToken(order)
	auth := strings.Title(token.TokenType) + " " + token.AccessToken

	req.Header.Add(HeaderContentType, MIMEApplicationJSON)
	req.Header.Add(HeaderAuthorization, auth)

	resp, err := h.httpClient.Do(req)

	if err != nil {
		zap.L().Error(
			"cardpay API: send payment request failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any("order", order),
			zap.ByteString(pkg.LogFieldRequest, b),
		)
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		zap.L().Error(
			"payment response returned with bad http status",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any("order", order),
			zap.ByteString(pkg.LogFieldRequest, b),
		)
		return "", paymentSystemErrorCreateRequestFailed
	}

	b, err = ioutil.ReadAll(resp.Body)

	if err != nil {
		zap.L().Error(
			"payment response body can't be read",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any("order", order),
			zap.ByteString(pkg.LogFieldRequest, b),
		)
		return "", err
	}

	if request.RecurringData != nil && request.RecurringData.Filing != nil {
		cpRsp := &CardPayOrderRecurringResponse{}
		err = json.Unmarshal(b, &cpRsp)

		if err != nil {
			zap.L().Error(
				"payment response contain invalid json",
				zap.Error(err),
				zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
				zap.String("url", u),
				zap.Any("order", order),
				zap.ByteString(pkg.LogFieldRequest, b),
			)
			return "", err
		}

		if cpRsp.IsSuccessStatus() == false {
			return "", paymentSystemErrorRecurringFailed
		}
	}

	cpResponse := &CardPayOrderResponse{}
	err = json.Unmarshal(b, &cpResponse)

	if err != nil {
		zap.L().Error(
			"payment response contain invalid json",
			zap.Error(err),
			zap.String("Method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any("order", order),
			zap.ByteString(pkg.LogFieldRequest, b),
		)
		return "", err
	}

	order.PrivateStatus = constant.OrderStatusPaymentSystemCreate

	return cpResponse.RedirectUrl, nil
}

func (h *cardPay) ProcessPayment(order *billing.Order, message proto.Message, raw, signature string) error {
	req := message.(*billing.CardPayPaymentCallback)
	order.PrivateStatus = constant.OrderStatusPaymentSystemReject
	err := h.checkCallbackRequestSignature(order, raw, signature)

	if err != nil {
		return err
	}

	if !req.IsPaymentAllowedStatus() {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestStatusIsInvalid)
	}

	if req.IsRecurring() && req.IsSuccess() && (req.RecurringData.Filing == nil || req.RecurringData.Filing.Id == "") {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestRecurringIdFieldIsInvalid)
	}

	t, err := time.Parse(cardPayDateFormat, req.CallbackTime)

	if err != nil {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestTimeFieldIsInvalid)
	}

	ts, err := ptypes.TimestampProto(t)

	if err != nil {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestTimeFieldIsInvalid)
	}

	if req.PaymentMethod != order.PaymentMethod.ExternalId {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestPaymentMethodIsInvalid)
	}

	reqAmount := req.GetAmount()

	if reqAmount != order.TotalPaymentAmount ||
		req.GetCurrency() != order.Currency {
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestAmountOrCurrencyIsInvalid)
	}

	switch req.PaymentMethod {
	case constant.PaymentSystemGroupAliasBankCard:
		order.PaymentMethodTxnParams = req.GetBankCardTxnParams()
		break
	case constant.PaymentSystemGroupAliasQiwi,
		constant.PaymentSystemGroupAliasWebMoney,
		constant.PaymentSystemGroupAliasNeteller,
		constant.PaymentSystemGroupAliasAlipay:
		order.PaymentMethodTxnParams = req.GetEWalletTxnParams()
		break
	case constant.PaymentSystemGroupAliasBitcoin:
		order.PaymentMethodTxnParams = req.GetCryptoCurrencyTxnParams()
		break
	default:
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestPaymentMethodIsInvalid)
	}

	status := req.GetStatus()

	switch status {
	case pkg.CardPayPaymentResponseStatusDeclined:
		order.PrivateStatus = constant.OrderStatusPaymentSystemDeclined
		break
	case pkg.CardPayPaymentResponseStatusCancelled:
		order.PrivateStatus = constant.OrderStatusPaymentSystemCanceled
		order.CanceledAt = ptypes.TimestampNow()
		break
	case pkg.CardPayPaymentResponseStatusCompleted:
		order.PrivateStatus = constant.OrderStatusPaymentSystemComplete
		break
	default:
		return newBillingServerResponseError(pkg.StatusTemporary, paymentSystemErrorRequestTemporarySkipped)
	}

	if status == pkg.CardPayPaymentResponseStatusDeclined || status == pkg.CardPayPaymentResponseStatusCancelled {
		declineCode, hasDeclineCode := order.PaymentMethodTxnParams[pkg.TxnParamsFieldDeclineCode]
		declineReason, hasDeclineReason := order.PaymentMethodTxnParams[pkg.TxnParamsFieldDeclineReason]

		if hasDeclineCode || hasDeclineReason {
			order.Cancellation = &billing.OrderNotificationCancellation{}
		}

		if declineCode != "" {
			order.Cancellation.Code = declineCode
		}

		if declineReason != "" {
			order.Cancellation.Reason = declineReason
		}
	}

	order.Transaction = req.GetId()
	order.PaymentMethodOrderClosedAt = ts

	return nil
}

func (h *cardPay) IsRecurringCallback(request proto.Message) bool {
	req := request.(*billing.CardPayPaymentCallback)
	return req.PaymentMethod == constant.PaymentSystemGroupAliasBankCard && req.IsRecurring()
}

func (h *cardPay) GetRecurringId(request proto.Message) string {
	return request.(*billing.CardPayPaymentCallback).RecurringData.Filing.Id
}

func (h *cardPay) auth(order *billing.Order) error {
	if token := h.getToken(order); token != nil {
		return nil
	}

	data := url.Values{
		cardPayRequestFieldGrantType:    []string{cardPayGrantTypePassword},
		cardPayRequestFieldTerminalCode: []string{order.PaymentMethod.Params.TerminalId},
		cardPayRequestFieldPassword:     []string{order.PaymentMethod.Params.Secret},
	}

	u, err := h.getUrl(order.GetPaymentSystemApiUrl(), pkg.PaymentSystemActionAuthenticate)

	if err != nil {
		return err
	}

	req, err := http.NewRequest(pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method, u, strings.NewReader(data.Encode()))

	if err != nil {
		zap.L().Error(
			"cardpay API: create auth request failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any(pkg.LogFieldRequest, data),
		)
		return err
	}

	req.Header.Add(HeaderContentType, MIMEApplicationForm)
	req.Header.Add(HeaderContentLength, strconv.Itoa(len(data.Encode())))

	rsp, err := h.httpClient.Do(req)

	if err != nil {
		zap.L().Error(
			"cardpay API: send auth request failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any(pkg.LogFieldRequest, data),
		)
		return err
	}

	if rsp.StatusCode != http.StatusOK {
		return paymentSystemErrorAuthenticateFailed
	}

	b, err := ioutil.ReadAll(rsp.Body)
	rsp.Body.Close()

	if err != nil {
		zap.L().Error(
			"cardpay API: reading auth response failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionAuthenticate].Method),
			zap.String("url", u),
			zap.Any(pkg.LogFieldRequest, data),
		)
		return err
	}

	err = h.setToken(b, order.PaymentMethod.ExternalId)

	if err != nil {
		return err
	}

	return nil
}

func (h *cardPay) refresh(order *billing.Order) error {
	data := url.Values{
		cardPayRequestFieldGrantType:    []string{cardPayGrantTypeRefreshToken},
		cardPayRequestFieldTerminalCode: []string{order.PaymentMethod.Params.TerminalId},
		cardPayRequestFieldRefreshToken: []string{cardPayTokens[order.PaymentMethod.ExternalId].RefreshToken},
	}

	qUrl, err := h.getUrl(order.GetPaymentSystemApiUrl(), pkg.PaymentSystemActionRefresh)

	if err != nil {
		return err
	}

	req, err := http.NewRequest(pkg.CardPayPaths[pkg.PaymentSystemActionRefresh].Method, qUrl, strings.NewReader(data.Encode()))

	if err != nil {
		return err
	}

	req.Header.Add(HeaderContentType, MIMEApplicationForm)
	req.Header.Add(HeaderContentLength, strconv.Itoa(len(data.Encode())))

	resp, err := h.httpClient.Do(req)

	if err != nil {
		return err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			return
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return paymentSystemErrorAuthenticateFailed
	}

	b, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	if err := h.setToken(b, order.PaymentMethod.ExternalId); err != nil {
		return err
	}

	return nil
}

func (h *cardPay) getUrl(apiUrl, action string) (string, error) {
	u, err := url.ParseRequestURI(apiUrl)

	if err != nil {
		zap.L().Error(
			"cardpay API: api url is invalid",
			zap.Error(err),
			zap.String("url", apiUrl),
		)
		return "", err
	}

	u.Path = pkg.CardPayPaths[action].Path

	return u.String(), nil
}

func (h *cardPay) setToken(b []byte, pmKey string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var token *cardPayToken

	if err := json.Unmarshal(b, &token); err != nil {
		return err
	}

	token.AccessTokenExpireTime = time.Now().Add(time.Second * time.Duration(token.AccessTokenExpire))
	token.RefreshTokenExpireTime = time.Now().Add(time.Second * time.Duration(token.RefreshTokenExpire))

	cardPayTokens[pmKey] = token

	return nil
}

func (h *cardPay) getToken(order *billing.Order) *cardPayToken {
	token, ok := cardPayTokens[order.PaymentMethod.ExternalId]

	if !ok {
		return nil
	}

	tn := time.Now().Unix()

	if token.AccessTokenExpire > 0 && token.AccessTokenExpireTime.Unix() >= tn {
		return token
	}

	if token.RefreshTokenExpire <= 0 || token.RefreshTokenExpireTime.Unix() < tn {
		return nil
	}

	err := h.refresh(order)

	if err != nil {
		return nil
	}

	return cardPayTokens[order.PaymentMethod.ExternalId]
}

func (h *cardPay) getCardPayOrder(
	order *billing.Order,
	successUrl, failUrl string,
	requisites map[string]string,
) (*CardPayOrder, error) {
	var items []*CardPayItem

	for _, it := range order.Items {
		name := []rune(it.Name)
		description := []rune(it.Description)

		if len(name) > cardPayMaxItemNameLength {
			name = name[:cardPayMaxItemNameLength]
		}

		if len(description) > cardPayMaxItemDescriptionLength {
			description = description[:cardPayMaxItemDescriptionLength]
		}

		items = append(items, &CardPayItem{
			Name:        string(name),
			Description: string(description),
			Count:       1,
			Price:       it.Amount,
		})
	}

	cardPayOrder := &CardPayOrder{
		Request: &CardPayRequest{
			Id:   order.Id,
			Time: time.Now().UTC().Format(cardPayDateFormat),
		},
		MerchantOrder: &CardPayMerchantOrder{
			Id:          order.Id,
			Description: order.Description,
			Items:       items,
		},
		Description:   order.Description,
		PaymentMethod: order.PaymentMethod.ExternalId,
		Customer: &CardPayCustomer{
			Ip:      order.User.Ip,
			Account: order.User.Id,
			Email:   order.User.TechEmail,
		},
		ReturnUrls: &CardPayReturnUrls{
			SuccessUrl: successUrl,
			DeclineUrl: failUrl,
			CancelUrl:  failUrl,
		},
	}

	storeData, okStoreData := requisites[pkg.PaymentCreateFieldStoreData]
	recurringId, okRecurringId := requisites[pkg.PaymentCreateFieldRecurringId]

	if order.PaymentMethod.IsBankCard() && (okStoreData && storeData == "1") ||
		(okRecurringId && recurringId != "") {
		cardPayOrder.RecurringData = &CardPayRecurringData{
			Currency:  order.Currency,
			Amount:    order.TotalPaymentAmount,
			Initiator: cardPayInitiatorCardholder,
		}

		if okRecurringId == true && recurringId != "" {
			cardPayOrder.RecurringData.Filing = &CardPayRecurringDataFiling{
				Id: recurringId,
			}

			return cardPayOrder, nil
		}
	} else {
		cardPayOrder.PaymentData = &CardPayPaymentData{
			Currency: order.Currency,
			Amount:   order.TotalPaymentAmount,
		}
	}

	switch order.PaymentMethod.ExternalId {
	case constant.PaymentSystemGroupAliasBankCard:
		h.geBankCardCardPayOrder(cardPayOrder, requisites)
		break
	case constant.PaymentSystemGroupAliasQiwi,
		constant.PaymentSystemGroupAliasWebMoney,
		constant.PaymentSystemGroupAliasNeteller,
		constant.PaymentSystemGroupAliasAlipay:
		h.getEWalletCardPayOrder(cardPayOrder, requisites)
		break
	case constant.PaymentSystemGroupAliasBitcoin:
		h.getCryptoCurrencyCardPayOrder(cardPayOrder, requisites)
		break
	default:
		zap.L().Error(
			"cardpay API: requested create payment for unknown payment Method",
			zap.Any("order", order),
		)
		return nil, paymentSystemErrorUnknownPaymentMethod
	}

	return cardPayOrder, nil
}

func (h *cardPay) geBankCardCardPayOrder(cpo *CardPayOrder, requisites map[string]string) {
	expire := requisites[pkg.PaymentCreateFieldMonth] + "/" + requisites[pkg.PaymentCreateFieldYear]

	cpo.CardAccount = &CardPayCardAccount{
		Card: &CardPayBankCardAccount{
			Pan:        requisites[pkg.PaymentCreateFieldPan],
			HolderName: strings.ToUpper(requisites[pkg.PaymentCreateFieldHolder]),
			Cvv:        requisites[pkg.PaymentCreateFieldCvv],
			Expire:     expire,
		},
	}
}

func (h *cardPay) getEWalletCardPayOrder(cpo *CardPayOrder, requisites map[string]string) {
	cpo.EWalletAccount = &CardPayEWalletAccount{
		Id: requisites[pkg.PaymentCreateFieldEWallet],
	}
}

func (h *cardPay) getCryptoCurrencyCardPayOrder(cpo *CardPayOrder, requisites map[string]string) {
	cpo.CryptoCurrencyAccount = &CardPayCryptoCurrencyAccount{
		RollbackAddress: requisites[pkg.PaymentCreateFieldCrypto],
	}
}

func (h *cardPay) checkCallbackRequestSignature(order *billing.Order, raw, signature string) error {
	hash := sha512.New()
	hash.Write([]byte(raw + order.PaymentMethod.Params.SecretCallback))

	if hex.EncodeToString(hash.Sum(nil)) != signature {
		zap.L().Error(
			"cardpay API: payment callback signature is invalid",
			zap.Any("order", order),
		)
		return newBillingServerResponseError(pkg.StatusErrorValidation, paymentSystemErrorRequestSignatureIsInvalid)
	}

	return nil
}

func (t *cardPayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := context.WithValue(req.Context(), &cardPayContextKey{name: "CardPayRequestStart"}, time.Now())
	req = req.WithContext(ctx)

	var reqBody []byte

	if req.Body != nil {
		reqBody, _ = ioutil.ReadAll(req.Body)
	}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBody))

	resp, err := t.transport().RoundTrip(req)
	if err != nil {
		return resp, err
	}

	t.log(req.URL.Path, req.Header, reqBody, resp)

	return resp, err
}

func (t *cardPayTransport) transport() http.RoundTripper {
	if t.Transport != nil {
		return t.Transport
	}

	return http.DefaultTransport
}

func (t *cardPayTransport) log(reqUrl string, reqHeader http.Header, reqBody []byte, rsp *http.Response) {
	var rspBody []byte

	if rsp.Body != nil {
		rspBody, _ = ioutil.ReadAll(rsp.Body)
	}
	rsp.Body = ioutil.NopCloser(bytes.NewBuffer(rspBody))

	cpOrder := &CardPayOrder{}
	err := json.Unmarshal(reqBody, cpOrder)
	request := reqBody

	if err == nil {
		if cpOrder.CardAccount != nil {
			cpOrder.CardAccount.Card.Pan = tools.MaskBankCardNumber(cpOrder.CardAccount.Card.Pan)
			cpOrder.CardAccount.Card.Cvv = "***"
		}

		request, err = json.Marshal(cpOrder)

		if err != nil {
			return
		}
	}

	zap.L().Info(
		reqUrl,
		zap.Any("request_headers", reqHeader),
		zap.ByteString("request_body", request),
		zap.Int("response_status", rsp.StatusCode),
		zap.Any("response_headers", rsp.Header),
		zap.ByteString("response_body", rspBody),
	)
}

func (h *cardPay) CreateRefund(order *billing.Order, refund *billing.Refund) error {
	err := h.auth(order)

	if err != nil {
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	u, err := h.getUrl(order.GetPaymentSystemApiUrl(), pkg.PaymentSystemActionRefund)

	if err != nil {
		return err
	}

	data := &CardPayRefundRequest{
		Request: &CardPayRequest{
			Id:   refund.Id,
			Time: time.Now().UTC().Format(cardPayDateFormat),
		},
		MerchantOrder: &CardPayMerchantOrder{
			Id:          refund.Id,
			Description: refund.Reason,
		},
		PaymentData: &CardPayRecurringDataFiling{
			Id: order.Transaction,
		},
		RefundData: &CardPayRefundData{
			Amount:   refund.Amount,
			Currency: refund.Currency,
		},
	}

	b, err := json.Marshal(data)

	if err != nil {
		zap.L().Error(
			"marshal refund request failed",
			zap.Error(err),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.Any(pkg.LogFieldRequest, data),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	req, err := http.NewRequest(pkg.CardPayPaths[pkg.PaymentSystemActionRefund].Method, u, bytes.NewBuffer(b))

	if err != nil {
		zap.L().Error(
			"create refund request failed",
			zap.Error(err),
			zap.String("method", pkg.CardPayPaths[pkg.PaymentSystemActionRefund].Method),
			zap.String("url", u),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.ByteString(pkg.LogFieldRequest, b),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	token := h.getToken(order)
	auth := strings.Title(token.TokenType) + " " + token.AccessToken

	req.Header.Add(HeaderContentType, MIMEApplicationJSON)
	req.Header.Add(HeaderAuthorization, auth)

	refund.Status = pkg.RefundStatusRejected
	resp, err := h.httpClient.Do(req)

	if err != nil {
		zap.L().Error(
			"refund request failed",
			zap.Error(err),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.Any(pkg.LogFieldRequest, data),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	if resp.StatusCode != http.StatusCreated {
		zap.L().Error(
			"refund response returned with bad http status",
			zap.Error(err),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.Any(pkg.LogFieldRequest, data),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	b, err = ioutil.ReadAll(resp.Body)

	if err != nil {
		zap.L().Error(
			"refund response body can't be read",
			zap.Error(err),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.Any(pkg.LogFieldRequest, data),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	rsp := &CardPayRefundResponse{}
	err = json.Unmarshal(b, &rsp)

	if err != nil {
		zap.L().Error(
			"refund response contain invalid json",
			zap.Error(err),
			zap.String(pkg.LogFieldHandler, pkg.PaymentSystemHandlerCardPay),
			zap.Any(pkg.LogFieldRequest, data),
			zap.ByteString(pkg.LogFieldResponse, b),
			zap.Any("refund", refund),
		)
		return errors.New(pkg.PaymentSystemErrorCreateRefundFailed)
	}

	if rsp.IsSuccessStatus() == false {
		return errors.New(pkg.PaymentSystemErrorCreateRefundRejected)
	}

	refund.Status = pkg.RefundStatusInProgress
	refund.ExternalId = rsp.RefundData.Id

	return nil
}

func (h *cardPay) ProcessRefund(
	order *billing.Order,
	refund *billing.Refund,
	message proto.Message,
	raw, signature string,
) error {
	req := message.(*billing.CardPayRefundCallback)
	refund.Status = pkg.RefundStatusRejected

	err := h.checkCallbackRequestSignature(order, raw, signature)

	if err != nil {
		err.(*grpc.ResponseError).Status = pkg.ResponseStatusBadData
		return err
	}

	if !req.IsRefundAllowedStatus() {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, paymentSystemErrorRequestStatusIsInvalid)
	}

	if req.PaymentMethod != order.PaymentMethod.ExternalId {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, paymentSystemErrorRequestPaymentMethodIsInvalid)
	}

	if req.RefundData.Amount != refund.Amount || req.RefundData.Currency != refund.Currency {
		return newBillingServerResponseError(pkg.ResponseStatusBadData, paymentSystemErrorRefundRequestAmountOrCurrencyIsInvalid)
	}

	switch req.RefundData.Status {
	case pkg.CardPayPaymentResponseStatusDeclined:
		refund.Status = pkg.RefundStatusPaymentSystemDeclined
		break
	case pkg.CardPayPaymentResponseStatusCancelled:
		refund.Status = pkg.RefundStatusPaymentSystemCanceled
		break
	case pkg.CardPayPaymentResponseStatusCompleted:
		refund.Status = pkg.RefundStatusCompleted
		break
	default:
		return newBillingServerResponseError(pkg.ResponseStatusTemporary, paymentSystemErrorRequestTemporarySkipped)
	}

	refund.ExternalId = req.RefundData.Id
	refund.UpdatedAt = ptypes.TimestampNow()

	return nil
}

func (h *CardPayOrderRecurringResponse) IsSuccessStatus() bool {
	if h.RecurringData == nil {
		return false
	}

	status := h.RecurringData.Status

	return status == pkg.CardPayPaymentResponseStatusInProgress || status == pkg.CardPayPaymentResponseStatusPending ||
		status == pkg.CardPayPaymentResponseStatusAuthorized || status == pkg.CardPayPaymentResponseStatusCompleted
}
