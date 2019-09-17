package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ProtocolONE/geoip-service/pkg/proto"
	"github.com/centrifugal/gocent"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	documentSignerProto "github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"github.com/paysuper/paysuper-tax-service/proto"
	"go.uber.org/zap"
	"gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
	"gopkg.in/gomail.v2"
	"strings"
	"sync"
)

const (
	errorNotFound                   = "%s not found"
	errorQueryMask                  = "Query from collection \"%s\" failed"
	errorAccountingCurrencyNotFound = "accounting currency not found"
	errorInterfaceCast              = "unable to cast interface to object %s"

	errorBbNotFoundMessage = "not found"

	HeaderContentType   = "Content-Type"
	HeaderAuthorization = "Authorization"
	HeaderContentLength = "Content-Length"

	MIMEApplicationForm = "application/x-www-form-urlencoded"
	MIMEApplicationJSON = "application/json"

	DefaultPaymentMethodFee               = float64(5)
	DefaultPaymentMethodPerTransactionFee = float64(0)
	DefaultPaymentMethodCurrency          = ""

	CountryCodeUSA = "US"

	DefaultLanguage = "en"

	centrifugoChannel = "paysuper-billing-server"
)

type Service struct {
	db                         *mongodb.Source
	mx                         sync.Mutex
	cfg                        *config.Config
	ctx                        context.Context
	geo                        proto.GeoIpService
	rep                        repository.RepositoryService
	tax                        tax_service.TaxService
	broker                     rabbitmq.BrokerInterface
	centrifugoClient           *gocent.Client
	redis                      redis.Cmdable
	cacher                     CacheInterface
	curService                 currencies.CurrencyratesService
	smtpCl                     gomail.SendCloser
	supportedCurrencies        []string
	country                    CountryServiceInterface
	project                    *Project
	merchant                   *Merchant
	paymentMethod              PaymentMethodInterface
	priceGroup                 PriceGroupServiceInterface
	paymentSystem              PaymentSystemServiceInterface
	zipCode                    *ZipCode
	paymentChannelCostSystem   *PaymentChannelCostSystem
	paymentChannelCostMerchant *PaymentChannelCostMerchant
	moneyBackCostSystem        *MoneyBackCostSystem
	moneyBackCostMerchant      *MoneyBackCostMerchant
	payoutCostSystem           *PayoutCostSystem
	priceTable                 PriceTableServiceInterface
	productService             ProductServiceInterface
	turnover                   *Turnover
	documentSigner             documentSignerProto.DocumentSignerService
	merchantTariffRates        MerchantTariffRatesInterface
	keyRepository              KeyRepositoryInterface
	dashboardRepository        DashboardRepositoryInterface
}

func newBillingServerResponseError(status int32, message *grpc.ResponseErrorMessage) *grpc.ResponseError {
	return &grpc.ResponseError{
		Status:  status,
		Message: message,
	}
}

func newBillingServerErrorMsg(code, msg string, details ...string) *grpc.ResponseErrorMessage {
	var det string
	if len(details) > 0 && details[0] != "" {
		det = details[0]
	} else {
		det = ""
	}
	return &grpc.ResponseErrorMessage{Code: code, Message: msg, Details: det}
}

func NewBillingService(
	db *mongodb.Source,
	cfg *config.Config,
	geo proto.GeoIpService,
	rep repository.RepositoryService,
	tax tax_service.TaxService,
	broker rabbitmq.BrokerInterface,
	redis redis.Cmdable,
	cache CacheInterface,
	curService currencies.CurrencyratesService,
	documentSigner documentSignerProto.DocumentSignerService,
) *Service {
	return &Service{
		db:             db,
		cfg:            cfg,
		geo:            geo,
		rep:            rep,
		tax:            tax,
		broker:         broker,
		redis:          redis,
		cacher:         cache,
		curService:     curService,
		documentSigner: documentSigner,
	}
}

func (s *Service) Init() (err error) {
	s.paymentMethod = newPaymentMethodService(s)
	s.merchant = newMerchantService(s)
	s.country = newCountryService(s)
	s.project = newProjectService(s)
	s.priceGroup = newPriceGroupService(s)
	s.paymentSystem = newPaymentSystemService(s)
	s.zipCode = newZipCodeService(s)
	s.paymentChannelCostSystem = newPaymentChannelCostSystemService(s)
	s.paymentChannelCostMerchant = newPaymentChannelCostMerchantService(s)
	s.moneyBackCostSystem = newMoneyBackCostSystemService(s)
	s.moneyBackCostMerchant = newMoneyBackCostMerchantService(s)
	s.payoutCostSystem = newPayoutCostSystemService(s)
	s.priceTable = newPriceTableService(s)
	s.productService = newProductService(s)
	s.turnover = newTurnoverService(s)
	s.merchantTariffRates = newMerchantsTariffRatesRepository(s)
	s.keyRepository = newKeyRepository(s)
	s.dashboardRepository = newDashboardRepository(s)

	s.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       s.cfg.CentrifugoURL,
			Key:        s.cfg.CentrifugoApiSecret,
			HTTPClient: tools.NewLoggedHttpClient(zap.S()),
		},
	)

	if s.cfg.AccountingCurrency == "" {
		return errors.New(errorAccountingCurrencyNotFound)
	}

	sCurr, err := s.curService.GetSupportedCurrencies(context.TODO(), &currencies.EmptyRequest{})
	if err != nil {
		zap.S().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "CurrencyRatesService"),
			zap.String(errorFieldMethod, "GetSupportedCurrencies"),
		)

		return err
	}

	s.supportedCurrencies = sCurr.Currencies

	return
}

func (s *Service) logError(msg string, data []interface{}) {
	zap.S().Errorw(msg, data...)
}

func (s *Service) UpdateOrder(ctx context.Context, req *billing.Order, rsp *grpc.EmptyResponse) error {
	err := s.updateOrder(req)

	if err != nil {
		return err
	}

	return nil
}

func (s *Service) UpdateMerchant(ctx context.Context, req *billing.Merchant, rsp *grpc.EmptyResponse) error {
	err := s.merchant.Update(req)

	if err != nil {
		zap.S().Errorf("Update merchant failed", "err", err.Error(), "order", req)
	}

	return nil
}

func (s *Service) IsDbNotFoundError(err error) bool {
	return err.Error() == errorBbNotFoundMessage
}

func (s *Service) getCountryFromAcceptLanguage(acceptLanguage string) (string, string) {
	it := strings.Split(acceptLanguage, ",")

	if strings.Index(it[0], "-") == -1 {
		return "", ""
	}

	it = strings.Split(it[0], "-")

	return strings.ToLower(it[0]), strings.ToUpper(it[1])
}

func (s *Service) sendCentrifugoMessage(msg map[string]interface{}) error {
	b, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	if err = s.centrifugoClient.Publish(context.Background(), centrifugoChannel, b); err != nil {
		return err
	}

	return nil
}

func (s *Service) mgoPipeSort(query []bson.M, sort []string) []bson.M {
	pipeSort := make(bson.M)

	for _, field := range sort {
		n := 1

		if field == "" {
			continue
		}

		sField := strings.Split(field, "")

		if sField[0] == "-" {
			n = -1
			field = field[1:]
		}

		pipeSort[field] = n
	}

	if len(pipeSort) > 0 {
		query = append(query, bson.M{"$sort": pipeSort})
	}

	return query
}

func (s *Service) getDefaultPaymentMethodCommissions() *billing.MerchantPaymentMethodCommissions {
	return &billing.MerchantPaymentMethodCommissions{
		Fee: DefaultPaymentMethodFee,
		PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
			Fee:      DefaultPaymentMethodPerTransactionFee,
			Currency: DefaultPaymentMethodCurrency,
		},
	}
}

func (s *Service) CheckProjectRequestSignature(
	ctx context.Context,
	req *grpc.CheckProjectRequestSignatureRequest,
	rsp *grpc.CheckProjectRequestSignatureResponse,
) error {
	p := &OrderCreateRequestProcessor{
		Service: s,
		request: &billing.OrderCreateRequest{ProjectId: req.ProjectId},
		checked: &orderCreateRequestProcessorChecked{},
	}

	err := p.processProject()

	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err)
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			rsp.Status = pkg.ResponseStatusBadData
			rsp.Message = e
			return nil
		}
		return err
	}

	hashString := req.Body + p.checked.project.SecretKey

	h := sha512.New()
	h.Write([]byte(hashString))

	if hex.EncodeToString(h.Sum(nil)) != req.Signature {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = orderErrorSignatureInvalid

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) getMerchantCentrifugoChannel(merchant *billing.Merchant) string {
	return fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, merchant.Id)
}

func (s *Service) sendMessageToCentrifugo(ctx context.Context, ch string, msg []byte) {
	err := s.centrifugoClient.Publish(ctx, ch, msg)

	if err != nil {
		zap.L().Error(
			"Publish message to centrifugo failed",
			zap.Error(err),
			zap.String("channel", ch),
			zap.ByteString("message", paysuperSignAgreementMessage),
		)
	}
}
