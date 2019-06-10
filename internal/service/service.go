package service

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ProtocolONE/geoip-service/pkg/proto"
	"github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/centrifugal/gocent"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"github.com/paysuper/paysuper-tax-service/proto"
	"go.uber.org/zap"
	"strings"
	"sync"
)

const (
	errorNotFound                   = "[PAYONE_BILLING] %s not found"
	errorQueryMask                  = "[PAYONE_BILLING] Query from collection \"%s\" failed"
	errorAccountingCurrencyNotFound = "[PAYONE_BILLING] Accounting currency not found"
	errorInterfaceCast              = "[PAYONE_BILLING] Unable to cast interface to object %s"

	errorBbNotFoundMessage = "not found"

	environmentProd = "prod"

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
	db               *database.Source
	mx               sync.Mutex
	cfg              *config.Config
	exitCh           chan bool
	ctx              context.Context
	geo              proto.GeoIpService
	rep              repository.RepositoryService
	tax              tax_service.TaxService
	broker           *rabbitmq.Broker
	centrifugoClient *gocent.Client
	redis            *redis.Client
	cacher           CacheInterface

	accountingCurrency *billing.Currency

	rebuild      bool
	rebuildError error

	currency      *Currency
	currencyRate  *CurrencyRate
	commission    *Commission
	country       *Country
	project       *Project
	merchant      *Merchant
	paymentMethod *PaymentMethod
	systemFees    *SystemFee
}

func NewBillingService(
	db *database.Source,
	cfg *config.Config,
	exitCh chan bool,
	geo proto.GeoIpService,
	rep repository.RepositoryService,
	tax tax_service.TaxService,
	broker *rabbitmq.Broker,
	redis *redis.Client,
	cache CacheInterface,
) *Service {
	s := &Service{
		db:     db,
		cfg:    cfg,
		exitCh: exitCh,
		geo:    geo,
		rep:    rep,
		tax:    tax,
		broker: broker,
		redis:  redis,
		cacher: cache,
	}

	return s
}

func (s *Service) Init() (err error) {
	s.paymentMethod = newPaymentMethodService(s)
	s.merchant = newMerchantService(s)
	s.currency = newCurrencyService(s)
	s.currencyRate = newCurrencyRateService(s)
	s.commission = newCommissionService(s)
	s.country = newCountryService(s)
	s.project = newProjectService(s)
	s.systemFees = newSystemFeesService(s)

	s.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       s.cfg.CentrifugoURL,
			Key:        s.cfg.CentrifugoSecret,
			HTTPClient: tools.NewLoggedHttpClient(zap.S()),
		},
	)

	s.accountingCurrency, err = s.currency.GetByCodeA3(s.cfg.AccountingCurrency)

	if err != nil {
		return errors.New(errorAccountingCurrencyNotFound)
	}

	return
}

func (s *Service) isProductionEnvironment() bool {
	return s.cfg.Environment == environmentProd
}

func (s *Service) logError(msg string, data []interface{}) {
	zap.S().Errorw(fmt.Sprintf("[PAYSUPER_BILLING] %s", msg), data...)
}

func (s *Service) UpdateOrder(ctx context.Context, req *billing.Order, rsp *grpc.EmptyResponse) error {
	err := s.db.Collection(pkg.CollectionOrder).UpdateId(bson.ObjectIdHex(req.Id), req)

	if err != nil {
		s.logError("Update order failed", []interface{}{"error", err.Error(), "order", req})
	}

	return nil
}

func (s *Service) UpdateMerchant(ctx context.Context, req *billing.Merchant, rsp *grpc.EmptyResponse) error {
	err := s.db.Collection(pkg.CollectionMerchant).UpdateId(bson.ObjectIdHex(req.Id), req)

	if err != nil {
		s.logError("Update merchant failed", []interface{}{"error", err.Error(), "order", req})
	}

	return nil
}

func (s *Service) GetConvertRate(ctx context.Context, req *grpc.ConvertRateRequest, rsp *grpc.ConvertRateResponse) error {
	rate, err := s.currencyRate.Convert(req.From, req.To, 1)

	if err != nil {
		s.logError("Get convert rate failed", []interface{}{"error", err.Error(), "from", req.From, "to", req.To})
	} else {
		rsp.Rate = rate
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
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = err.Error()

		return nil
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
