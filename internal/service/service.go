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
	"time"
)

const (
	errorNotFound                   = "%s not found"
	errorQueryMask                  = "Query from collection \"%s\" failed"
	errorAccountingCurrencyNotFound = "Accounting currency not found"

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

var (
	handlers = map[string]func(*Service) Cacher{
		pkg.CollectionCurrency:      newCurrencyHandler,
		pkg.CollectionCountry:       newCountryHandler,
		pkg.CollectionProject:       newProjectHandler,
		pkg.CollectionCurrencyRate:  newCurrencyRateHandler,
		pkg.CollectionPaymentMethod: newPaymentMethodHandler,
		pkg.CollectionCommission:    newCommissionHandler,
		pkg.CollectionMerchant:      newMerchantHandler,
		pkg.CollectionSystemFees:    newSystemFeeHandler,
	}
)

type Service struct {
	db               *database.Source
	mx               sync.Mutex
	cfg              *config.Config
	ctx              context.Context
	geo              proto.GeoIpService
	rep              repository.RepositoryService
	tax              tax_service.TaxService
	broker           *rabbitmq.Broker
	centrifugoClient *gocent.Client
	redis            *redis.Client

	accountingCurrency *billing.Currency

	currencyCache        map[string]*billing.Currency
	countryCache         map[string]*billing.Country
	projectCache         map[string]*billing.Project
	currencyRateCache    map[int32]map[int32]*billing.CurrencyRate
	paymentMethodCache   map[string]map[int32]*billing.PaymentMethod
	paymentMethodIdCache map[string]*billing.PaymentMethod

	merchantCache          map[string]*billing.Merchant
	merchantPaymentMethods map[string]map[string]*billing.MerchantPaymentMethod

	commissionCache map[string]map[string]*billing.MerchantPaymentMethodCommissions
	systemFeesCache map[string]map[string]map[string]*billing.SystemFees

	rebuild      bool
	rebuildError error

	wg               sync.WaitGroup
	exitCacheRebuild chan bool
}

type Cacher interface {
	getAll() ([]interface{}, error)
	setCache([]interface{})
}

func NewBillingService(
	db *database.Source,
	cfg *config.Config,
	geo proto.GeoIpService,
	rep repository.RepositoryService,
	tax tax_service.TaxService,
	broker *rabbitmq.Broker,
	redis *redis.Client,
) *Service {
	return &Service{
		db:               db,
		cfg:              cfg,
		geo:              geo,
		rep:              rep,
		tax:              tax,
		broker:           broker,
		redis:            redis,
		wg:               sync.WaitGroup{},
		exitCacheRebuild: make(chan bool, 1),
	}
}

func (s *Service) Init() (err error) {
	err = s.initCache()

	if err != nil {
		return
	}

	s.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       s.cfg.CentrifugoURL,
			Key:        s.cfg.CentrifugoSecret,
			HTTPClient: tools.NewLoggedHttpClient(zap.S()),
		},
	)

	s.accountingCurrency, err = s.GetCurrencyByCodeA3(s.cfg.AccountingCurrency)

	if err != nil {
		return errors.New(errorAccountingCurrencyNotFound)
	}

	s.wg.Add(1)
	go s.reBuildCache()

	return
}

func (s *Service) Shutdown() {
	zap.S().Info("Shutting down GRPC serivce...")
	s.exitCacheRebuild <- true
	s.wg.Wait()
}

func (s *Service) reBuildCache() {
	defer s.wg.Done()

	var err error
	var key string

	curTicker := time.NewTicker(time.Second * time.Duration(s.cfg.CurrencyTimeout))
	countryTicker := time.NewTicker(time.Second * time.Duration(s.cfg.CountryTimeout))
	projectTicker := time.NewTicker(time.Second * time.Duration(s.cfg.ProjectTimeout))
	currencyRateTicker := time.NewTicker(time.Second * time.Duration(s.cfg.CurrencyRateTimeout))
	paymentMethodTicker := time.NewTicker(time.Second * time.Duration(s.cfg.PaymentMethodTimeout))
	commissionTicker := time.NewTicker(time.Second * time.Duration(s.cfg.CommissionTimeout))
	systemFeesTimer := time.NewTicker(time.Second * time.Duration(s.cfg.SystemFeesTimeout))

	s.rebuild = true

	for {
		select {
		case <-curTicker.C:
			err = s.cache(pkg.CollectionCurrency, handlers[pkg.CollectionCurrency](s))
			key = pkg.CollectionCurrency
		case <-countryTicker.C:
			err = s.cache(pkg.CollectionCountry, handlers[pkg.CollectionCountry](s))
			key = pkg.CollectionCountry
		case <-projectTicker.C:
			err = s.cache(pkg.CollectionProject, handlers[pkg.CollectionProject](s))
			key = pkg.CollectionProject
		case <-currencyRateTicker.C:
			err = s.cache(pkg.CollectionCurrencyRate, handlers[pkg.CollectionCurrencyRate](s))
			key = pkg.CollectionCurrencyRate
		case <-paymentMethodTicker.C:
			err = s.cache(pkg.CollectionPaymentMethod, handlers[pkg.CollectionPaymentMethod](s))
			key = pkg.CollectionPaymentMethod
		case <-commissionTicker.C:
			err = s.cache(pkg.CollectionCommission, handlers[pkg.CollectionCommission](s))
			key = pkg.CollectionCommission
		case <-systemFeesTimer.C:
			s.mx.Lock()
			s.systemFeesCache = make(map[string]map[string]map[string]*billing.SystemFees)
			s.mx.Unlock()
		case <-s.exitCacheRebuild:
			s.rebuild = false
			zap.S().Info("Cache rebuilding stopped")
			return
		}

		if err != nil {
			s.rebuild = false
			s.rebuildError = err

			zap.S().Errorw("Rebuild cache failed", "error", err, "cached_collection", key)
		}
	}
}

func (s *Service) cache(key string, handler Cacher) error {
	rec, err := handler.getAll()

	if err != nil {
		return err
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	handler.setCache(rec)

	return nil
}

func (s *Service) initCache() error {
	for k, handler := range handlers {
		err := s.cache(k, handler(s))

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) isProductionEnvironment() bool {
	return s.cfg.Environment == environmentProd
}

func (s *Service) logError(msg string, data []interface{}) {
	zap.S().Errorw(fmt.Sprintf("[PAYSUPER_BILLING] %s", msg), data...)
}

func (s *Service) RebuildCache(ctx context.Context, req *grpc.EmptyRequest, res *grpc.EmptyResponse) error {
	return nil
}

func (s *Service) UpdateOrder(ctx context.Context, req *billing.Order, rsp *grpc.EmptyResponse) error {
	err := s.updateOrder(req)

	if err != nil {
		return err
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
	rate, err := s.Convert(req.From, req.To, 1)

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
