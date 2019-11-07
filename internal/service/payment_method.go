package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	cachePaymentMethodId       = "payment_method:id:%s"
	cachePaymentMethodGroup    = "payment_method:group:%s"
	cachePaymentMethodAll      = "payment_method:all"
	cachePaymentMethodCurrency = "payment_method:currency:%s"

	collectionPaymentMethod = "payment_method"

	paymentMethodErrorPaymentSystem              = "payment method must contain of payment system"
	paymentMethodErrorUnknownMethod              = "payment method is unknown"
	paymentMethodErrorNotFoundProductionSettings = "payment method is not contain requesting settings"
)

type PaymentMethods struct {
	PaymentMethods []*billing.PaymentMethod `json:"payment_methods"`
}

func (s *Service) CreateOrUpdatePaymentMethod(
	ctx context.Context,
	req *billing.PaymentMethod,
	rsp *grpc.ChangePaymentMethodResponse,
) error {
	var pm *billing.PaymentMethod
	var err error

	if _, err = s.paymentSystem.GetById(req.PaymentSystemId); err != nil {
		zap.S().Errorf("Invalid payment system id for update payment method", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorPaymentSystem

		return nil
	}

	if req.Id != "" {
		pm, err = s.paymentMethod.GetById(req.Id)

		if err != nil {
			zap.S().Errorf("Invalid id of payment method", "err", err.Error(), "data", req)
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = err.Error()

			return nil
		}
	}

	if req.IsActive == true && req.IsValid() == false {
		zap.S().Errorf("Set all parameters of the payment method before its activation", "data", req)
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = paymentMethodErrorPaymentSystem

		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()

	if pm == nil {
		req.CreatedAt = ptypes.TimestampNow()
		err = s.paymentMethod.Insert(req)
	} else {
		pm.ExternalId = req.ExternalId
		pm.TestSettings = req.TestSettings
		pm.ProductionSettings = req.ProductionSettings
		pm.Name = req.Name
		pm.IsActive = req.IsActive
		pm.Group = req.Group
		pm.Type = req.Type
		pm.AccountRegexp = req.AccountRegexp
		pm.MaxPaymentAmount = req.MaxPaymentAmount
		pm.MinPaymentAmount = req.MinPaymentAmount
		err = s.paymentMethod.Update(pm)
	}

	if err != nil {
		zap.S().Errorf("Query to insert|update project method is failed", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) CreateOrUpdatePaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.ChangePaymentMethodParamsRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	var pm *billing.PaymentMethod
	var err error

	pm, err = s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for update production settings", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if pm.ProductionSettings == nil {
		pm.ProductionSettings = map[string]*billing.PaymentMethodParams{}
	}

	pm.ProductionSettings[req.Params.Currency] = &billing.PaymentMethodParams{
		Currency:           req.Params.Currency,
		Secret:             req.Params.Secret,
		SecretCallback:     req.Params.SecretCallback,
		TerminalId:         req.Params.TerminalId,
		MccCode:            req.Params.MccCode,
		OperatingCompanyId: req.Params.OperatingCompanyId,
	}
	if err := s.paymentMethod.Update(pm); err != nil {
		zap.S().Errorf("Query to update production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetPaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodSettingsRequest,
	rsp *grpc.GetPaymentMethodSettingsResponse,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Query to get production settings of project method is failed", "err", err.Error(), "data", req)
		return nil
	}

	for key, param := range pm.ProductionSettings {
		rsp.Params = append(rsp.Params, &billing.PaymentMethodParams{
			Currency:           key,
			TerminalId:         param.TerminalId,
			Secret:             param.Secret,
			SecretCallback:     param.SecretCallback,
			MccCode:            param.MccCode,
			OperatingCompanyId: param.OperatingCompanyId,
		})
	}

	return nil
}

func (s *Service) DeletePaymentMethodProductionSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodSettingsRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for delete production settings", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if _, ok := pm.ProductionSettings[req.CurrencyA3]; !ok {
		zap.S().Errorf("Unable to get production settings for currency", "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorNotFoundProductionSettings

		return nil
	}

	delete(pm.ProductionSettings, req.CurrencyA3)

	if err := s.paymentMethod.Update(pm); err != nil {
		zap.S().Errorf("Query to delete production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) CreateOrUpdatePaymentMethodTestSettings(
	ctx context.Context,
	req *grpc.ChangePaymentMethodParamsRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	var pm *billing.PaymentMethod
	var err error

	pm, err = s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for update production settings", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if pm.TestSettings == nil {
		pm.TestSettings = map[string]*billing.PaymentMethodParams{}
	}

	pm.TestSettings[req.Params.Currency] = &billing.PaymentMethodParams{
		Currency:           req.Params.Currency,
		Secret:             req.Params.Secret,
		SecretCallback:     req.Params.SecretCallback,
		TerminalId:         req.Params.TerminalId,
		MccCode:            req.Params.MccCode,
		OperatingCompanyId: req.Params.OperatingCompanyId,
	}
	if err := s.paymentMethod.Update(pm); err != nil {
		zap.S().Errorf("Query to update production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetPaymentMethodTestSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodSettingsRequest,
	rsp *grpc.GetPaymentMethodSettingsResponse,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Query to get production settings of project method is failed", "err", err.Error(), "data", req)
		return nil
	}

	for key, param := range pm.TestSettings {
		rsp.Params = append(rsp.Params, &billing.PaymentMethodParams{
			Currency:           key,
			TerminalId:         param.TerminalId,
			Secret:             param.Secret,
			SecretCallback:     param.SecretCallback,
			MccCode:            param.MccCode,
			OperatingCompanyId: param.OperatingCompanyId,
		})
	}

	return nil
}

func (s *Service) DeletePaymentMethodTestSettings(
	ctx context.Context,
	req *grpc.GetPaymentMethodSettingsRequest,
	rsp *grpc.ChangePaymentMethodParamsResponse,
) error {
	pm, err := s.paymentMethod.GetById(req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for delete production settings", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if _, ok := pm.TestSettings[req.CurrencyA3]; !ok {
		zap.S().Errorf("Unable to get production settings for currency", "data", req)
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorNotFoundProductionSettings

		return nil
	}

	delete(pm.TestSettings, req.CurrencyA3)

	if err := s.paymentMethod.Update(pm); err != nil {
		zap.S().Errorf("Query to delete production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}

type PaymentMethodInterface interface {
	GetAll() (map[string]*billing.PaymentMethod, error)
	GetByGroupAndCurrency(project *billing.Project, group string, currency string) (*billing.PaymentMethod, error)
	GetById(string) (*billing.PaymentMethod, error)
	MultipleInsert([]*billing.PaymentMethod) error
	Insert(*billing.PaymentMethod) error
	Update(*billing.PaymentMethod) error
	GetPaymentSettings(paymentMethod *billing.PaymentMethod, currency, mccCode, operatingCompanyId string, project *billing.Project) (*billing.PaymentMethodParams, error)
	ListByParams(project *billing.Project, currency, mccCode, operatingCompanyId string) ([]*billing.PaymentMethod, error)
}

type paymentMethods struct {
	Methods map[string]*billing.PaymentMethod
}

func newPaymentMethodService(svc *Service) *PaymentMethod {
	s := &PaymentMethod{svc: svc}
	return s
}

func (h *PaymentMethod) GetAll() (map[string]*billing.PaymentMethod, error) {
	var c paymentMethods
	key := cachePaymentMethodAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		var data []*billing.PaymentMethod

		err = h.svc.db.Collection(collectionPaymentMethod).Find(bson.M{}).All(&data)
		if err != nil {
			return nil, err
		}

		pool := make(map[string]*billing.PaymentMethod, len(data))
		for _, v := range data {
			pool[v.Id] = v
		}
		c.Methods = pool
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return c.Methods, nil
}

func (h *PaymentMethod) GetByGroupAndCurrency(
	project *billing.Project,
	group string,
	currency string,
) (*billing.PaymentMethod, error) {
	var c *billing.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodGroup, group)
	err := h.svc.cacher.Get(key, &c)

	if err == nil {
		return c, nil
	}

	field := "test_settings"

	if project.IsProduction() {
		field = "production_settings"
	}

	query := bson.M{
		"group_alias": group,
		field: bson.M{
			"$elemMatch": bson.M{
				"currency": currency,
			},
		},
	}
	err = h.svc.db.Collection(collectionPaymentMethod).Find(query).One(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return c, nil
}

func (h *PaymentMethod) GetById(id string) (*billing.PaymentMethod, error) {
	var c billing.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionPaymentMethod).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h *PaymentMethod) MultipleInsert(pm []*billing.PaymentMethod) error {
	pms := make([]interface{}, len(pm))
	for i, v := range pm {
		pms[i] = v
	}

	if err := h.svc.db.Collection(collectionPaymentMethod).Insert(pms...); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) Insert(pm *billing.PaymentMethod) error {
	if err := h.svc.db.Collection(collectionPaymentMethod).Insert(pm); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentMethodId, pm.Id), pm, 0); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) Update(pm *billing.PaymentMethod) error {
	err := h.svc.db.Collection(collectionPaymentMethod).UpdateId(bson.ObjectIdHex(pm.Id), pm)
	if err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentMethodId, pm.Id), pm, 0); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) GetPaymentSettings(
	paymentMethod *billing.PaymentMethod,
	currency string,
	mccCode string,
	operatingCompanyId string,
	project *billing.Project,
) (*billing.PaymentMethodParams, error) {
	settings := paymentMethod.TestSettings

	if project.IsProduction() == true {
		settings = paymentMethod.ProductionSettings
	}

	if settings == nil {
		return nil, orderErrorPaymentMethodEmptySettings
	}

	key := fmt.Sprintf(pkg.PaymentMethodKey, currency, mccCode, operatingCompanyId)

	setting, ok := settings[key]

	if !ok || !setting.IsSettingComplete() {
		return nil, orderErrorPaymentMethodEmptySettings
	}

	setting.Currency = currency

	if project.IsProduction() == true {
		setting.ApiUrl = h.svc.cfg.CardPayApiUrl
	} else {
		setting.ApiUrl = h.svc.cfg.CardPayApiSandboxUrl
	}

	return setting, nil
}

func (h *PaymentMethod) ListByParams(project *billing.Project, currency, mccCode, operatingCompanyId string) ([]*billing.PaymentMethod, error) {
	val := new(PaymentMethods)
	key := fmt.Sprintf(cachePaymentMethodCurrency, currency)
	err := h.svc.cacher.Get(key, &val)

	if err == nil {
		return val.PaymentMethods, nil
	}

	field := "test_settings"

	if project.IsProduction() {
		field = "production_settings"
	}

	query := bson.M{
		field: bson.M{
			"$elemMatch": bson.M{
				"currency":             currency,
				"mcc_code":             mccCode,
				"operating_company_id": operatingCompanyId,
			},
		},
	}
	err = h.svc.db.Collection(collectionPaymentMethod).Find(query).All(&val.PaymentMethods)

	if err != nil {
		if err != mgo.ErrNotFound {
			zap.L().Error(
				pkg.ErrorDatabaseQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMethod),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)

			return nil, orderErrorUnknown
		}

		return nil, orderErrorPaymentMethodsNotFound
	}

	err = h.svc.cacher.Set(key, val, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, val),
		)
		return nil, orderErrorUnknown
	}

	return val.PaymentMethods, nil
}
