package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"strings"
)

const (
	cachePaymentMethodId                     = "payment_method:id:%s"
	cachePaymentMethodGroup                  = "payment_method:group:%s"
	cachePaymentMethodAll                    = "payment_method:all"
	cachePaymentMethodModeCurrencyMccCompany = "payment_method:mode:%s:currency:%s:mcc:%s:oc:%s"

	collectionPaymentMethod = "payment_method"

	paymentMethodErrorPaymentSystem              = "payment method must contain of payment system"
	paymentMethodErrorUnknownMethod              = "payment method is unknown"
	paymentMethodErrorNotFoundProductionSettings = "payment method is not contain requesting settings"

	fieldTestSettings       = "test_settings"
	fieldProductionSettings = "production_settings"
)

type PaymentMethods struct {
	PaymentMethods []*billingpb.PaymentMethod `json:"payment_methods"`
}

func (s *Service) CreateOrUpdatePaymentMethod(
	ctx context.Context,
	req *billingpb.PaymentMethod,
	rsp *billingpb.ChangePaymentMethodResponse,
) error {
	var pm *billingpb.PaymentMethod
	var err error

	if _, err = s.paymentSystem.GetById(ctx, req.PaymentSystemId); err != nil {
		zap.S().Errorf("Invalid payment system id for update payment method", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = paymentMethodErrorPaymentSystem

		return nil
	}

	if req.Id != "" {
		pm, err = s.paymentMethod.GetById(ctx, req.Id)

		if err != nil {
			zap.S().Errorf("Invalid id of payment method", "err", err.Error(), "data", req)
			rsp.Status = billingpb.ResponseStatusNotFound
			rsp.Message = err.Error()

			return nil
		}
	}

	if req.IsActive == true && req.IsValid() == false {
		zap.S().Errorf("Set all parameters of the payment method before its activation", "data", req)
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = paymentMethodErrorPaymentSystem

		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()

	if pm == nil {
		req.CreatedAt = ptypes.TimestampNow()
		err = s.paymentMethod.Insert(ctx, req)
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
		err = s.paymentMethod.Update(ctx, pm)
	}

	if err != nil {
		zap.S().Errorf("Query to insert|update project method is failed", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

func (s *Service) CreateOrUpdatePaymentMethodProductionSettings(
	ctx context.Context,
	req *billingpb.ChangePaymentMethodParamsRequest,
	rsp *billingpb.ChangePaymentMethodParamsResponse,
) error {
	var pm *billingpb.PaymentMethod
	var err error

	pm, err = s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for update production settings", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if pm.ProductionSettings == nil {
		pm.ProductionSettings = map[string]*billingpb.PaymentMethodParams{}
	}

	brands := []string{}
	for _, brand := range req.Params.Brand {
		brands = append(brands, strings.ToUpper(brand))
	}

	settings := &billingpb.PaymentMethodParams{
		Currency:           strings.ToUpper(req.Params.Currency),
		Secret:             req.Params.Secret,
		SecretCallback:     req.Params.SecretCallback,
		TerminalId:         req.Params.TerminalId,
		MccCode:            req.Params.MccCode,
		OperatingCompanyId: strings.ToLower(req.Params.OperatingCompanyId),
		Brand:              brands,
	}

	key := billingpb.GetPaymentMethodKey(req.Params.Currency, req.Params.MccCode, req.Params.OperatingCompanyId, "")
	pm.ProductionSettings[key] = settings

	for _, brand := range req.Params.Brand {
		key := billingpb.GetPaymentMethodKey(req.Params.Currency, req.Params.MccCode, req.Params.OperatingCompanyId, brand)
		pm.ProductionSettings[key] = settings
	}

	if err := s.paymentMethod.Update(ctx, pm); err != nil {
		zap.S().Errorf("Query to update production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

func (s *Service) GetPaymentMethodProductionSettings(
	ctx context.Context,
	req *billingpb.GetPaymentMethodSettingsRequest,
	rsp *billingpb.GetPaymentMethodSettingsResponse,
) error {
	pm, err := s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Query to get production settings of project method is failed", "err", err.Error(), "data", req)
		return nil
	}

	check := make(map[string]bool)

	for _, param := range pm.ProductionSettings {

		key := billingpb.GetPaymentMethodKey(param.Currency, param.MccCode, param.OperatingCompanyId, "")

		if check[key] == true {
			continue
		}

		check[key] = true

		rsp.Params = append(rsp.Params, &billingpb.PaymentMethodParams{
			Currency:           param.Currency,
			TerminalId:         param.TerminalId,
			Secret:             param.Secret,
			SecretCallback:     param.SecretCallback,
			MccCode:            param.MccCode,
			OperatingCompanyId: param.OperatingCompanyId,
			Brand:              param.Brand,
		})
	}

	return nil
}

func (s *Service) DeletePaymentMethodProductionSettings(
	ctx context.Context,
	req *billingpb.GetPaymentMethodSettingsRequest,
	rsp *billingpb.ChangePaymentMethodParamsResponse,
) error {
	pm, err := s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for delete production settings", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	deleteKeys := []string{}

	key := billingpb.GetPaymentMethodKey(req.CurrencyA3, req.MccCode, req.OperatingCompanyId, "")
	deleteKeys = append(deleteKeys, key)

	setting, ok := pm.ProductionSettings[key]
	if !ok {
		zap.S().Errorf("Unable to get production settings for currency", "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorNotFoundProductionSettings

		return nil
	}

	for _, brand := range setting.Brand {
		key := billingpb.GetPaymentMethodKey(req.CurrencyA3, req.MccCode, req.OperatingCompanyId, brand)
		deleteKeys = append(deleteKeys, key)
	}

	for _, key := range deleteKeys {
		delete(pm.ProductionSettings, key)
	}

	if err := s.paymentMethod.Update(ctx, pm); err != nil {
		zap.S().Errorf("Query to delete production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

func (s *Service) CreateOrUpdatePaymentMethodTestSettings(
	ctx context.Context,
	req *billingpb.ChangePaymentMethodParamsRequest,
	rsp *billingpb.ChangePaymentMethodParamsResponse,
) error {
	var pm *billingpb.PaymentMethod
	var err error

	pm, err = s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for update production settings", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	if pm.TestSettings == nil {
		pm.TestSettings = map[string]*billingpb.PaymentMethodParams{}
	}

	brands := []string{}
	for _, brand := range req.Params.Brand {
		brands = append(brands, strings.ToUpper(brand))
	}

	settings := &billingpb.PaymentMethodParams{
		Currency:           strings.ToUpper(req.Params.Currency),
		Secret:             req.Params.Secret,
		SecretCallback:     req.Params.SecretCallback,
		TerminalId:         req.Params.TerminalId,
		MccCode:            req.Params.MccCode,
		OperatingCompanyId: strings.ToLower(req.Params.OperatingCompanyId),
		Brand:              brands,
	}

	key := billingpb.GetPaymentMethodKey(req.Params.Currency, req.Params.MccCode, req.Params.OperatingCompanyId, "")
	pm.TestSettings[key] = settings

	for _, brand := range req.Params.Brand {
		key := billingpb.GetPaymentMethodKey(req.Params.Currency, req.Params.MccCode, req.Params.OperatingCompanyId, brand)
		pm.TestSettings[key] = settings
	}

	if err := s.paymentMethod.Update(ctx, pm); err != nil {
		zap.S().Errorf("Query to update production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

func (s *Service) GetPaymentMethodTestSettings(
	ctx context.Context,
	req *billingpb.GetPaymentMethodSettingsRequest,
	rsp *billingpb.GetPaymentMethodSettingsResponse,
) error {
	pm, err := s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Query to get production settings of project method is failed", "err", err.Error(), "data", req)
		return nil
	}

	check := make(map[string]bool)

	for _, param := range pm.TestSettings {

		key := billingpb.GetPaymentMethodKey(param.Currency, param.MccCode, param.OperatingCompanyId, "")

		if check[key] == true {
			continue
		}

		check[key] = true

		rsp.Params = append(rsp.Params, &billingpb.PaymentMethodParams{
			Currency:           param.Currency,
			TerminalId:         param.TerminalId,
			Secret:             param.Secret,
			SecretCallback:     param.SecretCallback,
			MccCode:            param.MccCode,
			OperatingCompanyId: param.OperatingCompanyId,
			Brand:              param.Brand,
		})
	}

	return nil
}

func (s *Service) DeletePaymentMethodTestSettings(
	ctx context.Context,
	req *billingpb.GetPaymentMethodSettingsRequest,
	rsp *billingpb.ChangePaymentMethodParamsResponse,
) error {
	pm, err := s.paymentMethod.GetById(ctx, req.PaymentMethodId)
	if err != nil {
		zap.S().Errorf("Unable to get payment method for delete production settings", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorUnknownMethod

		return nil
	}

	deleteKeys := []string{}

	key := billingpb.GetPaymentMethodKey(req.CurrencyA3, req.MccCode, req.OperatingCompanyId, "")
	deleteKeys = append(deleteKeys, key)

	setting, ok := pm.TestSettings[key]
	if !ok {
		zap.S().Errorf("Unable to get production settings for currency", "data", req)
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = paymentMethodErrorNotFoundProductionSettings

		return nil
	}

	for _, brand := range setting.Brand {
		key := billingpb.GetPaymentMethodKey(req.CurrencyA3, req.MccCode, req.OperatingCompanyId, brand)
		deleteKeys = append(deleteKeys, key)
	}

	for _, key := range deleteKeys {
		delete(pm.ProductionSettings, key)
	}

	if err := s.paymentMethod.Update(ctx, pm); err != nil {
		zap.S().Errorf("Query to delete production settings of project method is failed", "err", err.Error(), "data", req)
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.Error()

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	return nil
}

type PaymentMethodInterface interface {
	GetAll(ctx context.Context) (map[string]*billingpb.PaymentMethod, error)
	GetByGroupAndCurrency(ctx context.Context, isProduction bool, group string, currency string) (*billingpb.PaymentMethod, error)
	GetById(context.Context, string) (*billingpb.PaymentMethod, error)
	MultipleInsert(context.Context, []*billingpb.PaymentMethod) error
	Insert(context.Context, *billingpb.PaymentMethod) error
	Update(context.Context, *billingpb.PaymentMethod) error
	GetPaymentSettings(paymentMethod *billingpb.PaymentMethod, currency, mccCode, operatingCompanyId, paymentMethodBrand string, isProduction bool) (*billingpb.PaymentMethodParams, error)
	ListByOrder(ctx context.Context, order *billingpb.Order) ([]*billingpb.PaymentMethod, error)
}

type paymentMethods struct {
	Methods map[string]*billingpb.PaymentMethod
}

func newPaymentMethodService(svc *Service) *PaymentMethod {
	s := &PaymentMethod{svc: svc}
	return s
}

func (h *PaymentMethod) GetAll(ctx context.Context) (map[string]*billingpb.PaymentMethod, error) {
	var c paymentMethods
	key := cachePaymentMethodAll

	if err := h.svc.cacher.Get(key, c); err != nil {
		var data []*billingpb.PaymentMethod

		cursor, err := h.svc.db.Collection(collectionPaymentMethod).Find(ctx, bson.M{})

		if err != nil {
			return nil, err
		}

		err = cursor.All(ctx, &data)

		if err != nil {
			return nil, err
		}

		pool := make(map[string]*billingpb.PaymentMethod, len(data))
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
	ctx context.Context,
	isProduction bool,
	group string,
	currency string,
) (*billingpb.PaymentMethod, error) {
	var c *billingpb.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodGroup, group)
	err := h.svc.cacher.Get(key, &c)

	if err == nil {
		return c, nil
	}

	field := fieldTestSettings

	if isProduction {
		field = fieldProductionSettings
	}

	query := bson.M{
		"group_alias": group,
		field: bson.M{
			"$elemMatch": bson.M{
				"currency": currency,
			},
		},
	}
	err = h.svc.db.Collection(collectionPaymentMethod).FindOne(ctx, query).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return c, nil
}

func (h *PaymentMethod) GetById(ctx context.Context, id string) (*billingpb.PaymentMethod, error) {
	var c billingpb.PaymentMethod
	key := fmt.Sprintf(cachePaymentMethodId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionPaymentMethod).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPaymentMethod)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h *PaymentMethod) MultipleInsert(ctx context.Context, pm []*billingpb.PaymentMethod) error {
	pms := make([]interface{}, len(pm))
	for i, v := range pm {
		pms[i] = v
	}

	_, err := h.svc.db.Collection(collectionPaymentMethod).InsertMany(ctx, pms)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) Insert(ctx context.Context, pm *billingpb.PaymentMethod) error {
	_, err := h.svc.db.Collection(collectionPaymentMethod).InsertOne(ctx, pm)

	if err != nil {
		return err
	}

	err = h.resetCaches(pm)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentMethodId, pm.Id), pm, 0); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) Update(ctx context.Context, pm *billingpb.PaymentMethod) error {
	oid, _ := primitive.ObjectIDFromHex(pm.Id)
	filter := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionPaymentMethod).FindOneAndReplace(ctx, filter, pm).Err()

	if err != nil {
		return err
	}

	err = h.resetCaches(pm)
	if err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePaymentMethodId, pm.Id), pm, 0); err != nil {
		return err
	}

	return nil
}

func (h *PaymentMethod) GetPaymentSettings(
	paymentMethod *billingpb.PaymentMethod,
	currency string,
	mccCode string,
	operatingCompanyId string,
	paymentMethodBrand string,
	isProduction bool,
) (*billingpb.PaymentMethodParams, error) {
	settings := paymentMethod.TestSettings

	if isProduction == true {
		settings = paymentMethod.ProductionSettings
	}

	if settings == nil {
		return nil, orderErrorPaymentMethodEmptySettings
	}

	key := billingpb.GetPaymentMethodKey(currency, mccCode, operatingCompanyId, paymentMethodBrand)

	setting, ok := settings[key]

	if !ok || !setting.IsSettingComplete() {
		return nil, orderErrorPaymentMethodEmptySettings
	}

	setting.Currency = currency

	if isProduction == true {
		setting.ApiUrl = h.svc.cfg.CardPayApiUrl
	} else {
		setting.ApiUrl = h.svc.cfg.CardPayApiSandboxUrl
	}

	return setting, nil
}

func (h *PaymentMethod) ListByOrder(
	ctx context.Context,
	order *billingpb.Order,
) ([]*billingpb.PaymentMethod, error) {
	val := new(PaymentMethods)

	field := fieldTestSettings

	if order.IsProduction {
		field = fieldProductionSettings
	}

	key := fmt.Sprintf(cachePaymentMethodModeCurrencyMccCompany, field, order.Currency, order.MccCode, order.OperatingCompanyId)
	err := h.svc.cacher.Get(key, &val)

	if err == nil {
		zap.S().Infow("Found payment methods in cache", "key", key, "data", val.PaymentMethods)
		return val.PaymentMethods, nil
	}

	query := bson.M{
		"is_active": true,
		field: bson.M{
			"$elemMatch": bson.M{
				"currency":             order.Currency,
				"mcc_code":             order.MccCode,
				"operating_company_id": order.OperatingCompanyId,
			},
		},
	}

	zap.S().Infow("Find payment methods", "query", query)
	cursor, err := h.svc.db.Collection(collectionPaymentMethod).Find(ctx, query)

	if err != nil {
		if err != mongo.ErrNoDocuments {
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

	err = cursor.All(ctx, &val.PaymentMethods)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMethod),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, orderErrorUnknown
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

func (h *PaymentMethod) resetCaches(pm *billingpb.PaymentMethod) error {
	if err := h.svc.cacher.Delete(cachePaymentMethodAll); err != nil {
		return err
	}

	key := fmt.Sprintf(cachePaymentMethodId, pm.Id)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	key = fmt.Sprintf(cachePaymentMethodGroup, pm.Group)
	if err := h.svc.cacher.Delete(key); err != nil {
		return err
	}

	for _, param := range pm.TestSettings {
		key := fmt.Sprintf(cachePaymentMethodModeCurrencyMccCompany, fieldTestSettings, param.Currency, param.MccCode, param.OperatingCompanyId)
		if err := h.svc.cacher.Delete(key); err != nil {
			return err
		}
	}

	for _, param := range pm.ProductionSettings {
		key := fmt.Sprintf(cachePaymentMethodModeCurrencyMccCompany, fieldProductionSettings, param.Currency, param.MccCode, param.OperatingCompanyId)
		if err := h.svc.cacher.Delete(key); err != nil {
			return err
		}
	}

	return nil
}
