package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	cacheMerchantId = "merchant:id:%s"

	collectionMerchant                     = "merchant"
	collectionMerchantPaymentMethodHistory = "payment_method_history"
)

type MerchantRepositoryInterface interface {
	Update(merchant *billing.Merchant) error
	Insert(merchant *billing.Merchant) error
	Upsert(merchant *billing.Merchant) error
	MultipleInsert(merchants []*billing.Merchant) error
	GetById(id string) (*billing.Merchant, error)
	GetPaymentMethod(merchantId string, method string) (*billing.MerchantPaymentMethod, error)
	GetPaymentMethodTerminalId(merchantId, pmId string) (string, error)
	GetPaymentMethodTerminalPassword(merchantId, pmId string) (string, error)
	GetPaymentMethodTerminalCallbackPassword(merchantId, pmId string) (string, error)
}

func newMerchantService(svc *Service) MerchantRepositoryInterface {
	return &Merchant{svc: svc}
}

func (h *Merchant) Update(merchant *billing.Merchant) error {
	err := h.svc.db.Collection(collectionMerchant).UpdateId(bson.ObjectIdHex(merchant.Id), merchant)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.svc.cacher.Set(key, merchant, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	return nil
}

func (h *Merchant) Insert(merchant *billing.Merchant) error {
	err := h.svc.db.Collection(collectionMerchant).Insert(merchant)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheMerchantId, merchant.Id), merchant, 0)

	if err != nil {
		return err
	}

	return nil
}

func (h *Merchant) Upsert(merchant *billing.Merchant) error {
	_, err := h.svc.db.Collection(collectionMerchant).UpsertId(bson.ObjectIdHex(merchant.Id), merchant)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.svc.cacher.Set(key, merchant, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	return nil
}

func (h *Merchant) MultipleInsert(merchants []*billing.Merchant) error {
	m := make([]interface{}, len(merchants))
	for i, v := range merchants {
		m[i] = v
	}

	if err := h.svc.db.Collection(collectionMerchant).Insert(m...); err != nil {
		return err
	}

	return nil
}

func (h *Merchant) GetById(id string) (*billing.Merchant, error) {
	var c billing.Merchant
	key := fmt.Sprintf(cacheMerchantId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{"_id": bson.ObjectIdHex(id)}
	err := h.svc.db.Collection(collectionMerchant).Find(query).One(&c)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return &c, nil
}

func (h Merchant) GetPaymentMethod(merchantId string, method string) (*billing.MerchantPaymentMethod, error) {
	m, err := h.GetById(merchantId)
	if err != nil {
		return nil, err
	}

	merchantPaymentMethods := make(map[string]*billing.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm, err := h.svc.paymentMethod.GetAll()
	if err != nil {
		return nil, err
	}
	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billing.MerchantPaymentMethod{
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: DefaultPaymentMethodFee,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      DefaultPaymentMethodPerTransactionFee,
						Currency: DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[method]; !ok {
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	return merchantPaymentMethods[method], nil
}

func (h Merchant) GetPaymentMethodTerminalId(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalId == "" {
		return "", orderErrorPaymentMethodEmptySettings
	}

	return pm.Integration.TerminalId, nil
}

func (h Merchant) GetPaymentMethodTerminalPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalPassword == "" {
		return "", orderErrorPaymentMethodEmptySettings
	}

	return pm.Integration.TerminalPassword, nil
}

func (h Merchant) GetPaymentMethodTerminalCallbackPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalCallbackPassword == "" {
		return "", orderErrorPaymentMethodEmptySettings
	}

	return pm.Integration.TerminalCallbackPassword, nil
}
