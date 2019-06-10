package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

func newMerchantService(svc *Service) *Merchant {
	s := &Merchant{svc: svc}
	return s
}

func (h *Merchant) Update(merchant *billing.Merchant) error {
	key := fmt.Sprintf(pkg.CacheMerchantId, merchant.Id)
	if err := h.svc.cacher.Set(key, merchant, 0); err != nil {
		return err
	}

	return nil
}

func (h *Merchant) GetById(id string) (*billing.Merchant, error) {
	c := &billing.Merchant{}
	key := fmt.Sprintf(pkg.CacheMerchantId, id)
	res, err := h.svc.cacher.Get(key)

	if res != nil {
		err := json.Unmarshal(res, &c)
		if err != nil {
			return nil, fmt.Errorf(errorInterfaceCast, pkg.CollectionMerchant)
		}
		return c, nil
	}

	err = h.svc.db.Collection(pkg.CollectionMerchant).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionMerchant)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return c, nil
}

func (h Merchant) GetPaymentMethod(merchantId string, method string) (*billing.MerchantPaymentMethod, error) {
	m, err := h.GetById(merchantId)
	if err != nil {
		return nil, errors.New(orderErrorPaymentMethodNotAllowed)
	}

	merchantPaymentMethods := make(map[string]*billing.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm := h.svc.paymentMethod.GetAll()
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
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionMerchant)
	}

	return merchantPaymentMethods[method], nil
}

func (h Merchant) GetPaymentMethodTerminalId(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalId == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalId, nil
}

func (h Merchant) GetPaymentMethodTerminalPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalPassword == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalPassword, nil
}

func (h Merchant) GetPaymentMethodTerminalCallbackPassword(merchantId, pmId string) (string, error) {
	pm, err := h.GetPaymentMethod(merchantId, pmId)

	if err != nil {
		return "", err
	}

	if pm.Integration == nil || pm.Integration.TerminalCallbackPassword == "" {
		return "", errors.New(orderErrorPaymentMethodEmptySettings)
	}

	return pm.Integration.TerminalCallbackPassword, nil
}
