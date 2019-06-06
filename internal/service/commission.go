package service

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

func newCommissionService(svc *Service) *Commission {
	s := &Commission{svc: svc}
	s.loadAllToCache()

	return s
}

func (h *Commission) Update(projectId string, pmId string, commission *billing.MerchantPaymentMethodCommissions) error {
	pool, err := h.loadAllFromCache()
	if err != nil {
		return err
	}

	if _, ok := pool[projectId]; !ok {
		pool[projectId] = make(map[string]*billing.MerchantPaymentMethodCommissions)
	}
	pool[projectId][pmId] = commission

	if err := h.svc.cacher.Set(pkg.CollectionCommission, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Commission) GetAll() map[string]map[string]*billing.MerchantPaymentMethodCommissions {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	return pool
}

func (h *Commission) GetByProject(projectId string) map[string]*billing.MerchantPaymentMethodCommissions {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil
	}

	prjCom, ok := pool[projectId]
	if !ok {
		return nil
	}

	return prjCom
}

func (h *Commission) Get(projectId string, pmId string) (*billing.MerchantPaymentMethodCommissions, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return nil, err
	}

	prjCom, ok := pool[projectId]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	prjPmCom, ok := prjCom[pmId]
	if !ok {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	return prjPmCom, nil
}

func (h *Commission) CalculatePmCommission(projectId, pmId string, amount float64) (float64, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	pool, err := h.loadAllFromCache()
	if err != nil {
		return float64(0), err
	}

	prjCom, ok := pool[projectId]
	if !ok {
		return float64(0), fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	prjPmCom, ok := prjCom[pmId]
	if !ok {
		return float64(0), fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	return tools.FormatAmount(amount * (prjPmCom.Fee / 100)), nil
}

func (h *Commission) loadAllToCache() error {
	var merchants []*billing.Merchant
	var projects []*billing.Project

	if err := h.svc.db.Collection(pkg.CollectionMerchant).Find(bson.M{}).All(&merchants); err != nil {
		return err
	}

	pool := make(map[string]map[string]*billing.MerchantPaymentMethodCommissions)
	for _, v := range merchants {
		query := bson.M{"merchant_id": bson.ObjectIdHex(v.Id)}
		if err := h.svc.db.Collection(pkg.CollectionProject).Find(query).All(&projects); err != nil {
			continue
		}

		for _, v1 := range projects {
			pool[v1.Id] = make(map[string]*billing.MerchantPaymentMethodCommissions)

			for k, v2 := range v.PaymentMethods {
				pool[v1.Id][k] = v2.Commission
			}

			pm := h.svc.paymentMethod.GetAll()
			if len(pm) != len(pool[v1.Id]) {
				for k := range pm {
					if _, ok := pool[v1.Id][k]; ok {
						continue
					}

					pool[v1.Id][k] = h.svc.getDefaultPaymentMethodCommissions()
				}
			}
		}
	}

	if err := h.svc.cacher.Set(pkg.CollectionCommission, pool, 0); err != nil {
		return err
	}

	return nil
}

func (h *Commission) loadAllFromCache() (map[string]map[string]*billing.MerchantPaymentMethodCommissions, error) {
	b, err := h.svc.cacher.Get(pkg.CollectionCommission)
	if err != nil {
		return nil, err
	}

	var a map[string]map[string]*billing.MerchantPaymentMethodCommissions
	err = json.Unmarshal(*b, &a)
	if err != nil {
		return nil, err
	}

	return a, nil
}
