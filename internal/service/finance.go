package service

import (
	"fmt"
	"github.com/ProtocolONE/payone-billing-service/pkg/proto/billing"
	"github.com/globalsign/mgo/bson"
	"time"
)

type Currency struct {
	svc *Service
}

type CurrencyRate Currency
type Vat Currency
type Commission Currency

type OrderCommission struct {
	PMCommission     float64
	PspCommission    float64
	ToUserCommission float64
}

func newCurrencyHandler(svc *Service) Cacher {
	return &Currency{svc: svc}
}

func (h *Currency) setCache(recs []interface{}) {
	h.svc.currencyCache = make(map[string]*billing.Currency)

	for _, c := range recs {
		cur := c.(*billing.Currency)

		h.svc.mx.Lock()
		h.svc.currencyCache[cur.CodeA3] = cur
		h.svc.mx.Unlock()
	}
}

func (h *Currency) getAll() (recs []interface{}, err error) {
	var data []*billing.Currency

	err = h.svc.db.Collection(collectionCurrency).Find(bson.M{"is_active": true}).All(&data)

	if data != nil {
		for _, v := range data {
			recs = append(recs, v)
		}
	}

	return
}

func (s *Service) GetCurrencyByCodeA3(code string) (*billing.Currency, error) {
	rec, ok := s.currencyCache[code]

	if !ok {
		return nil, fmt.Errorf(errorNotFound, collectionCurrency)
	}

	return rec, nil
}

func newCurrencyRateHandler(svc *Service) Cacher {
	return &CurrencyRate{svc: svc}
}

func (h *CurrencyRate) setCache(recs []interface{}) {
	h.svc.currencyRateCache = make(map[int32]map[int32]*billing.CurrencyRate)

	for _, c := range recs {
		rate := c.(*billing.CurrencyRate)

		h.svc.mx.Lock()

		if _, ok := h.svc.currencyRateCache[rate.CurrencyFrom]; !ok {
			h.svc.currencyRateCache[rate.CurrencyFrom] = make(map[int32]*billing.CurrencyRate)
		}

		h.svc.currencyRateCache[rate.CurrencyFrom][rate.CurrencyTo] = rate

		h.svc.mx.Unlock()
	}
}

func (h *CurrencyRate) getAll() (recs []interface{}, err error) {
	var data []*billing.CurrencyRate

	err = h.svc.db.Collection(collectionCurrencyRate).Find(bson.M{"is_active": true}).All(&data)

	if data != nil {
		for _, v := range data {
			recs = append(recs, v)
		}
	}

	return
}

func (s *Service) Convert(from int32, to int32, value float64) (float64, error) {
	fRates, ok := s.currencyRateCache[from]

	if !ok {
		return 0, fmt.Errorf(errorNotFound, collectionCurrencyRate)
	}

	rec, ok := fRates[to]

	if !ok {
		return 0, fmt.Errorf(errorNotFound, collectionCurrencyRate)
	}

	value = value / rec.Rate

	return value, nil
}

func newVatHandler(svc *Service) Cacher {
	return &Vat{svc: svc}
}

func (h *Vat) setCache(recs []interface{}) {
	h.svc.vatCache = make(map[string]map[string]*billing.Vat)

	for _, c := range recs {
		vat := c.(*billing.Vat)

		h.svc.mx.Lock()

		if _, ok := h.svc.vatCache[vat.Country]; !ok {
			h.svc.vatCache[vat.Country] = make(map[string]*billing.Vat)
		}

		h.svc.vatCache[vat.Country][vat.Subdivision] = vat

		h.svc.mx.Unlock()
	}
}

func (h *Vat) getAll() (recs []interface{}, err error) {
	var data []*billing.Vat

	err = h.svc.db.Collection(collectionVat).Find(bson.M{"is_active": true}).All(&data)

	if data != nil {
		for _, v := range data {
			recs = append(recs, v)
		}
	}

	return
}

func (s *Service) CalculateVat(amount float64, country, subdivision string) (float64, error) {
	vatCountry, ok := s.vatCache[country]

	if !ok {
		return 0, fmt.Errorf(errorNotFound, collectionVat)
	}

	if vsFlag, ok := vatBySubdivisionCountries[country]; !ok || vsFlag == false {
		subdivision = ""
	}

	vat, ok := vatCountry[subdivision]

	if !ok {
		return 0, fmt.Errorf(errorNotFound, collectionVat)
	}

	amount = amount * (vat.Vat / 100)

	return amount, nil
}

func newCommissionHandler(svc *Service) Cacher {
	return &Commission{svc: svc}
}

func (h *Commission) setCache(recs []interface{}) {
	h.svc.commissionCache = make(map[string]map[string]*billing.MgoCommission)

	for _, c := range recs {
		commission := c.(*billing.MgoCommission)

		h.svc.mx.Lock()

		if _, ok := h.svc.commissionCache[commission.Id.ProjectId.Hex()]; !ok {
			h.svc.commissionCache[commission.Id.ProjectId.Hex()] = make(map[string]*billing.MgoCommission)
		}

		h.svc.commissionCache[commission.Id.ProjectId.Hex()][commission.Id.PaymentMethodId.Hex()] = commission

		h.svc.mx.Unlock()
	}
}

func (h *Commission) getAll() (recs []interface{}, err error) {
	var data []*billing.MgoCommission

	q := []bson.M{
		{"$match": bson.M{"start_date": bson.M{"$lte": time.Now()}}},
		{"$sort": bson.M{"start_date": -1}},
		{
			"$group": bson.M{
				"_id":                      bson.M{"pm_id": "$pm_id", "project_id": "$project_id"},
				"pm_commission":            bson.M{"$first": "$pm_commission"},
				"psp_commission":           bson.M{"$first": "$psp_commission"},
				"total_commission_to_user": bson.M{"$first": "$total_commission_to_user"},
				"start_date":               bson.M{"$first": "$start_date"},
			},
		},
	}

	err = h.svc.db.Collection(collectionCommission).Pipe(q).All(&data)

	if data != nil {
		for _, v := range data {
			recs = append(recs, v)
		}
	}

	return
}

func (s *Service) CalculateCommission(projectId, pmId string, amount float64) (*OrderCommission, error) {
	projectCommissions, ok := s.commissionCache[projectId]

	if !ok {
		return nil, fmt.Errorf(errorNotFound, collectionCommission)
	}

	commission, ok := projectCommissions[pmId]

	if !ok {
		return nil, fmt.Errorf(errorNotFound, collectionCommission)
	}

	c := &OrderCommission{
		PMCommission:     amount * (commission.PaymentMethodCommission / 100),
		PspCommission:    amount * (commission.PspCommission / 100),
		ToUserCommission: amount * (commission.ToUserCommission / 100),
	}

	return c, nil
}
