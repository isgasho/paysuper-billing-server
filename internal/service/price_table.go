package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"math"
)

type PriceTableServiceInterface interface {
	Insert(*billing.PriceTable) error
	GetByAmount(float64) (*billing.PriceTable, error)
	GetLatest() (*billing.PriceTable, error)
	InterpolateByAmount(*billing.PriceTable, float64) *billing.PriceTable
}

func newPriceTableService(svc *Service) PriceTableServiceInterface {
	s := &PriceTable{svc: svc}
	return s
}

func (h *PriceTable) Insert(pt *billing.PriceTable) error {
	if err := h.svc.db.Collection(collectionPriceTable).Insert(pt); err != nil {
		return err
	}

	return nil
}

func (h *PriceTable) GetByAmount(amount float64) (*billing.PriceTable, error) {
	var price *billing.PriceTable
	err := h.svc.db.Collection(collectionPriceTable).
		Find(bson.M{
			"from": bson.M{"$lt": amount},
			"to":   bson.M{"$gte": amount},
		}).
		One(&price)

	if err != nil {
		return nil, err
	}

	return price, nil
}

func (h *PriceTable) GetLatest() (*billing.PriceTable, error) {
	var price *billing.PriceTable
	err := h.svc.db.Collection(collectionPriceTable).
		Find(nil).
		Sort("-to").
		One(&price)

	if err != nil {
		return nil, err
	}

	return price, nil
}

func (h *PriceTable) InterpolateByAmount(price *billing.PriceTable, amount float64) *billing.PriceTable {
	delta := price.To - price.From
	step := math.Ceil((amount - price.To) / delta)
	price.To += delta * step
	price.From = price.To - delta

	for idx, curr := range price.Currencies {
		delta := curr.To - curr.From
		price.Currencies[idx].To += delta * step
		price.Currencies[idx].From = price.Currencies[idx].To - delta
	}

	return price
}
