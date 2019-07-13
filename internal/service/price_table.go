package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

type PriceTableServiceInterface interface {
	Insert(*billing.PriceTable) error
	GetByAmount(float64) (*billing.PriceTable, error)
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
			"from": bson.M{"$lte": amount},
			"to":   bson.M{"$gte": amount},
		}).
		One(&price)

	if price != nil {
		return price, nil
	}

	err = h.svc.db.Collection(collectionPriceTable).
		Find(nil).
		Sort("-to").
		One(&price)

	if err != nil {
		return nil, err
	}

	if price.To < amount {
		delta := price.To - price.From
		step := (amount - price.To) / delta
		price.To += delta * step
		price.From = price.To - delta
	}

	return price, nil
}
