package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

type PriceTableServiceInterface interface {
	Insert(*billing.PriceTable) error
	GetByRegion(string) (*billing.PriceTable, error)
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

func (h *PriceTable) GetByRegion(region string) (*billing.PriceTable, error) {
	var price *billing.PriceTable
	err := h.svc.db.Collection(collectionPriceTable).
		Find(bson.M{"currency": region}).
		One(&price)

	if err != nil {
		return nil, err
	}

	return price, nil
}

func (s *Service) GetRecommendedPriceTable(
	ctx context.Context,
	req *grpc.RecommendedPriceTableRequest,
	res *grpc.RecommendedPriceTableResponse,
) error {
	table, err := s.priceTable.GetByRegion(req.Currency)

	if err != nil {
		zap.L().Error("Price table not found", zap.Any("req", req))
		return nil
	}

	res.Ranges = table.Ranges

	return nil
}
