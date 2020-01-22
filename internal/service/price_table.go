package service

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

const (
	collectionPriceTable = "price_table"
)

type PriceTableServiceInterface interface {
	Insert(context.Context, *billingpb.PriceTable) error
	GetByRegion(context.Context, string) (*billingpb.PriceTable, error)
}

func newPriceTableService(svc *Service) PriceTableServiceInterface {
	s := &PriceTable{svc: svc}
	return s
}

func (h *PriceTable) Insert(ctx context.Context, pt *billingpb.PriceTable) error {
	_, err := h.svc.db.Collection(collectionPriceTable).InsertOne(ctx, pt)

	if err != nil {
		return err
	}

	return nil
}

func (h *PriceTable) GetByRegion(ctx context.Context, region string) (*billingpb.PriceTable, error) {
	var price *billingpb.PriceTable
	err := h.svc.db.Collection(collectionPriceTable).FindOne(ctx, bson.M{"currency": region}).Decode(&price)

	if err != nil {
		return nil, err
	}

	return price, nil
}

func (s *Service) GetRecommendedPriceTable(
	ctx context.Context,
	req *billingpb.RecommendedPriceTableRequest,
	res *billingpb.RecommendedPriceTableResponse,
) error {
	table, err := s.priceTable.GetByRegion(ctx, req.Currency)

	if err != nil {
		zap.L().Error("Price table not found", zap.Any("req", req))
		return nil
	}

	res.Ranges = table.Ranges

	return nil
}
