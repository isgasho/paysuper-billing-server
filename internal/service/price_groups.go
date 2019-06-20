package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	cachePriceGroupId = "price_group:id:%s"

	collectionPriceGroup = "price_group"
)

func (s *Service) GetPriceGroup(
	ctx context.Context,
	req *billing.GetPriceGroupRequest,
	res *billing.PriceGroup,
) error {
	pg, err := s.priceGroup.GetById(req.Id)
	if err != nil {
		return err
	}

	res.Id = pg.Id
	res.Currency = pg.Currency
	res.IsSimple = pg.IsSimple
	res.Region = pg.Region
	res.CreatedAt = pg.CreatedAt
	res.UpdatedAt = pg.UpdatedAt

	return nil
}

func (s *Service) UpdatePriceGroup(
	ctx context.Context,
	req *billing.PriceGroup,
	res *billing.PriceGroup,
) error {

	pg := &billing.PriceGroup{
		Currency:  req.Currency,
		IsSimple:  req.IsSimple,
		Region:    req.Region,
		UpdatedAt: ptypes.TimestampNow(),
	}

	var err error

	if req.Id != "" {
		data, err := s.priceGroup.GetById(req.Id)
		if err != nil {
			return err
		}
		pg.Id = data.Id
		pg.CreatedAt = data.CreatedAt
		err = s.priceGroup.Update(pg)
	} else {
		pg.Id = bson.NewObjectId().Hex()
		pg.CreatedAt = ptypes.TimestampNow()
		err = s.priceGroup.Insert(pg)
	}

	if err != nil {
		zap.S().Errorf("create/update price group failed", "err", err.Error(), "data", req)
		return err
	}

	res.Id = pg.Id
	res.Currency = pg.Currency
	res.IsSimple = pg.IsSimple
	res.Region = pg.Region
	res.CreatedAt = pg.CreatedAt
	res.UpdatedAt = pg.UpdatedAt

	return nil
}

func newPriceGroupService(svc *Service) *PriceGroup {
	s := &PriceGroup{svc: svc}
	return s
}

func (h *PriceGroup) Insert(pg *billing.PriceGroup) error {
	if err := h.svc.db.Collection(collectionPriceGroup).Insert(pg); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePriceGroupId, pg.Id), pg, 0); err != nil {
		return err
	}

	return nil
}

func (h PriceGroup) MultipleInsert(pg []*billing.PriceGroup) error {
	c := make([]interface{}, len(pg))
	for i, v := range pg {
		c[i] = v
	}

	if err := h.svc.db.Collection(collectionPriceGroup).Insert(c...); err != nil {
		return err
	}

	return nil
}

func (h PriceGroup) Update(pg *billing.PriceGroup) error {
	if err := h.svc.db.Collection(collectionPriceGroup).UpdateId(bson.ObjectIdHex(pg.Id), pg); err != nil {
		return err
	}

	if err := h.svc.cacher.Set(fmt.Sprintf(cachePriceGroupId, pg.Id), pg, 0); err != nil {
		return err
	}

	return nil
}

func (h PriceGroup) GetById(id string) (*billing.PriceGroup, error) {
	var c billing.PriceGroup
	key := fmt.Sprintf(cachePriceGroupId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	if err := h.svc.db.Collection(collectionPriceGroup).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&c); err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionPriceGroup)
	}

	_ = h.svc.cacher.Set(key, c, 0)
	return &c, nil
}
