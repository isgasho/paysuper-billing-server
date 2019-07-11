package service

import (
	"context"
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	collectionKeyProduct = "key_product"
)

func (s *Service) CreateOrUpdateKeyProduct(ctx context.Context, req *grpc.CreateOrUpdateKeyProductRequest, res *grpc.KeyProduct) error {
	var (
		err       error
		isNew     = req.Id == ""
		now       = ptypes.TimestampNow()
	)

	if isNew {
		res.Id = bson.NewObjectId().Hex()
		res.CreatedAt = now
		res.MerchantId = req.MerchantId
		res.ProjectId = req.ProjectId
		res.Sku = req.Sku
	} else {
		err = s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.Id, MerchantId: req.MerchantId}, res)
		if err != nil {
			zap.S().Errorf("Key product that requested to change is not found", "err", err.Error(), "data", req)
			return err
		}

		if req.Sku != "" && req.Sku != res.Sku {
			zap.S().Errorf("SKU mismatch", "data", req)
			return errors.New("SKU mismatch")
		}

		if req.MerchantId != res.MerchantId {
			zap.S().Errorf("MerchantId mismatch", "data", req)
			return errors.New("merchantId mismatch")
		}

		if req.ProjectId != res.ProjectId {
			zap.S().Errorf("ProjectId mismatch", "data", req)
			return errors.New("projectId mismatch")
		}
	}

	if _, ok := req.Name[DefaultLanguage]; !ok {
		zap.S().Errorf("No name in default language", "data", req)
		return errors.New("No name in default language")
	}

	if _, ok := req.Description[DefaultLanguage]; !ok {
		zap.S().Errorf("No description in default language", "data", req)
		return errors.New("No description in default language")
	}

	// Prevent duplicated key products (by projectId+sku)
	dupQuery := bson.M{"project_id": bson.ObjectIdHex(req.ProjectId), "sku": req.Sku, "deleted": false}
	found, err := s.db.Collection(collectionKeyProduct).Find(dupQuery).Count()
	if err != nil {
		zap.S().Errorf("Query to find duplicates failed", "err", err.Error(), "data", req)
		return err
	}
	allowed := 1
	if isNew {
		allowed = 0
	}

	if found > allowed {
		zap.S().Errorf("Pair projectId+Sku already exists", "data", req)
		return errors.New("pair projectId+Sku already exists")
	}

	res.Object = req.Object
	res.Name = req.Name
	res.DefaultCurrency = req.DefaultCurrency
	res.Enabled = req.Enabled
	res.Description = req.Description
	res.LongDescription = req.LongDescription
	res.Images = req.Images
	res.Url = req.Url
	res.UpdatedAt = now

	_, err = s.db.Collection(collectionKeyProduct).UpsertId(bson.ObjectIdHex(req.Id), res)

	if err != nil {
		zap.S().Errorf("Query to create/update product failed", "err", err.Error(), "data", req)
		return err
	}

	return nil
}

func (s *Service) GetKeyProducts(ctx context.Context, req *grpc.ListKeyProductsRequest, res *grpc.ListKeyProductsResponse) error {
	query := bson.M{"merchant_id": bson.ObjectIdHex(req.MerchantId), "deleted": false}

	if req.ProjectId != "" {
		query["project_id"] = bson.ObjectIdHex(req.ProjectId)
	}

	if req.Sku != "" {
		query["sku"] = bson.RegEx{req.Sku, "i"}
	}
	if req.Name != "" {
		query["name"] = bson.M{"$elemMatch": bson.M{"value": bson.RegEx{req.Name, "i"}}}
	}

	total, err := s.db.Collection(collectionKeyProduct).Find(query).Count()
	if err != nil {
		zap.S().Errorf("Query to find key products by id failed", "err", err.Error(), "data", req)
		return err
	}

	items := []*grpc.KeyProduct{}

	res.Limit = req.Limit
	res.Offset = req.Offset
	res.Count = int32(total)
	res.Products = items
	if res.Count == 0 || res.Offset > res.Count {
		return nil
	}

	err = s.db.Collection(collectionKeyProduct).Find(query).Skip(int(req.Offset)).Limit(int(req.Limit)).All(&items)

	if err != nil {
		zap.S().Errorf("Query to find key products by id failed", "err", err.Error(), "data", req)
		return err
	}
	res.Products = items

	return nil
}

func (s *Service) GetKeyProduct(ctx context.Context, req *grpc.RequestKeyProduct, res *grpc.KeyProduct) error {
	query := bson.M{
		"_id":         bson.ObjectIdHex(req.Id),
		"merchant_id": bson.ObjectIdHex(req.MerchantId),
		"deleted":     false,
	}
	err := s.db.Collection(collectionKeyProduct).Find(query).One(&res)

	if err != nil {
		zap.S().Errorf("Query to find key product by id failed", "err", err.Error(), "data", req)
		return err
	}

	return nil
}

func (s *Service) DeleteKeyProduct(ctx context.Context, req *grpc.RequestKeyProduct, res *grpc.EmptyResponse) error {
	product := &grpc.KeyProduct{}

	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.Id, MerchantId: req.MerchantId}, product)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return err
	}

	product.Deleted = true
	product.UpdatedAt = ptypes.TimestampNow()

	err = s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(product.Id), product)

	if err != nil {
		zap.S().Errorf("Query to delete key product failed", "err", err.Error(), "data", req)
		return err
	}

	return nil
}

func (s *Service) PublishKeyProduct(ctx context.Context, req *grpc.PublishKeyProductRequest, res *grpc.KeyProduct) error {
	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.KeyProductId, MerchantId: req.MerchantId}, res)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return err
	}

	res.UpdatedAt = ptypes.TimestampNow()
	res.PublishedAt = ptypes.TimestampNow()
	res.IsPublished = true

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(res.Id), res); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		return err
	}

	return nil
}

func (s *Service) GetKeyProductsForOrder(context.Context, *grpc.GetKeyProductsForOrderRequest, *grpc.ListKeyProductsResponse) error {
	panic("implement me")
}

func (s *Service) UpdatePlatformPrices(context.Context, *grpc.AddOrUpdatePlatformPricesRequest, *grpc.PlatformPrice) error {
	panic("implement me")
}

func (s *Service) DeletePlatformFromProduct(context.Context, *grpc.RemovePlatformRequest, *grpc.EmptyResponse) error {
	panic("implement me")
}
