package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	collectionKeyProduct = "key_product"
)

var (
	keyProductMerchantMismatch       = newBillingServerErrorMsg("kp000001", "merchant id mismatch")
	keyProductProjectMismatch        = newBillingServerErrorMsg("kp000002", "project id mismatch")
	keyProductSkuMismatch            = newBillingServerErrorMsg("kp000003", "sku mismatch")
	keyProductNameNotProvided        = newBillingServerErrorMsg("kp000004", "name must be set")
	keyProductDescriptionNotProvided = newBillingServerErrorMsg("kp000005", "description must be set")
	keyProductDuplicate              = newBillingServerErrorMsg("kp000006", "sku+project id already exist")
	keyProductIdsIsEmpty             = newBillingServerErrorMsg("kp000007", "ids is empty")
	keyProductAlreadyHasPlatform     = newBillingServerErrorMsg("kp000008", "product already has user defined platform")
	keyProductActivationUrlEmpty     = newBillingServerErrorMsg("kp000009", "activation url must be set")
	keyProductEulaEmpty              = newBillingServerErrorMsg("kp000010", "eula url must be set")
	keyProductPlatformName           = newBillingServerErrorMsg("kp000011", "platform name must be set")
	keyProductRetrieveError          = newBillingServerErrorMsg("kp000012", "query to retrieve key product failed")
	keyProductErrorUpsert            = newBillingServerErrorMsg("kp000013", "query to insert/update key product failed")
	keyProductErrorDelete            = newBillingServerErrorMsg("kp000014", "query to remove key product failed")
)

var availablePlatforms = map[string]grpc.Platform{
	"steam":    {Id: "steam", Name: "Steam"},
	"gog":      {Id: "gog", Name: "GOG"},
	"egs":      {Id: "egs", Name: "Epic Game Store"},
	"uplay":    {Id: "uplay", Name: "Uplay"},
	"origin":   {Id: "origin", Name: "Origin"},
	"psn":      {Id: "psn", Name: "PSN"},
	"xbox":     {Id: "xbox", Name: "XBOX Store"},
	"nintendo": {Id: "nintendo", Name: "Nintendo Store"},
}

func (s *Service) CreateOrUpdateKeyProduct(ctx context.Context, req *grpc.CreateOrUpdateKeyProductRequest, res *grpc.KeyProduct) error {
	var (
		err   error
		isNew = req.Id == ""
		now   = ptypes.TimestampNow()
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
			return keyProductSkuMismatch
		}

		if req.MerchantId != res.MerchantId {
			zap.S().Errorf("MerchantId mismatch", "data", req)
			return keyProductMerchantMismatch
		}

		if req.ProjectId != res.ProjectId {
			zap.S().Errorf("ProjectId mismatch", "data", req)
			return keyProductProjectMismatch
		}
	}

	if _, ok := req.Name[DefaultLanguage]; !ok {
		zap.S().Errorf("No name in default language", "data", req)
		return keyProductNameNotProvided
	}

	if _, ok := req.Description[DefaultLanguage]; !ok {
		zap.S().Errorf("No description in default language", "data", req)
		return keyProductDescriptionNotProvided
	}

	// Prevent duplicated key products (by projectId+sku)
	dupQuery := bson.M{"project_id": bson.ObjectIdHex(req.ProjectId), "sku": req.Sku, "deleted": false}
	found, err := s.db.Collection(collectionKeyProduct).Find(dupQuery).Count()
	if err != nil {
		zap.S().Errorf("Query to find duplicates failed", "err", err.Error(), "data", req)
		return keyProductRetrieveError
	}
	allowed := 1
	if isNew {
		allowed = 0
	}

	if found > allowed {
		zap.S().Errorf("Pair projectId+Sku already exists", "data", req)
		return keyProductDuplicate
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
		return keyProductErrorUpsert
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
		return keyProductRetrieveError
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
		return keyProductRetrieveError
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
		return keyProductRetrieveError
	}

	return nil
}

func (s *Service) DeleteKeyProduct(ctx context.Context, req *grpc.RequestKeyProduct, res *grpc.EmptyResponse) error {
	product := &grpc.KeyProduct{}

	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.Id, MerchantId: req.MerchantId}, product)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return keyProductRetrieveError
	}

	product.Deleted = true
	product.UpdatedAt = ptypes.TimestampNow()

	err = s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(product.Id), product)

	if err != nil {
		zap.S().Errorf("Query to delete key product failed", "err", err.Error(), "data", req)
		return keyProductErrorDelete
	}

	return nil
}

func (s *Service) PublishKeyProduct(ctx context.Context, req *grpc.PublishKeyProductRequest, res *grpc.KeyProduct) error {
	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.KeyProductId, MerchantId: req.MerchantId}, res)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return keyProductRetrieveError
	}

	res.UpdatedAt = ptypes.TimestampNow()
	res.PublishedAt = ptypes.TimestampNow()
	res.Enabled = true

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(res.Id), res); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		return keyProductErrorUpsert
	}

	return nil
}

func (s *Service) GetKeyProductsForOrder(ctx context.Context, req *grpc.GetKeyProductsForOrderRequest, res *grpc.ListKeyProductsResponse) error {
	if len(req.Ids) == 0 {
		zap.S().Errorf("Ids list is empty", "data", req)
		return keyProductIdsIsEmpty
	}
	query := bson.M{"enabled": true, "deleted": false, "project_id": bson.ObjectIdHex(req.ProjectId)}
	var items = []bson.ObjectId{}
	for _, id := range req.Ids {
		items = append(items, bson.ObjectIdHex(id))
	}
	query["_id"] = bson.M{"$in": items}

	found := []*grpc.KeyProduct{}

	err := s.db.Collection(collectionKeyProduct).Find(query).All(&found)

	if err != nil {
		zap.S().Errorf("Query to find key products for order is failed", "err", err.Error(), "data", req)
		return keyProductRetrieveError
	}

	res.Limit = int32(len(found))
	res.Offset = 0
	res.Count = res.Limit
	res.Products = found
	return nil
}

func (s *Service) UpdatePlatformPrices(ctx context.Context, req *grpc.AddOrUpdatePlatformPricesRequest, res *grpc.PlatformPrice) error {
	product := &grpc.KeyProduct{}
	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.KeyProductId, MerchantId: req.MerchantId}, product)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return err
	}

	res.Prices = req.Platform.Prices
	res.Id = req.Platform.Id
	res.Name = req.Platform.Name

	found := false
	productHasUserPlatform := false

	for _, platform := range product.Platforms {
		if platform.Id == req.Platform.Id {
			platform.Prices = req.Platform.Prices
			found = true
		}

		if _, ok := availablePlatforms[platform.Id]; !ok {
			productHasUserPlatform = true
		}
	}

	_, isAvailable := availablePlatforms[req.Platform.Id]

	if !isAvailable && productHasUserPlatform && found == false {
		zap.S().Errorf("Product already has user defined platform", "data", req)
		return keyProductAlreadyHasPlatform
	}

	if found == false {
		if isAvailable == false {
			if res.ActivationUrl == "" {
				zap.S().Errorf("Activation url must be set", "err", err.Error(), "data", req)
				return keyProductActivationUrlEmpty
			}

			if res.EulaUrl == "" {
				zap.S().Errorf("Eula url must be set", "err", err.Error(), "data", req)
				return keyProductEulaEmpty
			}

			if res.Name == "" {
				zap.S().Errorf("Name must be set", "err", err.Error(), "data", req)
				return keyProductPlatformName
			}

			res.ActivationUrl = req.Platform.ActivationUrl
			res.EulaUrl = req.Platform.EulaUrl
			res.Name = req.Platform.Name
		}
		product.Platforms = append(product.Platforms, res)
	}

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(req.KeyProductId), res); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		return keyProductErrorUpsert
	}

	return nil
}

func (s *Service) DeletePlatformFromProduct(ctx context.Context, req *grpc.RemovePlatformRequest, res *grpc.EmptyResponse) error {
	product := &grpc.KeyProduct{}
	err := s.GetKeyProduct(ctx, &grpc.RequestKeyProduct{Id: req.KeyProductId, MerchantId: req.MerchantId}, product)
	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		return keyProductRetrieveError
	}

	for i, platform := range product.Platforms {
		if platform.Id == req.PlatformId {
			// https://github.com/golang/go/wiki/SliceTricks
			copy(product.Platforms[i:], product.Platforms[i+1:])
			product.Platforms[len(product.Platforms)-1] = nil
			product.Platforms = product.Platforms[:len(product.Platforms)-1]
			break
		}
	}

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(req.KeyProductId), res); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		return keyProductErrorUpsert
	}

	return nil
}
