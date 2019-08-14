package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2"
	"net/http"
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
	keyProductMerchantNotFound       = newBillingServerErrorMsg("kp000015", "merchant not found")
	keyProductMerchantDbError        = newBillingServerErrorMsg("kp000016", "can't retrieve data from db for merchant")
	keyProductNotFound               = newBillingServerErrorMsg("kp000017", "key product not found")
	keyProductInternalError          = newBillingServerErrorMsg("kp000018", "unknown error")
)

var availablePlatforms = map[string]*grpc.Platform{
	"steam":    {Id: "steam", Name: "Steam"},
	"gog":      {Id: "gog", Name: "GOG"},
	"egs":      {Id: "egs", Name: "Epic Game Store"},
	"uplay":    {Id: "uplay", Name: "Uplay"},
	"origin":   {Id: "origin", Name: "Origin"},
	"psn":      {Id: "psn", Name: "PSN"},
	"xbox":     {Id: "xbox", Name: "XBOX Store"},
	"nintendo": {Id: "nintendo", Name: "Nintendo Store"},
}

func (s *Service) CreateOrUpdateKeyProduct(ctx context.Context, req *grpc.CreateOrUpdateKeyProductRequest, res *grpc.KeyProductResponse) error {
	var (
		err     error
		isNew   = req.Id == ""
		now     = ptypes.TimestampNow()
		product = &grpc.KeyProduct{}
	)

	if isNew {
		product.Id = bson.NewObjectId().Hex()
		product.CreatedAt = now
		product.MerchantId = req.MerchantId
		product.ProjectId = req.ProjectId
		product.Sku = req.Sku
	} else {
		product.Id = req.Id
		_ = s.GetKeyProduct(ctx, &grpc.RequestKeyProductMerchant{Id: req.Id, MerchantId: req.MerchantId}, res)
		if res.Message != nil {
			zap.S().Errorf("Key product that requested to change is not found", "data", req)
			return nil
		}

		if req.Sku != "" && req.Sku != product.Sku {
			zap.S().Errorf("SKU mismatch", "data", req)
			res.Status = http.StatusBadRequest
			res.Message = keyProductSkuMismatch
			return nil
		}

		if req.MerchantId != product.MerchantId {
			zap.S().Errorf("MerchantId mismatch", "data", req)
			res.Status = http.StatusBadRequest
			res.Message = keyProductMerchantMismatch
			return nil
		}

		if req.ProjectId != product.ProjectId {
			zap.S().Errorf("ProjectId mismatch", "data", req)
			res.Status = http.StatusBadRequest
			res.Message = keyProductProjectMismatch
			return nil
		}
	}

	if _, ok := req.Name[DefaultLanguage]; !ok {
		zap.S().Errorf("No name in default language", "data", req)
		res.Status = http.StatusBadRequest
		res.Message = keyProductNameNotProvided
		return nil
	}

	if _, ok := req.Description[DefaultLanguage]; !ok {
		zap.S().Errorf("No description in default language", "data", req)
		res.Status = http.StatusBadRequest
		res.Message = keyProductDescriptionNotProvided
		return nil
	}

	// Prevent duplicated key products (by projectId+sku)
	dupQuery := bson.M{"project_id": bson.ObjectIdHex(req.ProjectId), "sku": req.Sku, "deleted": false}
	found, err := s.db.Collection(collectionKeyProduct).Find(dupQuery).Count()
	if err != nil {
		zap.S().Errorf("Query to find duplicates failed", "err", err.Error(), "data", req)
		res.Status = http.StatusBadRequest
		res.Message = keyProductRetrieveError
		return nil
	}
	allowed := 1
	if isNew {
		allowed = 0
	}

	if found > allowed {
		zap.S().Errorf("Pair projectId+Sku already exists", "data", req)
		res.Status = http.StatusBadRequest
		res.Message = keyProductDuplicate
		return nil
	}

	product.Object = req.Object
	product.Name = req.Name
	product.DefaultCurrency = req.DefaultCurrency
	product.Enabled = req.Enabled
	product.Description = req.Description
	product.LongDescription = req.LongDescription
	product.Images = req.Images
	product.Url = req.Url
	product.UpdatedAt = now

	_, err = s.db.Collection(collectionKeyProduct).UpsertId(bson.ObjectIdHex(product.Id), product)

	if err != nil {
		zap.S().Errorf("Query to create/update product failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductErrorUpsert
		return nil
	}

	res.Product = product
	return nil
}

func (s *Service) checkMerchantExist(id string) (bool, error) {
	var c billing.Merchant
	err := s.db.Collection(collectionMerchant).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&c)

	if err == mgo.ErrNotFound {
		return false, nil
	}

	if err != nil {
		return false, keyProductMerchantDbError
	}

	return true, nil
}

func (s *Service) GetKeyProducts(ctx context.Context, req *grpc.ListKeyProductsRequest, res *grpc.ListKeyProductsResponse) error {
	if exist, err := s.checkMerchantExist(req.MerchantId); exist == false || err != nil {
		if err != nil {
			res.Status = http.StatusInternalServerError
			res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
			return nil
		}
		res.Status = http.StatusNotFound
		res.Message = keyProductMerchantNotFound
		return nil
	}

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
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
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
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
	}

	res.Products = items
	return nil
}

func (s *Service) GetKeyProductInfo(ctx context.Context, req *grpc.GetKeyProductInfoRequest, res *grpc.GetKeyProductInfoResponse) error {
	res.Status = pkg.ResponseStatusOk
	product, err := s.getKeyProductById(req.KeyProductId)
	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Query to find key product by id failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductRetrieveError
		return nil
	}

	if !product.Enabled {
		zap.S().Error("Product is disabled", "data", req)
		res.Status = pkg.ResponseStatusBadData
		res.Message = keyProductRetrieveError
		return nil
	}

	res.KeyProduct = &grpc.KeyProductInfo{
		Id:        product.Id,
		Images:    product.Images,
		ProjectId: product.ProjectId,
	}

	if res.KeyProduct.Name, err = product.GetLocalizedName(req.Language); err != nil {
		res.KeyProduct.Name, _ = product.GetLocalizedName(DefaultLanguage)
	}

	if res.KeyProduct.Description, err = product.GetLocalizedDescription(req.Language); err != nil {
		res.KeyProduct.Description, _ = product.GetLocalizedDescription(DefaultLanguage)
	}

	if res.KeyProduct.LongDescription, err = product.GetLocalizedLongDescription(req.Language); err != nil {
		res.KeyProduct.LongDescription, _ = product.GetLocalizedLongDescription(DefaultLanguage)
	}

	defaultPriceGroup, err := s.priceGroup.GetByRegion(product.DefaultCurrency)
	if err != nil {
		zap.S().Errorw("Failed to get price group for default currency", "currency", product.DefaultCurrency)
		return keyProductInternalError
	}

	priceGroup := defaultPriceGroup
	globalIsFallback := false
	if req.Currency != "" {
		priceGroup, err = s.priceGroup.GetByRegion(req.Currency)
		if err != nil {
			zap.S().Errorw("Failed to get price group for specified currency", "currency", req.Currency)
			priceGroup = defaultPriceGroup
			globalIsFallback = true
		}
	} else {
		if req.Country != "" {
			err = s.GetPriceGroupByCountry(ctx, &grpc.PriceGroupByCountryRequest{Country: req.Country}, priceGroup)
			if err != nil {
				zap.S().Error("could not get price group by country", "country", req.Country)
				priceGroup = defaultPriceGroup
				globalIsFallback = true
			}
		}
	}

	platforms := make([]*grpc.PlatformPriceInfo, len(product.Platforms))
	for i, p := range product.Platforms {
		currency := priceGroup.Currency
		region := priceGroup.Region
		amount, err := product.GetPriceInCurrencyAndPlatform(priceGroup, p.Id)
		isFallback := globalIsFallback
		if err != nil {
			zap.S().Error("could not get price in currency and platform", "price_group", priceGroup, "platform", p.Id)
			isFallback = true
			currency = defaultPriceGroup.Currency
			region = defaultPriceGroup.Region
			amount, err = product.GetPriceInCurrencyAndPlatform(defaultPriceGroup, p.Id)
			if err != nil {
				zap.S().Error("could not get price in currency and platform for default price group", "price_group", defaultPriceGroup, "platform", p.Id)
				res.Status = pkg.ResponseStatusSystemError
				res.Message = keyProductInternalError
				return nil
			}
		}

		platforms[i] = &grpc.PlatformPriceInfo{
			Name: p.Name,
			Id:   p.Id,
			Price: &grpc.ProductPriceInfo{
				Amount:   amount,
				Currency: currency,
				Region:   region,
				IsFallback: isFallback,
			},
		}
	}
	res.KeyProduct.Platforms = platforms

	return nil
}

func (s *Service) GetKeyProduct(ctx context.Context, req *grpc.RequestKeyProductMerchant, res *grpc.KeyProductResponse) error {
	product, err := s.getKeyProductById(req.Id)
	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Query to find key product by id failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductRetrieveError
		return nil
	}

	res.Product = product

	return nil
}

func (s *Service) getKeyProductById(id string) (*grpc.KeyProduct, error) {
	query := bson.M{
		"_id":     bson.ObjectIdHex(id),
		"deleted": false,
	}

	var product = &grpc.KeyProduct{}
	err := s.db.Collection(collectionKeyProduct).Find(query).One(product)
	return product, err
}

func (s *Service) DeleteKeyProduct(ctx context.Context, req *grpc.RequestKeyProductMerchant, res *grpc.EmptyResponseWithStatus) error {
	product, err := s.getKeyProductById(req.Id)
	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductRetrieveError
		return nil
	}

	product.Deleted = true
	product.UpdatedAt = ptypes.TimestampNow()

	err = s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(product.Id), product)

	if err != nil {
		zap.S().Errorf("Query to delete key product failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductErrorDelete
		return nil
	}

	return nil
}

func (s *Service) PublishKeyProduct(ctx context.Context, req *grpc.PublishKeyProductRequest, res *grpc.KeyProductResponse) error {
	product, err := s.getKeyProductById(req.KeyProductId)

	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductRetrieveError
		return nil
	}

	product.UpdatedAt = ptypes.TimestampNow()
	product.PublishedAt = ptypes.TimestampNow()
	product.Enabled = true

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(product.Id), res); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductErrorUpsert
		return nil
	}

	return nil
}

func (s *Service) GetKeyProductsForOrder(ctx context.Context, req *grpc.GetKeyProductsForOrderRequest, res *grpc.ListKeyProductsResponse) error {
	if len(req.Ids) == 0 {
		zap.S().Errorf("Ids list is empty", "data", req)
		res.Status = http.StatusBadRequest
		res.Message = keyProductIdsIsEmpty
		return nil
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
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
	}

	res.Limit = int32(len(found))
	res.Offset = 0
	res.Count = res.Limit
	res.Products = found
	return nil
}

func (s *Service) UpdatePlatformPrices(ctx context.Context, req *grpc.AddOrUpdatePlatformPricesRequest, res *grpc.UpdatePlatformPricesResponse) error {
	product, err := s.getKeyProductById(req.KeyProductId)

	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = keyProductRetrieveError
		return nil
	}

	price := req.Platform

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
		res.Status = http.StatusBadRequest
		res.Message = keyProductAlreadyHasPlatform
		return nil
	}

	if found == false {
		if isAvailable == false {
			if price.ActivationUrl == "" {
				zap.S().Errorf("Activation url must be set", "data", req)
				res.Status = http.StatusBadRequest
				res.Message = keyProductActivationUrlEmpty
				return nil
			}

			if price.EulaUrl == "" {
				zap.S().Errorf("Eula url must be set", "data", req)
				res.Status = http.StatusBadRequest
				res.Message = keyProductEulaEmpty
				return nil
			}

			if price.Name == "" {
				zap.S().Errorf("Name must be set", "data", req)
				res.Status = http.StatusBadRequest
				res.Message = keyProductPlatformName
				return nil
			}

			price.ActivationUrl = req.Platform.ActivationUrl
			price.EulaUrl = req.Platform.EulaUrl
			price.Name = req.Platform.Name
		}
		product.Platforms = append(product.Platforms, price)
	}

	if err := s.db.Collection(collectionKeyProduct).UpdateId(bson.ObjectIdHex(req.KeyProductId), product); err != nil {
		zap.S().Errorf("Query to update product failed", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
	}

	res.Price = price

	return nil
}

func (s *Service) DeletePlatformFromProduct(ctx context.Context, req *grpc.RemovePlatformRequest, res *grpc.EmptyResponseWithStatus) error {
	product, err := s.getKeyProductById(req.KeyProductId)

	if err == mgo.ErrNotFound {
		res.Status = http.StatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if err != nil {
		zap.S().Errorf("Error during getting key product", "err", err.Error(), "data", req)
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
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
		res.Status = http.StatusInternalServerError
		res.Message = newBillingServerErrorMsg(keyProductInternalError.Code, keyProductInternalError.Message, err.Error())
		return nil
	}

	return nil
}
