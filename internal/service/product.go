package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	cacheProductId    = "product:id:%s"
	collectionProduct = "product"
)

var (
	productErrorUnknown                    = newBillingServerErrorMsg("pd000001", "unknown error with product")
	productErrorNotFound                   = newBillingServerErrorMsg("pd000002", "products with specified ID not found")
	productErrorIdListEmpty                = newBillingServerErrorMsg("pd000007", "ids list is empty")
	productErrorMerchantNotEqual           = newBillingServerErrorMsg("pd000008", "merchant id is not equal in a product")
	productErrorProjectNotEqual            = newBillingServerErrorMsg("pd000009", "project id is not equal in a product")
	productErrorPriceDefaultCurrency       = newBillingServerErrorMsg("pd000010", "no price in default currency")
	productErrorNameDefaultLanguage        = newBillingServerErrorMsg("pd000011", "no name in default language")
	productErrorDescriptionDefaultLanguage = newBillingServerErrorMsg("pd000012", "no description in default language")
	productErrorUpsert                     = newBillingServerErrorMsg("pd000013", "query to insert/update product failed")
	productErrorDelete                     = newBillingServerErrorMsg("pd000014", "query to delete product failed")
	productErrorProjectAndSkuAlreadyExists = newBillingServerErrorMsg("pd000015", "pair projectId+Sku already exists")
	productErrorListPrices                 = newBillingServerErrorMsg("pd000017", "list of prices is empty")
	productErrorPricesUpdate               = newBillingServerErrorMsg("pd000017", "query to update product prices is failed")
	productSkuMismatch                     = newBillingServerErrorMsg("pd000008", "sku mismatch")
)

func (s *Service) CreateOrUpdateProduct(ctx context.Context, req *grpc.Product, res *grpc.Product) error {
	var (
		err     error
		product = &grpc.Product{}
		isNew   = req.Id == ""
		now     = ptypes.TimestampNow()
	)

	if isNew {
		req.Id = primitive.NewObjectID().Hex()
		req.CreatedAt = now
	} else {
		product, err = s.productService.GetById(ctx, req.Id)
		if err != nil {
			zap.S().Errorf("Product that requested to change is not found", "err", err.Error(), "data", req)
			return productErrorNotFound
		}

		if req.Sku != "" && req.Sku != product.Sku {
			zap.S().Errorf("SKU mismatch", "data", req)
			return productSkuMismatch
		}

		if req.MerchantId != product.MerchantId {
			zap.S().Errorf("MerchantId mismatch", "data", req)
			return productErrorMerchantNotEqual
		}

		if req.ProjectId != product.ProjectId {
			zap.S().Errorf("ProjectId mismatch", "data", req)
			return productErrorProjectNotEqual
		}

		req.CreatedAt = product.CreatedAt
	}
	req.UpdatedAt = now
	req.Deleted = false

	if !req.IsPricesContainDefaultCurrency() {
		zap.S().Errorf(productErrorPriceDefaultCurrency.Message, "data", req)
		return productErrorPriceDefaultCurrency
	}

	if _, err := req.GetLocalizedName(DefaultLanguage); err != nil {
		zap.S().Errorf("No name in default language", "data", req)
		return productErrorNameDefaultLanguage
	}

	if _, err := req.GetLocalizedDescription(DefaultLanguage); err != nil {
		zap.S().Errorf("No description in default language", "data", req)
		return productErrorDescriptionDefaultLanguage
	}

	count, err := s.productService.CountByProjectSku(ctx, req.ProjectId, req.Sku)

	if err != nil {
		zap.S().Errorf("Query to find duplicates failed", "err", err.Error(), "data", req)
		return productErrorUnknown
	}

	allowed := int64(1)

	if isNew {
		allowed = 0
	}

	if count > allowed {
		zap.S().Errorf("Pair projectId+Sku already exists", "data", req)
		return productErrorProjectAndSkuAlreadyExists
	}

	if err = s.productService.Upsert(ctx, req); err != nil {
		zap.S().Errorf("Query to create/update product failed", "err", err.Error(), "data", req)
		return productErrorUpsert
	}

	res.Id = req.Id
	res.Object = req.Object
	res.Type = req.Type
	res.Sku = req.Sku
	res.Name = req.Name
	res.DefaultCurrency = req.DefaultCurrency
	res.Enabled = req.Enabled
	res.Prices = req.Prices
	res.Description = req.Description
	res.LongDescription = req.LongDescription
	res.Images = req.Images
	res.Url = req.Url
	res.Metadata = req.Metadata
	res.CreatedAt = req.CreatedAt
	res.UpdatedAt = req.UpdatedAt
	res.Deleted = req.Deleted
	res.MerchantId = req.MerchantId
	res.ProjectId = req.ProjectId
	res.Pricing = req.Pricing
	res.BillingType = req.BillingType

	return nil
}

func (s *Service) GetProductsForOrder(ctx context.Context, req *grpc.GetProductsForOrderRequest, res *grpc.ListProductsResponse) error {
	if len(req.Ids) == 0 {
		zap.S().Errorf("Ids list is empty", "data", req)
		return productErrorIdListEmpty
	}

	var found []*grpc.Product
	for _, id := range req.Ids {
		p, err := s.productService.GetById(ctx, id)

		if err != nil {
			zap.S().Errorf("Unable to get product", "err", err.Error(), "req", req)
			continue
		}

		if p.Enabled != true || p.ProjectId != req.ProjectId {
			continue
		}

		found = append(found, p)
	}

	res.Limit = int64(len(found))
	res.Offset = 0
	res.Total = res.Limit
	res.Products = found
	return nil
}

func (s *Service) ListProducts(ctx context.Context, req *grpc.ListProductsRequest, res *grpc.ListProductsResponse) error {
	res.Total, res.Products = s.productService.List(
		ctx,
		req.MerchantId,
		req.ProjectId,
		req.Sku,
		req.Name,
		req.Offset,
		req.Limit,
		req.Enable,
	)

	res.Limit = req.Limit
	res.Offset = req.Offset

	return nil
}

func (s *Service) GetProduct(
	ctx context.Context,
	req *grpc.RequestProduct,
	rsp *grpc.GetProductResponse,
) error {
	product, err := s.productService.GetById(ctx, req.Id)

	if err != nil {
		zap.L().Error(
			"Unable to get product",
			zap.Error(err),
			zap.Any(pkg.LogFieldRequest, req),
		)

		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = productErrorNotFound

		return nil
	}

	if req.MerchantId != product.MerchantId {
		zap.L().Error(
			"Merchant id mismatch",
			zap.Any("product", product),
			zap.Any(pkg.LogFieldRequest, req),
		)

		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = productErrorMerchantNotEqual

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = product

	return nil
}

func (s *Service) DeleteProduct(ctx context.Context, req *grpc.RequestProduct, res *grpc.EmptyResponse) error {
	product, err := s.productService.GetById(ctx, req.Id)

	if err != nil {
		zap.S().Errorf("Unable to get product", "err", err.Error(), "req", req)
		return productErrorNotFound
	}

	if req.MerchantId != product.MerchantId {
		zap.S().Errorf("MerchantId mismatch", "product", product, "data", req)
		return productErrorMerchantNotEqual
	}

	product.Deleted = true
	product.UpdatedAt = ptypes.TimestampNow()

	err = s.productService.Upsert(ctx, product)

	if err != nil {
		zap.S().Errorf("Query to delete product failed", "err", err.Error(), "data", req)
		return productErrorDelete
	}

	return nil
}

func (s *Service) GetProductPrices(ctx context.Context, req *grpc.RequestProduct, res *grpc.ProductPricesResponse) error {
	product, err := s.productService.GetById(ctx, req.Id)

	if err != nil {
		zap.S().Errorf("Unable to get product", "err", err.Error(), "req", req)
		return productErrorNotFound
	}

	if req.MerchantId != product.MerchantId {
		zap.S().Errorf("MerchantId mismatch", "product", product, "data", req)
		return productErrorMerchantNotEqual
	}

	res.ProductPrice = product.Prices

	return nil
}

func (s *Service) UpdateProductPrices(ctx context.Context, req *grpc.UpdateProductPricesRequest, res *grpc.ResponseError) error {
	if len(req.Prices) == 0 {
		zap.S().Errorf("List of product prices is empty", "data", req)
		return productErrorListPrices
	}

	product, err := s.productService.GetById(ctx, req.ProductId)

	if err != nil {
		zap.S().Errorf("Unable to get product", "err", err.Error(), "req", req)
		return productErrorNotFound
	}

	if req.MerchantId != product.MerchantId {
		zap.S().Errorf("MerchantId mismatch", "product", product, "data", req)
		return productErrorMerchantNotEqual
	}

	product.Prices = req.Prices

	// note: virtual currency has IsVirtualCurrency=true && Currency=""
	for _, p := range product.Prices {
		if p.IsVirtualCurrency == true {
			p.Currency = ""
		}
	}

	merchant, err := s.merchant.GetById(ctx, product.MerchantId)
	if err != nil {
		res.Status = pkg.ResponseStatusNotFound
		res.Message = merchantErrorNotFound

		return nil
	}

	payoutCurrency := merchant.GetPayoutCurrency()

	if len(payoutCurrency) == 0 {
		zap.S().Errorw(merchantPayoutCurrencyMissed.Message, "data", req)
		res.Status = pkg.ResponseStatusBadData
		res.Message = merchantPayoutCurrencyMissed
		return nil
	}

	_, err = product.GetPriceInCurrency(&billing.PriceGroup{Currency: payoutCurrency})
	if err != nil {
		_, err = product.GetPriceInCurrency(&billing.PriceGroup{Currency: grpc.VirtualCurrencyPriceGroup})
	}

	if err != nil {
		zap.S().Errorw(productErrorPriceDefaultCurrency.Message, "data", req)
		return productErrorPriceDefaultCurrency
	}

	if !product.IsPricesContainDefaultCurrency() {
		zap.S().Errorf(productErrorPriceDefaultCurrency.Message, "data", req)
		return productErrorPriceDefaultCurrency
	}

	if err := s.productService.Upsert(ctx, product); err != nil {
		zap.S().Errorf("Query to create/update product failed", "err", err.Error(), "data", req)
		return productErrorPricesUpdate
	}

	return nil
}

func (s *Service) getProductsCountByProject(ctx context.Context, projectId string) int64 {
	oid, _ := primitive.ObjectIDFromHex(projectId)
	query := bson.M{"project_id": oid, "deleted": false}
	count, err := s.db.Collection(collectionProduct).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to get project products count failed", "err", err.Error(), "query", query)
	}

	return count
}

type ProductServiceInterface interface {
	Upsert(ctx context.Context, product *grpc.Product) error
	GetById(context.Context, string) (*grpc.Product, error)
	CountByProjectSku(context.Context, string, string) (int64, error)
	List(context.Context, string, string, string, string, int64, int64, int32) (int64, []*grpc.Product)
}

func newProductService(svc *Service) *Product {
	s := &Product{svc: svc}
	return s
}

func (h *Product) Upsert(ctx context.Context, p *grpc.Product) error {
	oid, _ := primitive.ObjectIDFromHex(p.Id)
	filter := bson.M{"_id": oid}
	opts := options.FindOneAndUpdate().SetUpsert(true)
	err := h.svc.db.Collection(collectionProduct).FindOneAndUpdate(ctx, filter, p, opts).Err()

	if err != nil {
		return err
	}

	if err := h.updateCache(p); err != nil {
		return err
	}

	return nil
}

func (h *Product) GetById(ctx context.Context, id string) (*grpc.Product, error) {
	var c grpc.Product
	key := fmt.Sprintf(cacheProductId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid, "deleted": false}
	err := h.svc.db.Collection(collectionProduct).FindOne(ctx, filter).Decode(&c)

	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionProduct)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}

func (h *Product) CountByProjectSku(ctx context.Context, projectId string, sku string) (int64, error) {
	oid, _ := primitive.ObjectIDFromHex(projectId)
	query := bson.M{"project_id": oid, "sku": sku, "deleted": false}
	count, err := h.svc.db.Collection(collectionProduct).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to find duplicates failed", "err", err.Error(), "projectId", projectId)
		return 0, productErrorUnknown
	}

	return count, nil
}

func (h *Product) List(
	ctx context.Context,
	merchantId string,
	projectId string,
	sku string,
	name string,
	offset int64,
	limit int64,
	enable int32,
) (int64, []*grpc.Product) {
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": merchantOid, "deleted": false}

	if projectId != "" {
		query["project_id"], _ = primitive.ObjectIDFromHex(projectId)
	}

	if sku != "" {
		query["sku"] = primitive.Regex{Pattern: sku, Options: "i"}
	}
	if name != "" {
		query["name"] = bson.M{"$elemMatch": bson.M{"value": primitive.Regex{Pattern: name, Options: "i"}}}
	}

	if enable > 0 {
		if enable == 1 {
			query["enabled"] = false
		} else {
			query["enabled"] = true
		}
	}

	count, err := h.svc.db.Collection(collectionProduct).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, nil
	}

	if count == 0 || offset > count {
		zap.L().Error(
			"total is empty or less then total",
			zap.Error(err),
			zap.Int64(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Int64(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return 0, nil
	}

	opts := options.Find().
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := h.svc.db.Collection(collectionProduct).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, nil
	}

	var list []*grpc.Product
	err = cursor.All(ctx, &list)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, nil
	}

	return count, list
}

func (h *Product) updateCache(p *grpc.Product) error {
	if err := h.svc.cacher.Set(fmt.Sprintf(cacheProductId, p.Id), p, 0); err != nil {
		return err
	}

	return nil
}
