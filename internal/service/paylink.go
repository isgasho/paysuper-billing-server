package service

import (
	"context"
	"fmt"
	u "github.com/PuerkitoBio/purell"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/go-querystring/query"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/paylink"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

type orderViewPaylinkStatFunc func(OrderViewServiceInterface, context.Context, string, string, int64, int64) (*paylink.GroupStatCommon, error)

type paylinkVisits struct {
	PaylinkId primitive.ObjectID `bson:"paylink_id"`
	Date      time.Time          `bson:"date"`
}

type utmQueryParams struct {
	UtmSource   string `url:"utm_source,omitempty"`
	UtmMedium   string `url:"utm_medium,omitempty"`
	UtmCampaign string `url:"utm_campaign,omitempty"`
}

type PaylinkServiceInterface interface {
	CountByQuery(ctx context.Context, query bson.M) (n int64, err error)
	GetListByQuery(ctx context.Context, query bson.M, limit, offset int64) (result []*paylink.Paylink, err error)
	GetById(ctx context.Context, id string) (pl *paylink.Paylink, err error)
	GetByIdAndMerchant(ctx context.Context, id, merchantId string) (pl *paylink.Paylink, err error)
	IncrVisits(ctx context.Context, id string) error
	GetUrl(ctx context.Context, id, merchantId, urlMask, utmSource, utmMedium, utmCampaign string) (string, error)
	Delete(ctx context.Context, id, merchantId string) error
	Insert(ctx context.Context, pl *paylink.Paylink) error
	Update(ctx context.Context, pl *paylink.Paylink) error
	UpdatePaylinkTotalStat(ctx context.Context, id, merchantId string) error
	GetPaylinkVisits(ctx context.Context, id string, from, to int64) (int64, error)
}

const (
	collectionPaylinks      = "paylinks"
	collectionPaylinkVisits = "paylink_visits"

	cacheKeyPaylink         = "paylink:id:%s"
	cacheKeyPaylinkMerchant = "paylink:id:%s:merhcant_id:%s"
)

var (
	errorPaylinkExpired                      = newBillingServerErrorMsg("pl000001", "payment link expired")
	errorPaylinkNotFound                     = newBillingServerErrorMsg("pl000002", "paylink not found")
	errorPaylinkProjectMismatch              = newBillingServerErrorMsg("pl000003", "projectId mismatch for existing paylink")
	errorPaylinkExpiresInPast                = newBillingServerErrorMsg("pl000004", "paylink expiry date in past")
	errorPaylinkProductsLengthInvalid        = newBillingServerErrorMsg("pl000005", "paylink products length invalid")
	errorPaylinkProductsTypeInvalid          = newBillingServerErrorMsg("pl000006", "paylink products type invalid")
	errorPaylinkProductNotBelongToMerchant   = newBillingServerErrorMsg("pl000007", "at least one of paylink products is not belongs to merchant")
	errorPaylinkProductNotBelongToProject    = newBillingServerErrorMsg("pl000008", "at least one of paylink products is not belongs to project")
	errorPaylinkStatDataInconsistent         = newBillingServerErrorMsg("pl000009", "paylink stat data inconsistent")
	errorPaylinkProductNotFoundOrInvalidType = newBillingServerErrorMsg("pl000010", "at least one of paylink products is not found or have type differ from given products_type value")

	orderViewPaylinkStatFuncMap = map[string]orderViewPaylinkStatFunc{
		"GetPaylinkStatByCountry":  OrderViewServiceInterface.GetPaylinkStatByCountry,
		"GetPaylinkStatByReferrer": OrderViewServiceInterface.GetPaylinkStatByReferrer,
		"GetPaylinkStatByDate":     OrderViewServiceInterface.GetPaylinkStatByDate,
		"GetPaylinkStatByUtm":      OrderViewServiceInterface.GetPaylinkStatByUtm,
	}
)

func newPaylinkService(svc *Service) *Paylink {
	s := &Paylink{svc: svc}
	return s
}

// GetPaylinks returns list of all payment links
func (s *Service) GetPaylinks(
	ctx context.Context,
	req *grpc.GetPaylinksRequest,
	res *grpc.GetPaylinksResponse,
) error {
	merchantOid, _ := primitive.ObjectIDFromHex(req.MerchantId)
	dbQuery := bson.M{
		"deleted":     false,
		"merchant_id": merchantOid,
	}

	if req.ProjectId != "" {
		dbQuery["project_id"], _ = primitive.ObjectIDFromHex(req.ProjectId)
	}

	n, err := s.paylinkService.CountByQuery(ctx, dbQuery)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Data = &grpc.PaylinksPaginate{}

	if n > 0 {
		res.Data.Items, err = s.paylinkService.GetListByQuery(ctx, dbQuery, req.Limit, req.Offset)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}

		for _, pl := range res.Data.Items {
			visits, err := s.paylinkService.GetPaylinkVisits(ctx, pl.Id, 0, 0)
			if err == nil {
				pl.Visits = int32(visits)
			}
			pl.UpdateConversion()
			pl.IsExpired = pl.GetIsExpired()
		}
	}

	res.Data.Count = int32(n)
	res.Status = pkg.ResponseStatusOk

	return nil
}

// GetPaylink returns one payment link
func (s *Service) GetPaylink(
	ctx context.Context,
	req *grpc.PaylinkRequest,
	res *grpc.GetPaylinkResponse,
) (err error) {

	res.Item, err = s.paylinkService.GetByIdAndMerchant(ctx, req.Id, req.MerchantId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	visits, err := s.paylinkService.GetPaylinkVisits(ctx, res.Item.Id, 0, 0)
	if err == nil {
		res.Item.Visits = int32(visits)
	}
	res.Item.UpdateConversion()

	res.Status = pkg.ResponseStatusOk
	return nil
}

// IncrPaylinkVisits adds a visit hit to stat
func (s *Service) IncrPaylinkVisits(
	ctx context.Context,
	req *grpc.PaylinkRequestById,
	res *grpc.EmptyResponse,
) error {
	err := s.paylinkService.IncrVisits(ctx, req.Id)
	if err != nil {
		return err
	}
	return nil
}

// GetPaylinkURL returns public url for Paylink
func (s *Service) GetPaylinkURL(
	ctx context.Context,
	req *grpc.GetPaylinkURLRequest,
	res *grpc.GetPaylinkUrlResponse,
) (err error) {

	res.Url, err = s.paylinkService.GetUrl(ctx, req.Id, req.MerchantId, req.UrlMask, req.UtmMedium, req.UtmMedium, req.UtmCampaign)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			if err == errorPaylinkExpired {
				res.Status = pkg.ResponseStatusGone
			} else {
				res.Status = pkg.ResponseStatusBadData
			}

			res.Message = e
			return nil
		}
		return err
	}

	res.Status = pkg.ResponseStatusOk
	return nil
}

// DeletePaylink deletes payment link
func (s *Service) DeletePaylink(
	ctx context.Context,
	req *grpc.PaylinkRequest,
	res *grpc.EmptyResponseWithStatus,
) error {

	err := s.paylinkService.Delete(ctx, req.Id, req.MerchantId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Status = pkg.ResponseStatusOk
	return nil
}

// CreateOrUpdatePaylink create or modify payment link
func (s *Service) CreateOrUpdatePaylink(
	ctx context.Context,
	req *paylink.CreatePaylinkRequest,
	res *grpc.GetPaylinkResponse,
) (err error) {

	isNew := req.GetId() == ""

	pl := &paylink.Paylink{}

	if isNew {
		pl.Id = primitive.NewObjectID().Hex()
		pl.CreatedAt = ptypes.TimestampNow()
		pl.Object = "paylink"
		pl.MerchantId = req.MerchantId
		pl.ProjectId = req.ProjectId
		pl.ProductsType = req.ProductsType
	} else {
		pl, err = s.paylinkService.GetByIdAndMerchant(ctx, req.Id, req.MerchantId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				res.Status = pkg.ResponseStatusNotFound
				res.Message = errorPaylinkNotFound
				return nil
			}
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}

		if pl.ProjectId != req.ProjectId {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaylinkProjectMismatch
			return nil
		}
	}

	pl.UpdatedAt = ptypes.TimestampNow()
	pl.Name = req.Name
	pl.NoExpiryDate = req.NoExpiryDate

	projectOid, _ := primitive.ObjectIDFromHex(pl.ProjectId)
	merchantOid, _ := primitive.ObjectIDFromHex(pl.MerchantId)
	dbQuery := bson.M{"_id": projectOid, "merchant_id": merchantOid}
	_, err = s.getProjectBy(ctx, dbQuery)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	if pl.NoExpiryDate == false {
		expiresAt := now.New(time.Unix(req.ExpiresAt, 0)).EndOfDay()

		if time.Now().After(expiresAt) {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaylinkExpiresInPast
			return nil
		}

		pl.ExpiresAt, err = ptypes.TimestampProto(expiresAt)
		if err != nil {
			zap.L().Error(
				pkg.ErrorTimeConversion,
				zap.Any(pkg.ErrorTimeConversionMethod, "ptypes.TimestampProto"),
				zap.Any(pkg.ErrorTimeConversionValue, expiresAt),
				zap.Error(err),
			)
			return err
		}
	}

	productsLength := len(req.Products)
	if productsLength < s.cfg.PaylinkMinProducts || productsLength > s.cfg.PaylinkMaxProducts {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaylinkProductsLengthInvalid
		return nil
	}

	for _, productId := range req.Products {
		switch req.ProductsType {

		case billing.OrderType_product:
			product, err := s.productService.GetById(ctx, productId)
			if err != nil {
				if err.Error() == "product not found" || err == mongo.ErrNoDocuments {
					res.Status = pkg.ResponseStatusNotFound
					res.Message = errorPaylinkProductNotFoundOrInvalidType
					return nil
				}

				if e, ok := err.(*grpc.ResponseErrorMessage); ok {
					res.Status = pkg.ResponseStatusBadData
					res.Message = e
					return nil
				}
				return err
			}

			if product.MerchantId != pl.MerchantId {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorPaylinkProductNotBelongToMerchant
				return nil
			}

			if product.ProjectId != pl.ProjectId {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorPaylinkProductNotBelongToProject
				return nil
			}

			break

		case billing.OrderType_key:
			product, err := s.keyProductRepository.GetById(ctx, productId)
			if err != nil {
				if err.Error() == "key_product not found" || err == mongo.ErrNoDocuments {
					res.Status = pkg.ResponseStatusNotFound
					res.Message = errorPaylinkProductNotFoundOrInvalidType
					return nil
				}

				if e, ok := err.(*grpc.ResponseErrorMessage); ok {
					res.Status = pkg.ResponseStatusBadData
					res.Message = e
					return nil
				}
				return err
			}

			if product.MerchantId != pl.MerchantId {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorPaylinkProductNotBelongToMerchant
				return nil
			}

			if product.ProjectId != pl.ProjectId {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorPaylinkProductNotBelongToProject
				return nil
			}
			break

		default:
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaylinkProductsTypeInvalid
			return nil
		}
	}

	pl.ProductsType = req.ProductsType
	pl.Products = req.Products

	if isNew {
		err = s.paylinkService.Insert(ctx, pl)
	} else {
		err = s.paylinkService.Update(ctx, pl)
	}
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Item = pl
	res.Status = pkg.ResponseStatusOk

	return nil
}

// GetPaylinkStatTotal returns total stat for requested paylink and period
func (s *Service) GetPaylinkStatTotal(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonResponse,
) (err error) {

	pl, err := s.paylinkService.GetByIdAndMerchant(ctx, req.Id, req.MerchantId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	visits, err := s.paylinkService.GetPaylinkVisits(ctx, pl.Id, req.PeriodFrom, req.PeriodTo)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Item, err = s.orderView.GetPaylinkStat(ctx, pl.Id, req.MerchantId, req.PeriodFrom, req.PeriodTo)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Item.PaylinkId = pl.Id
	res.Item.Visits = int32(visits)
	res.Item.UpdateConversion()

	res.Status = pkg.ResponseStatusOk
	return nil
}

// GetPaylinkStatByCountry returns stat groped by country for requested paylink and period
func (s *Service) GetPaylinkStatByCountry(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonGroupResponse,
) (err error) {
	err = s.getPaylinkStatGroup(ctx, req, res, "GetPaylinkStatByCountry")
	if err != nil {
		return err
	}
	return nil
}

// GetPaylinkStatByReferrer returns stat grouped by referer hosts for requested paylink and period
func (s *Service) GetPaylinkStatByReferrer(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonGroupResponse,
) (err error) {
	err = s.getPaylinkStatGroup(ctx, req, res, "GetPaylinkStatByReferrer")
	if err != nil {
		return err
	}
	return nil
}

// GetPaylinkStatByDate returns stat groped by date for requested paylink and period
func (s *Service) GetPaylinkStatByDate(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonGroupResponse,
) (err error) {
	err = s.getPaylinkStatGroup(ctx, req, res, "GetPaylinkStatByDate")
	if err != nil {
		return err
	}
	return nil
}

// GetPaylinkStatByUtm returns stat groped by utm labels for requested paylink and period
func (s *Service) GetPaylinkStatByUtm(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonGroupResponse,
) (err error) {
	err = s.getPaylinkStatGroup(ctx, req, res, "GetPaylinkStatByUtm")
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) getPaylinkStatGroup(
	ctx context.Context,
	req *grpc.GetPaylinkStatCommonRequest,
	res *grpc.GetPaylinkStatCommonGroupResponse,
	function string,
) (err error) {
	pl, err := s.paylinkService.GetByIdAndMerchant(ctx, req.Id, req.MerchantId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			res.Status = pkg.ResponseStatusNotFound
			res.Message = errorPaylinkNotFound
			return nil
		}
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	res.Item, err = orderViewPaylinkStatFuncMap[function](s.orderView, ctx, pl.Id, pl.MerchantId, req.PeriodFrom, req.PeriodTo)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}
	res.Status = pkg.ResponseStatusOk
	return nil
}

func (p Paylink) CountByQuery(ctx context.Context, query bson.M) (n int64, err error) {
	n, err = p.svc.db.Collection(collectionPaylinks).CountDocuments(ctx, query)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationCount),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
	}
	return
}

func (p Paylink) GetListByQuery(ctx context.Context, query bson.M, limit, offset int64) (result []*paylink.Paylink, err error) {
	if limit <= 0 {
		limit = pkg.DatabaseRequestDefaultLimit
	}

	if offset <= 0 {
		offset = 0
	}

	opts := options.Find().
		SetSort(bson.M{"_id": 1}).
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := p.svc.db.Collection(collectionPaylinks).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	return
}

func (p Paylink) GetById(ctx context.Context, id string) (pl *paylink.Paylink, err error) {
	key := fmt.Sprintf(cacheKeyPaylink, id)
	oid, _ := primitive.ObjectIDFromHex(id)
	dbQuery := bson.M{"_id": oid, "deleted": false}
	return p.getBy(ctx, key, dbQuery)
}

func (p Paylink) GetByIdAndMerchant(ctx context.Context, id, merchantId string) (pl *paylink.Paylink, err error) {
	oid, _ := primitive.ObjectIDFromHex(id)
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	key := fmt.Sprintf(cacheKeyPaylinkMerchant, id, merchantId)
	dbQuery := bson.M{"_id": oid, "merchant_id": merchantOid, "deleted": false}
	return p.getBy(ctx, key, dbQuery)
}

func (p Paylink) getBy(ctx context.Context, key string, dbQuery bson.M) (pl *paylink.Paylink, err error) {
	if err = p.svc.cacher.Get(key, &pl); err == nil {
		pl.IsExpired = pl.GetIsExpired()
		return pl, nil
	}

	err = p.svc.db.Collection(collectionPaylinks).FindOne(ctx, dbQuery).Decode(&pl)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.Any(pkg.ErrorDatabaseFieldQuery, dbQuery),
		)
		return
	}

	pl.IsExpired = pl.GetIsExpired()

	if pl.Deleted == false {
		err = p.updateCaches(pl)
	}
	return
}

func (p Paylink) IncrVisits(ctx context.Context, id string) (err error) {
	oid, _ := primitive.ObjectIDFromHex(id)
	visit := &paylinkVisits{PaylinkId: oid, Date: time.Now()}
	_, err = p.svc.db.Collection(collectionPaylinkVisits).InsertOne(ctx, visit)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinkVisits),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, visit),
		)
	}

	return
}

func (p *Paylink) GetUrl(ctx context.Context, id, merchantId, urlMask, utmSource, utmMedium, utmCampaign string) (string, error) {
	pl, err := p.GetByIdAndMerchant(ctx, id, merchantId)
	if err != nil {
		return "", err
	}
	if pl.GetIsExpired() {
		return "", errorPaylinkExpired
	}

	if urlMask == "" {
		urlMask = pkg.PaylinkUrlDefaultMask
	}

	urlString := fmt.Sprintf(urlMask, id)

	utmQuery := &utmQueryParams{
		UtmSource:   utmSource,
		UtmMedium:   utmMedium,
		UtmCampaign: utmCampaign,
	}

	q, err := query.Values(utmQuery)
	if err != nil {
		zap.L().Error(
			"Failed to serialize utm query params",
			zap.Error(err),
		)
		return "", err
	}
	encodedQuery := q.Encode()
	if encodedQuery != "" {
		urlString += "?" + encodedQuery
	}

	return u.NormalizeURLString(urlString, u.FlagsUsuallySafeGreedy|u.FlagRemoveDuplicateSlashes)
}

func (p Paylink) Delete(ctx context.Context, id, merchantId string) error {
	pl, err := p.GetByIdAndMerchant(ctx, id, merchantId)
	if err != nil {
		return err
	}

	pl.Deleted = true

	oid, _ := primitive.ObjectIDFromHex(pl.Id)
	filter := bson.M{"_id": oid}
	_, err = p.svc.db.Collection(collectionPaylinks).ReplaceOne(ctx, filter, pl)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldDocument, pl),
		)

		return err
	}

	err = p.updateCaches(pl)

	return nil
}

func (p Paylink) Update(ctx context.Context, pl *paylink.Paylink) (err error) {
	oid, _ := primitive.ObjectIDFromHex(pl.Id)
	dbQuery := bson.M{"_id": oid}

	set := bson.M{"$set": bson.M{
		"expires_at":     pl.ExpiresAt,
		"updated_at":     pl.UpdatedAt,
		"products":       pl.Products,
		"name":           pl.Name,
		"no_expiry_date": pl.NoExpiryDate,
		"products_type":  pl.ProductsType,
		"is_expired":     pl.GetIsExpired(),
	}}
	_, err = p.svc.db.Collection(collectionPaylinks).UpdateOne(ctx, dbQuery, set)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.Any(pkg.ErrorDatabaseFieldQuery, dbQuery),
			zap.Any(pkg.ErrorDatabaseFieldSet, set),
		)

		return err
	}

	err = p.updateCaches(pl)

	return
}

func (p Paylink) Insert(ctx context.Context, pl *paylink.Paylink) (err error) {
	oid, _ := primitive.ObjectIDFromHex(pl.Id)
	filter := bson.M{"_id": oid}
	opts := options.FindOneAndReplace().SetUpsert(true)
	err = p.svc.db.Collection(collectionPaylinks).FindOneAndReplace(ctx, filter, pl, opts).Err()

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, pl),
		)
	}

	err = p.updateCaches(pl)

	return
}

func (p Paylink) GetPaylinkVisits(ctx context.Context, id string, from, to int64) (n int64, err error) {
	oid, _ := primitive.ObjectIDFromHex(id)
	matchQuery := bson.M{"paylink_id": oid}

	if from > 0 || to > 0 {
		date := bson.M{}
		if from > 0 {
			date["$gte"] = time.Unix(from, 0)
		}
		if to > 0 {
			date["$lte"] = time.Unix(to, 0)
		}
		matchQuery["date"] = date
	}

	n, err = p.svc.db.Collection(collectionPaylinkVisits).CountDocuments(ctx, matchQuery)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationCount),
			zap.Any(pkg.ErrorDatabaseFieldQuery, matchQuery),
		)
	}
	return
}

func (p Paylink) UpdatePaylinkTotalStat(ctx context.Context, id, merchantId string) (err error) {
	pl, err := p.GetByIdAndMerchant(ctx, id, merchantId)
	if err != nil {
		return err
	}

	visits, err := p.GetPaylinkVisits(ctx, id, 0, 0)
	if err == nil {
		pl.Visits = int32(visits)
	}

	stat, err := p.svc.orderView.GetPaylinkStat(ctx, id, merchantId, 0, 0)
	if err != nil {
		return err
	}

	pl.TotalTransactions = stat.TotalTransactions
	pl.ReturnsCount = stat.ReturnsCount
	pl.SalesCount = stat.SalesCount
	pl.TransactionsCurrency = stat.TransactionsCurrency
	pl.GrossTotalAmount = stat.GrossTotalAmount
	pl.GrossSalesAmount = stat.GrossSalesAmount
	pl.GrossReturnsAmount = stat.GrossReturnsAmount
	pl.UpdateConversion()

	oid, _ := primitive.ObjectIDFromHex(pl.Id)
	dbQuery := bson.M{"_id": oid}

	set := bson.M{"$set": bson.M{
		"visits":                pl.Visits,
		"conversion":            pl.Conversion,
		"total_transactions":    stat.TotalTransactions,
		"sales_count":           stat.SalesCount,
		"returns_count":         stat.ReturnsCount,
		"gross_sales_amount":    stat.GrossSalesAmount,
		"gross_returns_amount":  stat.GrossReturnsAmount,
		"gross_total_amount":    stat.GrossTotalAmount,
		"transactions_currency": stat.TransactionsCurrency,
		"is_expired":            pl.GetIsExpired(),
	}}
	_, err = p.svc.db.Collection(collectionPaylinks).UpdateOne(ctx, dbQuery, set)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaylinks),
			zap.Any(pkg.ErrorDatabaseFieldQuery, dbQuery),
			zap.Any(pkg.ErrorDatabaseFieldSet, set),
		)

		return err
	}

	err = p.updateCaches(pl)

	pl, err = p.GetById(ctx, pl.Id)
	if err != nil {
		return err
	}

	return
}

func (p Paylink) updateCaches(pl *paylink.Paylink) (err error) {
	key1 := fmt.Sprintf(cacheKeyPaylink, pl.Id)
	key2 := fmt.Sprintf(cacheKeyPaylinkMerchant, pl.Id, pl.MerchantId)

	if pl.Deleted {
		err = p.svc.cacher.Delete(key1)
		if err != nil {
			return
		}

		err = p.svc.cacher.Delete(key2)
		return
	}

	err = p.svc.cacher.Set(key1, pl, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key1),
			zap.Any(pkg.ErrorCacheFieldData, pl),
		)
		return
	}

	err = p.svc.cacher.Set(key2, pl, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key2),
			zap.Any(pkg.ErrorCacheFieldData, pl),
		)
	}
	return
}
