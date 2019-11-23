package service

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	cacheZipCodeByZipAndCountry = "zip_code:zip_country:%s_%s"
	collectionZipCode           = "zip_code"
)

func newZipCodeService(svc *Service) *ZipCode {
	return &ZipCode{svc: svc}
}

func (s *Service) FindByZipCode(
	ctx context.Context,
	req *grpc.FindByZipCodeRequest,
	rsp *grpc.FindByZipCodeResponse,
) error {
	if req.Country != CountryCodeUSA {
		return nil
	}

	query := bson.D{{"zip", primitive.Regex{Pattern: req.Zip}}, {"country", req.Country}}
	count, err := s.db.Collection(collectionZipCode).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionZipCode),
			zap.Any("query", query),
		)

		return orderErrorUnknown
	}

	if count <= 0 {
		return nil
	}

	opts := options.Find().
		SetLimit(req.Limit).
		SetSkip(req.Offset)
	cursor, err := s.db.Collection(collectionZipCode).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return orderErrorUnknown
	}

	var data []*billing.ZipCode
	err = cursor.All(ctx, &data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return err
	}

	rsp.Count = int32(count)
	rsp.Items = data

	return nil
}

func (h *ZipCode) getByZipAndCountry(ctx context.Context, zip, country string) (*billing.ZipCode, error) {
	data := new(billing.ZipCode)
	key := fmt.Sprintf(cacheZipCodeByZipAndCountry, zip, country)

	err := h.svc.cacher.Get(key, data)

	if err == nil {
		return data, nil
	}

	query := bson.M{"zip": zip, "country": country}
	err = h.svc.db.Collection(collectionZipCode).FindOne(ctx, query).Decode(data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionZipCode),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, fmt.Errorf(errorNotFound, collectionZipCode)
	}

	err = h.svc.cacher.Set(key, data, 0)

	if err != nil {
		zap.S().Error("Save zip codes data to cache failed", zap.Error(err))
	}

	return data, nil
}
