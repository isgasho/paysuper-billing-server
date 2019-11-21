package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	collectionPaymentMinLimitSystem = "payment_min_limit_system"

	cacheKeyPaymentMinLimitSystem    = "payment_min_limit_system:currency:%s"
	cacheKeyAllPaymentMinLimitSystem = "payment_min_limit_system:all"
)

var (
	errorPaymentMinLimitSystemCurrencyUnknown = newBillingServerErrorMsg("pmls0001", "payment min limit system currency unknown")
	errorPaymentMinLimitSystemNotFound        = newBillingServerErrorMsg("pmls0002", "payment min limit system not found")
	errorPaymentMinLimitSystemInvalidAmount   = newBillingServerErrorMsg("pmls0003", "payment min limit system amount invalid")
)

type PaymentMinLimitSystemInterface interface {
	GetByCurrency(ctx context.Context, currency string) (pmls *billing.PaymentMinLimitSystem, err error)
	GetAll(ctx context.Context) (result []*billing.PaymentMinLimitSystem, err error)
	Upsert(ctx context.Context, pmls *billing.PaymentMinLimitSystem) (err error)
	MultipleInsert(ctx context.Context, pmlsArray []*billing.PaymentMinLimitSystem) (err error)
}

func newPaymentMinLimitSystem(svc *Service) PaymentMinLimitSystemInterface {
	s := &PaymentMinLimitSystem{svc: svc}
	return s
}

func (s *Service) GetPaymentMinLimitsSystem(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.GetPaymentMinLimitsSystemResponse,
) (err error) {
	res.Items, err = s.paymentMinLimitSystem.GetAll(ctx)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return
	}

	res.Status = pkg.ResponseStatusOk
	return
}

func (s *Service) SetPaymentMinLimitSystem(
	ctx context.Context,
	req *billing.PaymentMinLimitSystem,
	res *grpc.EmptyResponseWithStatus,
) (err error) {
	pmls, err := s.paymentMinLimitSystem.GetByCurrency(ctx, req.Currency)
	if err != nil && err != errorPaymentMinLimitSystemNotFound {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return
	}

	if err == errorPaymentMinLimitSystemNotFound || pmls == nil {
		if !contains(s.supportedCurrencies, req.Currency) {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorPaymentMinLimitSystemCurrencyUnknown
			return nil
		}

		pmls = &billing.PaymentMinLimitSystem{
			Id:        primitive.NewObjectID().Hex(),
			Currency:  req.Currency,
			CreatedAt: ptypes.TimestampNow(),
		}
	}

	if req.Amount < 0 {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorPaymentMinLimitSystemInvalidAmount
		return nil
	}

	pmls.Amount = req.Amount
	pmls.UpdatedAt = ptypes.TimestampNow()

	err = s.paymentMinLimitSystem.Upsert(ctx, pmls)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return
	}

	res.Status = pkg.ResponseStatusOk
	return
}

func (p PaymentMinLimitSystem) GetByCurrency(
	ctx context.Context,
	currency string,
) (pmls *billing.PaymentMinLimitSystem, err error) {
	key := fmt.Sprintf(cacheKeyPaymentMinLimitSystem, currency)
	if err = p.svc.cacher.Get(key, &pmls); err == nil {
		return pmls, nil
	}

	query := bson.M{"currency": currency}
	err = p.svc.db.Collection(collectionPaymentMinLimitSystem).FindOne(ctx, query).Decode(&pmls)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errorPaymentMinLimitSystemNotFound
		}
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMinLimitSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	err = p.svc.cacher.Set(key, pmls, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, pmls),
		)
	}

	return
}

func (p PaymentMinLimitSystem) GetAll(ctx context.Context) (result []*billing.PaymentMinLimitSystem, err error) {
	if err = p.svc.cacher.Get(cacheKeyAllPaymentMinLimitSystem, &result); err == nil {
		return result, nil
	}

	cursor, err := p.svc.db.Collection(collectionPaymentMinLimitSystem).Find(ctx, bson.M{})

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMinLimitSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, nil),
		)
		return
	}

	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMinLimitSystem),
			zap.Any(pkg.ErrorDatabaseFieldQuery, nil),
		)
		return
	}

	err = p.svc.cacher.Set(cacheKeyAllPaymentMinLimitSystem, result, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, collectionPaymentMinLimitSystem),
			zap.Any(pkg.ErrorCacheFieldData, result),
		)
	}

	return
}

func (p PaymentMinLimitSystem) Upsert(ctx context.Context, pmls *billing.PaymentMinLimitSystem) (err error) {
	pmls.Amount = tools.FormatAmount(pmls.Amount)
	oid, _ := primitive.ObjectIDFromHex(pmls.Id)
	filter := bson.M{"_id": oid}
	opts := options.FindOneAndUpdate().SetUpsert(true)
	err = p.svc.db.Collection(collectionPaymentMinLimitSystem).FindOneAndUpdate(ctx, filter, pmls, opts).Err()

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPaymentMinLimitSystem),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, pmls),
		)
		return
	}

	key := fmt.Sprintf(cacheKeyPaymentMinLimitSystem, pmls.Currency)
	err = p.svc.cacher.Set(key, pmls, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, pmls),
		)
		return
	}

	err = p.svc.cacher.Delete(cacheKeyAllPaymentMinLimitSystem)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, cacheKeyAllOperatingCompanies),
		)
	}

	return
}

func (p PaymentMinLimitSystem) MultipleInsert(
	ctx context.Context,
	pmlsArray []*billing.PaymentMinLimitSystem,
) (err error) {
	c := make([]interface{}, len(pmlsArray))
	for i, v := range pmlsArray {
		if v.Id == "" {
			v.Id = primitive.NewObjectID().Hex()
		}
		v.Amount = tools.FormatAmount(v.Amount)
		c[i] = v
	}

	_, err = p.svc.db.Collection(collectionPaymentMinLimitSystem).InsertMany(ctx, c)

	if err != nil {
		return err
	}

	return nil
}
