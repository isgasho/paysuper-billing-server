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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	collectionMerchantBalances = "merchant_balances"

	cacheKeyMerchantBalances = "balance:merchant_id:%s:currency:%s"
)

var (
	errorMerchantPayoutCurrencyNotSet = newBillingServerErrorMsg("ba000001", "merchant payout currency not set")

	accountingEntriesForRollingReserve = []string{
		pkg.AccountingEntryTypeMerchantRollingReserveCreate,
		pkg.AccountingEntryTypeMerchantRollingReserveRelease,
	}
)

type reserveQueryResItem struct {
	Type   string  `bson:"_id"`
	Amount float64 `bson:"amount"`
}

type MerchantBalanceServiceInterface interface {
	Insert(ctx context.Context, document *billing.MerchantBalance) error
	GetByMerchantIdAndCurrency(ctx context.Context, merchantId, currency string) (*billing.MerchantBalance, error)
}

func newMerchantBalance(svc *Service) MerchantBalanceServiceInterface {
	s := &MerchantBalance{svc: svc}
	return s
}

func (s *Service) GetMerchantBalance(
	ctx context.Context,
	req *grpc.GetMerchantBalanceRequest,
	res *grpc.GetMerchantBalanceResponse,
) error {
	var err error
	res.Status = pkg.ResponseStatusOk

	res.Item, err = s.getMerchantBalance(ctx, req.MerchantId)
	if err == nil {
		return nil
	}

	if err == mongo.ErrNoDocuments {
		res.Item, err = s.updateMerchantBalance(ctx, req.MerchantId)
		if err != nil {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusSystemError
				res.Message = e
				return nil
			}
			return err
		}
	} else {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = e
			return nil
		}
		return err
	}

	return nil
}

func (s *Service) getMerchantBalance(ctx context.Context, merchantId string) (*billing.MerchantBalance, error) {
	merchant, err := s.merchant.GetById(ctx, merchantId)
	if err != nil {
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.L().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	return s.merchantBalance.GetByMerchantIdAndCurrency(ctx, merchant.Id, merchant.GetPayoutCurrency())
}

func (s *Service) updateMerchantBalance(ctx context.Context, merchantId string) (*billing.MerchantBalance, error) {
	merchant, err := s.merchant.GetById(ctx, merchantId)
	if err != nil {
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.L().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	debit, err := s.royaltyReport.GetBalanceAmount(ctx, merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	credit, err := s.payoutDocument.GetBalanceAmount(ctx, merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	rr, err := s.getRollingReserveForBalance(ctx, merchantId, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	balance := &billing.MerchantBalance{
		Id:             primitive.NewObjectID().Hex(),
		MerchantId:     merchantId,
		Currency:       merchant.GetPayoutCurrency(),
		Debit:          debit,
		Credit:         credit,
		RollingReserve: rr,
		CreatedAt:      ptypes.TimestampNow(),
	}

	balance.Total = balance.Debit - balance.Credit - balance.RollingReserve

	err = s.merchantBalance.Insert(ctx, balance)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionMerchantBalances),
			zap.Any("balance", balance),
		)
		return nil, err
	}

	return balance, nil
}

func (s *Service) getRollingReserveForBalance(ctx context.Context, merchantId, currency string) (float64, error) {
	pd, err := s.payoutDocument.GetLast(ctx, merchantId, currency)
	if err != nil && err != mongo.ErrNoDocuments {
		return 0, err
	}

	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	matchQuery := bson.M{
		"merchant_id": merchantOid,
		"currency":    currency,
		"type":        bson.M{"$in": accountingEntriesForRollingReserve},
	}

	if pd != nil {
		createdAt, err := ptypes.Timestamp(pd.CreatedAt)
		if err != nil {
			zap.L().Error(
				"Time conversion error",
				zap.Error(err),
			)
			return 0, err
		}

		matchQuery["created_at"] = bson.M{"$gt": createdAt}
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
		{
			"$group": bson.M{"_id": "$type", "amount": bson.M{"$sum": "$amount"}},
		},
	}

	var items []*reserveQueryResItem
	cursor, err := s.db.Collection(collectionAccountingEntry).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAccountingEntry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, err
	}

	err = cursor.All(ctx, &items)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAccountingEntry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return 0, err
	}

	if len(items) == 0 {
		return 0, nil
	}

	result := float64(0)

	for _, i := range items {
		// in case of rolling reserve release, result will have negative amount, it is ok
		if i.Type == pkg.AccountingEntryTypeMerchantRollingReserveRelease {
			result -= i.Amount
		} else {
			result += i.Amount
		}
	}

	return result, nil
}

func (m MerchantBalance) Insert(ctx context.Context, mb *billing.MerchantBalance) (err error) {
	_, err = m.svc.db.Collection(collectionMerchantBalances).InsertOne(ctx, mb)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantBalances),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, mb),
		)
		return
	}

	key := fmt.Sprintf(cacheKeyMerchantBalances, mb.MerchantId, mb.Currency)
	err = m.svc.cacher.Set(key, mb, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, mb),
		)
		// suppress error returning here
		err = nil
	}

	return
}

func (m MerchantBalance) GetByMerchantIdAndCurrency(
	ctx context.Context,
	merchantId, currency string,
) (mb *billing.MerchantBalance, err error) {
	var c billing.MerchantBalance
	key := fmt.Sprintf(cacheKeyMerchantBalances, merchantId, currency)

	if err := m.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id": merchantOid,
		"currency":    currency,
	}

	sorts := bson.M{"_id": -1}
	opts := options.FindOne().SetSort(sorts)
	err = m.svc.db.Collection(collectionMerchantBalances).FindOne(ctx, query, opts).Decode(&mb)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionPayoutDocuments),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSorts, sorts),
		)
		return
	}

	err = m.svc.cacher.Set(key, mb, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, mb),
		)
		// suppress error returning here
		err = nil
	}

	return
}
