package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
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
	Insert(document *billing.MerchantBalance) error
	GetByMerchantIdAndCurrency(merchantId, currency string) (*billing.MerchantBalance, error)
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

	res.Item, err = s.getMerchantBalance(req.MerchantId)
	if err == nil {
		return nil
	}

	if err == mgo.ErrNotFound {
		res.Item, err = s.updateMerchantBalance(req.MerchantId)
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

func (s *Service) getMerchantBalance(merchantId string) (*billing.MerchantBalance, error) {
	merchant, err := s.merchant.GetById(merchantId)
	if err != nil {
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.L().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	return s.merchantBalance.GetByMerchantIdAndCurrency(merchant.Id, merchant.GetPayoutCurrency())
}

func (s *Service) updateMerchantBalance(merchantId string) (*billing.MerchantBalance, error) {

	merchant, err := s.merchant.GetById(merchantId)
	if err != nil {
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.L().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	debit, err := s.royaltyReport.GetBalanceAmount(merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	credit, err := s.payoutDocument.GetBalanceAmount(merchant.Id, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	rr, err := s.getRollingReserveForBalance(merchantId, merchant.GetPayoutCurrency())
	if err != nil {
		return nil, err
	}

	balance := &billing.MerchantBalance{
		Id:             bson.NewObjectId().Hex(),
		MerchantId:     merchantId,
		Currency:       merchant.GetPayoutCurrency(),
		Debit:          debit,
		Credit:         credit,
		RollingReserve: rr,
		CreatedAt:      ptypes.TimestampNow(),
	}

	balance.Total = balance.Debit - balance.Credit - balance.RollingReserve

	err = s.merchantBalance.Insert(balance)
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

func (s *Service) getRollingReserveForBalance(merchantId, currency string) (float64, error) {

	pd, err := s.payoutDocument.GetLast(merchantId, currency)
	if err != nil && err != mgo.ErrNotFound {
		return 0, err
	}

	matchQuery := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
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

	items := []*reserveQueryResItem{}
	err = s.db.Collection(collectionAccountingEntry).Pipe(query).All(&items)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionAccountingEntry),
			zap.Any("query", query),
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

func (m MerchantBalance) Insert(mb *billing.MerchantBalance) (err error) {
	err = m.svc.db.Collection(collectionMerchantBalances).Insert(mb)
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

func (m MerchantBalance) GetByMerchantIdAndCurrency(merchantId, currency string) (mb *billing.MerchantBalance, err error) {
	var c billing.MerchantBalance
	key := fmt.Sprintf(cacheKeyMerchantBalances, merchantId, currency)
	if err := m.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"currency":    currency,
	}

	sorts := "-_id"

	err = m.svc.db.Collection(collectionMerchantBalances).Find(query).Sort(sorts).One(&mb)
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
