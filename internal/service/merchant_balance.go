package service

import (
	"context"
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
)

var (
	errorMerchantPayoutCurrencyNotSet = newBillingServerErrorMsg("ba000001", "merchant payout currency not set")

	accountingEntriesForRollingReserve = []string{
		pkg.AccountingEntryTypeMerchantRollingReserveCreate,
		pkg.AccountingEntryTypeMerchantRollingReserveRelease,
	}
)

type balanceQueryResItem struct {
	Currency string  `bson:"_id"`
	Amount   float64 `bson:"amount"`
}

type reserveQueryResItem struct {
	Type   string  `bson:"_id"`
	Amount float64 `bson:"amount"`
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
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", merchantId))
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.S().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	query := bson.M{
		"merchant_id": bson.ObjectIdHex(merchantId),
		"currency":    merchant.GetPayoutCurrency(),
	}
	mb := &billing.MerchantBalance{}
	err = s.db.Collection(collectionMerchantBalances).Find(query).Sort("-_id").One(&mb)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionMerchantBalances),
		)
		return nil, err
	}

	return mb, nil
}

func (s *Service) updateMerchantBalance(merchantId string) (*billing.MerchantBalance, error) {

	merchant, err := s.merchant.GetById(merchantId)

	if err != nil {
		zap.S().Error("Merchant not found", zap.Error(err), zap.String("merchant_id", merchantId))
		return nil, err
	}

	if merchant.GetPayoutCurrency() == "" {
		zap.S().Error(errorMerchantPayoutCurrencyNotSet.Error(), zap.String("merchant_id", merchantId))
		return nil, errorMerchantPayoutCurrencyNotSet
	}

	debit_query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":      bson.ObjectIdHex(merchantId),
				"amounts.currency": merchant.GetPayoutCurrency(),
				"status":           pkg.RoyaltyReportStatusAccepted,
			},
		},
		{
			"$group": bson.M{
				"_id":    "$amounts.currency",
				"amount": bson.M{"$sum": "$amounts.payout_amount"},
			},
		},
	}

	debit_res := []*balanceQueryResItem{}
	err = s.db.Collection(collectionRoyaltyReport).Pipe(debit_query).All(&debit_res)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionRoyaltyReport),
			zap.Any("query", debit_query),
		)
		return nil, err
	}

	credit_query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":            bson.ObjectIdHex(merchantId),
				"currency":               merchant.GetPayoutCurrency(),
				"has_merchant_signature": true,
				"has_psp_signature":      true,
				"status":                 bson.M{"$in": []string{pkg.PayoutDocumentStatusPending, pkg.PayoutDocumentStatusInProgress, pkg.PayoutDocumentStatusPaid}},
			},
		},
		{
			"$group": bson.M{
				"_id":    "$currency",
				"amount": bson.M{"$sum": "$amount"},
			},
		},
	}

	credit_res := []*balanceQueryResItem{}
	err = s.db.Collection(collectionPayoutDocuments).Pipe(credit_query).All(&credit_res)
	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionPayoutDocuments),
			zap.Any("query", credit_query),
		)
		return nil, err
	}

	debit := float64(0)
	credit := float64(0)

	if len(debit_res) > 0 {
		debit = debit_res[0].Amount
	}

	if len(credit_res) > 0 {
		credit = credit_res[0].Amount
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

	err = s.db.Collection(collectionMerchantBalances).Insert(balance)
	if err != nil {
		zap.S().Error(
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
	last_payout_query := bson.M{
		"merchant_id":            bson.ObjectIdHex(merchantId),
		"has_merchant_signature": true,
		"has_psp_signature":      true,
		"status":                 bson.M{"$in": []string{pkg.PayoutDocumentStatusPending, pkg.PayoutDocumentStatusInProgress, pkg.PayoutDocumentStatusPaid}},
	}

	var pd *billing.PayoutDocument
	err := s.db.Collection(collectionPayoutDocuments).Find(last_payout_query).Sort("-created_at").One(&pd)

	if err != nil && err != mgo.ErrNotFound {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
		)
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
			zap.S().Error(
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
		zap.S().Error(
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
