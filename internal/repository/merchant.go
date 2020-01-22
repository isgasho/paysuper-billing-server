package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type merchantRepository repository

// NewMerchantRepository create and return an object for working with the merchant repository.
// The returned object implements the MerchantRepositoryInterface interface.
func NewMerchantRepository(db mongodb.SourceInterface, cache database.CacheInterface) MerchantRepositoryInterface {
	s := &merchantRepository{db: db, cache: cache}
	return s
}

func (h *merchantRepository) Insert(ctx context.Context, merchant *billingpb.Merchant) error {
	_, err := h.db.Collection(CollectionMerchant).InsertOne(ctx, merchant)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.cache.Set(key, merchant, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	return nil
}

func (h *merchantRepository) MultipleInsert(ctx context.Context, merchants []*billingpb.Merchant) error {
	m := make([]interface{}, len(merchants))
	for i, v := range merchants {
		m[i] = v
	}

	_, err := h.db.Collection(CollectionMerchant).InsertMany(ctx, m)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, m),
		)
		return err
	}

	return nil
}

func (h *merchantRepository) Update(ctx context.Context, merchant *billingpb.Merchant) error {
	oid, err := primitive.ObjectIDFromHex(merchant.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchant.Id),
		)
		return err
	}

	filter := bson.M{"_id": oid}
	_, err = h.db.Collection(CollectionMerchant).ReplaceOne(ctx, filter, merchant)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.cache.Set(key, merchant, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	return nil
}

func (h *merchantRepository) Upsert(ctx context.Context, merchant *billingpb.Merchant) error {
	oid, err := primitive.ObjectIDFromHex(merchant.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchant.Id),
		)
		return err
	}

	filter := bson.M{"_id": oid}
	opts := options.Replace().SetUpsert(true)
	_, err = h.db.Collection(CollectionMerchant).ReplaceOne(ctx, filter, merchant, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.cache.Set(key, merchant, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)
		return err
	}

	return nil
}

func (h *merchantRepository) UpdateTariffs(ctx context.Context, merchantId string, tariff *billingpb.PaymentChannelCostMerchant) error {
	set := bson.M{}
	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return err
	}

	query := bson.M{
		"_id":                                      oid,
		"tariff.payment.method_name":               tariff.Name,
		"tariff.payment.payer_region":              tariff.Region,
		"tariff.payment.mcc_code":                  tariff.MccCode,
		"tariff.payment.method_fixed_fee_currency": tariff.MethodFixAmountCurrency,
		"tariff.payment.ps_fixed_fee_currency":     tariff.PsFixedFeeCurrency,
	}

	set["$set"] = bson.M{
		"tariff.payment.$.min_amount":         tariff.MinAmount,
		"tariff.payment.$.method_percent_fee": tariff.MethodPercent,
		"tariff.payment.$.method_fixed_fee":   tariff.MethodFixAmount,
		"tariff.payment.$.ps_fixed_fee":       tariff.PsFixedFee,
		"tariff.payment.$.ps_percent_fee":     tariff.PsPercent,
		"tariff.payment.$.is_active":          tariff.IsActive,
	}

	if _, err := h.db.Collection(CollectionMerchant).UpdateOne(ctx, query, set); err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			zap.Any(pkg.ErrorDatabaseFieldSet, set),
		)
		return err
	}

	// We need to remove from cache partially updated entity because of multiple parallel update queries
	key := fmt.Sprintf(cacheMerchantId, merchantId)

	if err := h.cache.Delete(key); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, key),
		)
		return err
	}

	return nil
}

func (h *merchantRepository) GetMerchantsWithAutoPayouts(ctx context.Context) (merchants []*billingpb.Merchant, err error) {
	query := bson.M{
		"manual_payouts_enabled": false,
	}
	cursor, err := h.db.Collection(CollectionMerchant).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	err = cursor.All(ctx, &merchants)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	return
}

func (h *merchantRepository) GetById(ctx context.Context, id string) (*billingpb.Merchant, error) {
	var c billingpb.Merchant
	key := fmt.Sprintf(cacheMerchantId, id)

	if err := h.cache.Get(key, c); err == nil {
		return &c, nil
	}

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(CollectionMerchant).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err := h.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return &c, nil
}

func (h *merchantRepository) GetByUserId(ctx context.Context, userId string) (*billingpb.Merchant, error) {
	var c billingpb.Merchant

	query := bson.M{"user.id": userId}
	err := h.db.Collection(CollectionMerchant).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return &c, nil
}

func (h *merchantRepository) GetCommonById(ctx context.Context, id string) (*billingpb.MerchantCommon, error) {
	var c billingpb.MerchantCommon
	key := fmt.Sprintf(cacheMerchantCommonId, id)

	if err := h.cache.Get(key, c); err == nil {
		return &c, nil
	}

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(CollectionMerchant).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	if err := h.cache.Set(key, c, 0); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, c),
		)
	}

	return &c, nil
}

func (h *merchantRepository) GetAll(ctx context.Context) ([]*billingpb.Merchant, error) {
	c := []*billingpb.Merchant{}

	cursor, err := h.db.Collection(CollectionMerchant).Find(ctx, bson.D{})

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
		)
		return nil, err
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
		)
		return nil, err
	}

	return c, nil
}

func (h *merchantRepository) Find(ctx context.Context, query bson.M, sort []string, offset, limit int64) ([]*billingpb.Merchant, error) {
	var data []*billingpb.Merchant

	opts := options.Find().
		SetSort(mongodb.ToSortOption(sort)).
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := h.db.Collection(CollectionMerchant).Find(ctx, query, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return data, nil
}

func (h *merchantRepository) FindCount(ctx context.Context, query bson.M) (int64, error) {
	count, err := h.db.Collection(CollectionMerchant).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, CollectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return int64(0), err
	}

	return count, nil
}
