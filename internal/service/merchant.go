package service

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	cacheMerchantId = "merchant:id:%s"

	collectionMerchant                     = "merchant"
	collectionMerchantPaymentMethodHistory = "payment_method_history"
)

type MerchantRepositoryInterface interface {
	Update(ctx context.Context, merchant *billing.Merchant) error
	Insert(ctx context.Context, merchant *billing.Merchant) error
	Upsert(ctx context.Context, merchant *billing.Merchant) error
	MultipleInsert(ctx context.Context, merchants []*billing.Merchant) error
	GetById(ctx context.Context, id string) (*billing.Merchant, error)
	GetPaymentMethod(ctx context.Context, merchantId string, method string) (*billing.MerchantPaymentMethod, error)
	GetMerchantsWithAutoPayouts(ctx context.Context) ([]*billing.Merchant, error)
}

func newMerchantService(svc *Service) MerchantRepositoryInterface {
	return &Merchant{svc: svc}
}

func (h *Merchant) GetMerchantsWithAutoPayouts(ctx context.Context) ([]*billing.Merchant, error) {
	query := bson.M{"manual_payouts_enabled": false}
	cursor, err := h.svc.db.Collection(collectionMerchant).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	var merchants []*billing.Merchant
	err = cursor.All(ctx, &merchants)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return merchants, nil
}

func (h *Merchant) Update(ctx context.Context, merchant *billing.Merchant) error {
	oid, _ := primitive.ObjectIDFromHex(merchant.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMerchant).UpdateOne(ctx, filter, merchant)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.svc.cacher.Set(key, merchant, 0)

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

func (h *Merchant) Insert(ctx context.Context, merchant *billing.Merchant) error {
	_, err := h.svc.db.Collection(collectionMerchant).InsertOne(ctx, merchant)

	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheMerchantId, merchant.Id), merchant, 0)

	if err != nil {
		return err
	}

	return nil
}

func (h *Merchant) Upsert(ctx context.Context, merchant *billing.Merchant) error {
	oid, _ := primitive.ObjectIDFromHex(merchant.Id)
	filter := bson.M{"_id": oid}
	opts := options.FindOneAndUpdate().SetUpsert(true)
	err := h.svc.db.Collection(collectionMerchant).FindOneAndUpdate(ctx, filter, merchant, opts).Err()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, merchant),
		)

		return err
	}

	key := fmt.Sprintf(cacheMerchantId, merchant.Id)
	err = h.svc.cacher.Set(key, merchant, 0)

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

func (h *Merchant) MultipleInsert(ctx context.Context, merchants []*billing.Merchant) error {
	m := make([]interface{}, len(merchants))
	for i, v := range merchants {
		m[i] = v
	}

	_, err := h.svc.db.Collection(collectionMerchant).InsertMany(ctx, m)

	if err != nil {
		return err
	}

	return nil
}

func (h *Merchant) GetById(ctx context.Context, id string) (*billing.Merchant, error) {
	var c billing.Merchant
	key := fmt.Sprintf(cacheMerchantId, id)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionMerchant).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	if err := h.svc.cacher.Set(key, c, 0); err != nil {
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

func (h Merchant) GetPaymentMethod(ctx context.Context, merchantId string, method string) (*billing.MerchantPaymentMethod, error) {
	m, err := h.GetById(ctx, merchantId)
	if err != nil {
		return nil, err
	}

	merchantPaymentMethods := make(map[string]*billing.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm, err := h.svc.paymentMethod.GetAll()
	if err != nil {
		return nil, err
	}
	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billing.MerchantPaymentMethod{
				PaymentMethod: &billing.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billing.MerchantPaymentMethodCommissions{
					Fee: DefaultPaymentMethodFee,
					PerTransaction: &billing.MerchantPaymentMethodPerTransactionCommission{
						Fee:      DefaultPaymentMethodPerTransactionFee,
						Currency: DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billing.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[method]; !ok {
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	return merchantPaymentMethods[method], nil
}
