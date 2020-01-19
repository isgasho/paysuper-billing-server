package service

import (
	"context"
	"fmt"
	casbinProto "github.com/paysuper/casbin-server/pkg/generated/api/proto/casbinpb"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	cacheMerchantId       = "merchant:id:%s"
	cacheMerchantCommonId = "merchant:common:id:%s"

	collectionMerchant                     = "merchant"
	collectionMerchantPaymentMethodHistory = "payment_method_history"
)

func (s *Service) MerchantsMigrate(ctx context.Context) error {
	merchants, err := s.merchant.GetAll(ctx)

	if err != nil {
		zap.L().Error("[task merchants migrate] Unable to get merchants", zap.Error(err))
		return nil
	}

	for _, merchant := range merchants {
		if merchant.User == nil ||
			merchant.User.Id == "" ||
			merchant.User.Email == "" ||
			merchant.User.FirstName == "" ||
			merchant.User.LastName == "" {
			continue
		}

		userRole := &billingpb.UserRole{
			Id:         primitive.NewObjectID().Hex(),
			MerchantId: merchant.Id,
			UserId:     merchant.User.Id,
			Email:      merchant.User.Email,
			FirstName:  merchant.User.FirstName,
			LastName:   merchant.User.LastName,
			Role:       billingpb.RoleMerchantOwner,
			Status:     pkg.UserRoleStatusAccepted,
		}

		_, err := s.userRoleRepository.GetMerchantUserByUserId(ctx, userRole.MerchantId, userRole.UserId)

		if err != nil {
			err = s.userRoleRepository.AddMerchantUser(ctx, userRole)
		}

		if err != nil {
			zap.L().Error("[task merchants migrate] Unable to add merchant user role", zap.Error(err))
			continue
		}

		casbinRole := &casbinProto.UserRoleRequest{
			User: fmt.Sprintf(pkg.CasbinMerchantUserMask, merchant.Id, merchant.User.Id),
			Role: billingpb.RoleMerchantOwner,
		}

		roles, err := s.casbinService.GetRolesForUser(context.TODO(), casbinRole)

		if roles == nil || len(roles.Array) < 1 {
			_, err = s.casbinService.AddRoleForUser(context.TODO(), casbinRole)
		}

		if err != nil {
			zap.L().Error("[task merchants migrate] Unable to add user to casbin", zap.Error(err), zap.Any("role", casbinRole))
		}
	}

	zap.L().Info("[task merchants migrate] Finished successfully")

	return nil
}

type MerchantRepositoryInterface interface {
	UpdatePartial(ctx context.Context, query interface{}, update bson.M) error
	UpdateTariffs(ctx context.Context, id string, req *billingpb.PaymentChannelCostMerchant) error
	Update(ctx context.Context, merchant *billingpb.Merchant) error
	Insert(ctx context.Context, merchant *billingpb.Merchant) error
	Upsert(ctx context.Context, merchant *billingpb.Merchant) error
	MultipleInsert(ctx context.Context, merchants []*billingpb.Merchant) error
	GetById(ctx context.Context, id string) (*billingpb.Merchant, error)
	GetCommonById(ctx context.Context, id string) (*billingpb.MerchantCommon, error)
	GetPaymentMethod(ctx context.Context, merchantId string, method string) (*billingpb.MerchantPaymentMethod, error)
	GetMerchantsWithAutoPayouts(ctx context.Context) ([]*billingpb.Merchant, error)
	GetAll(ctx context.Context) ([]*billingpb.Merchant, error)
}

func newMerchantService(svc *Service) MerchantRepositoryInterface {
	return &Merchant{svc: svc}
}

func (h *Merchant) UpdateTariffs(ctx context.Context, id string, req *billingpb.PaymentChannelCostMerchant) error {
	set := bson.M{}
	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid,
		"tariff.payment.method_name":               req.Name,
		"tariff.payment.payer_region":              req.Region,
		"tariff.payment.mcc_code":                  req.MccCode,
		"tariff.payment.method_fixed_fee_currency": req.MethodFixAmountCurrency,
		"tariff.payment.ps_fixed_fee_currency":     req.PsFixedFeeCurrency,
	}

	set["$set"] = bson.M{
		"tariff.payment.$.min_amount":         req.MinAmount,
		"tariff.payment.$.method_percent_fee": req.MethodPercent,
		"tariff.payment.$.method_fixed_fee":   req.MethodFixAmount,
		"tariff.payment.$.ps_fixed_fee":       req.PsFixedFee,
		"tariff.payment.$.ps_percent_fee":     req.PsPercent,
		"tariff.payment.$.is_active":          req.IsActive,
	}

	if err := h.UpdatePartial(ctx, query, set); err != nil {
		return err
	}

	// We need to remove from cache partially updated entity because of multiple parallel update queries
	key := fmt.Sprintf(cacheMerchantCommonId, id)
	if err := h.svc.cacher.Delete(key); err != nil {
		zap.L().Error(pkg.ErrorCacheQueryFailed, zap.Error(err), zap.String(pkg.ErrorCacheFieldCmd, "DELETE"), zap.String(pkg.ErrorCacheFieldKey, key))
	}

	return nil
}

func (h *Merchant) GetMerchantsWithAutoPayouts(ctx context.Context) (merchants []*billingpb.Merchant, err error) {
	query := bson.M{
		"manual_payouts_enabled": false,
	}
	cursor, err := h.svc.db.Collection(collectionMerchant).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	err = cursor.All(ctx, &merchants)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	return
}

func (h *Merchant) Update(ctx context.Context, merchant *billingpb.Merchant) error {
	oid, _ := primitive.ObjectIDFromHex(merchant.Id)
	filter := bson.M{"_id": oid}
	opts := options.FindOneAndReplace().SetUpsert(true)
	err := h.svc.db.Collection(collectionMerchant).FindOneAndReplace(ctx, filter, merchant, opts).Err()

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

func (h *Merchant) Insert(ctx context.Context, merchant *billingpb.Merchant) error {
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

func (h *Merchant) Upsert(ctx context.Context, merchant *billingpb.Merchant) error {
	oid, _ := primitive.ObjectIDFromHex(merchant.Id)
	filter := bson.M{"_id": oid}
	opts := options.Replace().SetUpsert(true)
	_, err := h.svc.db.Collection(collectionMerchant).ReplaceOne(ctx, filter, merchant, opts)

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

func (h *Merchant) MultipleInsert(ctx context.Context, merchants []*billingpb.Merchant) error {
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

func (h *Merchant) GetById(ctx context.Context, id string) (*billingpb.Merchant, error) {
	var c billingpb.Merchant
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
		return nil, merchantErrorNotFound
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

func (h *Merchant) GetCommonById(ctx context.Context, id string) (*billingpb.MerchantCommon, error) {
	var c billingpb.MerchantCommon
	key := fmt.Sprintf(cacheMerchantCommonId, id)

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

func (h Merchant) GetPaymentMethod(
	ctx context.Context,
	merchantId string,
	method string,
) (*billingpb.MerchantPaymentMethod, error) {
	m, err := h.GetById(ctx, merchantId)
	if err != nil {
		return nil, err
	}

	merchantPaymentMethods := make(map[string]*billingpb.MerchantPaymentMethod)
	if len(m.PaymentMethods) > 0 {
		for k, v := range m.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm, err := h.svc.paymentMethod.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billingpb.MerchantPaymentMethod{
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billingpb.MerchantPaymentMethodCommissions{
					Fee: DefaultPaymentMethodFee,
					PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
						Fee:      DefaultPaymentMethodPerTransactionFee,
						Currency: DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billingpb.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[method]; !ok {
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	return merchantPaymentMethods[method], nil
}

func (h *Merchant) GetAll(ctx context.Context) ([]*billingpb.Merchant, error) {
	var c []*billingpb.Merchant
	cursor, err := h.svc.db.Collection(collectionMerchant).Find(ctx, nil)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	err = cursor.All(ctx, &c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
		)
		return nil, fmt.Errorf(errorNotFound, collectionMerchant)
	}

	return c, nil
}

func (h *Merchant) UpdatePartial(ctx context.Context, query interface{}, update bson.M) error {
	if _, err := h.svc.db.Collection(collectionMerchant).UpdateOne(ctx, query, update); err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
			zap.Any(pkg.ErrorDatabaseFieldQuery, update),
		)

		return err
	}

	return nil
}
