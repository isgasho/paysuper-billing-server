package service

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

const (
	collectionMerchantUsersTable = "user_merchant"
	collectionAdminUsersTable    = "user_admin"

	cacheUserMerchants = "user:merchants:%s"
)

type UserRoleServiceInterface interface {
	AddMerchantUser(context.Context, *billing.UserRole) error
	AddAdminUser(context.Context, *billing.UserRole) error
	UpdateMerchantUser(context.Context, *billing.UserRole) error
	UpdateAdminUser(context.Context, *billing.UserRole) error
	GetMerchantUserByEmail(context.Context, string, string) (*billing.UserRole, error)
	GetAdminUserByEmail(context.Context, string) (*billing.UserRole, error)
	GetMerchantUserById(context.Context, string) (*billing.UserRole, error)
	GetAdminUserById(context.Context, string) (*billing.UserRole, error)
	GetMerchantUserByUserId(context.Context, string, string) (*billing.UserRole, error)
	GetAdminUserByUserId(context.Context, string) (*billing.UserRole, error)
	GetUsersForMerchant(context.Context, string) ([]*billing.UserRole, error)
	GetUsersForAdmin(context.Context) ([]*billing.UserRole, error)
	GetMerchantsForUser(context.Context, string) ([]*billing.UserRole, error)
	DeleteAdminUser(context.Context, *billing.UserRole) error
	DeleteMerchantUser(context.Context, *billing.UserRole) error
	GetSystemAdmin(context.Context) (*billing.UserRole, error)
	GetMerchantOwner(context.Context, string) (*billing.UserRole, error)
}

func newUserRoleRepository(svc *Service) UserRoleServiceInterface {
	s := &UserRoleRepository{svc: svc}
	return s
}

func (h *UserRoleRepository) GetMerchantsForUser(ctx context.Context, userId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	if err := h.svc.cacher.Get(fmt.Sprintf(cacheUserMerchants, userId), users); err == nil {
		return users, nil
	}

	oid, _ := primitive.ObjectIDFromHex(userId)
	query := bson.M{"user_id": oid}
	cursor, err := h.svc.db.Collection(collectionMerchantUsersTable).Find(ctx, query)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &users)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheUserMerchants, userId), users, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", cacheUserMerchants, "data", users)
	}

	return users, nil
}

func (h *UserRoleRepository) GetUsersForAdmin(ctx context.Context) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	cursor, err := h.svc.db.Collection(collectionAdminUsersTable).Find(ctx, nil)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
		)

		return nil, err
	}

	err = cursor.All(ctx, &users)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
		)
		return nil, err
	}

	return users, nil
}

func (h *UserRoleRepository) GetUsersForMerchant(ctx context.Context, merchantId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": oid}
	cursor, err := h.svc.db.Collection(collectionMerchantUsersTable).Find(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, err
	}

	err = cursor.All(ctx, &users)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return users, nil
}

func (h *UserRoleRepository) AddMerchantUser(ctx context.Context, u *billing.UserRole) error {
	_, err := h.svc.db.Collection(collectionMerchantUsersTable).InsertOne(ctx, u)

	if err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) AddAdminUser(ctx context.Context, u *billing.UserRole) error {
	_, err := h.svc.db.Collection(collectionAdminUsersTable).InsertOne(ctx, u)

	if err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateMerchantUser(ctx context.Context, u *billing.UserRole) error {
	oid, _ := primitive.ObjectIDFromHex(u.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMerchantUsersTable).ReplaceOne(ctx, filter, u)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(fmt.Sprintf(cacheUserMerchants, u.UserId)); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateAdminUser(ctx context.Context, u *billing.UserRole) error {
	oid, _ := primitive.ObjectIDFromHex(u.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionAdminUsersTable).ReplaceOne(ctx, filter, u)

	if err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) GetAdminUserByEmail(ctx context.Context, email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).FindOne(ctx, bson.M{"email": email}).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserByEmail(ctx context.Context, merchantId string, email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(merchantId)
	filter := bson.M{"merchant_id": oid, "email": email}
	err := h.svc.db.Collection(collectionMerchantUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetAdminUserByUserId(ctx context.Context, userId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(userId)
	filter := bson.M{"user_id": oid}
	err := h.svc.db.Collection(collectionAdminUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserByUserId(ctx context.Context, merchantId string, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(id)
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)
	filter := bson.M{"merchant_id": merchantOid, "user_id": oid}
	err := h.svc.db.Collection(collectionMerchantUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetAdminUserById(ctx context.Context, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionAdminUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserById(ctx context.Context, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionMerchantUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) DeleteAdminUser(ctx context.Context, u *billing.UserRole) error {
	oid, _ := primitive.ObjectIDFromHex(u.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionAdminUsersTable).DeleteOne(ctx, filter)

	if err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) DeleteMerchantUser(ctx context.Context, u *billing.UserRole) error {
	oid, _ := primitive.ObjectIDFromHex(u.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionMerchantUsersTable).DeleteOne(ctx, filter)

	if err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(fmt.Sprintf(cacheUserMerchants, u.UserId)); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) GetSystemAdmin(ctx context.Context) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).FindOne(ctx, bson.M{"role": pkg.RoleSystemAdmin}).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantOwner(ctx context.Context, merchantId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(merchantId)
	filter := bson.M{"merchant_id": oid, "role": pkg.RoleMerchantOwner}
	err := h.svc.db.Collection(collectionMerchantUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}
