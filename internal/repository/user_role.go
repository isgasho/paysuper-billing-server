package repository

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type userRoleRepository repository

// NewUserRoleRepository create and return an object for working with the user role repository.
// The returned object implements the UserRoleRepositoryInterface interface.
func NewUserRoleRepository(db mongodb.SourceInterface, cache database.CacheInterface) UserRoleRepositoryInterface {
	s := &userRoleRepository{db: db, cache: cache}
	return s
}

func (h *userRoleRepository) AddMerchantUser(ctx context.Context, role *billing.UserRole) error {
	_, err := h.db.Collection(collectionMerchantUsersTable).InsertOne(ctx, role)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, role),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) AddAdminUser(ctx context.Context, role *billing.UserRole) error {
	_, err := h.db.Collection(collectionAdminUsersTable).InsertOne(ctx, role)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, role),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) UpdateMerchantUser(ctx context.Context, role *billing.UserRole) error {
	oid, err := primitive.ObjectIDFromHex(role.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, role.Id),
		)
		return err
	}

	filter := bson.M{"_id": oid}
	err = h.db.Collection(collectionMerchantUsersTable).FindOneAndReplace(ctx, filter, role).Err()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, role),
		)
		return err
	}

	key := fmt.Sprintf(cacheUserMerchants, role.UserId)
	if err := h.cache.Delete(key); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorCacheFieldCmd, "delete"),
			zap.Any(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, role.UserId),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) UpdateAdminUser(ctx context.Context, role *billing.UserRole) error {
	oid, err := primitive.ObjectIDFromHex(role.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, role.Id),
		)
		return err
	}

	filter := bson.M{"_id": oid}
	err = h.db.Collection(collectionAdminUsersTable).FindOneAndReplace(ctx, filter, role).Err()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, role),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) GetAdminUserById(ctx context.Context, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(collectionAdminUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetMerchantUserById(ctx context.Context, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(collectionMerchantUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) DeleteAdminUser(ctx context.Context, role *billing.UserRole) error {
	oid, err := primitive.ObjectIDFromHex(role.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, role.Id),
		)
		return err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(collectionAdminUsersTable).FindOneAndDelete(ctx, query).Err()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) DeleteMerchantUser(ctx context.Context, role *billing.UserRole) error {
	oid, err := primitive.ObjectIDFromHex(role.Id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, role.Id),
		)
		return err
	}

	query := bson.M{"_id": oid}
	err = h.db.Collection(collectionMerchantUsersTable).FindOneAndDelete(ctx, query).Err()

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return err
	}

	key := fmt.Sprintf(cacheUserMerchants, role.UserId)
	if err := h.cache.Delete(key); err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorCacheFieldCmd, "delete"),
			zap.Any(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, role.UserId),
		)
		return err
	}

	return nil
}

func (h *userRoleRepository) GetSystemAdmin(ctx context.Context) (*billing.UserRole, error) {
	var user *billing.UserRole

	query := bson.M{"role": pkg.RoleSystemAdmin}
	err := h.db.Collection(collectionAdminUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetMerchantOwner(ctx context.Context, merchantId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return nil, err
	}

	query := bson.M{"merchant_id": oid, "role": pkg.RoleMerchantOwner}
	err = h.db.Collection(collectionMerchantUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetMerchantsForUser(ctx context.Context, userId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	if err := h.cache.Get(fmt.Sprintf(cacheUserMerchants, userId), users); err == nil {
		return users, nil
	}

	oid, err := primitive.ObjectIDFromHex(userId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, userId),
		)
		return nil, err
	}

	query := bson.M{"user_id": oid}
	cursor, err := h.db.Collection(collectionMerchantUsersTable).Find(ctx, query)

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

	err = h.cache.Set(fmt.Sprintf(cacheUserMerchants, userId), users, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorCacheFieldCmd, "set"),
			zap.Any(pkg.ErrorCacheFieldKey, cacheUserMerchants),
			zap.Any(pkg.ErrorCacheFieldData, users),
		)
	}

	return users, nil
}

func (h *userRoleRepository) GetUsersForAdmin(ctx context.Context) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	cursor, err := h.db.Collection(collectionAdminUsersTable).Find(ctx, bson.M{})

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

func (h *userRoleRepository) GetUsersForMerchant(ctx context.Context, merchantId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return nil, err
	}

	query := bson.M{"merchant_id": oid}
	cursor, err := h.db.Collection(collectionMerchantUsersTable).Find(ctx, query)

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

func (h *userRoleRepository) GetAdminUserByEmail(ctx context.Context, email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	query := bson.M{"email": email}
	err := h.db.Collection(collectionAdminUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetMerchantUserByEmail(ctx context.Context, merchantId string, email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return nil, err
	}

	filter := bson.M{"merchant_id": oid, "email": email}
	err = h.db.Collection(collectionMerchantUsersTable).FindOne(ctx, filter).Decode(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetAdminUserByUserId(ctx context.Context, userId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, _ := primitive.ObjectIDFromHex(userId)
	query := bson.M{"user_id": oid}
	err := h.db.Collection(collectionAdminUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAdminUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}

func (h *userRoleRepository) GetMerchantUserByUserId(ctx context.Context, merchantId string, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	oid, err := primitive.ObjectIDFromHex(id)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, id),
		)
		return nil, err
	}

	merchantOid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseInvalidObjectId,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.String(pkg.ErrorDatabaseFieldQuery, merchantId),
		)
		return nil, err
	}

	query := bson.M{"merchant_id": merchantOid, "user_id": oid}
	err = h.db.Collection(collectionMerchantUsersTable).FindOne(ctx, query).Decode(&user)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	return user, nil
}
