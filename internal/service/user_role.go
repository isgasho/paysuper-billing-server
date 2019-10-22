package service

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	collectionMerchantUsersTable = "user_merchant"
	collectionAdminUsersTable    = "user_admin"

	cacheUserMerchants = "user:merchants:%s"
)

type UserRoleServiceInterface interface {
	AddMerchantUser(*billing.UserRole) error
	AddAdminUser(*billing.UserRole) error
	UpdateMerchantUser(*billing.UserRole) error
	UpdateAdminUser(*billing.UserRole) error
	GetMerchantUserByEmail(string, string) (*billing.UserRole, error)
	GetAdminUserByEmail(string) (*billing.UserRole, error)
	GetMerchantUserById(string) (*billing.UserRole, error)
	GetAdminUserById(string) (*billing.UserRole, error)
	GetMerchantUserByUserId(string, string) (*billing.UserRole, error)
	GetAdminUserByUserId(string) (*billing.UserRole, error)
	GetUsersForMerchant(string) ([]*billing.UserRole, error)
	GetUsersForAdmin() ([]*billing.UserRole, error)
	GetMerchantsForUser(string) ([]*billing.UserRole, error)
	DeleteAdminUser(*billing.UserRole) error
	DeleteMerchantUser(*billing.UserRole) error
	GetSystemAdmin() (*billing.UserRole, error)
	GetMerchantOwner(string) (*billing.UserRole, error)
}

func newUserRoleRepository(svc *Service) UserRoleServiceInterface {
	s := &UserRoleRepository{svc: svc}
	return s
}

func (h *UserRoleRepository) GetMerchantsForUser(userId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	if err := h.svc.cacher.Get(fmt.Sprintf(cacheUserMerchants, userId), users); err == nil {
		return users, nil
	}

	query := bson.M{"user.user_id": userId}
	err := h.svc.db.Collection(collectionMerchantUsersTable).Find(query).All(&users)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
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

func (h *UserRoleRepository) GetUsersForAdmin() ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).Find(nil).All(&users)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchant),
		)

		return nil, err
	}

	return users, nil
}

func (h *UserRoleRepository) GetUsersForMerchant(merchantId string) ([]*billing.UserRole, error) {
	var users []*billing.UserRole

	query := bson.M{"merchant_id": bson.ObjectIdHex(merchantId)}
	err := h.svc.db.Collection(collectionMerchantUsersTable).Find(query).All(&users)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionMerchantUsersTable),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, err
	}

	return users, nil
}

func (h *UserRoleRepository) AddMerchantUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).Insert(u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) AddAdminUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).Insert(u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateMerchantUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).UpdateId(bson.ObjectIdHex(u.Id), u); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(fmt.Sprintf(cacheUserMerchants, u.User.UserId)); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateAdminUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).UpdateId(bson.ObjectIdHex(u.Id), u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) GetAdminUserByEmail(email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).
		Find(bson.M{"user.email": email}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserByEmail(merchantId string, email string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).
		Find(bson.M{"merchant_id": bson.ObjectIdHex(merchantId), "user.email": email}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetAdminUserByUserId(userId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).
		Find(bson.M{"user.user_id": userId}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserByUserId(merchantId string, id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).
		Find(bson.M{"merchant_id": bson.ObjectIdHex(merchantId), "user.user_id": id}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetAdminUserById(id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).FindId(bson.ObjectIdHex(id)).One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserById(id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).FindId(bson.ObjectIdHex(id)).One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) DeleteAdminUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).RemoveId(bson.ObjectIdHex(u.Id)); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) DeleteMerchantUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).RemoveId(bson.ObjectIdHex(u.Id)); err != nil {
		return err
	}

	if err := h.svc.cacher.Delete(fmt.Sprintf(cacheUserMerchants, u.User.UserId)); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) GetSystemAdmin() (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).Find(bson.M{"role": pkg.RoleSystemAdmin}).One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantOwner(merchantId string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).
		Find(bson.M{"merchant_id": bson.ObjectIdHex(merchantId), "role": pkg.RoleMerchantOwner}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}
