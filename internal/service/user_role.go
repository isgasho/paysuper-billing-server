package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
)

const (
	collectionMerchantUsersTable = "user_merchant"
	collectionAdminUsersTable    = "user_admin"
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
}

func newUserRoleRepository(svc *Service) UserRoleServiceInterface {
	s := &UserRoleRepository{svc: svc}
	return s
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
	if err := h.svc.db.Collection(collectionMerchantUsersTable).UpdateId(u.Id, u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateAdminUser(u *billing.UserRole) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).UpdateId(u.Id, u); err != nil {
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
		Find(bson.M{"merchant_id": merchantId, "user.email": email}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetUsersForAdmin() ([]*billing.UserRoleAdmin, error) {
	users := []*billing.UserRoleAdmin{}
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
		Find(bson.M{"merchant_id": merchantId, "user.user_id": id}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetAdminUserById(id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionAdminUsersTable).FindId(id).One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserById(id string) (*billing.UserRole, error) {
	var user *billing.UserRole

	err := h.svc.db.Collection(collectionMerchantUsersTable).FindId(id).One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}
