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
	AddMerchantUser(*billing.UserRoleMerchant) error
	AddAdminUser(*billing.UserRoleAdmin) error
	UpdateMerchantUser(*billing.UserRoleMerchant) error
	UpdateAdminUser(*billing.UserRoleAdmin) error
	GetMerchantUserByEmail(string, string) (*billing.UserRoleMerchant, error)
	GetAdminUserByEmail(string) (*billing.UserRoleAdmin, error)
	GetUsersForMerchant(string) ([]*billing.UserRoleMerchant, error)
	GetUsersForAdmin() ([]*billing.UserRoleAdmin, error)
	GetMerchantsForUser(string) ([]*billing.UserRoleMerchant, error)
}

func newUserRoleRepository(svc *Service) UserRoleServiceInterface {
	s := &UserRoleRepository{svc: svc}
	return s
}

func (h *UserRoleRepository) GetMerchantsForUser(userId string) ([]*billing.UserRoleMerchant, error) {
	var users []*billing.UserRoleMerchant
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

	return users, nil
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

func (h *UserRoleRepository) GetUsersForMerchant(merchantId string) ([]*billing.UserRoleMerchant, error) {
	users := []*billing.UserRoleMerchant{}
	query := bson.M{"merchant_id": bson.ObjectIdHex(merchantId)}
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

	return users, nil
}

func (h *UserRoleRepository) AddMerchantUser(u *billing.UserRoleMerchant) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).Insert(u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) AddAdminUser(u *billing.UserRoleAdmin) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).Insert(u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateMerchantUser(u *billing.UserRoleMerchant) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).UpdateId(u.Id, u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) UpdateAdminUser(u *billing.UserRoleAdmin) error {
	if err := h.svc.db.Collection(collectionAdminUsersTable).UpdateId(u.Id, u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) GetAdminUserByEmail(email string) (*billing.UserRoleAdmin, error) {
	var user *billing.UserRoleAdmin

	err := h.svc.db.Collection(collectionAdminUsersTable).
		Find(bson.M{"user.email": email}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}

func (h *UserRoleRepository) GetMerchantUserByEmail(merchantId string, email string) (*billing.UserRoleMerchant, error) {
	var user *billing.UserRoleMerchant

	err := h.svc.db.Collection(collectionMerchantUsersTable).
		Find(bson.M{"merchant_id": merchantId, "user.email": email}).
		One(&user)

	if err != nil {
		return nil, err
	}

	return user, nil
}
