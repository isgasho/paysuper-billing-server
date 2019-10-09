package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
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
}

func newUserRoleRepository(svc *Service) UserRoleServiceInterface {
	s := &UserRoleRepository{svc: svc}
	return s
}

func (h *UserRoleRepository) AddMerchantUser(u *billing.UserRoleMerchant) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).Insert(u); err != nil {
		return err
	}

	return nil
}

func (h *UserRoleRepository) AddAdminUser(u *billing.UserRoleAdmin) error {
	if err := h.svc.db.Collection(collectionMerchantUsersTable).Insert(u); err != nil {
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
	if err := h.svc.db.Collection(collectionMerchantUsersTable).UpdateId(u.Id, u); err != nil {
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
