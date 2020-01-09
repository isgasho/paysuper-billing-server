package repository

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
)

const (
	collectionMerchantUsersTable = "user_merchant"
	collectionAdminUsersTable    = "user_admin"

	cacheUserMerchants = "user:merchants:%s"
)

// UserRoleRepositoryInterface is abstraction layer for working with user role and representation in database.
type UserRoleRepositoryInterface interface {
	// AddMerchantUser add user role to the merchant collection.
	AddMerchantUser(context.Context, *billing.UserRole) error

	// AddAdminUser add user role to the system collection.
	AddAdminUser(context.Context, *billing.UserRole) error

	// UpdateMerchantUser update user role in the merchant collection.
	UpdateMerchantUser(context.Context, *billing.UserRole) error

	// UpdateAdminUser update user role in the system collection.
	UpdateAdminUser(context.Context, *billing.UserRole) error

	// GetMerchantUserByEmail get merchant user role by merchant ID and user email.
	GetMerchantUserByEmail(context.Context, string, string) (*billing.UserRole, error)

	// GetAdminUserByEmail get system user role by user email.
	GetAdminUserByEmail(context.Context, string) (*billing.UserRole, error)

	// GetMerchantUserById get merchant user role by user role identifier.
	GetMerchantUserById(context.Context, string) (*billing.UserRole, error)

	// GetAdminUserById get system user role by user role identifier.
	GetAdminUserById(context.Context, string) (*billing.UserRole, error)

	// GetMerchantUserByUserId get merchant user role by merchant ID and user ID.
	GetMerchantUserByUserId(context.Context, string, string) (*billing.UserRole, error)

	// GetAdminUserByUserId get system user role by user ID.
	GetAdminUserByUserId(context.Context, string) (*billing.UserRole, error)

	// GetUsersForMerchant get list of merchant user roles by merchant ID.
	GetUsersForMerchant(context.Context, string) ([]*billing.UserRole, error)

	// GetUsersForAdmin get list of system users roles.
	GetUsersForAdmin(context.Context) ([]*billing.UserRole, error)

	// GetMerchantsForUser get list of merchant user roles by user ID.
	GetMerchantsForUser(context.Context, string) ([]*billing.UserRole, error)

	// DeleteAdminUser delete system user role from the collection.
	DeleteAdminUser(context.Context, *billing.UserRole) error

	// DeleteMerchantUser delete merchant user role from the collection.
	DeleteMerchantUser(context.Context, *billing.UserRole) error

	// GetSystemAdmin get user role with system admin role.
	GetSystemAdmin(context.Context) (*billing.UserRole, error)

	// GetMerchantOwner get user role with merchant owner role by merchant ID.
	GetMerchantOwner(context.Context, string) (*billing.UserRole, error)
}
