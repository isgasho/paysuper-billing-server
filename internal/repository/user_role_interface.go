package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionMerchantUsersTable = "user_merchant"
	collectionAdminUsersTable    = "user_admin"

	cacheUserMerchants = "user:merchants:%s"
)

// UserRoleRepositoryInterface is abstraction layer for working with user role and representation in database.
type UserRoleRepositoryInterface interface {
	// AddMerchantUser add user role to the merchant collection.
	AddMerchantUser(context.Context, *billingpb.UserRole) error

	// AddAdminUser add user role to the system collection.
	AddAdminUser(context.Context, *billingpb.UserRole) error

	// UpdateMerchantUser update user role in the merchant collection.
	UpdateMerchantUser(context.Context, *billingpb.UserRole) error

	// UpdateAdminUser update user role in the system collection.
	UpdateAdminUser(context.Context, *billingpb.UserRole) error

	// GetMerchantUserByEmail get merchant user role by merchant ID and user email.
	GetMerchantUserByEmail(context.Context, string, string) (*billingpb.UserRole, error)

	// GetAdminUserByEmail get system user role by user email.
	GetAdminUserByEmail(context.Context, string) (*billingpb.UserRole, error)

	// GetMerchantUserById get merchant user role by user role identifier.
	GetMerchantUserById(context.Context, string) (*billingpb.UserRole, error)

	// GetAdminUserById get system user role by user role identifier.
	GetAdminUserById(context.Context, string) (*billingpb.UserRole, error)

	// GetMerchantUserByUserId get merchant user role by merchant ID and user ID.
	GetMerchantUserByUserId(context.Context, string, string) (*billingpb.UserRole, error)

	// GetAdminUserByUserId get system user role by user ID.
	GetAdminUserByUserId(context.Context, string) (*billingpb.UserRole, error)

	// GetUsersForMerchant get list of merchant user roles by merchant ID.
	GetUsersForMerchant(context.Context, string) ([]*billingpb.UserRole, error)

	// GetUsersForAdmin get list of system users roles.
	GetUsersForAdmin(context.Context) ([]*billingpb.UserRole, error)

	// GetMerchantsForUser get list of merchant user roles by user ID.
	GetMerchantsForUser(context.Context, string) ([]*billingpb.UserRole, error)

	// DeleteAdminUser delete system user role from the collection.
	DeleteAdminUser(context.Context, *billingpb.UserRole) error

	// DeleteMerchantUser delete merchant user role from the collection.
	DeleteMerchantUser(context.Context, *billingpb.UserRole) error

	// GetSystemAdmin get user role with system admin role.
	GetSystemAdmin(context.Context) (*billingpb.UserRole, error)

	// GetMerchantOwner get user role with merchant owner role by merchant ID.
	GetMerchantOwner(context.Context, string) (*billingpb.UserRole, error)
}
