package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionUserProfile = "user_profile"
)

// UserProfileRepositoryInterface is abstraction layer for working with user profile information and representation in database.
type UserProfileRepositoryInterface interface {
	// Add adds user profile to the collection.
	Add(context.Context, *billingpb.UserProfile) error

	// Update updates the user profile in the collection.
	Update(context.Context, *billingpb.UserProfile) error

	// Upsert add or update the user profile in the collection.
	Upsert(ctx context.Context, profile *billingpb.UserProfile) error

	// GetById returns the user profile by unique identity.
	GetById(context.Context, string) (*billingpb.UserProfile, error)

	// GetByUserId returns the user profile by user identity.
	GetByUserId(context.Context, string) (*billingpb.UserProfile, error)
}
