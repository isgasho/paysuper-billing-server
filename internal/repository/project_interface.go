package repository

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionProject = "project"

	cacheProjectId = "project:id:%s"
)

// ProjectRepositoryInterface is abstraction layer for working with project and representation in database.
type ProjectRepositoryInterface interface {
	// Insert adds the project to the collection.
	Insert(context.Context, *billingpb.Project) error

	// Insert adds the multiple projects to the collection.
	MultipleInsert(context.Context, []*billingpb.Project) error

	// Update updates the project in the collection.
	Update(context.Context, *billingpb.Project) error

	// GetById returns the project by unique identity.
	GetById(context.Context, string) (*billingpb.Project, error)

	// Count return count of projects by merchant identifier.
	CountByMerchantId(context.Context, string) (int64, error)

	// Find projects by merchant, quick search and statuses with pagination and sortable.
	Find(context.Context, string, string, []int32, int64, int64, []string) ([]*billingpb.Project, error)

	// FindCount return count of projects by merchant, quick search and statuses.
	FindCount(context.Context, string, string, []int32) (int64, error)
}
