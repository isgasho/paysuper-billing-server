package repository

import (
	"github.com/paysuper/paysuper-billing-server/internal/database"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type repository struct {
	db    mongodb.SourceInterface
	cache database.CacheInterface
}
