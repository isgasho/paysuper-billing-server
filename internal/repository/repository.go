package repository

import (
	"github.com/paysuper/paysuper-billing-server/internal/database"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

type repository struct {
	db    *mongodb.Source
	cache database.CacheInterface
}
