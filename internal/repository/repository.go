package repository

import (
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

// Repository is general repositories structure with configuration parameters.
type Repository struct {
	db *mongodb.Source
}
