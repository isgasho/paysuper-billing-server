package repository

import (
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

type Repository struct {
	db *mongodb.Source
}
