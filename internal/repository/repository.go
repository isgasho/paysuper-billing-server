package repository

import (
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
)

type repository struct {
	db *mongodb.Source
}
