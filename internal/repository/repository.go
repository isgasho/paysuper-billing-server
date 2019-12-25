package repository

import (
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
)

type repository struct {
	db mongodb.SourceInterface
}
