package repository

import (
	"context"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

const (
	collectionMoneyBackCostMerchant = "money_back_cost_merchant"

	cacheMoneyBackCostMerchantKey   = "pucm:m:%s:n:%s:pc:%s:ur:%s:r:%s:c:%s:ps:%d:mcc:%s"
	cacheMoneyBackCostMerchantKeyId = "pucm:id:%s"
	cacheMoneyBackCostMerchantAll   = "pucm:all:m:%s"
)

// MoneyBackCostMerchantRepositoryInterface is abstraction layer for working with cost of merchant in money back case
// and representation in database.
type MoneyBackCostMerchantRepositoryInterface interface {
	Insert(context.Context, *billingpb.MoneyBackCostMerchant) error
	MultipleInsert(context.Context, []*billingpb.MoneyBackCostMerchant) error
	Update(context.Context, *billingpb.MoneyBackCostMerchant) error
	Find(context.Context, string, string, string, string, string, string, string, int32) ([]*internalPkg.MoneyBackCostMerchantSet, error)
	GetById(context.Context, string) (*billingpb.MoneyBackCostMerchant, error)
	Delete(context.Context, *billingpb.MoneyBackCostMerchant) error
	GetAllForMerchant(context.Context, string) (*billingpb.MoneyBackCostMerchantList, error)
}
