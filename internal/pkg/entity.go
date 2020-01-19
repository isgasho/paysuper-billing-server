package pkg

import (
	"github.com/paysuper/paysuper-proto/go/billingpb"
)

type CountryAndRegionItem struct {
	Country           string `bson:"iso_code_a2"`
	Region            string `bson:"region"`
	PayerTariffRegion string `bson:"payer_tariff_region"`
}

type CountryAndRegionItems struct {
	Items []*CountryAndRegionItem `json:"items"`
}

type PaymentChannelCostMerchantSet struct {
	Id  string                                  `bson:"_id"`
	Set []*billingpb.PaymentChannelCostMerchant `bson:"set"`
}

type PaymentChannelCostSystemSet struct {
	Id  string                                `bson:"_id"`
	Set []*billingpb.PaymentChannelCostSystem `bson:"set"`
}

type MoneyBackCostMerchantSet struct {
	Id  string                             `bson:"_id"`
	Set []*billingpb.MoneyBackCostMerchant `bson:"set"`
}

type MoneyBackCostSystemSet struct {
	Id  string                           `bson:"_id"`
	Set []*billingpb.MoneyBackCostSystem `bson:"set"`
}
