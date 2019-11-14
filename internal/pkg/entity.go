package pkg

import "github.com/paysuper/paysuper-billing-server/pkg/proto/billing"

type CountryAndRegionItem struct {
	Country           string `bson:"iso_code_a2"`
	Region            string `bson:"region"`
	PayerTariffRegion string `bson:"payer_tariff_region"`
}

type CountryAndRegionItems struct {
	Items []*CountryAndRegionItem `json:"items"`
}

type PaymentChannelCostMerchantSet struct {
	Id  string                                `bson:"_id"`
	Set []*billing.PaymentChannelCostMerchant `bson:"set"`
}

type PaymentChannelCostSystemSet struct {
	Id  string                              `bson:"_id"`
	Set []*billing.PaymentChannelCostSystem `bson:"set"`
}

type MoneyBackCostMerchantSet struct {
	Id  string                           `bson:"_id"`
	Set []*billing.MoneyBackCostMerchant `bson:"set"`
}

type MoneyBackCostSystemSet struct {
	Id  string                         `bson:"_id"`
	Set []*billing.MoneyBackCostSystem `bson:"set"`
}
