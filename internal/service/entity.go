package service

import (
	"sync"
	"time"
)

type Entity struct {
	svc *Service
	mx  sync.Mutex
}

type Currency Entity
type CurrencyRate Entity
type Commission Entity
type Country Entity
type Project Entity
type PaymentMethod Entity
type Merchant Entity
type PriceGroup Entity
type PaymentSystemService Entity
type ZipCode Entity
type PaymentChannelCostSystem Entity
type PaymentChannelCostMerchant Entity
type MoneyBackCostSystem Entity
type MoneyBackCostMerchant Entity
type PayoutCostSystem Entity

type kvIntFloat struct {
	Key   int
	Value float64
}

type kvIntInt struct {
	Key   int
	Value int32
}

// returns begin-of-day for passed date
func (s *Service) bod(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

// returns end-of-day for passed date
func (s *Service) eod(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 23, 59, 59, 999999999, t.Location())
}
