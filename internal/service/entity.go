package service

import (
	"go.uber.org/zap"
	"sync"
	"time"
)

type Entity struct {
	svc *Service
	mx  sync.Mutex
}

type Currency Entity
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
type Turnover Entity

type kvIntFloat struct {
	Key   int
	Value float64
}

type kvIntInt struct {
	Key   int
	Value int32
}

// returns begin-of-day for passed date
func bod(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

// returns end-of-day for passed date
func eod(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 23, 59, 59, int(time.Second-time.Nanosecond), t.Location())
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	zap.L().Info(
		"function execution time",
		zap.String("name", name),
		zap.Duration("time", elapsed),
	)
}
