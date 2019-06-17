package service

import (
	"sync"
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
type SystemFee Entity
type PriceGroup Entity
type PaymentSystemService Entity
