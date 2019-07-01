package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
	"time"
)

type turnoverQueryResItem struct {
	Id     string  `bson:"_id"`
	Amount float64 `bson:"amount"`
}

const (
	collectionAnnualTurnovers = "annual_turnovers"
	cacheTurnoverKey          = "turnover:country:%s:year:%d"
)

var (
	errorTurnoversCurrencyRatesPolicyNotSupported = newBillingServerErrorMsg("to000001", "vat currency rates policy not supported")
	errorTurnoversExchangeFailed                  = newBillingServerErrorMsg("to000002", "currency exchange failed")

	accountingEntriesForTurnover = []string{
		pkg.AccountingEntryTypePayment,
	}
)

func (s *Service) CalcAnnualTurnovers(ctx context.Context, req *grpc.EmptyRequest, res *grpc.EmptyResponse) error {
	countries, err := s.country.GetCountriesWithVatEnabled()
	if err != nil {
		return err
	}
	for _, country := range countries.Countries {
		err = s.calcAnnualTurnover(ctx, country.IsoCodeA2)
		if err != nil {
			return err
		}
	}

	err = s.calcAnnualTurnover(ctx, "")
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) calcAnnualTurnover(ctx context.Context, countryCode string) error {

	var (
		targetCurrency = "EUR"
		ratesSource    = curPkg.RateTypeOxr
		currencyPolicy = "on-day"
		year           = now.BeginningOfYear()
		from           = now.BeginningOfYear()
		to             = now.EndOfDay()
		amount         = float64(0)
		err            error
	)

	if countryCode != "" {
		country, err := s.country.GetByIsoCodeA2(countryCode)
		if err != nil {
			return errorCountryNotFound
		}
		targetCurrency = country.Currency
		currencyPolicy = country.VatCurrencyRatesPolicy
		ratesSource = curPkg.RateTypeCentralbanks
	}

	switch currencyPolicy {
	case "on-day":
		amount, err = s.getTurnover(ctx, from, to, countryCode, targetCurrency, currencyPolicy, ratesSource)
		break
	case "last-day":
		from, to, err = s.getLastVatReportTime(countryCode)
		if err != nil {
			return err
		}
		count := 0
		for from.Unix() >= year.Unix() {
			amnt, err := s.getTurnover(ctx, from, to, countryCode, targetCurrency, currencyPolicy, ratesSource)
			if err != nil {
				return err
			}
			amount += amnt
			count++
			from, to, err = s.getPreviousVatReportTime(countryCode, count)
			if err != nil {
				return err
			}
		}
		break
	default:
		err = errorTurnoversCurrencyRatesPolicyNotSupported
		return err
	}

	at := &billing.AnnualTurnover{
		Year:     int32(year.Year()),
		Country:  countryCode,
		Amount:   tools.FormatAmount(amount),
		Currency: targetCurrency,
	}

	err = s.turnover.Insert(at)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionAnnualTurnovers),
			zap.Any("value", at),
		)
		return err
	}
	return nil

}

func (s *Service) getTurnover(ctx context.Context, from, to time.Time, countryCode, targetCurrency, currencyPolicy, ratesSource string) (amount float64, err error) {

	matchQuery := bson.M{
		"created_at": bson.M{"$gte": from, "$lte": to},
		"type":       bson.M{"$in": accountingEntriesForTurnover},
	}
	if countryCode != "" {
		matchQuery["country"] = countryCode
	} else {
		matchQuery["country"] = bson.M{"$ne": ""}
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
	}

	switch currencyPolicy {
	case "on-day":
		query = append(query, bson.M{"$group": bson.M{"_id": "$local_currency", "amount": bson.M{"$sum": "$local_amount"}}})
		break
	case "last-day":
		query = append(query, bson.M{"$group": bson.M{"_id": "$original_currency", "amount": bson.M{"$sum": "$original_amount"}}})
		break
	default:
		err = errorTurnoversCurrencyRatesPolicyNotSupported
		return
	}

	var res []*turnoverQueryResItem

	err = s.db.Collection(collectionAccountingEntry).Pipe(query).All(&res)

	if err != nil {
		if err == mgo.ErrNotFound {
			err = nil
			return
		}
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(errorFieldCollection, collectionRoyaltyReport),
			zap.Any(errorFieldQuery, query),
		)
		return
	}

	toTimestamp, err := ptypes.TimestampProto(to)
	if err != nil {
		err = errorTurnoversExchangeFailed
		return
	}

	for _, v := range res {
		if v.Id == targetCurrency {
			amount += v.Amount
			continue
		}

		req := &currencies.ExchangeCurrencyByDateCommonRequest{
			From:     v.Id,
			To:       targetCurrency,
			RateType: ratesSource,
			Amount:   v.Amount,
			Datetime: toTimestamp,
		}

		rsp, err := s.curService.ExchangeCurrencyByDateCommon(ctx, req)

		if err != nil {
			zap.L().Error(
				pkg.ErrorGrpcServiceCallFailed,
				zap.Error(err),
				zap.String(errorFieldService, "CurrencyRatesService"),
				zap.String(errorFieldMethod, "ExchangeCurrencyCurrentCommon"),
				zap.Any(errorFieldRequest, req),
			)

			return 0, errorTurnoversExchangeFailed
		} else {
			amount += rsp.ExchangedAmount
		}
	}

	return
}

func newTurnoverService(svc *Service) *Turnover {
	s := &Turnover{svc: svc}
	return s
}

func (h *Turnover) Insert(turnover *billing.AnnualTurnover) error {
	_, err := h.svc.db.Collection(collectionAnnualTurnovers).Upsert(bson.M{"year": turnover.Year, "country": turnover.Country}, turnover)
	if err != nil {
		return err
	}

	err = h.svc.cacher.Set(fmt.Sprintf(cacheTurnoverKey, turnover.Country, turnover.Year), turnover, 0)
	if err != nil {
		return err
	}
	return nil
}

func (h *Turnover) Update(turnover *billing.AnnualTurnover) error {
	return h.Insert(turnover)
}

func (h *Turnover) Get(country string, year int) (*billing.AnnualTurnover, error) {
	var c billing.AnnualTurnover
	key := fmt.Sprintf(cacheTurnoverKey, country, year)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	err := h.svc.db.Collection(collectionAnnualTurnovers).
		Find(bson.M{"country": country, "year": year}).
		One(&c)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, collectionAnnualTurnovers)
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}
