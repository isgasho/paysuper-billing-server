package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"time"
)

const (
	collectionAnnualTurnovers = "annual_turnovers"
	cacheTurnoverKey          = "turnover:company:%s:country:%s:year:%d"

	errorCannotCalculateTurnoverCountry = "can not calculate turnover for country"
	errorCannotCalculateTurnoverWorld   = "can not calculate turnover for world"
)

var (
	errorTurnoversCurrencyRatesPolicyNotSupported = newBillingServerErrorMsg("to000001", "vat currency rates policy not supported")
	errorTurnoversExchangeFailed                  = newBillingServerErrorMsg("to000002", "currency exchange failed")

	accountingEntriesForTurnover = []string{
		pkg.AccountingEntryTypeRealGrossRevenue,
	}
)

type turnoverQueryResItem struct {
	Id     string  `bson:"_id"`
	Amount float64 `bson:"amount"`
}

func (s *Service) CalcAnnualTurnovers(ctx context.Context, req *grpc.EmptyRequest, res *grpc.EmptyResponse) error {
	operatingCompanies, err := s.operatingCompany.GetAll(ctx)
	if err != nil {
		return err
	}

	countries, err := s.country.GetCountriesWithVatEnabled(ctx)
	if err != nil {
		return err
	}
	for _, operatingCompany := range operatingCompanies {
		var cnt []*billing.Country

		if len(operatingCompany.PaymentCountries) == 0 {
			cnt = countries.Countries
		} else {
			for _, countryCode := range operatingCompany.PaymentCountries {
				country, err := s.country.GetByIsoCodeA2(ctx, countryCode)
				if err != nil {
					return err
				}
				cnt = append(cnt, country)
			}
		}
		for _, country := range cnt {
			err = s.calcAnnualTurnover(ctx, country.IsoCodeA2, operatingCompany.Id)
			if err != nil {
				if err != errorTurnoversCurrencyRatesPolicyNotSupported {
					zap.L().Error(errorCannotCalculateTurnoverCountry,
						zap.String("country", country.IsoCodeA2),
						zap.Error(err))
					return err
				}
				zap.L().Warn(errorCannotCalculateTurnoverCountry,
					zap.String("country", country.IsoCodeA2),
					zap.Error(err))
			}
		}

		err = s.calcAnnualTurnover(ctx, "", operatingCompany.Id)
		if err != nil {
			zap.L().Error(errorCannotCalculateTurnoverWorld, zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *Service) calcAnnualTurnover(ctx context.Context, countryCode, operatingCompanyId string) error {

	var (
		targetCurrency = "EUR"
		ratesType      = curPkg.RateTypeOxr
		ratesSource    = ""
		currencyPolicy = pkg.VatCurrencyRatesPolicyOnDay
		year           = now.BeginningOfYear()
		from           = now.BeginningOfYear()
		to             = now.EndOfDay()
		amount         = float64(0)
		VatPeriodMonth = int32(0)
		err            error
	)

	if countryCode != "" {
		country, err := s.country.GetByIsoCodeA2(ctx, countryCode)
		if err != nil {
			return errorCountryNotFound
		}
		if country.VatEnabled {
			targetCurrency = country.VatCurrency
		}
		if targetCurrency == "" {
			targetCurrency = country.Currency
		}
		VatPeriodMonth = country.VatPeriodMonth
		currencyPolicy = country.VatCurrencyRatesPolicy
		ratesType = curPkg.RateTypeCentralbanks
		ratesSource = country.VatCurrencyRatesSource
	}

	switch currencyPolicy {
	case pkg.VatCurrencyRatesPolicyOnDay:
		amount, err = s.getTurnover(ctx, from, to, countryCode, targetCurrency, currencyPolicy, ratesType, ratesSource, operatingCompanyId)
		break
	case pkg.VatCurrencyRatesPolicyLastDay:
		from, to, err = s.getLastVatReportTime(VatPeriodMonth)
		if err != nil {
			return err
		}
		count := 0
		for from.Unix() >= year.Unix() {
			amnt, err := s.getTurnover(ctx, from, to, countryCode, targetCurrency, currencyPolicy, ratesType, ratesSource, operatingCompanyId)
			if err != nil {
				return err
			}
			amount += amnt
			count++
			from, to, err = s.getVatReportTimeForDate(VatPeriodMonth, from.AddDate(0, 0, -1))
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
		Year:               int32(year.Year()),
		Country:            countryCode,
		Amount:             tools.FormatAmount(amount),
		Currency:           targetCurrency,
		OperatingCompanyId: operatingCompanyId,
	}

	err = s.turnover.Insert(ctx, at)

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

func (s *Service) getTurnover(
	ctx context.Context,
	from, to time.Time,
	countryCode, targetCurrency, currencyPolicy, ratesType, ratesSource, operatingCompanyId string,
) (amount float64, err error) {

	matchQuery := bson.M{
		"created_at":           bson.M{"$gte": from, "$lte": to},
		"type":                 bson.M{"$in": accountingEntriesForTurnover},
		"operating_company_id": operatingCompanyId,
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
	case pkg.VatCurrencyRatesPolicyOnDay:
		query = append(query, bson.M{"$group": bson.M{"_id": "$local_currency", "amount": bson.M{"$sum": "$local_amount"}}})
		break
	case pkg.VatCurrencyRatesPolicyLastDay:
		query = append(query, bson.M{"$group": bson.M{"_id": "$original_currency", "amount": bson.M{"$sum": "$original_amount"}}})
		break
	default:
		err = errorTurnoversCurrencyRatesPolicyNotSupported
		return
	}

	cursor, err := s.db.Collection(collectionAccountingEntry).Aggregate(ctx, query)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			err = nil
			return
		}
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAccountingEntry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	var res []*turnoverQueryResItem
	err = cursor.All(ctx, &res)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAccountingEntry),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
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
			RateType: ratesType,
			Source:   ratesSource,
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

func (h *Turnover) Insert(ctx context.Context, turnover *billing.AnnualTurnover) error {
	filter := bson.M{"year": turnover.Year, "country": turnover.Country}
	_, err := h.svc.db.Collection(collectionAnnualTurnovers).ReplaceOne(ctx, filter, turnover, options.Replace().SetUpsert(true))

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAnnualTurnovers),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, turnover),
		)
		return err
	}

	key := fmt.Sprintf(cacheTurnoverKey, turnover.OperatingCompanyId, turnover.Country, turnover.Year)
	err = h.svc.cacher.Set(key, turnover, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", turnover)
		return err
	}
	return nil
}

func (h *Turnover) Update(ctx context.Context, turnover *billing.AnnualTurnover) error {
	return h.Insert(ctx, turnover)
}

func (h *Turnover) Get(ctx context.Context, operatingCompanyId, country string, year int) (*billing.AnnualTurnover, error) {
	var c billing.AnnualTurnover
	key := fmt.Sprintf(cacheTurnoverKey, operatingCompanyId, country, year)

	if err := h.svc.cacher.Get(key, c); err == nil {
		return &c, nil
	}

	query := bson.M{"operating_company_id": operatingCompanyId, "country": country, "year": year}
	err := h.svc.db.Collection(collectionAnnualTurnovers).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionAnnualTurnovers),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = h.svc.cacher.Set(key, c, 0)
	if err != nil {
		zap.S().Errorf("Unable to set cache", "err", err.Error(), "key", key, "data", c)
	}

	return &c, nil
}
