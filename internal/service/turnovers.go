package service

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"time"
)

const (
	errorCannotCalculateTurnoverCountry = "can not calculate turnover for country"
	errorCannotCalculateTurnoverWorld   = "can not calculate turnover for world"
)

var (
	errorTurnoversCurrencyRatesPolicyNotSupported = newBillingServerErrorMsg("to000001", "vat currency rates policy not supported")
	errorTurnoversExchangeFailed                  = newBillingServerErrorMsg("to000002", "currency exchange failed")
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

	countries, err := s.country.FindByVatEnabled(ctx)
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
			diff := to.Sub(time.Now())
			if diff.Seconds() > 0 {
				to = now.EndOfDay()
			}
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

	err = s.turnoverRepository.Upsert(ctx, at)

	if err != nil {
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
		"pm_order_close_date": bson.M{
			"$gte": from,
			"$lte": to,
		},
		"operating_company_id": operatingCompanyId,
		"is_production":        true,
		"type":                 pkg.OrderTypeOrder,
		"status":               constant.OrderPublicStatusProcessed,
		"payment_gross_revenue_origin": bson.M{
			"$ne": nil,
		},
	}
	if countryCode != "" {
		matchQuery["country_code"] = countryCode
	} else {
		matchQuery["country_code"] = bson.M{"$ne": ""}
	}

	query := []bson.M{
		{
			"$match": matchQuery,
		},
	}

	switch currencyPolicy {
	case pkg.VatCurrencyRatesPolicyOnDay:
		query = append(query, bson.M{
			"$group": bson.M{
				"_id": "$payment_gross_revenue_local.currency",
				"amount": bson.M{
					"$sum": "$payment_gross_revenue_local.amount",
				},
			},
		})
		break

	case pkg.VatCurrencyRatesPolicyLastDay:
		query = append(query, bson.M{
			"$group": bson.M{
				"_id": "$payment_gross_revenue_origin.currency",
				"amount": bson.M{
					"$sum": "$payment_gross_revenue_origin.amount",
				},
			},
		})
		break

	default:
		err = errorTurnoversCurrencyRatesPolicyNotSupported
		return
	}

	cursor, err := s.db.Collection(collectionOrderView).Aggregate(ctx, query)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			err = nil
			return
		}
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
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
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
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
			From:              v.Id,
			To:                targetCurrency,
			RateType:          ratesType,
			ExchangeDirection: curPkg.ExchangeDirectionBuy,
			Source:            ratesSource,
			Amount:            v.Amount,
			Datetime:          toTimestamp,
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
