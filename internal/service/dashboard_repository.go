package service

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"go.uber.org/zap"
	"time"
)

const (
	dashboardMainCacheKey           = "dashboard:main:%x"
	dashboardRevenueDynamicCacheKey = "dashboard:revenue_dynamic:%x"

	baseReportsItemsLimit = 5

	baseReportsWeekPeriodsNameFirst  = "0 - 7"
	baseReportsWeekPeriodsNameSecond = "8 - 15"
	baseReportsWeekPeriodsNameThird  = "16 - 23"

	baseReportsWeekPeriodsFirstStartHour = 0
	baseReportsWeekPeriodsFirstEndHour   = 7
	baseReportsWeekPeriodSecondStartHour = 8
	baseReportsWeekPeriodSecondEndHour   = 15
	baseReportsWeekPeriodsThirdStartHour = 16
	baseReportsWeekPeriodsThirdEndHour   = 23
)

type DashboardRepositoryInterface interface{}

type dashboardReportProcessor struct {
	*Service
	match               bson.M
	periodMatchCurrent  bson.M
	periodMatchPrevious bson.M
	period              string
	groupBy             string
	dbQueryFn           func() (interface{}, error)
	cacheKeyMask        string
}

func (h *DashboardRepository) GetDashboardMain(merchantId, period string) (*grpc.GetDashboardMainResponseItem, error) {
	processor := &dashboardReportProcessor{
		Service:      h.svc,
		match:        h.getQueryConditions(merchantId, period),
		period:       period,
		cacheKeyMask: dashboardMainCacheKey,
	}
	processor.dbQueryFn = processor.ExecuteDashboardMainQuery
	data, err := processor.ExecuteReport(new(grpc.GetDashboardMainResponseItem))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	return data.(*grpc.GetDashboardMainResponseItem), nil
}

func (h *DashboardRepository) GetDashboardRevenueDynamic(
	merchantId, period string,
) ([]*grpc.GetDashboardRevenueDynamicResponseItem, error) {
	match := h.getQueryConditions(merchantId, period)
	match["status"] = constant.OrderPublicStatusProcessed
	groupBy := "$day"

	switch period {
	case pkg.DashboardPeriodCurrentQuarter, pkg.DashboardPeriodPreviousQuarter:
		groupBy = "$week"
		break
	case pkg.DashboardPeriodCurrentYear, pkg.DashboardPeriodPreviousYear:
		groupBy = "$month"
		break
	}

	processor := &dashboardReportProcessor{
		Service:      h.svc,
		match:        match,
		period:       period,
		groupBy:      groupBy,
		cacheKeyMask: dashboardRevenueDynamicCacheKey,
	}
	processor.dbQueryFn = processor.ExecuteDashboardRevenueDynamic
	data, err := processor.ExecuteReport(make([]*grpc.GetDashboardRevenueDynamicResponseItem, 1))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	return data.([]*grpc.GetDashboardRevenueDynamicResponseItem), nil
}

func (h *DashboardRepository) getQueryConditions(merchantId, period string) bson.M {
	current := time.Now()
	match := bson.M{"merchant_id": bson.ObjectIdHex(merchantId)}

	switch period {
	case pkg.DashboardPeriodCurrentMonth:
		match["month"] = bson.M{"$eq": int(current.Month())}
		break
	case pkg.DashboardPeriodPreviousMonth:
		match["month"] = bson.M{"$eq": int(current.AddDate(0, -1, 0).Month())}
		break
	case pkg.DashboardPeriodCurrentQuarter:
		match["month"] = bson.M{"$gte": int(current.AddDate(0, -2, 0).Month()), "$lte": int(current.Month())}
		break
	case pkg.DashboardPeriodPreviousQuarter:
		match["month"] = bson.M{
			"$gte": int(current.AddDate(0, -5, 0).Month()),
			"$lte": int(current.AddDate(0, -3, 0).Month()),
		}
		break
	case pkg.DashboardPeriodCurrentYear:
		match["year"] = bson.M{"$eq": current.Year()}
		break
	case pkg.DashboardPeriodPreviousYear:
		match["year"] = bson.M{"$eq": current.AddDate(-1, 0, 0).Year()}
		break
	}

	return match
}

func (h *dashboardReportProcessor) ExecuteReport(receiver interface{}) (interface{}, error) {
	expire := h.getCacheExpire(h.period)
	key, err := h.GetCacheKey(h.cacheKeyMask, h.match)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	if expire > 0 {
		err = h.cacher.Get(key, &receiver)

		if err == nil {
			return receiver, nil
		}
	}

	receiver, err = h.dbQueryFn()

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	if expire > 0 {
		err = h.cacher.Set(key, receiver, expire)

		if err != nil {
			zap.L().Error(
				pkg.ErrorCacheQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorCacheFieldCmd, "SET"),
				zap.String(pkg.ErrorCacheFieldKey, key),
				zap.Any(pkg.ErrorDatabaseFieldQuery, receiver),
			)

			return nil, dashboardErrorUnknown
		}
	}

	return receiver, nil
}

func (h *dashboardReportProcessor) getCacheExpire(period string) time.Duration {
	current := time.Now()
	expire := time.Duration(0)

	if period == pkg.DashboardPeriodPreviousMonth || period == pkg.DashboardPeriodPreviousQuarter {
		expire = now.New(current).EndOfMonth().Sub(current)
	}

	if period == pkg.DashboardPeriodPreviousYear {
		expire = now.New(current).EndOfYear().Sub(current)
	}

	return expire
}

func (h *dashboardReportProcessor) GetCacheKey(cacheMask string, req interface{}) (string, error) {
	b, err := json.Marshal(req)

	if err != nil {
		zap.L().Error(
			"Marshaling request to cache key failed",
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, req),
		)

		return "", err
	}

	return fmt.Sprintf(cacheMask, md5.Sum(b)), nil
}

func (h *dashboardReportProcessor) ExecuteDashboardMainQuery() (interface{}, error) {
	data := new(grpc.GetDashboardMainResponseItem)
	query := []bson.M{
		{
			"$project": bson.M{
				"month":          bson.M{"$month": "$pm_order_close_date"},
				"year":           bson.M{"$year": "$pm_order_close_date"},
				"merchant_id":    true,
				"status":         true,
				"revenue_amount": "$payment_gross_revenue.amount",
				"vat_amount":     "$payment_tax_fee.amount",
				"currency":       "$payment_gross_revenue.currency",
			},
		},
		{"$match": h.match},
		{
			"$project": bson.M{
				"_id":         0,
				"merchant_id": "$merchant_id",
				"revenue_amount": bson.M{
					"$cond": []interface{}{bson.M{
						"$eq": []string{"$status", constant.OrderPublicStatusProcessed}}, "$revenue_amount", 0,
					},
				},
				"total_transactions": bson.M{
					"$cond": []interface{}{
						bson.M{"$in": []interface{}{
							"$status", []string{
								constant.OrderPublicStatusProcessed,
								constant.OrderPublicStatusRefunded,
								constant.OrderPublicStatusChargeback,
							}, 1, 0}},
					},
				},
				"vat_amount": bson.M{
					"$cond": []interface{}{bson.M{
						"$eq": []string{"$status", constant.OrderPublicStatusProcessed}}, "$vat_amount", 0,
					},
				},
				"currency": "$currency",
			},
		},
		{
			"$group": bson.M{
				"_id":                "$merchant_id",
				"gross_revenue":      bson.M{"$sum": "$revenue_amount"},
				"currency":           bson.M{"$first": "$currency"},
				"total_transactions": bson.M{"$sum": "$total_transactions"},
				"vat_amount":         bson.M{"$sum": "$vat_amount"},
			},
		},
		{
			"$addFields": bson.M{"arpu": bson.M{"$divide": []string{"$gross_revenue", "$total_transactions"}}},
		},
		{
			"$project": bson.M{
				"gross_revenue":  bson.M{"amount": "$gross_revenue", "currency": "$currency"},
				"total_payments": "$total_transactions",
				"arpu":           bson.M{"amount": "$arpu", "currency": "$currency"},
				"vat":            bson.M{"amount": "$vat_amount", "currency": "$currency"},
			},
		},
	}

	err := h.db.Collection(collectionOrderView).Pipe(query).One(data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return data, nil
}

func (h *dashboardReportProcessor) ExecuteDashboardRevenueDynamic() (interface{}, error) {
	var data []*grpc.GetDashboardRevenueDynamicResponseItem
	query := []bson.M{
		{
			"$project": bson.M{
				"day":         bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"week":        bson.M{"$week": "$pm_order_close_date"},
				"month":       bson.M{"$month": "$pm_order_close_date"},
				"year":        bson.M{"$year": "$pm_order_close_date"},
				"merchant_id": true,
				"status":      true,
				"amount":      "$net_revenue.amount",
				"currency":    "$net_revenue.currency",
			},
		},
		{"$match": h.match},
		{
			"$group": bson.M{
				"_id":      h.groupBy,
				"amount":   bson.M{"$sum": "$amount"},
				"currency": bson.M{"$first": "$currency"},
				"count":    bson.M{"$sum": 1},
			},
		},
	}

	err := h.db.Collection(collectionOrderView).Pipe(query).All(data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return data, nil
}

func (h *dashboardReportProcessor) ExecuteDashboardBaseReports() (interface{}, error) {
	executeDashboardBaseReportsRevenueByCountry
}

func (h *dashboardReportProcessor) executeDashboardBaseReportsRevenueByCountry() (*grpc.DashboardBaseReportsRevenueByCountry, error) {
	data := new(grpc.DashboardBaseReportsRevenueByCountry)
	query := []bson.M{
		{
			"$project": bson.M{
				"hour":        bson.M{"$hour": "$pm_order_close_date"},
				"day":         bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"month":       bson.M{"$month": "$pm_order_close_date"},
				"week":        bson.M{"$week": "$pm_order_close_date"},
				"year":        bson.M{"$year": "$pm_order_close_date"},
				"merchant_id": true,
				"status":      true,
				"country": bson.M{
					"$cond": []interface{}{
						bson.M{"$eq": []string{"$billing_address.country", ""}},
						"$user.address.country",
						"$billing_address.country",
					},
				},
				"amount":   "$net_revenue.amount",
				"currency": "$net_revenue.currency",
			},
		},
		{"$match": h.match},
		{
			"$facet": bson.M{
				"by_country": []bson.M{
					{"$match": h.periodMatchCurrent},
					{
						"$group": bson.M{
							"_id":    "$country",
							"amount": bson.M{"$sum": "$amount"},
						},
					},
					{"$limit": baseReportsItemsLimit},
				},
				"by_period": []bson.M{
					{"$match": h.periodMatchCurrent},
					{
						"$group": bson.M{
							"_id":      "$hour",
							"day":      bson.M{"$first": "$day"},
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
				"total_current": []bson.M{
					{"$match": h.periodMatchCurrent},
					{
						"$group": bson.M{
							"_id":      "$merchant_id",
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
				"total_previous": []bson.M{
					{"$match": h.periodMatchPrevious},
					{
						"$group": bson.M{
							"_id":      "$merchant_id",
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
			},
		},
	}

	err := h.db.Collection(collectionOrderView).Pipe(query).All(data)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return data, nil
}
