package service

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"go.uber.org/zap"
	"time"
)

const (
	dashboardMainCacheKey                 = "dashboard:main:%x"
	dashboardRevenueDynamicCacheKey       = "dashboard:revenue_dynamic:%x"
	dashboardBaseRevenueByCountryCacheKey = "dashboard:base:revenue_by_country:%x"
	dashboardBaseSalesTodayCacheKey       = "dashboard:base:sales_today:%x"
	dashboardBaseSourcesCacheKey          = "dashboard:base:sources:%x"

	baseReportsItemsLimit = 5

	dashboardReportGroupByHour        = "$hour"
	dashboardReportGroupByDay         = "$day"
	dashboardReportGroupByMonth       = "$month"
	dashboardReportGroupByWeek        = "$week"
	dashboardReportGroupByPeriodInDay = "$period_in_day"
)

var (
	dashboardReportBasePreviousPeriodsNames = map[string]string{
		pkg.DashboardPeriodCurrentDay:      pkg.DashboardPeriodPreviousDay,
		pkg.DashboardPeriodPreviousDay:     pkg.DashboardPeriodTwoDaysAgo,
		pkg.DashboardPeriodCurrentWeek:     pkg.DashboardPeriodPreviousWeek,
		pkg.DashboardPeriodPreviousWeek:    pkg.DashboardPeriodTwoWeeksAgo,
		pkg.DashboardPeriodCurrentMonth:    pkg.DashboardPeriodPreviousMonth,
		pkg.DashboardPeriodPreviousMonth:   pkg.DashboardPeriodTwoMonthsAgo,
		pkg.DashboardPeriodCurrentQuarter:  pkg.DashboardPeriodPreviousQuarter,
		pkg.DashboardPeriodPreviousQuarter: pkg.DashboardPeriodTwoQuarterAgo,
		pkg.DashboardPeriodCurrentYear:     pkg.DashboardPeriodPreviousYear,
		pkg.DashboardPeriodPreviousYear:    pkg.DashboardPeriodTwoYearsAgo,
	}
)

type DashboardRepositoryInterface interface {
	NewDashboardReportProcessor(string, string, string, interface{}, *mongodb.Source, CacheInterface) (*dashboardReportProcessor, error)
	GetDashboardMainReport(string, string) (*grpc.DashboardMainReport, error)
	GetDashboardRevenueDynamicsReport(string, string) ([]*grpc.DashboardRevenueDynamicReport, error)
	GetDashboardBaseReport(string, string) (*grpc.DashboardBaseReports, error)
	GetDashboardBaseRevenueByCountryReport(string, string) (*grpc.DashboardRevenueByCountryReport, error)
	GetDashboardBaseSalesTodayReport(string, string) (*grpc.DashboardSalesTodayReport, error)
	GetDashboardBaseSourcesReport(string, string) (*grpc.DashboardSourcesReport, error)
}

type DashboardReportProcessorInterface interface {
	ExecuteReport(interface{}) (interface{}, error)
	ExecuteDashboardMainReport(interface{}) (interface{}, error)
	ExecuteDashboardRevenueDynamicReport(interface{}) (interface{}, error)
	ExecuteDashboardRevenueByCountryReport(interface{}) (interface{}, error)
	ExecuteDashboardSalesTodayReport(interface{}) (interface{}, error)
	ExecuteDashboardSourcesReport(interface{}) (interface{}, error)
}

type dashboardReportProcessor struct {
	match       bson.M
	groupBy     string
	dbQueryFn   func(interface{}) (interface{}, error)
	cacheKey    string
	cacheExpire time.Duration
	db          *mongodb.Source
	cache       CacheInterface
}

func newDashboardRepository(s *Service) DashboardRepositoryInterface {
	return &DashboardRepository{svc: s}
}

func (m *DashboardRepository) GetDashboardMainReport(merchantId, period string) (*grpc.DashboardMainReport, error) {
	processor, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardMainCacheKey,
		bson.M{"$in": []string{"processed", "refunded", "chargeback"}},
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processor.dbQueryFn = processor.ExecuteDashboardMainReport
	data, err := processor.ExecuteReport(new(grpc.DashboardMainReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	return data.(*grpc.DashboardMainReport), nil
}

func (m *DashboardRepository) GetDashboardRevenueDynamicsReport(
	merchantId, period string,
) ([]*grpc.DashboardRevenueDynamicReport, error) {
	processor, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardRevenueDynamicCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processor.dbQueryFn = processor.ExecuteDashboardRevenueDynamicReport
	data, err := processor.ExecuteReport(make([]*grpc.DashboardRevenueDynamicReport, 1))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	return data.([]*grpc.DashboardRevenueDynamicReport), nil
}

func (m *DashboardRepository) GetDashboardBaseReport(merchantId, period string) (*grpc.DashboardBaseReports, error) {
	revenueByCountryReport, err := m.GetDashboardBaseRevenueByCountryReport(merchantId, period)

	if err != nil {
		return nil, err
	}

	salesTodayReport, err := m.GetDashboardBaseSalesTodayReport(merchantId, period)

	if err != nil {
		return nil, err
	}

	sourcesReport, err := m.GetDashboardBaseSourcesReport(merchantId, period)

	if err != nil {
		return nil, err
	}

	reports := &grpc.DashboardBaseReports{
		RevenueByCountry: revenueByCountryReport,
		SalesToday:       salesTodayReport,
		Sources:          sourcesReport,
	}

	return reports, nil
}

func (m *DashboardRepository) GetDashboardBaseRevenueByCountryReport(
	merchantId, period string,
) (*grpc.DashboardRevenueByCountryReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseRevenueByCountryCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorPrevious, err := m.NewDashboardReportProcessor(
		merchantId,
		dashboardReportBasePreviousPeriodsNames[period],
		dashboardBaseRevenueByCountryCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.dbQueryFn = processorCurrent.ExecuteDashboardRevenueByCountryReport
	processorPrevious.dbQueryFn = processorPrevious.ExecuteDashboardRevenueByCountryReport
	dataCurrent, err := processorCurrent.ExecuteReport(new(grpc.DashboardRevenueByCountryReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(new(grpc.DashboardRevenueByCountryReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataCurrentTyped := dataCurrent.(*grpc.DashboardRevenueByCountryReport)

	if dataCurrentTyped == nil || dataCurrentTyped.TotalCurrent == nil {
		dataCurrentTyped = &grpc.DashboardRevenueByCountryReport{
			TotalCurrent: &grpc.DashboardRevenueByCountryReportTotal{},
		}
	}

	dataPreviousTyped := dataPrevious.(*grpc.DashboardRevenueByCountryReport)

	if dataPreviousTyped == nil || dataPreviousTyped.TotalCurrent == nil {
		dataPreviousTyped = &grpc.DashboardRevenueByCountryReport{
			TotalCurrent: &grpc.DashboardRevenueByCountryReportTotal{},
		}
	}

	dataCurrentTyped.TotalPrevious = dataPreviousTyped.TotalCurrent

	return dataCurrentTyped, nil
}

func (m *DashboardRepository) GetDashboardBaseSalesTodayReport(
	merchantId, period string,
) (*grpc.DashboardSalesTodayReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseSalesTodayCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorPrevious, err := m.NewDashboardReportProcessor(
		merchantId,
		dashboardReportBasePreviousPeriodsNames[period],
		dashboardBaseSalesTodayCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.dbQueryFn = processorCurrent.ExecuteDashboardSalesTodayReport
	processorPrevious.dbQueryFn = processorPrevious.ExecuteDashboardSalesTodayReport
	dataCurrent, err := processorCurrent.ExecuteReport(new(grpc.DashboardSalesTodayReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(new(grpc.DashboardSalesTodayReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataCurrentTyped := dataCurrent.(*grpc.DashboardSalesTodayReport)
	dataPreviousTyped := dataPrevious.(*grpc.DashboardSalesTodayReport)
	dataCurrentTyped.TotalPrevious = dataPreviousTyped.TotalCurrent

	return dataCurrentTyped, nil
}

func (m *DashboardRepository) GetDashboardBaseSourcesReport(
	merchantId, period string,
) (*grpc.DashboardSourcesReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseSourcesCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorPrevious, err := m.NewDashboardReportProcessor(
		merchantId,
		dashboardReportBasePreviousPeriodsNames[period],
		dashboardBaseSourcesCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.dbQueryFn = processorCurrent.ExecuteDashboardSourcesReport
	processorPrevious.dbQueryFn = processorPrevious.ExecuteDashboardSourcesReport
	dataCurrent, err := processorCurrent.ExecuteReport(new(grpc.DashboardSourcesReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(new(grpc.DashboardSourcesReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataCurrentTyped := dataCurrent.(*grpc.DashboardSourcesReport)
	dataPreviousTyped := dataPrevious.(*grpc.DashboardSourcesReport)
	dataCurrentTyped.TotalPrevious = dataPreviousTyped.TotalCurrent

	return dataCurrentTyped, nil
}

func (m *DashboardRepository) NewDashboardReportProcessor(
	merchantId, period, cacheKeyMask string,
	status interface{},
	db *mongodb.Source,
	cache CacheInterface,
) (*dashboardReportProcessor, error) {
	current := time.Now()
	processor := &dashboardReportProcessor{
		match:       bson.M{"merchant_id": bson.ObjectIdHex(merchantId), "status": status},
		db:          db,
		cache:       cache,
		cacheExpire: time.Duration(0),
	}

	switch period {
	case pkg.DashboardPeriodCurrentDay:
		processor.groupBy = dashboardReportGroupByHour
		gte := now.BeginningOfDay()
		lte := now.EndOfDay()
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousDay, pkg.DashboardPeriodTwoDaysAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoDaysAgo {
			decrement = decrement * 2
		}
		previousDay := time.Now().AddDate(0, 0, decrement)
		gte := now.New(previousDay).BeginningOfDay()
		lte := now.New(previousDay).EndOfDay()

		processor.groupBy = dashboardReportGroupByHour
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.cacheExpire = now.New(current).EndOfDay().Sub(current)
		break
	case pkg.DashboardPeriodCurrentWeek:
		processor.groupBy = dashboardReportGroupByPeriodInDay
		gte := now.BeginningOfWeek()
		lte := now.EndOfWeek()
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousWeek, pkg.DashboardPeriodTwoWeeksAgo:
		decrement := -7
		if period == pkg.DashboardPeriodTwoWeeksAgo {
			decrement = decrement * 2
		}
		previousWeek := time.Now().AddDate(0, 0, decrement)
		gte := now.New(previousWeek).BeginningOfWeek()
		lte := now.New(previousWeek).EndOfWeek()

		processor.groupBy = dashboardReportGroupByPeriodInDay
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.cacheExpire = now.New(current).EndOfWeek().Sub(current)
		break
	case pkg.DashboardPeriodCurrentMonth:
		gte := now.BeginningOfMonth()
		lte := now.EndOfMonth()

		processor.groupBy = dashboardReportGroupByDay
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousMonth, pkg.DashboardPeriodTwoMonthsAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoMonthsAgo {
			decrement = decrement * 2
		}
		previousMonth := time.Now().AddDate(0, decrement, 0)
		gte := now.New(previousMonth).BeginningOfMonth()
		lte := now.New(previousMonth).EndOfMonth()

		processor.groupBy = dashboardReportGroupByDay
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}

		processor.cacheExpire = now.New(current).EndOfMonth().Sub(current)
		break
	case pkg.DashboardPeriodCurrentQuarter:
		gte := now.BeginningOfQuarter()
		lte := now.EndOfQuarter()
		processor.groupBy = dashboardReportGroupByWeek
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousQuarter, pkg.DashboardPeriodTwoQuarterAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoQuarterAgo {
			decrement = decrement * 2
		}
		previousQuarter := now.BeginningOfQuarter().AddDate(0, decrement, 0)
		gte := now.New(previousQuarter).BeginningOfQuarter()
		lte := now.New(previousQuarter).EndOfQuarter()

		processor.groupBy = dashboardReportGroupByWeek
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}

		processor.cacheExpire = now.New(current).EndOfQuarter().Sub(current)
		break
	case pkg.DashboardPeriodCurrentYear:
		gte := now.BeginningOfYear()
		lte := now.EndOfYear()

		processor.groupBy = dashboardReportGroupByMonth
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousYear, pkg.DashboardPeriodTwoYearsAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoYearsAgo {
			decrement = decrement * 2
		}
		previousYear := time.Now().AddDate(-1, 0, 0)
		gte := now.New(previousYear).BeginningOfYear()
		lte := now.New(previousYear).EndOfYear()

		processor.groupBy = dashboardReportGroupByMonth
		processor.match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.cacheExpire = now.New(current).EndOfYear().Sub(current)
		break
	default:
		return nil, dashboardErrorIncorrectPeriod
	}

	if processor.cacheExpire > 0 {
		b, err := json.Marshal(processor.match)

		if err != nil {
			zap.L().Error(
				"Generate dashboard report cache key failed",
				zap.Error(err),
				zap.Any(pkg.ErrorDatabaseFieldQuery, processor.match),
			)

			return nil, err
		}

		processor.cacheKey = fmt.Sprintf(cacheKeyMask, md5.Sum(b))
	}

	return processor, nil
}

func (m *dashboardReportProcessor) ExecuteReport(receiver interface{}) (interface{}, error) {
	if m.cacheExpire > 0 {
		err := m.cache.Get(m.cacheKey, &receiver)

		if err == nil {
			return receiver, nil
		}
	}

	receiver, err := m.dbQueryFn(receiver)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	if m.cacheExpire > 0 {
		err = m.cache.Set(m.cacheKey, receiver, m.cacheExpire)

		if err != nil {
			zap.L().Error(
				pkg.ErrorCacheQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorCacheFieldCmd, "SET"),
				zap.String(pkg.ErrorCacheFieldKey, m.cacheKey),
				zap.Any(pkg.ErrorDatabaseFieldQuery, receiver),
			)

			return nil, dashboardErrorUnknown
		}
	}

	return receiver, nil
}

func (m *dashboardReportProcessor) ExecuteDashboardMainReport(receiver interface{}) (interface{}, error) {
	query := []bson.M{
		{"$match": m.match},
		{
			"$project": bson.M{
				"day":   bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"week":  bson.M{"$week": "$pm_order_close_date"},
				"month": bson.M{"$month": "$pm_order_close_date"},
				"revenue_amount": bson.M{
					"$cond": []interface{}{
						bson.M{"$eq": []string{"$status", "processed"}}, "$payment_gross_revenue.amount", 0,
					},
				},
				"vat_amount": bson.M{
					"$cond": []interface{}{
						bson.M{"$eq": []string{"$status", "processed"}}, "$payment_tax_fee.amount", 0,
					},
				},
				"currency": "$payment_gross_revenue.currency",
			},
		},
		{
			"$facet": bson.M{
				"main": []bson.M{
					{
						"$group": bson.M{
							"_id":                nil,
							"gross_revenue":      bson.M{"$sum": "$revenue_amount"},
							"currency":           bson.M{"$first": "$currency"},
							"vat_amount":         bson.M{"$sum": "$vat_amount"},
							"total_transactions": bson.M{"$sum": 1},
						},
					},
					{"$addFields": bson.M{"arpu": bson.M{"$divide": []string{"$gross_revenue", "$total_transactions"}}}},
				},
				"chart_gross_revenue": []bson.M{
					{
						"$group": bson.M{
							"_id":   m.groupBy,
							"label": bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"value": bson.M{"$sum": "$revenue_amount"},
						},
					},
				},
				"chart_vat": []bson.M{
					{
						"$group": bson.M{
							"_id":   m.groupBy,
							"label": bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"value": bson.M{"$sum": "$vat_amount"},
						},
					},
				},
				"chart_total_transactions": []bson.M{
					{
						"$group": bson.M{
							"_id":   m.groupBy,
							"label": bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"value": bson.M{"$sum": 1},
						},
					},
				},
				"chart_arpu": []bson.M{
					{
						"$group": bson.M{
							"_id":                m.groupBy,
							"label":              bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"gross_revenue":      bson.M{"$sum": "$revenue_amount"},
							"total_transactions": bson.M{"$sum": 1},
						},
					},
					{"$addFields": bson.M{"value": bson.M{"$divide": []string{"$gross_revenue", "$total_transactions"}}}},
					{"$project": bson.M{"label": "$label", "value": "$value"}},
				},
			},
		},
		{
			"$project": bson.M{
				"gross_revenue": bson.M{
					"amount":   bson.M{"$arrayElemAt": []interface{}{"$main.gross_revenue", 0}},
					"currency": bson.M{"$arrayElemAt": []interface{}{"$main.currency", 0}},
					"chart":    "$chart_gross_revenue",
				},
				"vat": bson.M{
					"amount":   bson.M{"$arrayElemAt": []interface{}{"$main.vat_amount", 0}},
					"currency": bson.M{"$arrayElemAt": []interface{}{"$main.currency", 0}},
					"chart":    "$chart_vat",
				},
				"total_transactions": bson.M{
					"count": bson.M{"$arrayElemAt": []interface{}{"$main.total_transactions", 0}},
					"chart": "$chart_total_transactions",
				},
				"arpu": bson.M{
					"amount":   bson.M{"$arrayElemAt": []interface{}{"$main.arpu", 0}},
					"currency": bson.M{"$arrayElemAt": []interface{}{"$main.currency", 0}},
					"chart":    "$chart_arpu",
				},
			},
		},
	}

	err := m.db.Collection(collectionOrderView).Pipe(query).One(receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return receiver, nil
}

func (m *dashboardReportProcessor) ExecuteDashboardRevenueDynamicReport(receiver interface{}) (interface{}, error) {
	query := []bson.M{
		{"$match": m.match},
		{
			"$project": bson.M{
				"day":      bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"week":     bson.M{"$week": "$pm_order_close_date"},
				"month":    bson.M{"$month": "$pm_order_close_date"},
				"amount":   "$net_revenue.amount",
				"currency": "$net_revenue.currency",
			},
		},
		{
			"$group": bson.M{
				"_id":      m.groupBy,
				"label":    bson.M{"$first": bson.M{"$toString": m.groupBy}},
				"amount":   bson.M{"$sum": "$amount"},
				"currency": bson.M{"$first": "$currency"},
				"count":    bson.M{"$sum": 1},
			},
		},
	}

	err := m.db.Collection(collectionOrderView).Pipe(query).All(&receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return receiver, nil
}

func (m *dashboardReportProcessor) ExecuteDashboardRevenueByCountryReport(receiver interface{}) (interface{}, error) {
	query := []bson.M{
		{"$match": m.match},
		{
			"$project": bson.M{
				"hour":  bson.M{"$hour": "$pm_order_close_date"},
				"day":   bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"month": bson.M{"$month": "$pm_order_close_date"},
				"week":  bson.M{"$week": "$pm_order_close_date"},
				"country": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$or": []bson.M{
								{"$eq": []interface{}{"$billing_address", nil}},
								{"$eq": []interface{}{"$billing_address.country", ""}},
							},
						},
						"$user.address.country", "$billing_address.country",
					},
				},
				"amount":   "$net_revenue.amount",
				"currency": "$net_revenue.currency",
				"period_in_day": bson.M{
					"$cond": []interface{}{
						bson.M{"$and": []bson.M{{"$gte": []interface{}{"$hour", 0}}, {"$lte": []interface{}{"$hour", 7}}}},
						"00-07",
						bson.M{"$cond": []interface{}{
							bson.M{
								"$and": []bson.M{{"$gte": []interface{}{"$hour", 8}}, {"$lte": []interface{}{"$hour", 15}}},
							}, "08-15", "16-23",
						}},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"hour":          "$hour",
				"day":           "$day",
				"month":         "$month",
				"week":          "$week",
				"country":       "$country",
				"amount":        "$amount",
				"currency":      "$currency",
				"period_in_day": bson.M{"$concat": []interface{}{bson.M{"$toString": "$day"}, " ", "$period_in_day"}},
			},
		},
		{
			"$facet": bson.M{
				"top": []bson.M{
					{
						"$group": bson.M{
							"_id":      "$country",
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
					{"$limit": baseReportsItemsLimit},
				},
				"total": []bson.M{
					{
						"$group": bson.M{
							"_id":      nil,
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
				"chart": []bson.M{
					{
						"$group": bson.M{
							"_id":      m.groupBy,
							"label":    bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"amount":   bson.M{"$sum": "$amount"},
							"currency": bson.M{"$first": "$currency"},
						},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": []interface{}{"$total", 0}},
				"chart": "$chart",
			},
		},
	}

	err := m.db.Collection(collectionOrderView).Pipe(query).One(receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return receiver, nil
}

func (m *dashboardReportProcessor) ExecuteDashboardSalesTodayReport(receiver interface{}) (interface{}, error) {
	query := []bson.M{
		{"$match": m.match},
		{
			"$project": bson.M{
				"names": bson.M{
					"$filter": bson.M{
						"input": "$project.name",
						"as":    "name",
						"cond":  bson.M{"$eq": []string{"$$name.lang", "en"}},
					},
				},
				"items": bson.M{
					"$cond": []interface{}{
						bson.M{"$ne": []interface{}{"$items", []interface{}{}}}, "$items", []string{""}},
				},
				"hour":  bson.M{"$hour": "$pm_order_close_date"},
				"day":   bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"month": bson.M{"$month": "$pm_order_close_date"},
				"week":  bson.M{"$week": "$pm_order_close_date"},
				"period_in_day": bson.M{
					"$cond": []interface{}{
						bson.M{"$and": []bson.M{{"$gte": []interface{}{"$hour", 0}}, {"$lte": []interface{}{"$hour", 7}}}},
						"00-07",
						bson.M{"$cond": []interface{}{
							bson.M{
								"$and": []bson.M{{"$gte": []interface{}{"$hour", 8}}, {"$lte": []interface{}{"$hour", 15}}},
							}, "08-15", "16-23",
						}},
					},
				},
			},
		},
		{"$unwind": "$items"},
		{
			"$project": bson.M{
				"item": bson.M{
					"$cond": []interface{}{
						bson.M{"$eq": []string{"$items", ""}},
						bson.M{"$arrayElemAt": []interface{}{"$names.value", 0}},
						"$items.name",
					},
				},
				"hour":          "$hour",
				"day":           "$day",
				"month":         "$month",
				"week":          "$week",
				"period_in_day": bson.M{"$concat": []interface{}{bson.M{"$toString": "$day"}, " ", "$period_in_day"}},
			},
		},
		{
			"$facet": bson.M{
				"top": []bson.M{
					{
						"$group": bson.M{
							"_id":   "$item",
							"name":  bson.M{"$first": "$item"},
							"count": bson.M{"$sum": 1},
						},
					},
					{"$limit": baseReportsItemsLimit},
				},
				"total": []bson.M{
					{
						"$group": bson.M{
							"_id":   nil,
							"count": bson.M{"$sum": 1},
						},
					},
				},
				"chart": []bson.M{
					{
						"$group": bson.M{
							"_id":   m.groupBy,
							"label": bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"value": bson.M{"$sum": 1},
						},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": []interface{}{"$total.count", 0}},
				"chart": "$chart",
			},
		},
	}

	err := m.db.Collection(collectionOrderView).Pipe(query).One(receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return receiver, nil
}

func (m *dashboardReportProcessor) ExecuteDashboardSourcesReport(receiver interface{}) (interface{}, error) {
	delete(m.match, "status")
	query := []bson.M{
		{"$match": m.match},
		{
			"$project": bson.M{
				"hour":  bson.M{"$hour": "$pm_order_close_date"},
				"day":   bson.M{"$dayOfMonth": "$pm_order_close_date"},
				"month": bson.M{"$month": "$pm_order_close_date"},
				"week":  bson.M{"$week": "$pm_order_close_date"},
				"period_in_day": bson.M{
					"$cond": []interface{}{
						bson.M{"$and": []bson.M{{"$gte": []interface{}{"$hour", 0}}, {"$lte": []interface{}{"$hour", 7}}}},
						"00-07",
						bson.M{"$cond": []interface{}{
							bson.M{
								"$and": []bson.M{{"$gte": []interface{}{"$hour", 8}}, {"$lte": []interface{}{"$hour", 15}}},
							}, "08-15", "16-23",
						}},
					},
				},
				"issuer": "$issuer.url",
			}},
		{
			"$project": bson.M{
				"hour":          "$hour",
				"day":           "$day",
				"month":         "$month",
				"week":          "$week",
				"period_in_day": bson.M{"$concat": []interface{}{bson.M{"$toString": "$day"}, " ", "$period_in_day"}},
				"issuer":        "$issuer",
			},
		},
		{
			"$facet": bson.M{
				"top": []bson.M{
					{
						"$group": bson.M{
							"_id":   "$issuer",
							"name":  bson.M{"$first": "$issuer"},
							"count": bson.M{"$sum": 1},
						},
					},
					{"$limit": baseReportsItemsLimit},
				},
				"total": []bson.M{
					{
						"$group": bson.M{
							"_id":   nil,
							"count": bson.M{"$sum": 1},
						},
					},
				},
				"chart": []bson.M{
					{
						"$group": bson.M{
							"_id":   m.groupBy,
							"label": bson.M{"$first": bson.M{"$toString": m.groupBy}},
							"value": bson.M{"$sum": 1},
						},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": []interface{}{"$total.count", 0}},
				"chart": "$chart",
			},
		},
	}

	err := m.db.Collection(collectionOrderView).Pipe(query).One(receiver)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return nil, dashboardErrorUnknown
	}

	return receiver, nil
}
