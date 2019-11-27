package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v1"
	"time"
)

const (
	dashboardMainGrossRevenueAndVatCacheKey       = "dashboard:main:gross_revenue_and_vat:%x"
	dashboardMainTotalTransactionsAndArpuCacheKey = "dashboard:main:total_transactions_and_arpu:%x"
	dashboardRevenueDynamicCacheKey               = "dashboard:revenue_dynamic:%x"
	dashboardBaseRevenueByCountryCacheKey         = "dashboard:base:revenue_by_country:%x"
	dashboardBaseSalesTodayCacheKey               = "dashboard:base:sales_today:%x"
	dashboardBaseSourcesCacheKey                  = "dashboard:base:sources:%x"

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
	GetMainReport(context.Context, string, string) (*grpc.DashboardMainReport, error)
	GetRevenueDynamicsReport(context.Context, string, string) (*grpc.DashboardRevenueDynamicReport, error)
	GetBaseReport(context.Context, string, string) (*grpc.DashboardBaseReports, error)
	GetBaseRevenueByCountryReport(context.Context, string, string) (*grpc.DashboardRevenueByCountryReport, error)
	GetBaseSalesTodayReport(context.Context, string, string) (*grpc.DashboardSalesTodayReport, error)
	GetBaseSourcesReport(context.Context, string, string) (*grpc.DashboardSourcesReport, error)
}

type DashboardReportProcessorInterface interface {
	ExecuteReport(interface{}) (interface{}, error)
	ExecuteGrossRevenueAndVatReports(interface{}) (interface{}, error)
	ExecuteTotalTransactionsAndArpuReports(interface{}) (interface{}, error)
	ExecuteRevenueDynamicReport(interface{}) (interface{}, error)
	ExecuteRevenueByCountryReport(interface{}) (interface{}, error)
	ExecuteSalesTodayReport(interface{}) (interface{}, error)
	ExecuteSourcesReport(interface{}) (interface{}, error)
}

type GrossRevenueAndVatReports struct {
	GrossRevenue *grpc.DashboardAmountItemWithChart `bson:"gross_revenue"`
	Vat          *grpc.DashboardAmountItemWithChart `bson:"vat"`
}

type TotalTransactionsAndArpuReports struct {
	TotalTransactions *grpc.DashboardMainReportTotalTransactions `bson:"total_transactions"`
	Arpu              *grpc.DashboardAmountItemWithChart         `bson:"arpu"`
}

func newDashboardRepository(s *Service) DashboardRepositoryInterface {
	return &DashboardRepository{svc: s}
}

func (m *DashboardRepository) GetMainReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardMainReport, error) {
	processorGrossRevenueAndVatCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardMainGrossRevenueAndVatCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorGrossRevenueAndVatCurrent.DbQueryFn = processorGrossRevenueAndVatCurrent.ExecuteGrossRevenueAndVatReports
	dataGrossRevenueAndVatCurrent, err := processorGrossRevenueAndVatCurrent.ExecuteReport(ctx, new(GrossRevenueAndVatReports))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorGrossRevenueAndVatPrevious, err := m.NewDashboardReportProcessor(
		merchantId,
		dashboardReportBasePreviousPeriodsNames[period],
		dashboardMainGrossRevenueAndVatCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorGrossRevenueAndVatPrevious.DbQueryFn = processorGrossRevenueAndVatPrevious.ExecuteGrossRevenueAndVatReports
	dataGrossRevenueAndVatPrevious, err := processorGrossRevenueAndVatPrevious.ExecuteReport(ctx, new(GrossRevenueAndVatReports))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorTotalTransactionsAndArpuCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardMainTotalTransactionsAndArpuCacheKey,
		bson.M{"$in": []string{"processed", "refunded", "chargeback"}},
		m.svc.db,
		m.svc.cacher,
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorTotalTransactionsAndArpuCurrent.DbQueryFn = processorTotalTransactionsAndArpuCurrent.ExecuteTotalTransactionsAndArpuReports
	dataTotalTransactionsAndArpuCurrent, err := processorTotalTransactionsAndArpuCurrent.ExecuteReport(ctx, new(TotalTransactionsAndArpuReports))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorTotalTransactionsAndArpuPrevious, err := m.NewDashboardReportProcessor(
		merchantId,
		dashboardReportBasePreviousPeriodsNames[period],
		dashboardMainTotalTransactionsAndArpuCacheKey,
		bson.M{"$in": []string{"processed", "refunded", "chargeback"}},
		m.svc.db,
		m.svc.cacher,
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorTotalTransactionsAndArpuPrevious.DbQueryFn = processorTotalTransactionsAndArpuPrevious.ExecuteTotalTransactionsAndArpuReports
	dataTotalTransactionsAndArpuPrevious, err := processorTotalTransactionsAndArpuPrevious.ExecuteReport(ctx, new(TotalTransactionsAndArpuReports))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataGrossRevenueAndVatCurrentTyped := dataGrossRevenueAndVatCurrent.(*GrossRevenueAndVatReports)
	dataGrossRevenueAndVatPreviousTyped := dataGrossRevenueAndVatPrevious.(*GrossRevenueAndVatReports)

	if dataGrossRevenueAndVatPreviousTyped == nil {
		dataGrossRevenueAndVatPreviousTyped = &GrossRevenueAndVatReports{
			GrossRevenue: &grpc.DashboardAmountItemWithChart{},
			Vat:          &grpc.DashboardAmountItemWithChart{},
		}
	}
	dataGrossRevenueAndVatCurrentTyped.GrossRevenue.AmountPrevious = dataGrossRevenueAndVatPreviousTyped.GrossRevenue.AmountCurrent
	dataGrossRevenueAndVatCurrentTyped.Vat.AmountPrevious = dataGrossRevenueAndVatPreviousTyped.Vat.AmountCurrent

	dataTotalTransactionsAndArpuCurrentTyped := dataTotalTransactionsAndArpuCurrent.(*TotalTransactionsAndArpuReports)
	dataTotalTransactionsAndArpuPreviousTyped := dataTotalTransactionsAndArpuPrevious.(*TotalTransactionsAndArpuReports)

	if dataTotalTransactionsAndArpuPreviousTyped == nil {
		dataTotalTransactionsAndArpuPreviousTyped = &TotalTransactionsAndArpuReports{
			TotalTransactions: &grpc.DashboardMainReportTotalTransactions{},
			Arpu:              &grpc.DashboardAmountItemWithChart{},
		}
	}
	dataTotalTransactionsAndArpuCurrentTyped.TotalTransactions.CountPrevious = dataTotalTransactionsAndArpuPreviousTyped.TotalTransactions.CountCurrent
	dataTotalTransactionsAndArpuCurrentTyped.Arpu.AmountPrevious = dataTotalTransactionsAndArpuPreviousTyped.Arpu.AmountCurrent

	result := &grpc.DashboardMainReport{
		GrossRevenue:      dataGrossRevenueAndVatCurrentTyped.GrossRevenue,
		Vat:               dataGrossRevenueAndVatCurrentTyped.Vat,
		TotalTransactions: dataTotalTransactionsAndArpuCurrentTyped.TotalTransactions,
		Arpu:              dataTotalTransactionsAndArpuCurrentTyped.Arpu,
	}

	return result, nil
}

func (m *DashboardRepository) GetRevenueDynamicsReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardRevenueDynamicReport, error) {
	processor, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardRevenueDynamicCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processor.DbQueryFn = processor.ExecuteRevenueDynamicReport
	data, err := processor.ExecuteReport(ctx, new(grpc.DashboardRevenueDynamicReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataTyped := data.(*grpc.DashboardRevenueDynamicReport)

	if len(dataTyped.Items) > 0 {
		dataTyped.Currency = dataTyped.Items[0].Currency
	}

	return dataTyped, nil
}

func (m *DashboardRepository) GetBaseReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardBaseReports, error) {
	revenueByCountryReport, err := m.GetBaseRevenueByCountryReport(ctx, merchantId, period)

	if err != nil {
		return nil, err
	}

	salesTodayReport, err := m.GetBaseSalesTodayReport(ctx, merchantId, period)

	if err != nil {
		return nil, err
	}

	sourcesReport, err := m.GetBaseSourcesReport(ctx, merchantId, period)

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

func (m *DashboardRepository) GetBaseRevenueByCountryReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardRevenueByCountryReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseRevenueByCountryCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
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
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.DbQueryFn = processorCurrent.ExecuteRevenueByCountryReport
	processorPrevious.DbQueryFn = processorPrevious.ExecuteRevenueByCountryReport
	dataCurrent, err := processorCurrent.ExecuteReport(ctx, new(grpc.DashboardRevenueByCountryReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(ctx, new(grpc.DashboardRevenueByCountryReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataCurrentTyped := dataCurrent.(*grpc.DashboardRevenueByCountryReport)

	if dataCurrentTyped == nil {
		dataCurrentTyped = &grpc.DashboardRevenueByCountryReport{}
	}

	dataPreviousTyped := dataPrevious.(*grpc.DashboardRevenueByCountryReport)

	if dataPreviousTyped == nil {
		dataPreviousTyped = &grpc.DashboardRevenueByCountryReport{}
	}

	dataCurrentTyped.TotalPrevious = dataPreviousTyped.TotalCurrent

	return dataCurrentTyped, nil
}

func (m *DashboardRepository) GetBaseSalesTodayReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardSalesTodayReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseSalesTodayCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
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
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.DbQueryFn = processorCurrent.ExecuteSalesTodayReport
	processorPrevious.DbQueryFn = processorPrevious.ExecuteSalesTodayReport
	dataCurrent, err := processorCurrent.ExecuteReport(ctx, new(grpc.DashboardSalesTodayReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(ctx, new(grpc.DashboardSalesTodayReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataCurrentTyped := dataCurrent.(*grpc.DashboardSalesTodayReport)
	dataPreviousTyped := dataPrevious.(*grpc.DashboardSalesTodayReport)
	dataCurrentTyped.TotalPrevious = dataPreviousTyped.TotalCurrent

	return dataCurrentTyped, nil
}

func (m *DashboardRepository) GetBaseSourcesReport(
	ctx context.Context,
	merchantId, period string,
) (*grpc.DashboardSourcesReport, error) {
	processorCurrent, err := m.NewDashboardReportProcessor(
		merchantId,
		period,
		dashboardBaseSourcesCacheKey,
		"processed",
		m.svc.db,
		m.svc.cacher,
		ctx,
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
		ctx,
	)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processorCurrent.DbQueryFn = processorCurrent.ExecuteSourcesReport
	processorPrevious.DbQueryFn = processorPrevious.ExecuteSourcesReport
	dataCurrent, err := processorCurrent.ExecuteReport(ctx, new(grpc.DashboardSourcesReport))

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	dataPrevious, err := processorPrevious.ExecuteReport(ctx, new(grpc.DashboardSourcesReport))

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
	cache internalPkg.CacheInterface,
	ctx context.Context,
) (*internalPkg.DashboardReportProcessor, error) {
	current := time.Now()
	merchantOid, err := primitive.ObjectIDFromHex(merchantId)

	if err != nil {
		return nil, dashboardErrorUnknown
	}

	processor := &internalPkg.DashboardReportProcessor{
		Match:       bson.M{"merchant_id": merchantOid, "status": status, "type": "order"},
		Db:          db,
		Collection:  collectionOrderView,
		Cache:       cache,
		CacheExpire: time.Duration(0),
		Errors: map[string]*grpc.ResponseErrorMessage{
			"unknown": dashboardErrorUnknown,
		},
	}

	switch period {
	case pkg.DashboardPeriodCurrentDay:
		processor.GroupBy = dashboardReportGroupByHour
		gte := now.BeginningOfDay()
		lte := now.EndOfDay()
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousDay, pkg.DashboardPeriodTwoDaysAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoDaysAgo {
			decrement = decrement * 2
		}
		previousDay := time.Now().AddDate(0, 0, decrement)
		gte := now.New(previousDay).BeginningOfDay()
		lte := now.New(previousDay).EndOfDay()

		processor.GroupBy = dashboardReportGroupByHour
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.CacheExpire = now.New(current).EndOfDay().Sub(current)
		break
	case pkg.DashboardPeriodCurrentWeek:
		processor.GroupBy = dashboardReportGroupByPeriodInDay
		gte := now.BeginningOfWeek()
		lte := now.EndOfWeek()
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousWeek, pkg.DashboardPeriodTwoWeeksAgo:
		decrement := -7
		if period == pkg.DashboardPeriodTwoWeeksAgo {
			decrement = decrement * 2
		}
		previousWeek := time.Now().AddDate(0, 0, decrement)
		gte := now.New(previousWeek).BeginningOfWeek()
		lte := now.New(previousWeek).EndOfWeek()

		processor.GroupBy = dashboardReportGroupByPeriodInDay
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.CacheExpire = now.New(current).EndOfWeek().Sub(current)
		break
	case pkg.DashboardPeriodCurrentMonth:
		gte := now.BeginningOfMonth()
		lte := now.EndOfMonth()

		processor.GroupBy = dashboardReportGroupByDay
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousMonth, pkg.DashboardPeriodTwoMonthsAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoMonthsAgo {
			decrement = decrement * 2
		}
		previousMonth := now.New(time.Now()).BeginningOfMonth().AddDate(0, decrement, 0)
		gte := now.New(previousMonth).BeginningOfMonth()
		lte := now.New(previousMonth).EndOfMonth()

		processor.GroupBy = dashboardReportGroupByDay
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}

		processor.CacheExpire = now.New(current).EndOfMonth().Sub(current)
		break
	case pkg.DashboardPeriodCurrentQuarter:
		gte := now.BeginningOfQuarter()
		lte := now.EndOfQuarter()
		processor.GroupBy = dashboardReportGroupByWeek
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousQuarter, pkg.DashboardPeriodTwoQuarterAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoQuarterAgo {
			decrement = decrement * 4
		}
		previousQuarter := now.BeginningOfQuarter().AddDate(0, decrement, 0)
		gte := now.New(previousQuarter).BeginningOfQuarter()
		lte := now.New(previousQuarter).EndOfQuarter()

		processor.GroupBy = dashboardReportGroupByWeek
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}

		processor.CacheExpire = now.New(current).EndOfQuarter().Sub(current)
		break
	case pkg.DashboardPeriodCurrentYear:
		gte := now.BeginningOfYear()
		lte := now.EndOfYear()

		processor.GroupBy = dashboardReportGroupByMonth
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		break
	case pkg.DashboardPeriodPreviousYear, pkg.DashboardPeriodTwoYearsAgo:
		decrement := -1
		if period == pkg.DashboardPeriodTwoYearsAgo {
			decrement = decrement * 2
		}
		previousYear := time.Now().AddDate(decrement, 0, 0)
		gte := now.New(previousYear).BeginningOfYear()
		lte := now.New(previousYear).EndOfYear()

		processor.GroupBy = dashboardReportGroupByMonth
		processor.Match["pm_order_close_date"] = bson.M{"$gte": gte, "$lte": lte}
		processor.CacheExpire = now.New(current).EndOfYear().Sub(current)
		break
	default:
		return nil, dashboardErrorIncorrectPeriod
	}

	if processor.CacheExpire > 0 {
		b, err := json.Marshal(processor.Match)

		if err != nil {
			zap.L().Error(
				"Generate dashboard report cache key failed",
				zap.Error(err),
				zap.Any(pkg.ErrorDatabaseFieldQuery, processor.Match),
			)

			return nil, err
		}

		processor.CacheKey = fmt.Sprintf(cacheKeyMask, md5.Sum(b))
	}

	return processor, nil
}
