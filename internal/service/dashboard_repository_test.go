package service

//1. Тесты GetMainReport:
//1.1. Ошибка формирования процессора (processor)
//1.2. Ошибка выполнения запроса к БД
//1.3. Ошибка - не известный период
//1.4. Ошибка - не сформировался ключ кэша
//1.5. Получение отчета из кэша
//1.6. Ошибка выполнения запроса на запись в редис

//2. Тесты GetRevenueDynamicsReport:
//2.1. Ошибка формирования процессора (processor)
//2.2. Ошибка выполнения запроса к БД
//2.3. Успешное получение данных за предыдущий месяц
//2.4. Успешное получение данных за текущий квартал
//2.5. Успешное получение данных за предыдущий квартал
//2.6. Успешное получение данных за текущий год
//2.7. Успешное получение данных за предыдущий год
//2.8. Ошибка - не известный период
//2.9. Ошибка - не сформировался ключ кэша
//2.10. Получение отчета из кэша
//2.11. Ошибка выполнения запроса на запись в редис

//3. Тесты GetBaseReport:
//3.1. Ошибка получения результата выборки данных по отчету "Revenue by country" из БД
//3.2. Ошибка получения результата выборки данных по отчету "Sales today" из БД
//3.3. Ошибка получения результата выборки данных по отчету "Sources" из БД
//3.4. Успешное получение данных за текущий день
//3.5. Успешное получение данных за предыдущий день
//3.6. Успешное получение данных за текущую неделю
//3.7. Успешное получение данных за предыдущую неделю
//3.8. Успешное получение данных за предыдущий месяц
//3.8. Успешное получение данных за текущий квартал
//3.9. Успешное получение данных за предыдущий квартал
//3.10. Успешное получение данных за текущий год
//3.11. Успешное получение данных за предыдущий год
//3.12. Ошибка - не известный период
//3.13. Ошибка - не сформировался ключ кэша
//3.14. Получение отчета из кэша
//3.15. Ошибка выполнения запроса на запись в редис для отчета "Revenue by country"
//3.16. Ошибка выполнения запроса на запись в редис для отчета "Sales today"
//3.17. Ошибка выполнения запроса на запись в редис для отчета "Sources"

//4. Тесты GetBaseRevenueByCountryReport:
//4.1. Ошибка формирования процессора для текущего периода
//4.2. Ошибка формирования процессора для предыдущего периода
//4.3. Ошибка получения результата выборки за текущий период
//4.4. Ошибка получения результата выборки за предыдущий период

//5. Тесты GetBaseSalesTodayReport:
//5.1. Ошибка формирования процессора для текущего периода
//5.2. Ошибка формирования процессора для предыдущего периода
//5.3. Ошибка получения результата выборки за текущий период
//5.4. Ошибка получения результата выборки за предыдущий период

//6. Тесты GetBaseSourcesReport:
//6.1. Ошибка формирования процессора для текущего периода
//6.2. Ошибка формирования процессора для предыдущего периода
//6.3. Ошибка получения результата выборки за текущий период
//6.4. Ошибка получения результата выборки за предыдущий период

import (
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jinzhu/now"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"math/rand"
	"testing"
	"time"
)

type DashboardRepositoryTestSuite struct {
	suite.Suite
	service *Service
	cache   CacheInterface
	log     *zap.Logger

	project       *billing.Project
	merchant      *billing.Merchant
	paymentMethod *billing.PaymentMethod

	products    []*grpc.Product
	keyProducts []*grpc.KeyProduct
}

func Test_DashboardRepository(t *testing.T) {
	suite.Run(t, new(DashboardRepositoryTestSuite))
}

func (suite *DashboardRepositoryTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"
	cfg.CardPayApiUrl = "https://sandbox.cardpay.com"

	m, err := migrate.New(
		"file://../../migrations/tests",
		cfg.MongoDsn)
	assert.NoError(suite.T(), err, "Migrate init failed")

	err = m.Up()
	if err != nil && err.Error() != "no change" {
		suite.FailNow("Migrations failed", "%v", err)
	}

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mocks.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(db, cfg, mocks.NewGeoIpServiceTestOk(), mocks.NewRepositoryServiceOk(), mocks.NewTaxServiceOkMock(), mocks.NewBrokerMockOk(), nil, suite.cache, mocks.NewCurrencyServiceMockOk(), mocks.NewDocumentSignerMockOk(), &reportingMocks.ReporterService{}, mocks.NewFormatterOK(), )

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	suite.merchant, suite.project, suite.paymentMethod, _ = helperCreateEntitiesForTests(suite.Suite, suite.service)
	suite.products = createProductsForProject(suite.Suite, suite.service, suite.project, 3)
	suite.keyProducts = createKeyProductsFroProject(suite.Suite, suite.service, suite.project, 3)
}

func (suite *DashboardRepositoryTestSuite) TearDownTest() {
	suite.cache.Clean()
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}
	suite.service.db.Close()
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_EmptyResult_Ok() {
	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.Zero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.Currency)
	assert.Empty(suite.T(), report.GrossRevenue.Chart)
	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.Zero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Empty(suite.T(), report.TotalTransactions.Chart)
	assert.NotNil(suite.T(), report.Vat)
	assert.Zero(suite.T(), report.Vat.Currency)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Zero(suite.T(), report.Vat.AmountCurrent)
	assert.Empty(suite.T(), report.Vat.Chart)
	assert.NotNil(suite.T(), report.Arpu)
	assert.Zero(suite.T(), report.Arpu.Currency)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Zero(suite.T(), report.Arpu.AmountCurrent)
	assert.Empty(suite.T(), report.Arpu.Chart)
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_CurrentMonth_Ok() {
	current := time.Now()
	monthBeginning := now.BeginningOfMonth()
	iterations := (current.Day() - monthBeginning.Day()) + 1
	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodCurrentMonth, monthBeginning)

	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.NotZero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.NotZero(suite.T(), report.GrossRevenue.Currency)
	assert.NotNil(suite.T(), report.GrossRevenue.Chart)
	assert.Len(suite.T(), report.GrossRevenue.Chart, iterations)

	for _, v := range report.GrossRevenue.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.NotZero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.NotNil(suite.T(), report.TotalTransactions.Chart)
	assert.Len(suite.T(), report.TotalTransactions.Chart, iterations)

	for _, v := range report.TotalTransactions.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Vat)
	assert.NotZero(suite.T(), report.Vat.Currency)
	assert.NotZero(suite.T(), report.Vat.AmountCurrent)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Len(suite.T(), report.Vat.Chart, iterations)

	for _, v := range report.Vat.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Arpu)
	assert.NotZero(suite.T(), report.Arpu.Currency)
	assert.NotZero(suite.T(), report.Arpu.AmountCurrent)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Len(suite.T(), report.Arpu.Chart, iterations)

	for _, v := range report.Arpu.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_PreviousMonth_Ok() {
	monthBeginning := now.BeginningOfMonth().AddDate(0, -1, 0)
	monthEnd := now.New(monthBeginning).EndOfMonth()
	iterations := (monthEnd.Day() - monthBeginning.Day()) + 1
	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodPreviousMonth, monthBeginning)

	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodPreviousMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.NotZero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.NotZero(suite.T(), report.GrossRevenue.Currency)
	assert.NotNil(suite.T(), report.GrossRevenue.Chart)
	assert.Len(suite.T(), report.GrossRevenue.Chart, iterations)

	for _, v := range report.GrossRevenue.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.NotZero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.NotNil(suite.T(), report.TotalTransactions.Chart)
	assert.Len(suite.T(), report.TotalTransactions.Chart, iterations)

	for _, v := range report.TotalTransactions.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Vat)
	assert.NotZero(suite.T(), report.Vat.Currency)
	assert.NotZero(suite.T(), report.Vat.AmountCurrent)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Len(suite.T(), report.Vat.Chart, iterations)

	for _, v := range report.Vat.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Arpu)
	assert.NotZero(suite.T(), report.Arpu.Currency)
	assert.NotZero(suite.T(), report.Arpu.AmountCurrent)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Len(suite.T(), report.Arpu.Chart, iterations)

	for _, v := range report.Arpu.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_CurrentQuarter_Ok() {
	current := time.Now()
	monthBeginning := now.BeginningOfQuarter()
	iterations := int((current.Unix() - monthBeginning.Unix()) / 604800)

	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodCurrentQuarter, monthBeginning)
	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentQuarter)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.NotZero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.NotZero(suite.T(), report.GrossRevenue.Currency)
	assert.NotNil(suite.T(), report.GrossRevenue.Chart)
	assert.Len(suite.T(), report.GrossRevenue.Chart, iterations)

	for _, v := range report.GrossRevenue.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.NotZero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.NotNil(suite.T(), report.TotalTransactions.Chart)
	assert.Len(suite.T(), report.TotalTransactions.Chart, iterations)

	for _, v := range report.TotalTransactions.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Vat)
	assert.NotZero(suite.T(), report.Vat.Currency)
	assert.NotZero(suite.T(), report.Vat.AmountCurrent)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Len(suite.T(), report.Vat.Chart, iterations)

	for _, v := range report.Vat.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Arpu)
	assert.NotZero(suite.T(), report.Arpu.Currency)
	assert.NotZero(suite.T(), report.Arpu.AmountCurrent)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Len(suite.T(), report.Arpu.Chart, iterations)

	for _, v := range report.Arpu.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_PreviousQuarter_Ok() {
	previousQuarter := now.BeginningOfQuarter().AddDate(0, -1, 0)
	quarterBeginning := now.New(previousQuarter).BeginningOfQuarter()
	iterations := 3

	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodPreviousQuarter, quarterBeginning)
	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodPreviousQuarter)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.NotZero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.NotZero(suite.T(), report.GrossRevenue.Currency)
	assert.NotNil(suite.T(), report.GrossRevenue.Chart)
	assert.Len(suite.T(), report.GrossRevenue.Chart, iterations)

	for _, v := range report.GrossRevenue.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.NotZero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.NotNil(suite.T(), report.TotalTransactions.Chart)
	assert.Len(suite.T(), report.TotalTransactions.Chart, iterations)

	for _, v := range report.TotalTransactions.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Vat)
	assert.NotZero(suite.T(), report.Vat.Currency)
	assert.NotZero(suite.T(), report.Vat.AmountCurrent)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Len(suite.T(), report.Vat.Chart, iterations)

	for _, v := range report.Vat.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Arpu)
	assert.NotZero(suite.T(), report.Arpu.Currency)
	assert.NotZero(suite.T(), report.Arpu.AmountCurrent)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Len(suite.T(), report.Arpu.Chart, iterations)

	for _, v := range report.Arpu.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardMainReport_CurrentYear_Ok() {
	current := time.Now()
	beginning := now.BeginningOfYear()
	iterations := int((current.Month() - beginning.Month()) + 1)
	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodCurrentYear, beginning)

	report, err := suite.service.dashboardRepository.GetMainReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentYear)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.GrossRevenue)
	assert.NotZero(suite.T(), report.GrossRevenue.AmountCurrent)
	assert.Zero(suite.T(), report.GrossRevenue.AmountPrevious)
	assert.NotZero(suite.T(), report.GrossRevenue.Currency)
	assert.NotNil(suite.T(), report.GrossRevenue.Chart)
	assert.Len(suite.T(), report.GrossRevenue.Chart, iterations)

	for _, v := range report.GrossRevenue.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.TotalTransactions)
	assert.NotZero(suite.T(), report.TotalTransactions.CountCurrent)
	assert.Zero(suite.T(), report.TotalTransactions.CountPrevious)
	assert.NotNil(suite.T(), report.TotalTransactions.Chart)
	assert.Len(suite.T(), report.TotalTransactions.Chart, iterations)

	for _, v := range report.TotalTransactions.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Vat)
	assert.NotZero(suite.T(), report.Vat.Currency)
	assert.NotZero(suite.T(), report.Vat.AmountCurrent)
	assert.Zero(suite.T(), report.Vat.AmountPrevious)
	assert.Len(suite.T(), report.Vat.Chart, iterations)

	for _, v := range report.Vat.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}

	assert.NotNil(suite.T(), report.Arpu)
	assert.NotZero(suite.T(), report.Arpu.Currency)
	assert.NotZero(suite.T(), report.Arpu.AmountCurrent)
	assert.Zero(suite.T(), report.Arpu.AmountPrevious)
	assert.Len(suite.T(), report.Arpu.Chart, iterations)

	for _, v := range report.Arpu.Chart {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Value)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardRevenueDynamicsReport_CurrentMonth_Ok() {
	current := time.Now()
	monthBeginning := now.BeginningOfMonth()
	iterations := (current.Day() - monthBeginning.Day()) + 1
	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodCurrentMonth, monthBeginning)

	report, err := suite.service.dashboardRepository.GetRevenueDynamicsReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.Len(suite.T(), report.Items, iterations)
	assert.NotZero(suite.T(), report.Currency)

	for _, v := range report.Items {
		assert.NotZero(suite.T(), v.Label)
		assert.NotZero(suite.T(), v.Amount)
		assert.NotZero(suite.T(), v.Count)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardRevenueDynamicsReport_EmptyResult_Ok() {
	report, err := suite.service.dashboardRepository.GetRevenueDynamicsReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), report)
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardBaseReport_CurrentMonth_Ok() {
	current := time.Now()
	monthBeginning := now.BeginningOfMonth()
	iterations := (current.Day() - monthBeginning.Day()) + 1
	suite.createOrdersForPeriod(iterations, pkg.DashboardPeriodCurrentMonth, monthBeginning)
	report, err := suite.service.dashboardRepository.GetBaseReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)

	assert.NotNil(suite.T(), report.RevenueByCountry)
	assert.Len(suite.T(), report.RevenueByCountry.Top, 1)
	assert.NotZero(suite.T(), report.RevenueByCountry.Currency)
	assert.NotZero(suite.T(), report.RevenueByCountry.TotalCurrent)
	assert.Zero(suite.T(), report.RevenueByCountry.TotalPrevious)
	assert.NotEmpty(suite.T(), report.RevenueByCountry.Chart)
	assert.Len(suite.T(), report.RevenueByCountry.Chart, iterations)

	for _, v := range report.RevenueByCountry.Top {
		assert.NotZero(suite.T(), v.Amount)
		assert.NotZero(suite.T(), v.Country)
	}

	for _, v := range report.RevenueByCountry.Chart {
		assert.NotZero(suite.T(), v.Amount)
		assert.NotZero(suite.T(), v.Label)
	}

	assert.NotNil(suite.T(), report.Sources)
	assert.Len(suite.T(), report.Sources.Top, 1)
	assert.NotZero(suite.T(), report.Sources.TotalCurrent)
	assert.Zero(suite.T(), report.Sources.TotalPrevious)
	assert.Len(suite.T(), report.Sources.Chart, iterations)

	for _, v := range report.Sources.Top {
		assert.NotZero(suite.T(), v.Count)
		assert.NotZero(suite.T(), v.Name)
	}

	for _, v := range report.Sources.Chart {
		assert.NotZero(suite.T(), v.Value)
		assert.NotZero(suite.T(), v.Label)
	}

	assert.NotNil(suite.T(), report.SalesToday)
	assert.Len(suite.T(), report.SalesToday.Top, 1)
	assert.NotZero(suite.T(), report.SalesToday.TotalCurrent)
	assert.Zero(suite.T(), report.SalesToday.TotalPrevious)
	assert.Len(suite.T(), report.SalesToday.Chart, iterations)

	for _, v := range report.SalesToday.Top {
		assert.NotZero(suite.T(), v.Count)
		assert.NotZero(suite.T(), v.Name)
	}

	for _, v := range report.SalesToday.Chart {
		assert.NotZero(suite.T(), v.Value)
		assert.NotZero(suite.T(), v.Label)
	}
}

func (suite *DashboardRepositoryTestSuite) Test_GetDashboardBaseReport_EmptyResult_Ok() {
	report, err := suite.service.dashboardRepository.GetBaseReport(suite.project.MerchantId, pkg.DashboardPeriodCurrentMonth)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), report)
	assert.NotNil(suite.T(), report.SalesToday)
	assert.Empty(suite.T(), report.SalesToday.Top)
	assert.Zero(suite.T(), report.SalesToday.TotalCurrent)
	assert.Zero(suite.T(), report.SalesToday.TotalPrevious)
	assert.Empty(suite.T(), report.SalesToday.Chart)
	assert.NotNil(suite.T(), report.RevenueByCountry)
	assert.Empty(suite.T(), report.RevenueByCountry.Top)
	assert.Zero(suite.T(), report.RevenueByCountry.TotalCurrent)
	assert.Zero(suite.T(), report.RevenueByCountry.TotalPrevious)
	assert.Empty(suite.T(), report.RevenueByCountry.Chart)
	assert.NotNil(suite.T(), report.Sources)
	assert.Empty(suite.T(), report.Sources.Top)
	assert.Zero(suite.T(), report.Sources.TotalCurrent)
	assert.Zero(suite.T(), report.Sources.TotalPrevious)
	assert.Empty(suite.T(), report.Sources.Chart)
}

func (suite *DashboardRepositoryTestSuite) createOrdersForPeriod(
	iterations int,
	periodType string,
	periodStart time.Time,
) {
	amountMin := 100
	amountMax := 1000
	ordersCountMin := 1
	ordersCountMax := 10
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < iterations; i++ {
		rnd := rand.Intn(ordersCountMax-ordersCountMin+1) + ordersCountMin
		amount := float64(rand.Intn(amountMax-amountMin+1) + amountMin)
		date := now.New(periodStart).Time

		if i > 0 {
			if periodType == pkg.DashboardPeriodCurrentMonth || periodType == pkg.DashboardPeriodPreviousMonth {
				date = now.New(periodStart).AddDate(0, 0, i)
			}

			if periodType == pkg.DashboardPeriodCurrentQuarter || periodType == pkg.DashboardPeriodPreviousQuarter {
				date = now.New(periodStart).AddDate(0, 0, i*7)
			}

			if periodType == pkg.DashboardPeriodCurrentYear || periodType == pkg.DashboardPeriodPreviousYear {
				date = now.New(periodStart).AddDate(0, i, 0)
			}
		}

		for j := 0; j < rnd; j++ {
			helperCreateAndPayOrder2(suite.Suite, suite.service, amount, "USD", "RU", suite.project, suite.paymentMethod, date, nil, nil, "http://127.0.0.1")
		}
	}
}
