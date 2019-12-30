package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"testing"
	"time"
)

type CountryTestSuite struct {
	suite.Suite
	db         mongodb.SourceInterface
	repository *countryRepository
	log        *zap.Logger
}

func Test_Country(t *testing.T) {
	suite.Run(t, new(CountryTestSuite))
}

func (suite *CountryTestSuite) SetupTest() {
	_, err := config.NewConfig()
	assert.NoError(suite.T(), err, "Config load failed")

	suite.log, err = zap.NewProduction()
	assert.NoError(suite.T(), err, "Logger initialization failed")

	suite.db, err = mongodb.NewDatabase()
	assert.NoError(suite.T(), err, "Database connection failed")

	suite.repository = &countryRepository{db: suite.db, cache: &mocks.CacheInterface{}}
}

func (suite *CountryTestSuite) TearDownTest() {
	if err := suite.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	if err := suite.db.Close(); err != nil {
		suite.FailNow("Database close failed", "%v", err)
	}
}

func (suite *CountryTestSuite) TestCountry_NewCountryRepository_Ok() {
	repository := NewCountryRepository(suite.db, &mocks.CacheInterface{})
	assert.IsType(suite.T(), &countryRepository{}, repository)
}

func (suite *CountryTestSuite) TestCountry_Insert_Ok() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), country.Id, country2.Id)
	assert.Equal(suite.T(), country.IsoCodeA2, country2.IsoCodeA2)
}

func (suite *CountryTestSuite) TestCountry_Insert_ErrorDb() {
	country := suite.getCountryTemplate()
	country.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err := suite.repository.Insert(context.TODO(), country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_Insert_ErrorCache() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_MultipleInsert_Ok() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	country.VatThreshold = nil
	list := []*billing.Country{country}
	err := suite.repository.MultipleInsert(context.TODO(), list)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), country.Id, country2.Id)
	assert.Equal(suite.T(), country.IsoCodeA2, country2.IsoCodeA2)
}

func (suite *CountryTestSuite) TestCountry_MultipleInsert_ErrorDb() {
	country := suite.getCountryTemplate()
	country.CreatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	list := []*billing.Country{country}
	err := suite.repository.MultipleInsert(context.TODO(), list)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_MultipleInsert_ErrorCache() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(errors.New("error"))
	suite.repository.cache = cache

	list := []*billing.Country{country}
	err := suite.repository.MultipleInsert(context.TODO(), list)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_Update_Ok() {
	country := suite.getCountryTemplate()
	updatedCode := "UA"

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Times(1).Return(nil)
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, updatedCode), mock.Anything, time.Duration(0)).Times(2).Return(nil)
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, updatedCode), mock.Anything, time.Duration(0)).Times(3).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, updatedCode), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country.IsoCodeA2 = updatedCode
	err = suite.repository.Update(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), country.Id, country2.Id)
}

func (suite *CountryTestSuite) TestCountry_Update_ErrorInvalidId() {
	country := suite.getCountryTemplate()
	country.Id = "id"
	err := suite.repository.Update(context.TODO(), country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_Update_ErrorDb() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country.UpdatedAt = &timestamp.Timestamp{Seconds: -100000000000000}
	err = suite.repository.Update(context.TODO(), country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_Update_ErrorCache() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	err = suite.repository.Update(context.TODO(), country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_GetByIsoCodeA2_NotFound() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.Id), mock.Anything).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.Id)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), country2)
}

func (suite *CountryTestSuite) TestCountry_GetByIsoCodeA2_SkipGetByCacheError() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), billing.Country{}).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Times(2).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), country.Id, country2.Id)
}

func (suite *CountryTestSuite) TestCountry_GetByIsoCodeA2_ReturnByCache() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), billing.Country{}).Return(nil)
	suite.repository.cache = cache

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.Country{}, country2)
}

func (suite *CountryTestSuite) TestCountry_GetByIsoCodeA2_SkipSetToCacheError() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), billing.Country{}).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), mock.Anything, time.Duration(0)).Times(2).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	country2, err := suite.repository.GetByIsoCodeA2(context.TODO(), country.IsoCodeA2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), country.Id, country2.Id)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_ReturnByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cacheCountriesWithVatEnabled, &billing.CountriesList{}).Return(nil)
	suite.repository.cache = cache

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_ErrorSetCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cacheCountriesWithVatEnabled, &billing.CountriesList{}).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, &billing.CountriesList{}, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_SkipVatDisable() {
	country := suite.getCountryTemplate()
	country.VatEnabled = false

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountriesWithVatEnabled, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 0)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_SkipUS() {
	country := suite.getCountryTemplate()
	country.IsoCodeA2 = "US"

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountriesWithVatEnabled, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 0)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_SkipEmptyRatePolicy() {
	country := suite.getCountryTemplate()
	country.VatCurrencyRatesPolicy = ""

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountriesWithVatEnabled, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 0)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_SkipEmptyPeriodMonth() {
	country := suite.getCountryTemplate()
	country.VatPeriodMonth = int32(0)

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountriesWithVatEnabled, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 0)
}

func (suite *CountryTestSuite) TestCountry_FindByVatEnabled_Ok() {
	country := suite.getCountryTemplate()
	country.VatEnabled = true
	country.IsoCodeA2 = "RU"
	country.VatCurrencyRatesPolicy = "yes"
	country.VatPeriodMonth = int32(1)

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Times(1).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountriesWithVatEnabled, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountriesWithVatEnabled, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByVatEnabled(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 1)
}

func (suite *CountryTestSuite) TestCountry_GetAll_ReturnByCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cacheCountryAll, &billing.CountriesList{}).Return(nil)
	suite.repository.cache = cache

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_GetAll_ErrorSetCache() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cacheCountryAll, &billing.CountriesList{}).Return(errors.New("error"))
	cache.On("Set", cacheCountryAll, &billing.CountriesList{}, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_GetAll_EmptyList() {
	cache := &mocks.CacheInterface{}
	cache.On("Get", cacheCountryAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountryAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 0)
}

func (suite *CountryTestSuite) TestCountry_GetAll_NotEmptyList() {
	country := suite.getCountryTemplate()

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", cacheCountryAll, mock.Anything).Return(errors.New("error"))
	cache.On("Set", cacheCountryAll, mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.GetAll(context.TODO())
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 1)
}

func (suite *CountryTestSuite) TestCountry_FindByHighRisk_ReturnByCache() {
	isHighRiskOrder := true
	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheCountryRisk, isHighRiskOrder), &billing.CountriesList{}).Return(nil)
	suite.repository.cache = cache

	list, err := suite.repository.FindByHighRisk(context.TODO(), isHighRiskOrder)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_FindByHighRisk_ErrorSetCache() {
	isHighRiskOrder := true
	cache := &mocks.CacheInterface{}
	cache.On("Get", fmt.Sprintf(cacheCountryRisk, isHighRiskOrder), &billing.CountriesList{}).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryRisk, isHighRiskOrder), &billing.CountriesList{}, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	list, err := suite.repository.FindByHighRisk(context.TODO(), isHighRiskOrder)
	assert.NoError(suite.T(), err)
	assert.IsType(suite.T(), &billing.CountriesList{}, list)
}

func (suite *CountryTestSuite) TestCountry_FindByHighRisk_NotEmptyListHighRisk() {
	country := suite.getCountryTemplate()
	country.PaymentsAllowed = false
	country.HighRiskPaymentsAllowed = true

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryRisk, true), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryRisk, true), mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByHighRisk(context.TODO(), true)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 1)
	assert.Equal(suite.T(), list.Countries[0].PaymentsAllowed, country.PaymentsAllowed)
	assert.Equal(suite.T(), list.Countries[0].HighRiskPaymentsAllowed, country.HighRiskPaymentsAllowed)
}

func (suite *CountryTestSuite) TestCountry_FindByHighRisk_NotEmptyListLowRisk() {
	country := suite.getCountryTemplate()
	country.PaymentsAllowed = true
	country.HighRiskPaymentsAllowed = false

	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Return(nil)
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	cache.On("Get", fmt.Sprintf(cacheCountryRisk, false), mock.Anything).Return(errors.New("error"))
	cache.On("Set", fmt.Sprintf(cacheCountryRisk, false), mock.Anything, time.Duration(0)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.Insert(context.TODO(), country)
	assert.NoError(suite.T(), err)

	list, err := suite.repository.FindByHighRisk(context.TODO(), false)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), list.Countries, 1)
	assert.Equal(suite.T(), list.Countries[0].PaymentsAllowed, country.PaymentsAllowed)
	assert.Equal(suite.T(), list.Countries[0].HighRiskPaymentsAllowed, country.HighRiskPaymentsAllowed)
}

func (suite *CountryTestSuite) TestCountry_IsTariffRegionSupported_False() {
	isSupported := suite.repository.IsTariffRegionSupported("unknown")
	assert.False(suite.T(), isSupported)
}

func (suite *CountryTestSuite) TestCountry_IsTariffRegionSupported_True_TariffRegionRussiaAndCis() {
	isSupported := suite.repository.IsTariffRegionSupported(pkg.TariffRegionRussiaAndCis)
	assert.True(suite.T(), isSupported)
}

func (suite *CountryTestSuite) TestCountry_IsTariffRegionSupported_True_TariffRegionEurope() {
	isSupported := suite.repository.IsTariffRegionSupported(pkg.TariffRegionEurope)
	assert.True(suite.T(), isSupported)
}

func (suite *CountryTestSuite) TestCountry_IsTariffRegionSupported_True_TariffRegionLatAm() {
	isSupported := suite.repository.IsTariffRegionSupported(pkg.TariffRegionLatAm)
	assert.True(suite.T(), isSupported)
}

func (suite *CountryTestSuite) TestCountry_IsTariffRegionSupported_True_TariffRegionWorldwide() {
	isSupported := suite.repository.IsTariffRegionSupported(pkg.TariffRegionWorldwide)
	assert.True(suite.T(), isSupported)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Set_cacheCountryCodeA2() {
	country := &billing.Country{IsoCodeA2: "test"}
	cache := &mocks.CacheInterface{}
	cache.On("Set", fmt.Sprintf(cacheCountryCodeA2, country.IsoCodeA2), country, time.Duration(0)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(country)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Delete_cacheCountryAll() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Delete_cacheCountryRegions() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Delete_cacheCountriesWithVatEnabled() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Delete_cacheCountryRiskLow() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Error_Delete_cacheCountryRiskHigh() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(errors.New("error"))
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.Error(suite.T(), err)
}

func (suite *CountryTestSuite) TestCountry_updateCache_Ok() {
	cache := &mocks.CacheInterface{}
	cache.On("Delete", cacheCountryAll).Return(nil)
	cache.On("Delete", cacheCountryRegions).Return(nil)
	cache.On("Delete", cacheCountriesWithVatEnabled).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, false)).Return(nil)
	cache.On("Delete", fmt.Sprintf(cacheCountryRisk, true)).Return(nil)
	suite.repository.cache = cache

	err := suite.repository.updateCache(nil)
	assert.NoError(suite.T(), err)
}

func (suite *CountryTestSuite) getCountryTemplate() *billing.Country {
	return &billing.Country{
		Id:                      primitive.NewObjectID().Hex(),
		Currency:                "RUB",
		Region:                  pkg.TariffRegionRussiaAndCis,
		ChangeAllowed:           true,
		HighRiskChangeAllowed:   true,
		HighRiskPaymentsAllowed: true,
		PaymentsAllowed:         true,
		IsoCodeA2:               "RU",
		PayerTariffRegion:       pkg.TariffRegionRussiaAndCis,
		PriceGroupId:            primitive.NewObjectID().Hex(),
		VatEnabled:              true,
		VatCurrency:             "RU",
		VatPeriodMonth:          0,
		VatThreshold:            &billing.CountryVatThreshold{World: float64(1), Year: float64(2)},
	}
}
