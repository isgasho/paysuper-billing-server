package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mocks"
	internalPkg "github.com/paysuper/paysuper-billing-server/internal/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	reportingMocks "github.com/paysuper/paysuper-reporter/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type OperatingCompanyTestSuite struct {
	suite.Suite
	service           *Service
	log               *zap.Logger
	cache             internalPkg.CacheInterface
	operatingCompany  *billing.OperatingCompany
	operatingCompany2 *billing.OperatingCompany
}

func Test_OperatingCompany(t *testing.T) {
	suite.Run(t, new(OperatingCompanyTestSuite))
}

func (suite *OperatingCompanyTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
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
	suite.service = NewBillingService(
		db,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		suite.cache,
		mocks.NewCurrencyServiceMockOk(),
		mocks.NewDocumentSignerMockOk(),
		&reportingMocks.ReporterService{},
		mocks.NewFormatterOK(),
		mocks.NewBrokerMockOk(),
		nil,
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	countryRu := &billing.Country{
		Id:                bson.NewObjectId().Hex(),
		IsoCodeA2:         "RU",
		Region:            "Russia",
		Currency:          "RUB",
		PaymentsAllowed:   true,
		ChangeAllowed:     true,
		VatEnabled:        true,
		PriceGroupId:      bson.NewObjectId().Hex(),
		VatCurrency:       "RUB",
		PayerTariffRegion: pkg.TariffRegionRussiaAndCis,
	}
	countryUa := &billing.Country{
		Id:                bson.NewObjectId().Hex(),
		IsoCodeA2:         "UA",
		Region:            "UA",
		Currency:          "UAH",
		PaymentsAllowed:   true,
		ChangeAllowed:     true,
		VatEnabled:        false,
		PriceGroupId:      "",
		VatCurrency:       "",
		PayerTariffRegion: pkg.TariffRegionRussiaAndCis,
	}
	countries := []*billing.Country{countryRu, countryUa}
	if err := suite.service.country.MultipleInsert(countries); err != nil {
		suite.FailNow("Insert country test data failed", "%v", err)
	}

	suite.operatingCompany = &billing.OperatingCompany{
		Name:               "Legal name",
		Country:            "RU",
		RegistrationNumber: "some number",
		VatNumber:          "some vat number",
		Address:            "Home, home 0",
		VatAddress:         "Address for VAT purposes",
		SignatoryName:      "Vassiliy Poupkine",
		SignatoryPosition:  "CEO",
		BankingDetails:     "bank details including bank, bank address, account number, swift/ bic, intermediary bank",
		PaymentCountries:   []string{},
	}

	suite.operatingCompany2 = &billing.OperatingCompany{
		Name:               "Legal name 2",
		Country:            "ML",
		RegistrationNumber: "some number 2",
		VatNumber:          "some vat number 2",
		Address:            "Home, home 1",
		VatAddress:         "Address for VAT purposes 2",
		SignatoryName:      "Ivan Petroff",
		SignatoryPosition:  "CEO",
		BankingDetails:     "bank details including bank, bank address, account number, swift/ bic, intermediary bank",
		PaymentCountries:   []string{"RU", "UA"},
	}
}

func (suite *OperatingCompanyTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *OperatingCompanyTestSuite) Test_OperatingCompany_AddOk() {
	count, err := suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 0)

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.AddOperatingCompany(context.TODO(), suite.operatingCompany, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	count, err = suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 1)
}

func (suite *OperatingCompanyTestSuite) Test_OperatingCompany_ListOk() {
	res := &grpc.EmptyResponseWithStatus{}
	err := suite.service.AddOperatingCompany(context.TODO(), suite.operatingCompany, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	res2 := &grpc.GetOperatingCompaniesListResponse{}
	err = suite.service.GetOperatingCompaniesList(context.TODO(), &grpc.EmptyRequest{}, res2)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)
	assert.Len(suite.T(), res2.Items, 1)
}

func (suite *OperatingCompanyTestSuite) Test_OperatingCompany_AddFail_DuplicatePaymentCountry() {
	count, err := suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 0)

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.AddOperatingCompany(context.TODO(), suite.operatingCompany, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusOk)

	count, err = suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 1)

	err = suite.service.AddOperatingCompany(context.TODO(), suite.operatingCompany, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorOperatingCompanyCountryAlreadyExists)
}

func (suite *OperatingCompanyTestSuite) Test_OperatingCompany_AddFail_PaymentCountryUnknown() {
	count, err := suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 0)

	suite.operatingCompany.PaymentCountries = []string{"RU", "UA", "XXX"}

	res := &grpc.EmptyResponseWithStatus{}
	err = suite.service.AddOperatingCompany(context.TODO(), suite.operatingCompany, res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res.Status, pkg.ResponseStatusBadData)
	assert.Equal(suite.T(), res.Message, errorOperatingCompanyCountryUnknown)
}

func (suite *OperatingCompanyTestSuite) Test_OperatingCompany_GetByPaymentCountry() {
	count, err := suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 0)

	suite.operatingCompany.Id = bson.NewObjectId().Hex()
	err = suite.service.operatingCompany.Upsert(suite.operatingCompany)
	assert.NoError(suite.T(), err)

	suite.operatingCompany2.Id = bson.NewObjectId().Hex()
	err = suite.service.operatingCompany.Upsert(suite.operatingCompany2)
	assert.NoError(suite.T(), err)

	count, err = suite.service.db.Collection(collectionOperatingCompanies).Find(nil).Count()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), count, 2)

	oc, err := suite.service.operatingCompany.GetByPaymentCountry("")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), oc.Id, suite.operatingCompany.Id)

	oc, err = suite.service.operatingCompany.GetByPaymentCountry("RU")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), oc.Id, suite.operatingCompany2.Id)

	oc, err = suite.service.operatingCompany.GetByPaymentCountry("XXX")
	assert.Error(suite.T(), err)
	assert.EqualError(suite.T(), err, errorOperatingCompanyCountryUnknown.Error())
}
