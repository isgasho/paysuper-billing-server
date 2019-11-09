package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	collectionOperatingCompanies = "operating_companies"

	cacheKeyOperatingCompany                 = "operating_company:id:%s"
	cacheKeyOperatingCompanyByPaymentCountry = "operating_company:country:%s"
	cacheKeyAllOperatingCompanies            = "operating_company:all"
)

var (
	errorOperatingCompanyCountryAlreadyExists = newBillingServerErrorMsg("oc000001", "operating company for one of passed country already exists")
	errorOperatingCompanyCountryUnknown       = newBillingServerErrorMsg("oc000002", "operating company country unknown")
	errorOperatingCompanyNotFound             = newBillingServerErrorMsg("oc000003", "operating company not found")
)

type OperatingCompanyInterface interface {
	GetById(id string) (oc *billing.OperatingCompany, err error)
	GetByPaymentCountry(countryCode string) (oc *billing.OperatingCompany, err error)
	GetAll() (result []*billing.OperatingCompany, err error)
	Upsert(oc *billing.OperatingCompany) (err error)
	Exists(id string) bool
}

func newOperatingCompanyService(svc *Service) OperatingCompanyInterface {
	s := &OperatingCompany{svc: svc}
	return s
}

func (s *Service) GetOperatingCompaniesList(
	ctx context.Context,
	req *grpc.EmptyRequest,
	res *grpc.GetOperatingCompaniesListResponse,
) (err error) {
	res.Items, err = s.operatingCompany.GetAll()
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return
	}

	res.Status = pkg.ResponseStatusOk
	return
}

func (s *Service) AddOperatingCompany(
	ctx context.Context,
	req *billing.OperatingCompany,
	res *grpc.EmptyResponseWithStatus,
) (err error) {
	oc := &billing.OperatingCompany{
		Id:               bson.NewObjectId().Hex(),
		PaymentCountries: []string{},
		CreatedAt:        ptypes.TimestampNow(),
	}

	if req.Id != "" {
		oc, err = s.operatingCompany.GetById(req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorOperatingCompanyNotFound
			return nil
		}
	}

	if req.PaymentCountries == nil || len(req.PaymentCountries) == 0 {
		ocCheck, err := s.operatingCompany.GetByPaymentCountry("")
		if err != nil && err != errorOperatingCompanyNotFound {
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}
		if ocCheck != nil && ocCheck.Id != oc.Id {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorOperatingCompanyCountryAlreadyExists
			return nil
		}
		oc.PaymentCountries = []string{}

	} else {
		for _, countryCode := range req.PaymentCountries {
			ocCheck, err := s.operatingCompany.GetByPaymentCountry(countryCode)
			if err != nil && err != errorOperatingCompanyNotFound {
				if e, ok := err.(*grpc.ResponseErrorMessage); ok {
					res.Status = pkg.ResponseStatusBadData
					res.Message = e
					return nil
				}
				return err
			}
			if ocCheck != nil && ocCheck.Id != oc.Id {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorOperatingCompanyCountryAlreadyExists
				return nil
			}

			_, err = s.country.GetByIsoCodeA2(countryCode)
			if err != nil {
				res.Status = pkg.ResponseStatusBadData
				res.Message = errorOperatingCompanyCountryUnknown
				return nil
			}
		}
		oc.PaymentCountries = req.PaymentCountries
	}

	oc.UpdatedAt = ptypes.TimestampNow()
	oc.Name = req.Name
	oc.Country = req.Country
	oc.RegistrationNumber = req.RegistrationNumber
	oc.VatNumber = req.VatNumber
	oc.Address = req.Address
	oc.SignatoryName = req.SignatoryName
	oc.SignatoryPosition = req.SignatoryPosition
	oc.BankingDetails = req.BankingDetails

	err = s.operatingCompany.Upsert(oc)
	if err != nil {
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return
	}

	res.Status = pkg.ResponseStatusOk
	return
}

func (s *Service) GetOperatingCompany(
	ctx context.Context,
	req *grpc.GetOperatingCompanyRequest,
	res *grpc.GetOperatingCompanyResponse,
) (err error) {
	oc, err := s.operatingCompany.GetById(req.Id)

	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorOperatingCompanyNotFound
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Company = oc

	return
}

func (o OperatingCompany) GetById(id string) (oc *billing.OperatingCompany, err error) {
	key := fmt.Sprintf(cacheKeyOperatingCompany, id)
	if err = o.svc.cacher.Get(key, &oc); err == nil {
		return oc, nil
	}

	err = o.svc.db.Collection(collectionOperatingCompanies).FindId(bson.ObjectIdHex(id)).One(&oc)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errorOperatingCompanyNotFound
		}
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
			zap.String(pkg.ErrorDatabaseFieldDocumentId, id),
		)
		return
	}

	err = o.svc.cacher.Set(key, oc, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, oc),
		)
	}

	return
}

func (o OperatingCompany) GetByPaymentCountry(countryCode string) (oc *billing.OperatingCompany, err error) {
	key := fmt.Sprintf(cacheKeyOperatingCompanyByPaymentCountry, countryCode)
	if err = o.svc.cacher.Get(key, &oc); err == nil {
		return oc, nil
	}

	query := bson.M{
		"payment_countries": countryCode,
	}

	if countryCode == "" {
		query["payment_countries"] = bson.M{"$size": 0}
	} else {
		_, err = o.svc.country.GetByIsoCodeA2(countryCode)
		if err != nil {
			return nil, errorOperatingCompanyCountryUnknown
		}
		query["payment_countries"] = countryCode
	}

	err = o.svc.db.Collection(collectionOperatingCompanies).Find(query).One(&oc)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, errorOperatingCompanyNotFound
		}

		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return
	}

	err = o.svc.cacher.Set(key, oc, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, oc),
		)
	}

	return
}

func (o OperatingCompany) GetAll() (result []*billing.OperatingCompany, err error) {
	if err = o.svc.cacher.Get(cacheKeyAllOperatingCompanies, &result); err == nil {
		return result, nil
	}

	err = o.svc.db.Collection(collectionOperatingCompanies).Find(nil).All(&result)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
			zap.Any(pkg.ErrorDatabaseFieldQuery, nil),
		)
		return
	}

	err = o.svc.cacher.Set(cacheKeyAllOperatingCompanies, result, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, cacheKeyAllOperatingCompanies),
			zap.Any(pkg.ErrorCacheFieldData, result),
		)
	}

	return
}

func (o OperatingCompany) Upsert(oc *billing.OperatingCompany) (err error) {
	_, err = o.svc.db.Collection(collectionOperatingCompanies).UpsertId(bson.ObjectIdHex(oc.Id), oc)

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, oc),
		)
		return
	}

	key := fmt.Sprintf(cacheKeyOperatingCompany, oc.Id)
	err = o.svc.cacher.Set(key, oc, 0)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorCacheFieldData, oc),
		)
		return
	}

	err = o.svc.cacher.Delete(cacheKeyAllOperatingCompanies)
	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "DELETE"),
			zap.String(pkg.ErrorCacheFieldKey, cacheKeyAllOperatingCompanies),
		)
	}

	return
}

func (o OperatingCompany) Exists(id string) bool {
	_, err := o.GetById(id)
	if err != nil {
		return false
	}
	return true
}
