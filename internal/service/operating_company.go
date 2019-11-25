package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	GetById(ctx context.Context, id string) (oc *billing.OperatingCompany, err error)
	GetByPaymentCountry(ctx context.Context, countryCode string) (oc *billing.OperatingCompany, err error)
	GetAll(ctx context.Context) (result []*billing.OperatingCompany, err error)
	Upsert(ctx context.Context, oc *billing.OperatingCompany) (err error)
	Exists(ctx context.Context, id string) bool
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
	res.Items, err = s.operatingCompany.GetAll(ctx)
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
		Id:               primitive.NewObjectID().Hex(),
		PaymentCountries: []string{},
		CreatedAt:        ptypes.TimestampNow(),
	}

	if req.Id != "" {
		oc, err = s.operatingCompany.GetById(ctx, req.Id)
		if err != nil {
			res.Status = pkg.ResponseStatusBadData
			res.Message = errorOperatingCompanyNotFound
			return nil
		}
	}

	if req.PaymentCountries == nil || len(req.PaymentCountries) == 0 {
		ocCheck, err := s.operatingCompany.GetByPaymentCountry(ctx, "")
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
			ocCheck, err := s.operatingCompany.GetByPaymentCountry(ctx, countryCode)
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

			_, err = s.country.GetByIsoCodeA2(ctx, countryCode)
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
	oc.RegistrationDate = req.RegistrationDate
	oc.VatNumber = req.VatNumber
	oc.Address = req.Address
	oc.SignatoryName = req.SignatoryName
	oc.SignatoryPosition = req.SignatoryPosition
	oc.BankingDetails = req.BankingDetails
	oc.VatAddress = req.VatAddress
	oc.Email = req.Email

	err = s.operatingCompany.Upsert(ctx, oc)
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
	oc, err := s.operatingCompany.GetById(ctx, req.Id)

	if err != nil {
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorOperatingCompanyNotFound
		return nil
	}

	res.Status = pkg.ResponseStatusOk
	res.Company = oc

	return
}

func (o OperatingCompany) GetById(ctx context.Context, id string) (oc *billing.OperatingCompany, err error) {
	key := fmt.Sprintf(cacheKeyOperatingCompany, id)
	if err = o.svc.cacher.Get(key, &oc); err == nil {
		return oc, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err = o.svc.db.Collection(collectionOperatingCompanies).FindOne(ctx, filter).Decode(&oc)

	if err != nil {
		if err == mongo.ErrNoDocuments {
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

func (o OperatingCompany) GetByPaymentCountry(
	ctx context.Context,
	countryCode string,
) (oc *billing.OperatingCompany, err error) {
	key := fmt.Sprintf(cacheKeyOperatingCompanyByPaymentCountry, countryCode)
	if err = o.svc.cacher.Get(key, &oc); err == nil {
		return oc, nil
	}

	query := bson.M{"payment_countries": countryCode}

	if countryCode == "" {
		query["payment_countries"] = bson.M{"$size": 0}
	} else {
		_, err = o.svc.country.GetByIsoCodeA2(ctx, countryCode)
		if err != nil {
			return nil, errorOperatingCompanyCountryUnknown
		}
		query["payment_countries"] = countryCode
	}

	err = o.svc.db.Collection(collectionOperatingCompanies).FindOne(ctx, query).Decode(&oc)

	if err != nil {
		if err == mongo.ErrNoDocuments {
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

func (o OperatingCompany) GetAll(ctx context.Context) ([]*billing.OperatingCompany, error) {
	var result []*billing.OperatingCompany
	err := o.svc.cacher.Get(cacheKeyAllOperatingCompanies, &result)

	if err == nil {
		return result, nil
	}

	cursor, err := o.svc.db.Collection(collectionOperatingCompanies).Find(ctx, bson.M{})

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
		)
		return nil, err
	}

	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
		)
		return nil, err
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

	return result, nil
}

func (o OperatingCompany) Upsert(ctx context.Context, oc *billing.OperatingCompany) error {
	oid, _ := primitive.ObjectIDFromHex(oc.Id)
	filter := bson.M{"_id": oid}
	_, err := o.svc.db.Collection(collectionOperatingCompanies).ReplaceOne(ctx, filter, oc, options.Replace().SetUpsert(true))

	if err != nil {
		zap.S().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOperatingCompanies),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldDocument, oc),
		)
		return err
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
		return err
	}

	if len(oc.Country) == 0 {
		key = fmt.Sprintf(cacheKeyOperatingCompanyByPaymentCountry, "")
		err = o.svc.cacher.Set(key, oc, 0)
		if err != nil {
			zap.L().Error(
				pkg.ErrorCacheQueryFailed,
				zap.Error(err),
				zap.String(pkg.ErrorCacheFieldCmd, "SET"),
				zap.String(pkg.ErrorCacheFieldKey, key),
				zap.Any(pkg.ErrorCacheFieldData, oc),
			)
			return err
		}
	} else {
		for _, countryCode := range oc.PaymentCountries {
			key = fmt.Sprintf(cacheKeyOperatingCompanyByPaymentCountry, countryCode)
			err = o.svc.cacher.Set(key, oc, 0)
			if err != nil {
				zap.L().Error(
					pkg.ErrorCacheQueryFailed,
					zap.Error(err),
					zap.String(pkg.ErrorCacheFieldCmd, "SET"),
					zap.String(pkg.ErrorCacheFieldKey, key),
					zap.Any(pkg.ErrorCacheFieldData, oc),
				)
				return err
			}
		}
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

	return nil
}

func (o OperatingCompany) Exists(ctx context.Context, id string) bool {
	_, err := o.GetById(ctx, id)
	return err == nil
}
