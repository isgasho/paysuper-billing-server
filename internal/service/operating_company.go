package service

import (
	"context"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
)

const (
	collectionOperatingCompanies = "operating_companies"

	cacheKeyOperatingCompany      = "operating_company:id:%s"
	cacheKeyAllOperatingCompanies = "operating_company:all"
)

type OperatingCompanyInterface interface {
	GetById(id string) (oc *billing.OperatingCompany, err error)
	GetAll() (result []*billing.OperatingCompany, err error)
	Insert(oc *billing.OperatingCompany) (err error)
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
	err = s.operatingCompany.Insert(req)
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

func (o OperatingCompany) GetById(id string) (oc *billing.OperatingCompany, err error) {
	key := fmt.Sprintf(cacheKeyOperatingCompany, id)
	if err = o.svc.cacher.Get(key, &oc); err == nil {
		return oc, nil
	}

	err = o.svc.db.Collection(collectionOperatingCompanies).FindId(bson.ObjectIdHex(id)).One(&oc)
	if err != nil {
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

func (o OperatingCompany) Insert(oc *billing.OperatingCompany) (err error) {
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
