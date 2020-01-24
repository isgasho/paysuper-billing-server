package service

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/currenciespb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodb "gopkg.in/paysuper/paysuper-database-mongo.v2"
	"sort"
)

var (
	errorMoneybackSystemGetAll                    = newBillingServerErrorMsg("mbs000001", "can't get list of money back setting for system")
	errorMoneybackSystemGet                       = newBillingServerErrorMsg("mbs000002", "can't get money back setting for system")
	errorMoneybackSystemSetFailed                 = newBillingServerErrorMsg("mbs000003", "can't set money back setting for system")
	errorMoneybackSystemDelete                    = newBillingServerErrorMsg("mbs000004", "can't delete money back setting for system")
	errorMoneybackSystemCurrency                  = newBillingServerErrorMsg("mbs000005", "currency not supported")
	errorMoneybackCostAlreadyExist                = newBillingServerErrorMsg("mbs000006", "cost with specified parameters already exist")
	errorMoneybackSystemMccCode                   = newBillingServerErrorMsg("mbs000007", "mcc code not supported")
	errorMoneybackSystemOperatingCompanyNotExists = newBillingServerErrorMsg("mbs000008", "operating company not exists")
	errorMoneybackSystemDaysMatchedNotFound       = newBillingServerErrorMsg("mbs000009", "days matched not found")
)

func (s *Service) GetAllMoneyBackCostSystem(
	ctx context.Context,
	req *billingpb.EmptyRequest,
	res *billingpb.MoneyBackCostSystemListResponse,
) error {
	val, err := s.moneyBackCostSystemRepository.GetAll(ctx)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackSystemGetAll
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetMoneyBackCostSystem(
	ctx context.Context,
	req *billingpb.MoneyBackCostSystemRequest,
	res *billingpb.MoneyBackCostSystemResponse,
) error {
	val, err := s.getMoneyBackCostSystem(ctx, req)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorMoneybackSystemGet
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetMoneyBackCostSystem(
	ctx context.Context,
	req *billingpb.MoneyBackCostSystem,
	res *billingpb.MoneyBackCostSystemResponse,
) error {
	var err error

	if req.Country != "" {
		country, err := s.country.GetByIsoCodeA2(ctx, req.Country)
		if err != nil {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorCountryNotFound
			return nil
		}
		req.Region = country.PayerTariffRegion
	} else {
		exists := s.country.IsTariffRegionSupported(req.Region)
		if !exists {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorCountryRegionNotExists
			return nil
		}
	}

	sCurr, err := s.curService.GetSettlementCurrencies(ctx, &currenciespb.EmptyRequest{})
	if err != nil {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackSystemCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackSystemMccCode
		return nil
	}
	if !s.operatingCompany.Exists(ctx, req.OperatingCompanyId) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackSystemOperatingCompanyNotExists
		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostSystemRepository.GetById(ctx, req.Id)
		if err != nil {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorMoneybackSystemSetFailed
			return nil
		}
		req.Id = val.Id
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostSystemRepository.Update(ctx, req)
	} else {
		req.Id = primitive.NewObjectID().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostSystemRepository.Insert(ctx, req)
	}
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackSystemSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = billingpb.ResponseStatusBadData
			res.Message = errorMoneybackCostAlreadyExist
		}

		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeleteMoneyBackCostSystem(
	ctx context.Context,
	req *billingpb.PaymentCostDeleteRequest,
	res *billingpb.ResponseError,
) error {
	pc, err := s.moneyBackCostSystemRepository.GetById(ctx, req.Id)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.moneyBackCostSystemRepository.Delete(ctx, pc)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackSystemDelete
		return nil
	}
	res.Status = billingpb.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostSystem(
	ctx context.Context,
	req *billingpb.MoneyBackCostSystemRequest,
) (*billingpb.MoneyBackCostSystem, error) {
	val, err := s.moneyBackCostSystemRepository.Find(
		ctx,
		req.Name,
		req.PayoutCurrency,
		req.UndoReason,
		req.Region,
		req.Country,
		req.MccCode,
		req.OperatingCompanyId,
		req.PaymentStage,
	)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorMoneybackSystemDaysMatchedNotFound
	}

	var matched []*kvIntInt
	for _, set := range val {
		for k, i := range set.Set {
			if req.Days >= i.DaysFrom {
				matched = append(matched, &kvIntInt{k, i.DaysFrom})
			}
		}
		if len(matched) == 0 {
			continue
		}

		sort.Slice(matched, func(i, j int) bool {
			return matched[i].Value > matched[j].Value
		})
		return set.Set[matched[0].Key], nil
	}

	return nil, errorMoneybackSystemDaysMatchedNotFound
}
