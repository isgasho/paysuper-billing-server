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
	errorMoneybackMerchantGetAll              = newBillingServerErrorMsg("mbm000001", "can't get list of money back setting for merchant")
	errorMoneybackMerchantGet                 = newBillingServerErrorMsg("mbm000002", "can't get money back setting for merchant")
	errorMoneybackMerchantSetFailed           = newBillingServerErrorMsg("mbm000003", "can't set money back setting for merchant")
	errorMoneybackMerchantDelete              = newBillingServerErrorMsg("mbm000004", "can't delete money back setting for merchant")
	errorMoneybackMerchantCurrency            = newBillingServerErrorMsg("mbm000005", "currency not supported")
	errorMoneybackMerchantCostAlreadyExist    = newBillingServerErrorMsg("mbm000006", "cost with specified parameters already exist")
	errorMoneybackMerchantMccCode             = newBillingServerErrorMsg("mbm000007", "mcc code not supported")
	errorMoneybackMerchantDaysMatchedNotFound = newBillingServerErrorMsg("mbm000008", "days matched not found")
	errorCostRateNotFound                     = newBillingServerErrorMsg("cr000001", "cost rate with specified identifier not found")
)

func (s *Service) GetAllMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantListRequest,
	res *billingpb.MoneyBackCostMerchantListResponse,
) error {
	val, err := s.moneyBackCostMerchantRepository.GetAllForMerchant(ctx, req.MerchantId)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantGetAll
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) GetMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantRequest,
	res *billingpb.MoneyBackCostMerchantResponse,
) error {
	val, err := s.getMoneyBackCostMerchant(ctx, req)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorMoneybackMerchantGet
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = val

	return nil
}

func (s *Service) SetMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchant,
	res *billingpb.MoneyBackCostMerchantResponse,
) error {

	var err error

	if _, err := s.merchantRepository.GetById(ctx, req.MerchantId); err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = merchantErrorNotFound
		return nil
	}

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
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.PayoutCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(sCurr.Currencies, req.FixAmountCurrency) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantCurrency
		return nil
	}
	if !helper.Contains(pkg.SupportedMccCodes, req.MccCode) {
		res.Status = billingpb.ResponseStatusBadData
		res.Message = errorMoneybackMerchantMccCode
		return nil
	}

	req.UpdatedAt = ptypes.TimestampNow()
	req.IsActive = true

	if req.Id != "" {
		val, err := s.moneyBackCostMerchantRepository.GetById(ctx, req.Id)
		if err != nil {
			res.Status = billingpb.ResponseStatusNotFound
			res.Message = errorMoneybackMerchantSetFailed
			return nil
		}
		req.Id = val.Id
		req.MerchantId = val.MerchantId
		req.CreatedAt = val.CreatedAt
		err = s.moneyBackCostMerchantRepository.Update(ctx, req)
	} else {
		req.Id = primitive.NewObjectID().Hex()
		req.CreatedAt = ptypes.TimestampNow()
		err = s.moneyBackCostMerchantRepository.Insert(ctx, req)
	}

	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantSetFailed

		if mongodb.IsDuplicate(err) {
			res.Status = billingpb.ResponseStatusBadData
			res.Message = errorMoneybackMerchantCostAlreadyExist
		}

		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	res.Item = req

	return nil
}

func (s *Service) DeleteMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.PaymentCostDeleteRequest,
	res *billingpb.ResponseError,
) error {
	pc, err := s.moneyBackCostMerchantRepository.GetById(ctx, req.Id)
	if err != nil {
		res.Status = billingpb.ResponseStatusNotFound
		res.Message = errorCostRateNotFound
		return nil
	}
	err = s.moneyBackCostMerchantRepository.Delete(ctx, pc)
	if err != nil {
		res.Status = billingpb.ResponseStatusSystemError
		res.Message = errorMoneybackMerchantDelete
		return nil
	}

	res.Status = billingpb.ResponseStatusOk
	return nil
}

func (s *Service) getMoneyBackCostMerchant(
	ctx context.Context,
	req *billingpb.MoneyBackCostMerchantRequest,
) (*billingpb.MoneyBackCostMerchant, error) {
	val, err := s.moneyBackCostMerchantRepository.Find(
		ctx,
		req.MerchantId,
		req.Name,
		req.PayoutCurrency,
		req.UndoReason,
		req.Region,
		req.Country,
		req.MccCode,
		req.PaymentStage,
	)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, errorMoneybackMerchantDaysMatchedNotFound
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

	return nil, errorMoneybackMerchantDaysMatchedNotFound
}
