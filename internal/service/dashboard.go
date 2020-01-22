package service

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	dashboardErrorUnknown         = newBillingServerErrorMsg("db000001", "unknown error. try request later")
	dashboardErrorIncorrectPeriod = newBillingServerErrorMsg("db000002", "incorrect dashboard period")
)

func (s *Service) GetDashboardMainReport(
	ctx context.Context,
	req *billingpb.GetDashboardMainRequest,
	rsp *billingpb.GetDashboardMainResponse,
) error {
	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		if err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetMainReport(ctx, req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = report

	return nil
}

func (s *Service) GetDashboardRevenueDynamicsReport(
	ctx context.Context,
	req *billingpb.GetDashboardMainRequest,
	rsp *billingpb.GetDashboardRevenueDynamicsReportResponse,
) error {
	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		if err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetRevenueDynamicsReport(ctx, req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = report

	return nil
}

func (s *Service) GetDashboardBaseReport(
	ctx context.Context,
	req *billingpb.GetDashboardBaseReportRequest,
	rsp *billingpb.GetDashboardBaseReportResponse,
) error {
	_, err := s.merchantRepository.GetById(ctx, req.MerchantId)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		if err != mongo.ErrNoDocuments {
			rsp.Status = billingpb.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetBaseReport(ctx, req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = err.(*billingpb.ResponseErrorMessage)

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = report

	return nil
}
