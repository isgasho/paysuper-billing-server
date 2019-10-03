package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

var (
	dashboardErrorUnknown         = newBillingServerErrorMsg("db000001", "unknown error. try request later")
	dashboardErrorIncorrectPeriod = newBillingServerErrorMsg("db000002", "incorrect dashboard period")
)

func (s *Service) GetDashboardMainReport(
	ctx context.Context,
	req *grpc.GetDashboardMainRequest,
	rsp *grpc.GetDashboardMainResponse,
) error {
	_, err := s.merchant.GetById(req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantErrorUnknown {
			rsp.Status = pkg.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetMainReport(req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = report

	return nil
}

func (s *Service) GetDashboardRevenueDynamicsReport(
	ctx context.Context,
	req *grpc.GetDashboardMainRequest,
	rsp *grpc.GetDashboardRevenueDynamicsReportResponse,
) error {
	_, err := s.merchant.GetById(req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantErrorUnknown {
			rsp.Status = pkg.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetRevenueDynamicsReport(req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = report

	return nil
}

func (s *Service) GetDashboardBaseReport(
	ctx context.Context,
	req *grpc.GetDashboardBaseReportRequest,
	rsp *grpc.GetDashboardBaseReportResponse,
) error {
	_, err := s.merchant.GetById(req.MerchantId)

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantErrorUnknown {
			rsp.Status = pkg.ResponseStatusSystemError
		}

		return nil
	}

	report, err := s.dashboardRepository.GetBaseReport(req.MerchantId, req.Period)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk
	rsp.Item = report

	return nil
}
