package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

func (s *Service) CreateRoyaltyReport(context.Context, *grpc.CreateRoyaltyReportRequest, *grpc.CreateRoyaltyReportRequest) error {
	panic("implement me")
}

func (s *Service) ListRoyaltyReports(context.Context, *grpc.ListRoyaltyReportsRequest, *grpc.ListRoyaltyReportsResponse) error {
	panic("implement me")
}

func (s *Service) ChangeRoyaltyReport(context.Context, *grpc.ChangeRoyaltyReportRequest, *grpc.ResponseError) error {
	panic("implement me")
}

func (s *Service) ListRoyaltyReportOrders(context.Context, *grpc.ListRoyaltyReportOrdersRequest, *grpc.ListRoyaltyReportOrdersResponse) error {
	panic("implement me")
}

func (s *Service) AutoAcceptRoyaltyReports(context.Context, *grpc.EmptyRequest, *grpc.EmptyResponse) error {
	panic("implement me")
}
