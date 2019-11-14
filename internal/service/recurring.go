package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
)

var (
	recurringErrorIncorrectCookie = newBillingServerErrorMsg("re000001", "customer cookie value is incorrect")
	recurringCustomerNotFound     = newBillingServerErrorMsg("re000002", "customer not found")
	recurringErrorUnknown         = newBillingServerErrorMsg("re000003", "unknown error")
	recurringSavedCardNotFount    = newBillingServerErrorMsg("re000005", "saved card for customer not found")
)

func (s *Service) DeleteSavedCard(
	ctx context.Context,
	req *grpc.DeleteSavedCardRequest,
	rsp *grpc.EmptyResponseWithStatus,
) error {
	customer, err := s.decryptBrowserCookie(req.Cookie)

	if err != nil {
		rsp.Status = pkg.ResponseStatusBadData
		rsp.Message = recurringErrorIncorrectCookie
		return nil
	}

	if customer.CustomerId == "" && customer.VirtualCustomerId == "" {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = recurringCustomerNotFound
		return nil
	}

	if customer.CustomerId != "" {
		_, err = s.getCustomerById(customer.CustomerId)

		if err != nil {
			rsp.Status = pkg.ResponseStatusNotFound
			rsp.Message = recurringCustomerNotFound
			return nil
		}
	}

	req1 := &repository.DeleteSavedCardRequest{
		Id:    req.Id,
		Token: customer.CustomerId,
	}

	if req1.Token == "" {
		req1.Token = customer.VirtualCustomerId
	}

	rsp1, err := s.rep.DeleteSavedCard(ctx, req1)

	if err != nil {
		rsp.Status = pkg.ResponseStatusSystemError
		rsp.Message = recurringErrorUnknown
		return nil
	}

	if rsp1.Status != pkg.ResponseStatusOk {
		rsp.Status = rsp1.Status

		if rsp.Status == pkg.ResponseStatusSystemError {
			rsp.Message = recurringErrorUnknown
		} else {
			rsp.Message = recurringSavedCardNotFount
		}

		return nil
	}

	rsp.Status = pkg.ResponseStatusOk

	return nil
}
