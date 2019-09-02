package service

import (
	"context"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
)

var (
	dashboardErrorUnknown = newBillingServerErrorMsg("db000001", "unknown error. try request later")
)

func (s *Service) GetDashboardMain(
	ctx context.Context,
	req *grpc.GetDashboardMainRequest,
	rsp *grpc.GetDashboardMainResponse,
) error {
	_, err := s.getMerchantBy(bson.M{"_id": bson.ObjectIdHex(req.MerchantId)})

	if err != nil {
		rsp.Status = pkg.ResponseStatusNotFound
		rsp.Message = err.(*grpc.ResponseErrorMessage)

		if err == merchantErrorUnknown {
			rsp.Status = pkg.ResponseStatusSystemError
		}

		return nil
	}

}
