package service

import (
	"context"
	"fmt"
	"github.com/paysuper/paysuper-billing-server/internal/repository"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	casbinProto "github.com/paysuper/paysuper-proto/go/casbinpb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

func (s *Service) MerchantsMigrate(ctx context.Context) error {
	merchants, err := s.merchantRepository.GetAll(ctx)

	if err != nil {
		zap.L().Error("[task merchants migrate] Unable to get merchants", zap.Error(err))
		return nil
	}

	for _, merchant := range merchants {
		if merchant.User == nil ||
			merchant.User.Id == "" ||
			merchant.User.Email == "" ||
			merchant.User.FirstName == "" ||
			merchant.User.LastName == "" {
			continue
		}

		userRole := &billingpb.UserRole{
			Id:         primitive.NewObjectID().Hex(),
			MerchantId: merchant.Id,
			UserId:     merchant.User.Id,
			Email:      merchant.User.Email,
			FirstName:  merchant.User.FirstName,
			LastName:   merchant.User.LastName,
			Role:       billingpb.RoleMerchantOwner,
			Status:     pkg.UserRoleStatusAccepted,
		}

		_, err := s.userRoleRepository.GetMerchantUserByUserId(ctx, userRole.MerchantId, userRole.UserId)

		if err != nil {
			err = s.userRoleRepository.AddMerchantUser(ctx, userRole)
		}

		if err != nil {
			zap.L().Error("[task merchants migrate] Unable to add merchant user role", zap.Error(err))
			continue
		}

		casbinRole := &casbinProto.UserRoleRequest{
			User: fmt.Sprintf(pkg.CasbinMerchantUserMask, merchant.Id, merchant.User.Id),
			Role: billingpb.RoleMerchantOwner,
		}

		roles, err := s.casbinService.GetRolesForUser(context.TODO(), casbinRole)

		if roles == nil || len(roles.Array) < 1 {
			_, err = s.casbinService.AddRoleForUser(context.TODO(), casbinRole)
		}

		if err != nil {
			zap.L().Error("[task merchants migrate] Unable to add user to casbin", zap.Error(err), zap.Any("role", casbinRole))
		}
	}

	zap.L().Info("[task merchants migrate] Finished successfully")

	return nil
}

func (s *Service) getMerchantPaymentMethod(ctx context.Context, merchantId, method string) (*billingpb.MerchantPaymentMethod, error) {
	merchant, err := s.merchantRepository.GetById(ctx, merchantId)

	if err != nil {
		return nil, merchantErrorNotFound
	}

	merchantPaymentMethods := make(map[string]*billingpb.MerchantPaymentMethod)

	if len(merchant.PaymentMethods) > 0 {
		for k, v := range merchant.PaymentMethods {
			merchantPaymentMethods[k] = v
		}
	}

	pm, err := s.paymentMethod.GetAll(ctx)

	if err != nil {
		return nil, err
	}

	if len(merchantPaymentMethods) != len(pm) {
		for k, v := range pm {
			_, ok := merchantPaymentMethods[k]

			if ok {
				continue
			}

			merchantPaymentMethods[k] = &billingpb.MerchantPaymentMethod{
				PaymentMethod: &billingpb.MerchantPaymentMethodIdentification{
					Id:   k,
					Name: v.Name,
				},
				Commission: &billingpb.MerchantPaymentMethodCommissions{
					Fee: pkg.DefaultPaymentMethodFee,
					PerTransaction: &billingpb.MerchantPaymentMethodPerTransactionCommission{
						Fee:      pkg.DefaultPaymentMethodPerTransactionFee,
						Currency: pkg.DefaultPaymentMethodCurrency,
					},
				},
				Integration: &billingpb.MerchantPaymentMethodIntegration{},
				IsActive:    true,
			}
		}
	}

	if _, ok := merchantPaymentMethods[method]; !ok {
		return nil, fmt.Errorf(errorNotFound, repository.CollectionMerchant)
	}

	return merchantPaymentMethods[method], nil
}
