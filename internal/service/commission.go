package service

import (
	"fmt"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
)

func newCommissionService(svc *Service) *Commission {
	s := &Commission{svc: svc}
	return s
}

func (h *Commission) GetByProjectIdAndMethod(projectId string, method string) (*billing.MerchantPaymentMethodCommissions, error) {
	project, err := h.svc.project.GetById(projectId)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	merchant, err := h.svc.merchant.GetById(project.MerchantId)
	if err != nil {
		return nil, fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	pm, ok := merchant.PaymentMethods[method]
	if ok {
		return pm.Commission, nil
	}

	return h.svc.getDefaultPaymentMethodCommissions(), nil
}

func (h *Commission) CalculatePmCommission(projectId string, method string, amount float64) (float64, error) {
	c, err := h.GetByProjectIdAndMethod(projectId, method)
	if err != nil {
		return float64(0), fmt.Errorf(errorNotFound, pkg.CollectionCommission)
	}

	return tools.FormatAmount(amount * (c.Fee / 100)), nil
}
