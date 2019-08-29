package service

type DashboardRepositoryInterface interface {
}

func (h *DashboardRepository) GetMerchantDashboardMain(merchantId, period string) error {
	return nil
}
