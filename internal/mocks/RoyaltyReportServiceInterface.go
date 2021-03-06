// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import billingpb "github.com/paysuper/paysuper-proto/go/billingpb"
import context "context"
import mock "github.com/stretchr/testify/mock"

import time "time"

// RoyaltyReportServiceInterface is an autogenerated mock type for the RoyaltyReportServiceInterface type
type RoyaltyReportServiceInterface struct {
	mock.Mock
}

// GetBalanceAmount provides a mock function with given fields: ctx, merchantId, currency
func (_m *RoyaltyReportServiceInterface) GetBalanceAmount(ctx context.Context, merchantId string, currency string) (float64, error) {
	ret := _m.Called(ctx, merchantId, currency)

	var r0 float64
	if rf, ok := ret.Get(0).(func(context.Context, string, string) float64); ok {
		r0 = rf(ctx, merchantId, currency)
	} else {
		r0 = ret.Get(0).(float64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, merchantId, currency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetById provides a mock function with given fields: ctx, id
func (_m *RoyaltyReportServiceInterface) GetById(ctx context.Context, id string) (*billingpb.RoyaltyReport, error) {
	ret := _m.Called(ctx, id)

	var r0 *billingpb.RoyaltyReport
	if rf, ok := ret.Get(0).(func(context.Context, string) *billingpb.RoyaltyReport); ok {
		r0 = rf(ctx, id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.RoyaltyReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, id)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByPayoutId provides a mock function with given fields: ctx, payoutId
func (_m *RoyaltyReportServiceInterface) GetByPayoutId(ctx context.Context, payoutId string) ([]*billingpb.RoyaltyReport, error) {
	ret := _m.Called(ctx, payoutId)

	var r0 []*billingpb.RoyaltyReport
	if rf, ok := ret.Get(0).(func(context.Context, string) []*billingpb.RoyaltyReport); ok {
		r0 = rf(ctx, payoutId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*billingpb.RoyaltyReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, payoutId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetNonPayoutReports provides a mock function with given fields: ctx, merchantId, currency
func (_m *RoyaltyReportServiceInterface) GetNonPayoutReports(ctx context.Context, merchantId string, currency string) ([]*billingpb.RoyaltyReport, error) {
	ret := _m.Called(ctx, merchantId, currency)

	var r0 []*billingpb.RoyaltyReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) []*billingpb.RoyaltyReport); ok {
		r0 = rf(ctx, merchantId, currency)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*billingpb.RoyaltyReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, merchantId, currency)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReportExists provides a mock function with given fields: ctx, merchantId, currency, from, to
func (_m *RoyaltyReportServiceInterface) GetReportExists(ctx context.Context, merchantId string, currency string, from time.Time, to time.Time) *billingpb.RoyaltyReport {
	ret := _m.Called(ctx, merchantId, currency, from, to)

	var r0 *billingpb.RoyaltyReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string, time.Time, time.Time) *billingpb.RoyaltyReport); ok {
		r0 = rf(ctx, merchantId, currency, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.RoyaltyReport)
		}
	}

	return r0
}

// Insert provides a mock function with given fields: ctx, document, ip, source
func (_m *RoyaltyReportServiceInterface) Insert(ctx context.Context, document *billingpb.RoyaltyReport, ip string, source string) error {
	ret := _m.Called(ctx, document, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *billingpb.RoyaltyReport, string, string) error); ok {
		r0 = rf(ctx, document, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetPaid provides a mock function with given fields: ctx, reportIds, payoutDocumentId, ip, source
func (_m *RoyaltyReportServiceInterface) SetPaid(ctx context.Context, reportIds []string, payoutDocumentId string, ip string, source string) error {
	ret := _m.Called(ctx, reportIds, payoutDocumentId, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, string, string, string) error); ok {
		r0 = rf(ctx, reportIds, payoutDocumentId, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetPayoutDocumentId provides a mock function with given fields: ctx, reportIds, payoutDocumentId, ip, source
func (_m *RoyaltyReportServiceInterface) SetPayoutDocumentId(ctx context.Context, reportIds []string, payoutDocumentId string, ip string, source string) error {
	ret := _m.Called(ctx, reportIds, payoutDocumentId, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, string, string, string) error); ok {
		r0 = rf(ctx, reportIds, payoutDocumentId, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UnsetPaid provides a mock function with given fields: ctx, reportIds, ip, source
func (_m *RoyaltyReportServiceInterface) UnsetPaid(ctx context.Context, reportIds []string, ip string, source string) error {
	ret := _m.Called(ctx, reportIds, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, string, string) error); ok {
		r0 = rf(ctx, reportIds, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UnsetPayoutDocumentId provides a mock function with given fields: ctx, reportIds, ip, source
func (_m *RoyaltyReportServiceInterface) UnsetPayoutDocumentId(ctx context.Context, reportIds []string, ip string, source string) error {
	ret := _m.Called(ctx, reportIds, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, string, string) error); ok {
		r0 = rf(ctx, reportIds, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: ctx, document, ip, source
func (_m *RoyaltyReportServiceInterface) Update(ctx context.Context, document *billingpb.RoyaltyReport, ip string, source string) error {
	ret := _m.Called(ctx, document, ip, source)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *billingpb.RoyaltyReport, string, string) error); ok {
		r0 = rf(ctx, document, ip, source)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
