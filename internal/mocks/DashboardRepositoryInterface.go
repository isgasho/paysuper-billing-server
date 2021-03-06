// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import billingpb "github.com/paysuper/paysuper-proto/go/billingpb"
import context "context"
import mock "github.com/stretchr/testify/mock"

// DashboardRepositoryInterface is an autogenerated mock type for the DashboardRepositoryInterface type
type DashboardRepositoryInterface struct {
	mock.Mock
}

// GetBaseReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetBaseReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardBaseReports, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardBaseReports
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardBaseReports); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardBaseReports)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBaseRevenueByCountryReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetBaseRevenueByCountryReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardRevenueByCountryReport, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardRevenueByCountryReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardRevenueByCountryReport); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardRevenueByCountryReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBaseSalesTodayReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetBaseSalesTodayReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardSalesTodayReport, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardSalesTodayReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardSalesTodayReport); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardSalesTodayReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBaseSourcesReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetBaseSourcesReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardSourcesReport, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardSourcesReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardSourcesReport); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardSourcesReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetMainReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetMainReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardMainReport, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardMainReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardMainReport); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardMainReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRevenueDynamicsReport provides a mock function with given fields: _a0, _a1, _a2
func (_m *DashboardRepositoryInterface) GetRevenueDynamicsReport(_a0 context.Context, _a1 string, _a2 string) (*billingpb.DashboardRevenueDynamicReport, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *billingpb.DashboardRevenueDynamicReport
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *billingpb.DashboardRevenueDynamicReport); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.DashboardRevenueDynamicReport)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
