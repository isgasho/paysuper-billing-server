// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import billingpb "github.com/paysuper/paysuper-proto/go/billingpb"
import context "context"
import mock "github.com/stretchr/testify/mock"
import primitive "go.mongodb.org/mongo-driver/bson/primitive"

import time "time"

// OrderViewServiceInterface is an autogenerated mock type for the OrderViewServiceInterface type
type OrderViewServiceInterface struct {
	mock.Mock
}

// CountTransactions provides a mock function with given fields: ctx, match
func (_m *OrderViewServiceInterface) CountTransactions(ctx context.Context, match primitive.M) (int64, error) {
	ret := _m.Called(ctx, match)

	var r0 int64
	if rf, ok := ret.Get(0).(func(context.Context, primitive.M) int64); ok {
		r0 = rf(ctx, match)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, primitive.M) error); ok {
		r1 = rf(ctx, match)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetOrderBy provides a mock function with given fields: ctx, id, uuid, merchantId, receiver
func (_m *OrderViewServiceInterface) GetOrderBy(ctx context.Context, id string, uuid string, merchantId string, receiver interface{}) (interface{}, error) {
	ret := _m.Called(ctx, id, uuid, merchantId, receiver)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, interface{}) interface{}); ok {
		r0 = rf(ctx, id, uuid, merchantId, receiver)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, string, interface{}) error); ok {
		r1 = rf(ctx, id, uuid, merchantId, receiver)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPaylinkStat provides a mock function with given fields: ctx, paylinkId, merchantId, from, to
func (_m *OrderViewServiceInterface) GetPaylinkStat(ctx context.Context, paylinkId string, merchantId string, from int64, to int64) (*billingpb.StatCommon, error) {
	ret := _m.Called(ctx, paylinkId, merchantId, from, to)

	var r0 *billingpb.StatCommon
	if rf, ok := ret.Get(0).(func(context.Context, string, string, int64, int64) *billingpb.StatCommon); ok {
		r0 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.StatCommon)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, int64, int64) error); ok {
		r1 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPaylinkStatByCountry provides a mock function with given fields: ctx, paylinkId, merchantId, from, to
func (_m *OrderViewServiceInterface) GetPaylinkStatByCountry(ctx context.Context, paylinkId string, merchantId string, from int64, to int64) (*billingpb.GroupStatCommon, error) {
	ret := _m.Called(ctx, paylinkId, merchantId, from, to)

	var r0 *billingpb.GroupStatCommon
	if rf, ok := ret.Get(0).(func(context.Context, string, string, int64, int64) *billingpb.GroupStatCommon); ok {
		r0 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.GroupStatCommon)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, int64, int64) error); ok {
		r1 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPaylinkStatByDate provides a mock function with given fields: ctx, paylinkId, merchantId, from, to
func (_m *OrderViewServiceInterface) GetPaylinkStatByDate(ctx context.Context, paylinkId string, merchantId string, from int64, to int64) (*billingpb.GroupStatCommon, error) {
	ret := _m.Called(ctx, paylinkId, merchantId, from, to)

	var r0 *billingpb.GroupStatCommon
	if rf, ok := ret.Get(0).(func(context.Context, string, string, int64, int64) *billingpb.GroupStatCommon); ok {
		r0 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.GroupStatCommon)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, int64, int64) error); ok {
		r1 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPaylinkStatByReferrer provides a mock function with given fields: ctx, paylinkId, merchantId, from, to
func (_m *OrderViewServiceInterface) GetPaylinkStatByReferrer(ctx context.Context, paylinkId string, merchantId string, from int64, to int64) (*billingpb.GroupStatCommon, error) {
	ret := _m.Called(ctx, paylinkId, merchantId, from, to)

	var r0 *billingpb.GroupStatCommon
	if rf, ok := ret.Get(0).(func(context.Context, string, string, int64, int64) *billingpb.GroupStatCommon); ok {
		r0 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.GroupStatCommon)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, int64, int64) error); ok {
		r1 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPaylinkStatByUtm provides a mock function with given fields: ctx, paylinkId, merchantId, from, to
func (_m *OrderViewServiceInterface) GetPaylinkStatByUtm(ctx context.Context, paylinkId string, merchantId string, from int64, to int64) (*billingpb.GroupStatCommon, error) {
	ret := _m.Called(ctx, paylinkId, merchantId, from, to)

	var r0 *billingpb.GroupStatCommon
	if rf, ok := ret.Get(0).(func(context.Context, string, string, int64, int64) *billingpb.GroupStatCommon); ok {
		r0 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.GroupStatCommon)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, int64, int64) error); ok {
		r1 = rf(ctx, paylinkId, merchantId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetPublicByOrderId provides a mock function with given fields: ctx, merchantId
func (_m *OrderViewServiceInterface) GetPublicByOrderId(ctx context.Context, merchantId string) (*billingpb.OrderViewPublic, error) {
	ret := _m.Called(ctx, merchantId)

	var r0 *billingpb.OrderViewPublic
	if rf, ok := ret.Get(0).(func(context.Context, string) *billingpb.OrderViewPublic); ok {
		r0 = rf(ctx, merchantId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*billingpb.OrderViewPublic)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, merchantId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetRoyaltySummary provides a mock function with given fields: ctx, merchantId, currency, from, to
func (_m *OrderViewServiceInterface) GetRoyaltySummary(ctx context.Context, merchantId string, currency string, from time.Time, to time.Time) ([]*billingpb.RoyaltyReportProductSummaryItem, *billingpb.RoyaltyReportProductSummaryItem, error) {
	ret := _m.Called(ctx, merchantId, currency, from, to)

	var r0 []*billingpb.RoyaltyReportProductSummaryItem
	if rf, ok := ret.Get(0).(func(context.Context, string, string, time.Time, time.Time) []*billingpb.RoyaltyReportProductSummaryItem); ok {
		r0 = rf(ctx, merchantId, currency, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*billingpb.RoyaltyReportProductSummaryItem)
		}
	}

	var r1 *billingpb.RoyaltyReportProductSummaryItem
	if rf, ok := ret.Get(1).(func(context.Context, string, string, time.Time, time.Time) *billingpb.RoyaltyReportProductSummaryItem); ok {
		r1 = rf(ctx, merchantId, currency, from, to)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*billingpb.RoyaltyReportProductSummaryItem)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string, string, time.Time, time.Time) error); ok {
		r2 = rf(ctx, merchantId, currency, from, to)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// GetTransactionsPrivate provides a mock function with given fields: ctx, match, limit, offset
func (_m *OrderViewServiceInterface) GetTransactionsPrivate(ctx context.Context, match primitive.M, limit int64, offset int64) ([]*billingpb.OrderViewPrivate, error) {
	ret := _m.Called(ctx, match, limit, offset)

	var r0 []*billingpb.OrderViewPrivate
	if rf, ok := ret.Get(0).(func(context.Context, primitive.M, int64, int64) []*billingpb.OrderViewPrivate); ok {
		r0 = rf(ctx, match, limit, offset)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*billingpb.OrderViewPrivate)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, primitive.M, int64, int64) error); ok {
		r1 = rf(ctx, match, limit, offset)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetTransactionsPublic provides a mock function with given fields: ctx, match, limit, offset
func (_m *OrderViewServiceInterface) GetTransactionsPublic(ctx context.Context, match primitive.M, limit int64, offset int64) ([]*billingpb.OrderViewPublic, error) {
	ret := _m.Called(ctx, match, limit, offset)

	var r0 []*billingpb.OrderViewPublic
	if rf, ok := ret.Get(0).(func(context.Context, primitive.M, int64, int64) []*billingpb.OrderViewPublic); ok {
		r0 = rf(ctx, match, limit, offset)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*billingpb.OrderViewPublic)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, primitive.M, int64, int64) error); ok {
		r1 = rf(ctx, match, limit, offset)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
