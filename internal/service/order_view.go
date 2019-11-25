package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/paylink"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"strings"
	"time"
)

const (
	collectionOrderView = "order_view"

	errorOrderViewUpdateQuery = "order query view update failed"
)

var (
	statusForRoyaltySummary = []string{
		constant.OrderPublicStatusProcessed,
		constant.OrderPublicStatusRefunded,
		constant.OrderPublicStatusChargeback,
	}
)

type royaltySummaryResult struct {
	Items []*billing.RoyaltyReportProductSummaryItem `bson:"top"`
	Total *billing.RoyaltyReportProductSummaryItem   `bson:"total"`
}

type conformPaylinkStatItemFn func(item *paylink.StatCommon)

type list []interface{}

type OrderViewServiceInterface interface {
	CountTransactions(ctx context.Context, match bson.M) (n int64, err error)
	GetTransactionsPublic(ctx context.Context, match bson.M, limit, offset int64) (result []*billing.OrderViewPublic, err error)
	GetTransactionsPrivate(ctx context.Context, match bson.M, limit, offset int64) (result []*billing.OrderViewPrivate, err error)
	GetRoyaltyOperatingCompaniesIds(ctx context.Context, merchantId, currency string, from, to time.Time) (ids []string, err error)
	GetRoyaltySummary(ctx context.Context, merchantId, operatingCompanyId, currency string, from, to time.Time) (items []*billing.RoyaltyReportProductSummaryItem, total *billing.RoyaltyReportProductSummaryItem, err error)
	GetOrderBy(ctx context.Context, id, uuid, merchantId string, receiver interface{}) (interface{}, error)
	GetPaylinkStat(ctx context.Context, paylinkId, merchantId string, from, to int64) (*paylink.StatCommon, error)
	GetPaylinkStatByCountry(ctx context.Context, paylinkId, merchantId string, from, to int64) (result *paylink.GroupStatCommon, err error)
	GetPaylinkStatByReferrer(ctx context.Context, paylinkId, merchantId string, from, to int64) (result *paylink.GroupStatCommon, err error)
	GetPaylinkStatByDate(ctx context.Context, paylinkId, merchantId string, from, to int64) (result *paylink.GroupStatCommon, err error)
	GetPaylinkStatByUtm(ctx context.Context, paylinkId, merchantId string, from, to int64) (result *paylink.GroupStatCommon, err error)
}

func newOrderView(svc *Service) OrderViewServiceInterface {
	s := &OrderView{svc: svc}
	return s
}

// emulate update batching, because aggregarion pipeline, ended with $merge,
// does not return any documents in result,
// so, this query cannot be iterated with driver's BatchSize() and Next() methods
func (s *Service) updateOrderView(ctx context.Context, ids []string) error {
	batchSize := s.cfg.OrderViewUpdateBatchSize
	count := len(ids)

	if count == 0 {
		res, err := s.db.Collection(collectionAccountingEntry).Distinct(ctx, "source.id", bson.M{})

		if err != nil {
			zap.S().Errorf(pkg.ErrorDatabaseQueryFailed, "err", err.Error(), "collection", collectionAccountingEntry)
			return err
		}

		for _, id := range res {
			ids = append(ids, id.(primitive.ObjectID).Hex())
		}

		count = len(ids)
	}

	if count > 0 && count <= batchSize {
		matchQuery := s.getUpdateOrderViewMatchQuery(ids)
		return s.doUpdateOrderView(ctx, matchQuery)
	}

	var batches [][]string

	for batchSize < len(ids) {
		ids, batches = ids[batchSize:], append(batches, ids[0:batchSize:batchSize])
	}
	batches = append(batches, ids)
	for _, batchIds := range batches {
		matchQuery := s.getUpdateOrderViewMatchQuery(batchIds)
		err := s.doUpdateOrderView(ctx, matchQuery)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) getUpdateOrderViewMatchQuery(ids []string) bson.M {
	idsHex := []primitive.ObjectID{}

	for _, id := range ids {
		oid, err := primitive.ObjectIDFromHex(id)

		if err != nil {
			continue
		}

		idsHex = append(idsHex, oid)
	}

	if len(idsHex) == 1 {
		return bson.M{
			"$match": bson.M{
				"_id": idsHex[0],
			},
		}
	}

	return bson.M{
		"$match": bson.M{
			"_id": bson.M{"$in": idsHex},
		},
	}
}

func (s *Service) doUpdateOrderView(ctx context.Context, match bson.M) error {
	defer timeTrack(time.Now(), "updateOrderView")

	orderViewQuery := []bson.M{
		match,
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_gross_revenue",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$local_amount",
							"currency": "$local_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_gross_revenue",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$original_amount",
							"currency": "$original_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue_origin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue_origin",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_gross_revenue",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_tax_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_tax_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$local_amount",
							"currency": "$local_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_tax_fee_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_tax_fee_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$original_amount",
							"currency": "$original_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_tax_fee_origin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_tax_fee_origin",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "central_bank_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_tax_fee_current_exchange_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_tax_fee_current_exchange_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "ps_gross_revenue_fx",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue_fx",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue_fx",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "ps_gross_revenue_fx_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue_fx_tax_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue_fx_tax_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_tax_fee_cost_value",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "tax_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$tax_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_tax_fee_central_bank_fx",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "tax_fee_currency_exchange_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$tax_fee_currency_exchange_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "ps_method_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "method_fee_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$method_fee_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_method_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "method_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$method_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_method_fee_cost_value",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_fee_tariff_self_cost",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_fee_tariff_self_cost",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_method_fixed_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "method_fixed_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$method_fixed_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_merchant_method_fixed_fee_cost_value",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_fixed_fee_tariff_self_cost",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_fixed_fee_tariff_self_cost",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_ps_fixed_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_fixed_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_fixed_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$local_amount",
							"currency": "$local_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_gross_revenue_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_gross_revenue_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$original_amount",
							"currency": "$original_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_gross_revenue_origin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_gross_revenue_origin",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_gross_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_gross_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$local_amount",
							"currency": "$local_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_tax_fee_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_tax_fee_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   "$original_amount",
							"currency": "$original_currency",
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_tax_fee_origin",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_tax_fee_origin",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_tax_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_tax_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_refund_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_refund_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "real_refund_fixed_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "method_refund_fixed_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$method_refund_fixed_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_refund",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_gross_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_gross_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_refund_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "method_refund_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$method_refund_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_refund_fixed_fee_cost_value",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_refund_fixed_fee_tariff_self_cost",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_refund_fixed_fee_tariff_self_cost",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "merchant_refund_fixed_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "merchant_refund_fixed_fee_tariff",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$merchant_refund_fixed_fee_tariff",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "reverse_tax_fee",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_tax_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_tax_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "reverse_tax_fee_delta",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_tax_fee_currency_exchange_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_tax_fee_currency_exchange_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": "ps_reverse_tax_fee_delta",
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_refund_tax_fee_currency_exchange_fee",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_refund_tax_fee_currency_exchange_fee",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"real_tax_fee",
									"central_bank_tax_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": "$amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_tax_fee_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_tax_fee_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_tax_fee_cost_value",
									"merchant_tax_fee_central_bank_fx",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": "$amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "tax_fee_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$tax_fee_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"reverse_tax_fee",
									"reverse_tax_fee_delta",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": "$amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_tax_fee_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_tax_fee_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"ps_method_fee",
									"merchant_ps_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": "$amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "fees_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$fees_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"ps_method_fee",
									"merchant_ps_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$local_currency",
							"amount": bson.M{
								"$sum": "$local_amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "fees_total_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$fees_total_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund_fee",
									"merchant_refund_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": "$amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_fees_total",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_fees_total",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund_fee",
									"merchant_refund_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$local_currency",
							"amount": bson.M{
								"$sum": "$local_amount",
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_fees_total_local",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_fees_total_local",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"ps_gross_revenue_fx",
									"ps_gross_revenue_fx_tax_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"ps_gross_revenue_fx_tax_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "payment_gross_revenue_fx_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$payment_gross_revenue_fx_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"real_gross_revenue",
									"ps_gross_revenue_fx",
								},
							},
						},
					},

					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"ps_gross_revenue_fx",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "gross_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$gross_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_method_fee",
									"merchant_method_fee_cost_value",
								},
							},
						},
					},

					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"merchant_method_fee_cost_value",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_fee_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_fee_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_method_fixed_fee",
									"real_merchant_method_fixed_fee",
								},
							},
						},
					},

					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_merchant_method_fixed_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_fixed_fee_tariff_fx_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_fixed_fee_tariff_fx_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"real_merchant_method_fixed_fee",
									"real_merchant_method_fixed_fee_cost_value",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_merchant_method_fixed_fee_cost_value",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_fixed_fee_tariff_total_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_fixed_fee_tariff_total_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_ps_fixed_fee",
									"real_merchant_ps_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_merchant_ps_fixed_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_fixed_fee_fx_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_fixed_fee_fx_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund",
									"real_refund",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_refund",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_gross_revenue_fx",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_gross_revenue_fx",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},

							"type": bson.M{
								"$in": []string{
									"merchant_refund_fee",
									"real_refund_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_refund_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_refund_fee_tariff_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_refund_fee_tariff_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund_fixed_fee",
									"real_refund_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"real_refund_fixed_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_refund_fixed_fee_tariff_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_refund_fixed_fee_tariff_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"real_gross_revenue",
									"merchant_tax_fee_central_bank_fx",
									"ps_gross_revenue_fx",
									"merchant_tax_fee_cost_value",
									"ps_method_fee",
									"merchant_ps_fixed_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$in": list{"$type", []string{
												"ps_gross_revenue_fx",
												"merchant_tax_fee_central_bank_fx",
												"merchant_tax_fee_cost_value",
												"ps_method_fee",
												"merchant_ps_fixed_fee",
											},
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "net_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$net_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"ps_method_fee",
									"merchant_ps_fixed_fee",
									"merchant_method_fee_cost_value",
									"real_merchant_method_fixed_fee_cost_value",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$in": list{"$type", []string{
												"merchant_method_fee_cost_value",
												"real_merchant_method_fixed_fee_cost_value",
											},
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_method_total_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_method_total_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"ps_gross_revenue_fx",
									"ps_method_fee",
									"merchant_ps_fixed_fee",
									"central_bank_tax_fee",
									"ps_gross_revenue_fx_tax_fee",
									"merchant_method_fee_cost_value",
									"real_merchant_method_fixed_fee_cost_value",
								},
							},
						},
					},

					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$in": list{"$type", []string{
												"central_bank_tax_fee",
												"ps_gross_revenue_fx_tax_fee",
												"merchant_method_fee_cost_value",
												"real_merchant_method_fixed_fee_cost_value",
											},
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_total_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_total_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund",
									"merchant_refund_fee",
									"merchant_refund_fixed_fee",
									"reverse_tax_fee_delta",
									"reverse_tax_fee",
								},
							},
						},
					},
					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$eq": []string{
												"$type",
												"reverse_tax_fee",
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "refund_reverse_revenue",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$refund_reverse_revenue",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$lookup": bson.M{
				"from": "accounting_entry",
				"let": bson.M{
					"order_id":    "$_id",
					"object_type": "$type",
				},
				"pipeline": []bson.M{
					{
						"$match": bson.M{
							"$expr": bson.M{
								"$and": list{
									bson.M{
										"$eq": []string{
											"$source.id",
											"$$order_id",
										}},
									bson.M{
										"$eq": []string{
											"$source.type",
											"$$object_type",
										}},
								},
							},
							"type": bson.M{
								"$in": []string{
									"merchant_refund_fee",
									"merchant_refund_fixed_fee",
									"ps_reverse_tax_fee_delta",
									"real_refund_fixed_fee",
									"real_refund_fee",
								},
							},
						},
					},

					{
						"$group": bson.M{
							"_id": "$currency",
							"amount": bson.M{
								"$sum": bson.M{
									"$cond": list{
										bson.M{
											"$in": list{"$type", []string{
												"real_refund_fixed_fee",
												"real_refund_fee",
											},
											},
										},
										bson.M{
											"$subtract": list{
												0,
												"$amount",
											},
										},
										"$amount",
									},
								},
							},
							"currency": bson.M{
								"$first": "$currency",
							},
						},
					},
					{
						"$project": bson.M{
							"amount":   1,
							"currency": 1,
							"_id":      0,
						},
					},
				},
				"as": "paysuper_refund_total_profit",
			},
		},
		{
			"$unwind": bson.M{
				"path":                       "$paysuper_refund_total_profit",
				"preserveNullAndEmptyArrays": true,
			},
		},
		{
			"$project": bson.M{
				"_id":                  1,
				"uuid":                 1,
				"pm_order_id":          1,
				"project_order_id":     1,
				"project":              1,
				"created_at":           1,
				"pm_order_close_date":  1,
				"total_payment_amount": 1,
				"amount_before_vat":    "$private_amount",
				"currency":             1,
				"user":                 1,
				"billing_address":      1,
				"payment_method":       1,
				"country_code":         1,
				"merchant_id":          "$project.merchant_id",
				"status":               1,
				"locale": bson.M{
					"$cond": list{
						bson.M{
							"$ne": list{"$user", nil},
						},
						"$user.locale",
						"",
					},
				},
				"type":                                              1,
				"royalty_report_id":                                 1,
				"is_vat_deduction":                                  1,
				"payment_gross_revenue_local":                       1,
				"payment_gross_revenue_origin":                      1,
				"payment_gross_revenue":                             1,
				"payment_tax_fee":                                   1,
				"payment_tax_fee_local":                             1,
				"payment_tax_fee_origin":                            1,
				"payment_tax_fee_current_exchange_fee":              1,
				"payment_tax_fee_total":                             1,
				"payment_gross_revenue_fx":                          1,
				"payment_gross_revenue_fx_tax_fee":                  1,
				"payment_gross_revenue_fx_profit":                   1,
				"gross_revenue":                                     1,
				"tax_fee":                                           1,
				"tax_fee_currency_exchange_fee":                     1,
				"tax_fee_total":                                     1,
				"method_fee_total":                                  1,
				"method_fee_tariff":                                 1,
				"paysuper_method_fee_tariff_self_cost":              1,
				"paysuper_method_fee_profit":                        1,
				"method_fixed_fee_tariff":                           1,
				"paysuper_method_fixed_fee_tariff_fx_profit":        1,
				"paysuper_method_fixed_fee_tariff_self_cost":        1,
				"paysuper_method_fixed_fee_tariff_total_profit":     1,
				"paysuper_fixed_fee":                                1,
				"paysuper_fixed_fee_fx_profit":                      1,
				"fees_total":                                        1,
				"fees_total_local":                                  1,
				"net_revenue":                                       1,
				"paysuper_method_total_profit":                      1,
				"paysuper_total_profit":                             1,
				"payment_refund_gross_revenue_local":                1,
				"payment_refund_gross_revenue_origin":               1,
				"payment_refund_gross_revenue":                      1,
				"payment_refund_tax_fee":                            1,
				"payment_refund_tax_fee_local":                      1,
				"payment_refund_tax_fee_origin":                     1,
				"payment_refund_fee_tariff":                         1,
				"method_refund_fixed_fee_tariff":                    1,
				"refund_gross_revenue":                              1,
				"refund_gross_revenue_fx":                           1,
				"method_refund_fee_tariff":                          1,
				"paysuper_method_refund_fee_tariff_profit":          1,
				"paysuper_method_refund_fixed_fee_tariff_self_cost": 1,
				"merchant_refund_fixed_fee_tariff":                  1,
				"paysuper_method_refund_fixed_fee_tariff_profit":    1,
				"refund_tax_fee":                                    1,
				"refund_tax_fee_currency_exchange_fee":              1,
				"paysuper_refund_tax_fee_currency_exchange_fee":     1,
				"refund_tax_fee_total":                              1,
				"refund_reverse_revenue":                            1,
				"refund_fees_total":                                 1,
				"refund_fees_total_local":                           1,
				"paysuper_refund_total_profit":                      1,
				"issuer":                                            1,
				"items":                                             1,
				"parent_order":                                      1,
				"refund":                                            1,
				"cancellation":                                      1,
				"mcc_code":                                          1,
				"operating_company_id":                              1,
				"is_high_risk":                                      1,
				"merchant_payout_currency": bson.M{
					"$ifNull": list{"$net_revenue.currency", "$refund_reverse_revenue.currency"},
				},
				"refund_allowed": bson.M{
					"$cond": list{
						bson.M{
							"$and": []bson.M{
								{"$eq": list{"$payment_method.refund_allowed", true}},
								{"$eq": list{"$type", "order"}},
								{"$eq": list{"$refunded", false}},
							},
						},
						true,
						false,
					},
				},
			},
		},
		{
			"$project": bson.M{
				"payment_method.params":            0,
				"payment_method.payment_system_id": 0,
			},
		},
		{
			"$merge": bson.M{
				"into":        "order_view",
				"whenMatched": "replace",
			},
		},
	}

	cursor, err := s.db.Collection(collectionOrder).Aggregate(ctx, orderViewQuery)

	if err != nil {
		zap.L().Error(
			errorOrderViewUpdateQuery,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrder),
		)
		return err
	}

	defer cursor.Close(ctx)

	cursor.Next(ctx)
	err = cursor.Err()

	if err != nil {
		zap.L().Error(
			errorOrderViewUpdateQuery,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrder),
		)
		return err
	}

	return nil
}

func (s *Service) RebuildOrderView(ctx context.Context) error {
	zap.L().Info("start rebuilding order view")

	err := s.updateOrderView(ctx, []string{})

	if err != nil {
		zap.L().Error("rebuilding order view failed with error", zap.Error(err))
		return err
	}

	zap.L().Info("rebuilding order view finished successfully")

	return nil
}

func (ow *OrderView) CountTransactions(ctx context.Context, match bson.M) (n int64, err error) {
	n, err = ow.svc.db.Collection(collectionOrderView).CountDocuments(ctx, match)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, match),
		)
	}
	return
}

func (ow *OrderView) GetTransactionsPublic(
	ctx context.Context,
	match bson.M,
	limit, offset int64,
) ([]*billing.OrderViewPublic, error) {
	sort := bson.M{"created_at": 1}
	opts := options.Find().
		SetSort(sort).
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := ow.svc.db.Collection(collectionOrderView).Find(ctx, match, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, match),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return nil, err
	}

	var result []*billing.OrderViewPublic
	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, match),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return nil, err
	}

	return result, nil
}

func (ow *OrderView) GetTransactionsPrivate(
	ctx context.Context,
	match bson.M,
	limit, offset int64,
) ([]*billing.OrderViewPrivate, error) {
	sort := bson.M{"created_at": 1}
	opts := options.Find().
		SetSort(sort).
		SetLimit(limit).
		SetSkip(offset)
	cursor, err := ow.svc.db.Collection(collectionOrderView).Find(ctx, match, opts)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, match),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return nil, err
	}

	var result []*billing.OrderViewPrivate
	err = cursor.All(ctx, &result)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, match),
			zap.Any(pkg.ErrorDatabaseFieldLimit, limit),
			zap.Any(pkg.ErrorDatabaseFieldOffset, offset),
		)
		return nil, err
	}

	return result, nil
}

func (ow *OrderView) getRoyaltySummaryGroupingQuery(isTotal bool) []bson.M {
	var groupingId bson.M
	if !isTotal {
		groupingId = bson.M{"product": "$product", "region": "$region"}
	} else {
		groupingId = nil
	}

	return []bson.M{
		{
			"$group": bson.M{
				"_id":                          groupingId,
				"product":                      bson.M{"$first": "$product"},
				"region":                       bson.M{"$first": "$region"},
				"currency":                     bson.M{"$first": "$currency"},
				"total_transactions":           bson.M{"$sum": 1},
				"gross_sales_amount":           bson.M{"$sum": "$purchase_gross_revenue"},
				"gross_returns_amount":         bson.M{"$sum": "$refund_gross_revenue"},
				"purchase_fees":                bson.M{"$sum": "$purchase_fees_total"},
				"refund_fees":                  bson.M{"$sum": "$refund_fees_total"},
				"purchase_tax":                 bson.M{"$sum": "$purchase_tax_fee_total"},
				"refund_tax":                   bson.M{"$sum": "$refund_tax_fee_total"},
				"net_revenue_total":            bson.M{"$sum": "$net_revenue"},
				"refund_reverse_revenue_total": bson.M{"$sum": "$refund_reverse_revenue"},
				"sales_count":                  bson.M{"$sum": bson.M{"$cond": list{bson.M{"$eq": list{"$status", "processed"}}, 1, 0}}},
			},
		},
		{
			"$addFields": bson.M{
				"returns_count":      bson.M{"$subtract": list{"$total_transactions", "$sales_count"}},
				"gross_total_amount": bson.M{"$subtract": list{"$gross_sales_amount", "$gross_returns_amount"}},
				"total_fees":         bson.M{"$sum": list{"$purchase_fees", "$refund_fees"}},
				"total_vat":          bson.M{"$subtract": list{"$purchase_tax", "$refund_tax"}},
				"payout_amount":      bson.M{"$subtract": list{"$net_revenue_total", "$refund_reverse_revenue_total"}},
			},
		},
		{
			"$sort": bson.M{"product": 1, "region": 1},
		},
	}
}

func (ow *OrderView) GetRoyaltySummary(
	ctx context.Context,
	merchantId, operatingCompanyId, currency string,
	from, to time.Time,
) (items []*billing.RoyaltyReportProductSummaryItem, total *billing.RoyaltyReportProductSummaryItem, err error) {
	items = []*billing.RoyaltyReportProductSummaryItem{}
	total = &billing.RoyaltyReportProductSummaryItem{}
	merchantOid, _ := primitive.ObjectIDFromHex(merchantId)

	query := []bson.M{
		{
			"$match": bson.M{
				"merchant_id":              merchantOid,
				"merchant_payout_currency": currency,
				"pm_order_close_date":      bson.M{"$gte": from, "$lte": to},
				"status":                   bson.M{"$in": statusForRoyaltySummary},
				"operating_company_id":     operatingCompanyId,
			},
		},
		{
			"$project": bson.M{
				"names": bson.M{
					"$filter": bson.M{
						"input": "$project.name",
						"as":    "name",
						"cond":  bson.M{"$eq": list{"$$name.lang", "en"}},
					},
				},
				"items": bson.M{
					"$cond": list{
						bson.M{"$ne": list{"$items", list{}}}, "$items", []string{""}},
				},
				"region":                 "$country_code",
				"status":                 1,
				"purchase_gross_revenue": "$gross_revenue.amount",
				"refund_gross_revenue":   "$refund_gross_revenue.amount",
				"purchase_tax_fee_total": "$tax_fee_total.amount",
				"refund_tax_fee_total":   "$refund_tax_fee_total.amount",
				"purchase_fees_total":    "$fees_total.amount",
				"refund_fees_total":      "$refund_fees_total.amount",
				"net_revenue":            "$net_revenue.amount",
				"refund_reverse_revenue": "$refund_reverse_revenue.amount",
				"amount_before_vat":      1,
				"currency":               "$merchant_payout_currency",
			},
		},
		{
			"$unwind": "$items",
		},
		{
			"$project": bson.M{
				"region":                   1,
				"status":                   1,
				"purchase_gross_revenue":   1,
				"refund_gross_revenue":     1,
				"purchase_tax_fee_total":   1,
				"refund_tax_fee_total":     1,
				"purchase_fees_total":      1,
				"refund_fees_total":        1,
				"net_revenue":              1,
				"refund_reverse_revenue":   1,
				"order_amount_without_vat": 1,
				"currency":                 1,
				"product": bson.M{
					"$cond": list{
						bson.M{"$eq": []string{"$items", ""}},
						bson.M{"$arrayElemAt": list{"$names.value", 0}},
						"$items.name",
					},
				},
				"correction": bson.M{
					"$cond": list{
						bson.M{"$eq": []string{"$items", ""}},
						1,
						bson.M{"$divide": list{"$items.amount", "$amount_before_vat"}},
					},
				},
			},
		},
		{
			"$project": bson.M{
				"product":                  1,
				"region":                   1,
				"status":                   1,
				"currency":                 1,
				"purchase_gross_revenue":   bson.M{"$multiply": list{"$purchase_gross_revenue", "$correction"}},
				"refund_gross_revenue":     bson.M{"$multiply": list{"$refund_gross_revenue", "$correction"}},
				"purchase_tax_fee_total":   bson.M{"$multiply": list{"$purchase_tax_fee_total", "$correction"}},
				"refund_tax_fee_total":     bson.M{"$multiply": list{"$refund_tax_fee_total", "$correction"}},
				"purchase_fees_total":      bson.M{"$multiply": list{"$purchase_fees_total", "$correction"}},
				"refund_fees_total":        bson.M{"$multiply": list{"$refund_fees_total", "$correction"}},
				"net_revenue":              bson.M{"$multiply": list{"$net_revenue", "$correction"}},
				"refund_reverse_revenue":   bson.M{"$multiply": list{"$refund_reverse_revenue", "$correction"}},
				"order_amount_without_vat": bson.M{"$multiply": list{"$order_amount_without_vat", "$correction"}},
			},
		},
		{
			"$facet": bson.M{
				"top":   ow.getRoyaltySummaryGroupingQuery(false),
				"total": ow.getRoyaltySummaryGroupingQuery(true),
			},
		},
		{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": list{"$total", 0}},
			},
		},
	}

	cursor, err := ow.svc.db.Collection(collectionOrderView).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrderView),
			zap.Any("query", query),
		)
		return
	}

	defer cursor.Close(ctx)

	var result *royaltySummaryResult
	if cursor.Next(ctx) {
		err = cursor.Decode(&result)
		if err != nil {
			return
		}
	}

	if result == nil {
		return
	}

	if result.Items != nil {
		items = result.Items
	}

	if result.Total != nil {
		total = result.Total
		total.Product = ""
		total.Region = ""
	}

	for _, item := range items {
		ow.royaltySummaryItemPrecise(item)
	}

	ow.royaltySummaryItemPrecise(total)

	return
}

func (ow *OrderView) royaltySummaryItemPrecise(item *billing.RoyaltyReportProductSummaryItem) {
	item.GrossSalesAmount = tools.ToPrecise(item.GrossSalesAmount)
	item.GrossReturnsAmount = tools.ToPrecise(item.GrossReturnsAmount)
	item.GrossTotalAmount = tools.ToPrecise(item.GrossTotalAmount)
	item.TotalFees = tools.ToPrecise(item.TotalFees)
	item.TotalVat = tools.ToPrecise(item.TotalVat)
	item.PayoutAmount = tools.ToPrecise(item.PayoutAmount)
}

func (ow *OrderView) GetOrderBy(
	ctx context.Context,
	id, uuid, merchantId string,
	receiver interface{},
) (interface{}, error) {
	query := bson.M{}

	if id != "" {
		query["_id"], _ = primitive.ObjectIDFromHex(id)
	}

	if uuid != "" {
		query["uuid"] = uuid
	}

	if merchantId != "" {
		query["project.merchant_id"], _ = primitive.ObjectIDFromHex(merchantId)
	}

	err := ow.svc.db.Collection(collectionOrderView).FindOne(ctx, query).Decode(receiver)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, orderErrorNotFound
		}

		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, orderErrorNotFound
	}

	return receiver, nil
}

func (ow *OrderView) GetPaylinkStatMatchQuery(paylinkId, merchantId string, from, to int64) []bson.M {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	matchQuery := bson.M{
		"merchant_id":           oid,
		"issuer.reference_type": pkg.OrderIssuerReferenceTypePaylink,
		"issuer.reference":      paylinkId,
	}

	if from > 0 || to > 0 {
		date := bson.M{}
		if from > 0 {
			date["$gte"] = time.Unix(from, 0)
		}
		if to > 0 {
			date["$lte"] = time.Unix(to, 0)
		}
		matchQuery["pm_order_close_date"] = date
	}

	return []bson.M{
		{
			"$match": matchQuery,
		},
	}
}

func (ow *OrderView) getPaylinkStatGroupingQuery(groupingId interface{}) []bson.M {
	return []bson.M{
		{
			"$group": bson.M{
				"_id":                   groupingId,
				"total_transactions":    bson.M{"$sum": 1},
				"gross_sales_amount":    bson.M{"$sum": "$payment_gross_revenue.amount"},
				"gross_returns_amount":  bson.M{"$sum": "$refund_gross_revenue.amount"},
				"sales_count":           bson.M{"$sum": bson.M{"$cond": list{bson.M{"$eq": list{"$status", "processed"}}, 1, 0}}},
				"country_code":          bson.M{"$first": "$country_code"},
				"transactions_currency": bson.M{"$first": "$merchant_payout_currency"},
			},
		},
		{
			"$addFields": bson.M{
				"returns_count":      bson.M{"$subtract": list{"$total_transactions", "$sales_count"}},
				"gross_total_amount": bson.M{"$subtract": list{"$gross_sales_amount", "$gross_returns_amount"}},
			},
		},
	}
}

func (ow *OrderView) GetPaylinkStat(
	ctx context.Context,
	paylinkId, merchantId string,
	from, to int64,
) (*paylink.StatCommon, error) {
	query := append(
		ow.GetPaylinkStatMatchQuery(paylinkId, merchantId, from, to),
		ow.getPaylinkStatGroupingQuery("$merchant_payout_currency")...,
	)

	var results []*paylink.StatCommon
	cursor, err := ow.svc.db.Collection(collectionOrderView).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	err = cursor.All(ctx, &results)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	resultsCount := len(results)

	if resultsCount == 1 {
		ow.paylinkStatItemPrecise(results[0])
		results[0].PaylinkId = paylinkId
		return results[0], nil
	}

	if resultsCount == 0 {
		return &paylink.StatCommon{}, nil
	}

	zap.L().Error(
		errorPaylinkStatDataInconsistent.Message,
	)

	return nil, errorPaylinkStatDataInconsistent
}

func (ow *OrderView) GetPaylinkStatByCountry(
	ctx context.Context,
	paylinkId, merchantId string,
	from, to int64,
) (result *paylink.GroupStatCommon, err error) {
	return ow.getPaylinkGroupStat(ctx, paylinkId, merchantId, from, to, "$country_code", func(item *paylink.StatCommon) {
		item.PaylinkId = paylinkId
		item.CountryCode = item.Id
	})
}

func (ow *OrderView) GetPaylinkStatByReferrer(
	ctx context.Context,
	paylinkId, merchantId string,
	from, to int64,
) (result *paylink.GroupStatCommon, err error) {
	return ow.getPaylinkGroupStat(ctx, paylinkId, merchantId, from, to, "$issuer.referrer_host", func(item *paylink.StatCommon) {
		item.PaylinkId = paylinkId
		item.ReferrerHost = item.Id
	})
}

func (ow *OrderView) GetPaylinkStatByDate(
	ctx context.Context,
	paylinkId, merchantId string,
	from, to int64,
) (result *paylink.GroupStatCommon, err error) {
	return ow.getPaylinkGroupStat(ctx, paylinkId, merchantId, from, to,
		bson.M{
			"$dateToString": bson.M{"format": "%Y-%m-%d", "date": "$pm_order_close_date"},
		},
		func(item *paylink.StatCommon) {
			item.Date = item.Id
			item.PaylinkId = paylinkId
		})
}

func (ow *OrderView) GetPaylinkStatByUtm(
	ctx context.Context,
	paylinkId, merchantId string,
	from, to int64,
) (result *paylink.GroupStatCommon, err error) {
	return ow.getPaylinkGroupStat(ctx, paylinkId, merchantId, from, to,
		bson.M{
			"$concat": list{"$issuer.utm_source", "&", "$issuer.utm_medium", "&", "$issuer.utm_campaign"},
		},
		func(item *paylink.StatCommon) {
			if item.Id != "" {
				utm := strings.Split(item.Id, "&")
				item.Utm = &paylink.Utm{
					UtmSource:   utm[0],
					UtmMedium:   utm[1],
					UtmCampaign: utm[2],
				}
			}
			item.PaylinkId = paylinkId
		})
}

func (ow *OrderView) getPaylinkGroupStat(
	ctx context.Context,
	paylinkId string,
	merchantId string,
	from, to int64,
	groupingId interface{},
	conformFn conformPaylinkStatItemFn,
) (result *paylink.GroupStatCommon, err error) {
	query := ow.GetPaylinkStatMatchQuery(paylinkId, merchantId, from, to)
	query = append(query, bson.M{
		"$facet": bson.M{
			"top": append(
				ow.getPaylinkStatGroupingQuery(groupingId),
				bson.M{"$sort": bson.M{"_id": 1}},
				bson.M{"$limit": 10},
			),
			"total": ow.getPaylinkStatGroupingQuery(""),
		},
	},
		bson.M{
			"$project": bson.M{
				"top":   "$top",
				"total": bson.M{"$arrayElemAt": list{"$total", 0}},
			},
		})

	cursor, err := ow.svc.db.Collection(collectionOrderView).Aggregate(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, err
	}

	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		err = cursor.Decode(&result)
		if err != nil {
			zap.L().Error(
				pkg.ErrorQueryCursorExecutionFailed,
				zap.Error(err),
				zap.String(pkg.ErrorDatabaseFieldCollection, collectionOrderView),
				zap.Any(pkg.ErrorDatabaseFieldQuery, query),
			)
			return nil, err
		}
	}

	if result == nil {
		return
	}

	for _, item := range result.Top {
		ow.paylinkStatItemPrecise(item)
		conformFn(item)
	}

	if result.Total == nil {
		result.Total = &paylink.StatCommon{
			PaylinkId: paylinkId,
		}
	}

	ow.paylinkStatItemPrecise(result.Total)
	conformFn(result.Total)

	return
}

func (ow *OrderView) paylinkStatItemPrecise(item *paylink.StatCommon) {
	item.GrossSalesAmount = tools.ToPrecise(item.GrossSalesAmount)
	item.GrossReturnsAmount = tools.ToPrecise(item.GrossReturnsAmount)
	item.GrossTotalAmount = tools.ToPrecise(item.GrossTotalAmount)
}

func (ow *OrderView) GetRoyaltyOperatingCompaniesIds(
	ctx context.Context,
	merchantId, currency string,
	from, to time.Time,
) ([]string, error) {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{
		"merchant_id":              oid,
		"merchant_payout_currency": currency,
		"pm_order_close_date":      bson.M{"$gte": from, "$lte": to},
		"status":                   bson.M{"$in": statusForRoyaltySummary},
	}
	res, err := ow.svc.db.Collection(collectionOrderView).Distinct(ctx, "operating_company_id", bson.M{})

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String("collection", collectionOrderView),
			zap.Any("query", query),
		)
		return nil, err
	}

	ids := make([]string, len(res))

	for i, v := range res {
		//ids = append(ids, v.(string))
		ids[i] = v.(string)
	}

	return ids, nil
}
