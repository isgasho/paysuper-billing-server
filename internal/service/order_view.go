package service

import (
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"go.uber.org/zap"
	"time"
)

const (
	collectionOrderView = "order_view"

	errorOrderViewUpdateQuery = "order query view update failed"
)

type list []interface{}

func (s *Service) getOrderFromViewPublic(id string) (*billing.OrderViewPublic, error) {
	result := &billing.OrderViewPublic{}
	err := s.db.Collection(collectionOrderView).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) getOrderFromViewPrivate(id string) (*billing.OrderViewPrivate, error) {
	result := &billing.OrderViewPrivate{}
	err := s.db.Collection(collectionOrderView).
		Find(bson.M{"_id": bson.ObjectIdHex(id)}).
		One(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) getTransactionsPublic(match bson.M, pagination ...int) ([]*billing.OrderViewPublic, error) {
	result := []*billing.OrderViewPublic{}

	dbRequest := s.db.Collection(collectionOrderView).Find(match).Sort("created_at")

	if pagination != nil {
		if val := pagination[0]; val > 0 {
			dbRequest.Limit(val)
		}
		if val := pagination[1]; val > 0 {
			dbRequest.Skip(val)
		}
	}

	err := dbRequest.All(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Service) getTransactionsPrivate(match bson.M, pagination ...int) ([]*billing.OrderViewPrivate, error) {
	result := []*billing.OrderViewPrivate{}

	dbRequest := s.db.Collection(collectionOrderView).Find(match).Sort("created_at")

	if pagination != nil {
		if val := pagination[0]; val > 0 {
			dbRequest.Limit(val)
		}
		if val := pagination[1]; val > 0 {
			dbRequest.Skip(val)
		}
	}

	err := dbRequest.All(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// emulate update batching, because aggregarion pipeline, ended with $merge,
// does not return any documents in result,
// so, this query cannot be iterated with driver's BatchSize() and Next() methods
func (s *Service) updateOrderView(ids []string) error {
	batchSize := s.cfg.OrderViewUpdateBatchSize
	count := len(ids)
	if count == 0 {
		var orderIds []*billing.Id
		err := s.db.Collection(collectionOrder).Find(nil).All(&orderIds)
		if err != nil {
			return err
		}
		for _, id := range orderIds {
			ids = append(ids, id.Id)
		}
		count = len(ids)
	}

	if count > 0 && count <= batchSize {
		matchQuery := s.getUpdateOrderViewMatchQuery(ids)
		return s.doUpdateOrderView(matchQuery)
	}

	var batches [][]string

	for batchSize < len(ids) {
		ids, batches = ids[batchSize:], append(batches, ids[0:batchSize:batchSize])
	}
	batches = append(batches, ids)
	for _, batch_ids := range batches {
		matchQuery := s.getUpdateOrderViewMatchQuery(batch_ids)
		err := s.doUpdateOrderView(matchQuery)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) getUpdateOrderViewMatchQuery(ids []string) bson.M {
	idsHex := []bson.ObjectId{}
	for _, id := range ids {
		idsHex = append(idsHex, bson.ObjectIdHex(id))
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

func (s *Service) doUpdateOrderView(match bson.M) error {
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
				"project":              1,
				"created_at":           1,
				"pm_order_close_date":  1,
				"total_payment_amount": 1,
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

	err := s.db.Collection(collectionOrder).Pipe(orderViewQuery).Iter().Err()
	if err != nil {
		zap.L().Error(
			errorOrderViewUpdateQuery,
			zap.Error(err),
		)
		return err
	}
	return nil
}
