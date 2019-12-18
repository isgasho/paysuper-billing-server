package billing

import (
	"encoding/json"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jinzhu/now"
	"time"
)

type JsonRefund struct {
	Id              string               `json:"id"`
	OriginalOrderId string               `json:"original_order_id"`
	ExternalId      string               `json:"external_id"`
	Amount          float64              `json:"amount"`
	CreatorId       string               `json:"creator_id"`
	Reason          string               `json:"reason"`
	Currency        string               `json:"currency"`
	Status          int32                `json:"status"`
	CreatedAt       *timestamp.Timestamp `json:"created_at"`
	UpdatedAt       *timestamp.Timestamp `json:"updated_at"`
	PayerData       *RefundPayerData     `json:"payer_data"`
	SalesTax        float32              `json:"sales_tax"`
}

type JsonVatReport struct {
	Id                    string  `json:"id"`
	Country               string  `json:"country"`
	VatRate               float64 `json:"vat_rate"`
	Currency              string  `json:"currency"`
	TransactionsCount     int32   `json:"transactions_count"`
	GrossRevenue          float64 `json:"gross_revenue"`
	VatAmount             float64 `json:"vat_amount"`
	FeesAmount            float64 `json:"fees_amount"`
	DeductionAmount       float64 `json:"deduction_amount"`
	CorrectionAmount      float64 `json:"correction_amount"`
	Status                string  `json:"status"`
	CountryAnnualTurnover float64 `json:"country_annual_turnover"`
	WorldAnnualTurnover   float64 `json:"world_annual_turnover"`
	AmountsApproximate    bool    `json:"amounts_approximate"`
	DateFrom              string  `json:"date_from"`
	DateTo                string  `json:"date_to"`
	PayUntilDate          string  `json:"pay_until_date"`
	CreatedAt             string  `json:"created_at"`
	UpdatedAt             string  `json:"updated_at"`
	PaidAt                string  `json:"paid_at"`
}

type JsonRoyaltyReportOrder struct {
	Date         int64   `json:"date"`
	Country      string  `json:"country"`
	PaymentId    string  `json:"payment_id"`
	Method       string  `json:"method"`
	Amount       float64 `json:"amount"`
	Vat          float64 `json:"vat"`
	Commission   float64 `json:"commission"`
	PayoutAmount float64 `json:"payout_amount"`
}

type JsonMerchantBalance struct {
	MerchantId     string  `json:"merchant_id"`
	Currency       string  `json:"currency"`
	Debit          float64 `json:"debit"`
	Credit         float64 `json:"credit"`
	RollingReserve float64 `json:"rolling_reserve"`
	Total          float64 `json:"total"`
	CreatedAt      string  `json:"created_at"`
}

func (m *Order) MarshalJSON() ([]byte, error) {
	type Alias Order

	createdAt, _ := ptypes.Timestamp(m.CreatedAt)
	canceledAt, _ := ptypes.Timestamp(m.CanceledAt)
	refundedAt, _ := ptypes.Timestamp(m.RefundedAt)

	return json.Marshal(&struct {
		*Alias
		CreatedAt  time.Time `json:"created_at"`
		CanceledAt time.Time `json:"canceled_at"`
		RefundedAt time.Time `json:"refunded_at"`
	}{
		Alias:      (*Alias)(m),
		CreatedAt:  createdAt,
		CanceledAt: canceledAt,
		RefundedAt: refundedAt,
	})
}

func (m *OrderItem) MarshalJSON() ([]byte, error) {
	type Alias OrderItem

	createdAt, _ := ptypes.Timestamp(m.CreatedAt)
	updatedAt, _ := ptypes.Timestamp(m.UpdatedAt)

	return json.Marshal(&struct {
		*Alias
		CreatedAt time.Time `json:"created_at"`
		UpdatedAt time.Time `json:"updated_at"`
	}{
		Alias:     (*Alias)(m),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	})
}

func (m *Refund) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		&JsonRefund{
			Id:              m.Id,
			OriginalOrderId: m.OriginalOrder.Uuid,
			ExternalId:      m.ExternalId,
			Amount:          m.Amount,
			CreatorId:       m.CreatorId,
			Reason:          m.Reason,
			Currency:        m.Currency,
			Status:          m.Status,
			CreatedAt:       m.CreatedAt,
			UpdatedAt:       m.UpdatedAt,
			PayerData:       m.PayerData,
			SalesTax:        m.SalesTax,
		},
	)
}

func (m *VatReport) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		&JsonVatReport{
			Id:                    m.Id,
			Country:               m.Country,
			VatRate:               m.VatRate,
			Currency:              m.Currency,
			TransactionsCount:     m.TransactionsCount,
			GrossRevenue:          m.GrossRevenue,
			VatAmount:             m.VatAmount,
			FeesAmount:            m.FeesAmount,
			DeductionAmount:       m.DeductionAmount,
			CorrectionAmount:      m.CorrectionAmount,
			Status:                m.Status,
			CountryAnnualTurnover: m.CountryAnnualTurnover,
			WorldAnnualTurnover:   m.WorldAnnualTurnover,
			AmountsApproximate:    m.AmountsApproximate,
			DateFrom:              ptypes.TimestampString(m.DateFrom),
			DateTo:                ptypes.TimestampString(m.DateTo),
			PayUntilDate:          ptypes.TimestampString(m.PayUntilDate),
			CreatedAt:             ptypes.TimestampString(m.CreatedAt),
			UpdatedAt:             ptypes.TimestampString(m.UpdatedAt),
			PaidAt:                ptypes.TimestampString(m.PaidAt),
		},
	)
}

func (m *VatReport) UnmarshalJSON(b []byte) error {
	var decoded JsonVatReport
	err := json.Unmarshal(b, &decoded)
	if err != nil {
		return err
	}

	m.Id = decoded.Id
	m.Country = decoded.Country
	m.VatRate = decoded.VatRate
	m.Currency = decoded.Currency
	m.TransactionsCount = decoded.TransactionsCount
	m.GrossRevenue = decoded.GrossRevenue
	m.VatAmount = decoded.VatAmount
	m.FeesAmount = decoded.FeesAmount
	m.DeductionAmount = decoded.DeductionAmount
	m.CorrectionAmount = decoded.CorrectionAmount
	m.Status = decoded.Status
	m.CountryAnnualTurnover = decoded.CountryAnnualTurnover
	m.WorldAnnualTurnover = decoded.WorldAnnualTurnover
	m.AmountsApproximate = decoded.AmountsApproximate

	DateFrom, err := time.Parse(time.RFC3339Nano, decoded.DateFrom)
	if err != nil {
		return err
	}
	DateFrom = now.New(DateFrom).BeginningOfDay()
	m.DateFrom, err = ptypes.TimestampProto(DateFrom)
	if err != nil {
		return err
	}

	DateTo, err := time.Parse(time.RFC3339Nano, decoded.DateTo)
	if err != nil {
		return err
	}
	DateTo = now.New(DateTo).EndOfDay()
	m.DateTo, err = ptypes.TimestampProto(DateTo)
	if err != nil {
		return err
	}

	PayUntilDate, err := time.Parse(time.RFC3339Nano, decoded.PayUntilDate)
	if err != nil {
		return err
	}
	PayUntilDate = now.New(PayUntilDate).EndOfDay()
	m.DateTo, err = ptypes.TimestampProto(PayUntilDate)
	if err != nil {
		return err
	}

	CreatedAt, err := time.Parse(time.RFC3339Nano, decoded.CreatedAt)
	if err != nil {
		return err
	}
	m.CreatedAt, err = ptypes.TimestampProto(CreatedAt)
	if err != nil {
		return err
	}

	UpdatedAt, err := time.Parse(time.RFC3339Nano, decoded.UpdatedAt)
	if err != nil {
		return err
	}
	m.UpdatedAt, err = ptypes.TimestampProto(UpdatedAt)
	if err != nil {
		return err
	}

	PaidAt, err := time.Parse(time.RFC3339Nano, decoded.PaidAt)
	if err != nil {
		return err
	}
	m.PaidAt, err = ptypes.TimestampProto(PaidAt)
	if err != nil {
		return err
	}

	return nil
}

func (m *MerchantBalance) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		&JsonMerchantBalance{
			MerchantId:     m.MerchantId,
			Currency:       m.Currency,
			Debit:          m.Debit,
			Credit:         m.Credit,
			RollingReserve: m.RollingReserve,
			Total:          m.Total,
			CreatedAt:      ptypes.TimestampString(m.CreatedAt),
		},
	)
}
