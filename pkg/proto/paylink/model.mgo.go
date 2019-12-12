package paylink

import (
	"errors"
	"github.com/golang/protobuf/ptypes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

const (
	errorInvalidObjectId = "invalid bson object id"
)

type MgoPaylink struct {
	Id                   primitive.ObjectID `bson:"_id"`
	Object               string             `bson:"object"`
	Products             []string           `bson:"products"`
	ExpiresAt            time.Time          `bson:"expires_at"`
	CreatedAt            time.Time          `bson:"created_at"`
	UpdatedAt            time.Time          `bson:"updated_at"`
	MerchantId           primitive.ObjectID `bson:"merchant_id"`
	ProjectId            primitive.ObjectID `bson:"project_id"`
	Name                 string             `bson:"name"`
	ProductsType         string             `bson:"products_type"`
	IsExpired            bool               `bson:"is_expired"`
	Visits               int32              `bson:"visits"`
	NoExpiryDate         bool               `bson:"no_expiry_date"`
	TotalTransactions    int32              `bson:"total_transactions"`
	SalesCount           int32              `bson:"sales_count"`
	ReturnsCount         int32              `bson:"returns_count"`
	Conversion           float64            `bson:"conversion"`
	GrossSalesAmount     float64            `bson:"gross_sales_amount"`
	GrossReturnsAmount   float64            `bson:"gross_returns_amount"`
	GrossTotalAmount     float64            `bson:"gross_total_amount"`
	TransactionsCurrency string             `bson:"transactions_currency"`
	Deleted              bool               `bson:"deleted"`
}

// RateData.SetBSON
func (p *Paylink) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoPaylink)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	p.Id = decoded.Id.Hex()
	p.MerchantId = decoded.MerchantId.Hex()
	p.ProjectId = decoded.ProjectId.Hex()
	p.Object = decoded.Object
	p.Products = decoded.Products
	p.Name = decoded.Name
	p.ProductsType = decoded.ProductsType
	p.Deleted = decoded.Deleted
	p.NoExpiryDate = decoded.NoExpiryDate
	p.TotalTransactions = decoded.TotalTransactions
	p.SalesCount = decoded.SalesCount
	p.ReturnsCount = decoded.ReturnsCount
	p.Conversion = decoded.Conversion
	p.GrossSalesAmount = decoded.GrossSalesAmount
	p.GrossReturnsAmount = decoded.GrossReturnsAmount
	p.GrossTotalAmount = decoded.GrossTotalAmount
	p.TransactionsCurrency = decoded.TransactionsCurrency
	p.Visits = decoded.Visits

	p.CreatedAt, err = ptypes.TimestampProto(decoded.CreatedAt)
	if err != nil {
		return err
	}

	p.UpdatedAt, err = ptypes.TimestampProto(decoded.UpdatedAt)
	if err != nil {
		return err
	}

	p.ExpiresAt, err = ptypes.TimestampProto(decoded.ExpiresAt)
	if err != nil {
		return err
	}

	p.IsExpired = p.IsPaylinkExpired()

	return nil
}

// RateData.GetBSON
func (p *Paylink) MarshalBSON() ([]byte, error) {
	st := &MgoPaylink{
		Object:               p.Object,
		Products:             p.Products,
		Name:                 p.Name,
		ProductsType:         p.ProductsType,
		Deleted:              p.Deleted,
		NoExpiryDate:         p.NoExpiryDate,
		TotalTransactions:    p.TotalTransactions,
		SalesCount:           p.SalesCount,
		ReturnsCount:         p.ReturnsCount,
		Conversion:           p.Conversion,
		GrossSalesAmount:     p.GrossSalesAmount,
		GrossReturnsAmount:   p.GrossReturnsAmount,
		GrossTotalAmount:     p.GrossTotalAmount,
		TransactionsCurrency: p.TransactionsCurrency,
		Visits:               p.Visits,
	}

	if len(p.Id) <= 0 {
		st.Id = primitive.NewObjectID()
	} else {
		oid, err := primitive.ObjectIDFromHex(p.Id)

		if err != nil {
			return nil, errors.New(errorInvalidObjectId)
		}
		st.Id = oid
	}

	oid, err := primitive.ObjectIDFromHex(p.MerchantId)

	if err != nil {
		return nil, errors.New(errorInvalidObjectId)
	}

	st.MerchantId = oid
	oid, err = primitive.ObjectIDFromHex(p.ProjectId)

	if err != nil {
		return nil, errors.New(errorInvalidObjectId)
	}

	st.ProjectId = oid
	st.CreatedAt, err = ptypes.Timestamp(p.CreatedAt)

	if err != nil {
		return nil, err
	}

	st.UpdatedAt, err = ptypes.Timestamp(p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	st.ExpiresAt, err = ptypes.Timestamp(p.ExpiresAt)
	if err != nil && err.Error() != "timestamp: nil Timestamp" {
		return nil, err
	}

	st.IsExpired = p.IsPaylinkExpired()

	return bson.Marshal(st)
}
