package grpc

import (
	"errors"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

const (
	errorInvalidObjectId = "invalid bson object id"
)

type MgoKeyProduct struct {
	Id              primitive.ObjectID       `bson:"_id" json:"id"`
	Object          string                   `bson:"object" json:"object"`
	Sku             string                   `bson:"sku" json:"sku"`
	Name            []*I18NTextSearchable    `bson:"name" json:"name"`
	DefaultCurrency string                   `bson:"default_currency" json:"default_currency"`
	Enabled         bool                     `bson:"enabled" json:"enabled"`
	Platforms       []*MgoPlatformPrice      `bson:"platforms" json:"platforms"`
	Description     map[string]string        `bson:"description" json:"description"`
	LongDescription map[string]string        `bson:"long_description,omitempty" json:"long_description"`
	CreatedAt       time.Time                `bson:"created_at" json:"created_at"`
	UpdatedAt       time.Time                `bson:"updated_at" json:"updated_at"`
	PublishedAt     *time.Time               `bson:"published_at" json:"published_at"`
	Cover           *billing.ImageCollection `bson:"cover" json:"cover"`
	Url             string                   `bson:"url,omitempty" json:"url"`
	Metadata        map[string]string        `bson:"metadata,omitempty" json:"metadata"`
	Deleted         bool                     `bson:"deleted" json:"deleted"`
	MerchantId      primitive.ObjectID       `bson:"merchant_id" json:"-"`
	ProjectId       primitive.ObjectID       `bson:"project_id" json:"project_id"`
	Pricing         string                   `bson:"pricing" json:"pricing"`
}

type MgoPlatformPrice struct {
	Prices        []*billing.ProductPrice `bson:"prices" json:"prices"`
	Id            string                  `bson:"id" json:"id"`
	Name          string                  `bson:"name" json:"name"`
	EulaUrl       string                  `bson:"eula_url" json:"eula_url"`
	ActivationUrl string                  `bson:"activation_url" json:"activation_url"`
}

type MgoProduct struct {
	Id              primitive.ObjectID      `bson:"_id" json:"id"`
	Object          string                  `bson:"object" json:"object"`
	Type            string                  `bson:"type" json:"type"`
	Sku             string                  `bson:"sku" json:"sku"`
	Name            []*I18NTextSearchable   `bson:"name" json:"name"`
	DefaultCurrency string                  `bson:"default_currency" json:"default_currency"`
	Enabled         bool                    `bson:"enabled" json:"enabled"`
	Prices          []*billing.ProductPrice `bson:"prices" json:"prices"`
	Description     map[string]string       `bson:"description" json:"description"`
	LongDescription map[string]string       `bson:"long_description,omitempty" json:"long_description"`
	CreatedAt       time.Time               `bson:"created_at" json:"created_at"`
	UpdatedAt       time.Time               `bson:"updated_at" json:"updated_at"`
	Images          []string                `bson:"images,omitempty" json:"images"`
	Url             string                  `bson:"url,omitempty" json:"url"`
	Metadata        map[string]string       `bson:"metadata,omitempty" json:"metadata"`
	Deleted         bool                    `bson:"deleted" json:"deleted"`
	MerchantId      primitive.ObjectID      `bson:"merchant_id" json:"-"`
	ProjectId       primitive.ObjectID      `bson:"project_id" json:"project_id"`
	Pricing         string                  `bson:"pricing" json:"pricing"`
	BillingType     string                  `bson:"billing_type" json:"billing_type"`
}

type MgoUserProfileEmail struct {
	Email                   string    `bson:"email"`
	Confirmed               bool      `bson:"confirmed"`
	ConfirmedAt             time.Time `bson:"confirmed_at"`
	IsConfirmationEmailSent bool      `bson:"is_confirmation_email_sent"`
}

type MgoUserProfile struct {
	Id        primitive.ObjectID   `bson:"_id"`
	UserId    string               `bson:"user_id"`
	Email     *MgoUserProfileEmail `bson:"email"`
	Personal  *UserProfilePersonal `bson:"personal"`
	Help      *UserProfileHelp     `bson:"help"`
	Company   *UserProfileCompany  `bson:"company"`
	LastStep  string               `bson:"last_step"`
	CreatedAt time.Time            `bson:"created_at"`
	UpdatedAt time.Time            `bson:"updated_at"`
}

type MgoPageReview struct {
	Id        primitive.ObjectID `bson:"_id"`
	UserId    string             `bson:"user_id"`
	Review    string             `bson:"review"`
	Url       string             `bson:"url"`
	IsRead    bool               `bson:"is_read"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

type MgoDashboardAmountItemWithChart struct {
	Amount   float64                    `bson:"amount"`
	Currency string                     `bson:"currency"`
	Chart    []*DashboardChartItemFloat `bson:"chart"`
}

type MgoDashboardRevenueDynamicReportItem struct {
	Label    int64   `bson:"label"`
	Amount   float64 `bson:"amount"`
	Currency string  `bson:"currency"`
	Count    int64   `bson:"count"`
}

type MgoDashboardRevenueByCountryReportTop struct {
	Country string  `bson:"_id"`
	Amount  float64 `bson:"amount"`
}

type MgoDashboardRevenueByCountryReportChartItem struct {
	Label  int64   `bson:"label"`
	Amount float64 `bson:"amount"`
}

type MgoDashboardRevenueByCountryReport struct {
	Currency      string                                      `bson:"currency"`
	Top           []*DashboardRevenueByCountryReportTop       `bson:"top"`
	TotalCurrent  float64                                     `bson:"total"`
	TotalPrevious float64                                     `bson:"total_previous"`
	Chart         []*DashboardRevenueByCountryReportChartItem `bson:"chart"`
}

func (p *Product) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoProduct)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	p.Id = decoded.Id.Hex()
	p.Object = decoded.Object
	p.Type = decoded.Type
	p.Sku = decoded.Sku
	p.DefaultCurrency = decoded.DefaultCurrency
	p.Enabled = decoded.Enabled
	p.Prices = decoded.Prices
	p.Description = decoded.Description
	p.LongDescription = decoded.LongDescription
	p.Images = decoded.Images
	p.Url = decoded.Url
	p.Metadata = decoded.Metadata
	p.Deleted = decoded.Deleted
	p.MerchantId = decoded.MerchantId.Hex()
	p.ProjectId = decoded.ProjectId.Hex()
	p.Pricing = decoded.Pricing
	p.BillingType = decoded.BillingType

	p.CreatedAt, err = ptypes.TimestampProto(decoded.CreatedAt)

	if err != nil {
		return err
	}

	p.UpdatedAt, err = ptypes.TimestampProto(decoded.UpdatedAt)

	if err != nil {
		return err
	}

	p.Name = map[string]string{}
	for _, i := range decoded.Name {
		p.Name[i.Lang] = i.Value
	}

	return nil
}

func (p *Product) MarshalBSON() ([]byte, error) {
	st := &MgoProduct{
		Object:          p.Object,
		Type:            p.Type,
		Sku:             p.Sku,
		DefaultCurrency: p.DefaultCurrency,
		Enabled:         p.Enabled,
		Description:     p.Description,
		LongDescription: p.LongDescription,
		Images:          p.Images,
		Url:             p.Url,
		Metadata:        p.Metadata,
		Deleted:         p.Deleted,
		Pricing:         p.Pricing,
		BillingType:     p.BillingType,
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

	if len(p.MerchantId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		merchantOid, err := primitive.ObjectIDFromHex(p.MerchantId)

		if err != nil {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.MerchantId = merchantOid
	}

	if len(p.ProjectId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		projectOid, err := primitive.ObjectIDFromHex(p.ProjectId)

		if err != nil {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.ProjectId = projectOid
	}

	if p.CreatedAt != nil {
		t, err := ptypes.Timestamp(p.CreatedAt)

		if err != nil {
			return nil, err
		}

		st.CreatedAt = t
	} else {
		st.CreatedAt = time.Now()
	}

	if p.UpdatedAt != nil {
		t, err := ptypes.Timestamp(p.UpdatedAt)

		if err != nil {
			return nil, err
		}

		st.UpdatedAt = t
	} else {
		st.UpdatedAt = time.Now()
	}

	st.Name = []*I18NTextSearchable{}
	for k, v := range p.Name {
		st.Name = append(st.Name, &I18NTextSearchable{Lang: k, Value: v})
	}

	for _, price := range p.Prices {
		st.Prices = append(st.Prices, &billing.ProductPrice{
			Currency:          price.Currency,
			Region:            price.Region,
			Amount:            tools.FormatAmount(price.Amount),
			IsVirtualCurrency: price.IsVirtualCurrency,
		})
	}

	return bson.Marshal(st)
}

func (m *UserProfile) MarshalBSON() ([]byte, error) {
	oid, _ := primitive.ObjectIDFromHex(m.Id)
	st := &MgoUserProfile{
		Id:     oid,
		UserId: m.UserId,
		Email: &MgoUserProfileEmail{
			Email:                   m.Email.Email,
			Confirmed:               m.Email.Confirmed,
			IsConfirmationEmailSent: m.Email.IsConfirmationEmailSent,
		},
		Personal: m.Personal,
		Help:     m.Help,
		Company:  m.Company,
		LastStep: m.LastStep,
	}

	if m.CreatedAt != nil {
		t, err := ptypes.Timestamp(m.CreatedAt)

		if err != nil {
			return nil, err
		}

		st.CreatedAt = t
	} else {
		st.CreatedAt = time.Now()
	}

	if m.UpdatedAt != nil {
		t, err := ptypes.Timestamp(m.UpdatedAt)

		if err != nil {
			return nil, err
		}

		st.UpdatedAt = t
	} else {
		st.UpdatedAt = time.Now()
	}

	if m.Email.ConfirmedAt != nil {
		t, err := ptypes.Timestamp(m.Email.ConfirmedAt)

		if err != nil {
			return nil, err
		}

		st.Email.ConfirmedAt = t
	}

	return bson.Marshal(st)
}

func (m *UserProfile) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoUserProfile)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Id = decoded.Id.Hex()
	m.UserId = decoded.UserId
	m.Email = &UserProfileEmail{
		Email:                   decoded.Email.Email,
		Confirmed:               decoded.Email.Confirmed,
		IsConfirmationEmailSent: decoded.Email.IsConfirmationEmailSent,
	}
	m.Personal = decoded.Personal
	m.Help = decoded.Help
	m.Company = decoded.Company
	m.LastStep = decoded.LastStep

	m.CreatedAt, err = ptypes.TimestampProto(decoded.CreatedAt)

	if err != nil {
		return err
	}

	m.UpdatedAt, err = ptypes.TimestampProto(decoded.UpdatedAt)

	if err != nil {
		return err
	}

	m.Email.ConfirmedAt, err = ptypes.TimestampProto(decoded.Email.ConfirmedAt)

	if err != nil {
		return err
	}

	return nil
}

func (m *PageReview) MarshalBSON() ([]byte, error) {
	oid, _ := primitive.ObjectIDFromHex(m.Id)
	st := &MgoPageReview{
		Id:     oid,
		UserId: m.UserId,
		Review: m.Review,
		Url:    m.Url,
		IsRead: m.IsRead,
	}

	if m.CreatedAt != nil {
		t, err := ptypes.Timestamp(m.CreatedAt)

		if err != nil {
			return nil, err
		}

		st.CreatedAt = t
	} else {
		st.CreatedAt = time.Now()
	}

	if m.UpdatedAt != nil {
		t, err := ptypes.Timestamp(m.UpdatedAt)

		if err != nil {
			return nil, err
		}

		st.UpdatedAt = t
	} else {
		st.UpdatedAt = time.Now()
	}

	return bson.Marshal(st)
}

func (m *PageReview) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoPageReview)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Id = decoded.Id.Hex()
	m.UserId = decoded.UserId
	m.Review = decoded.Review
	m.Url = decoded.Url

	m.CreatedAt, err = ptypes.TimestampProto(decoded.CreatedAt)

	if err != nil {
		return err
	}

	m.UpdatedAt, err = ptypes.TimestampProto(decoded.UpdatedAt)

	if err != nil {
		return err
	}

	return nil
}

func (p *KeyProduct) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoKeyProduct)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	p.Id = decoded.Id.Hex()
	p.Object = decoded.Object
	p.Sku = decoded.Sku
	p.DefaultCurrency = decoded.DefaultCurrency
	p.Enabled = decoded.Enabled
	p.Description = decoded.Description
	p.LongDescription = decoded.LongDescription
	p.Cover = decoded.Cover
	p.Url = decoded.Url
	p.Metadata = decoded.Metadata
	p.Deleted = decoded.Deleted
	p.MerchantId = decoded.MerchantId.Hex()
	p.ProjectId = decoded.ProjectId.Hex()
	p.Pricing = decoded.Pricing

	platforms := make([]*PlatformPrice, len(decoded.Platforms))
	for i, pl := range decoded.Platforms {
		platforms[i] = &PlatformPrice{
			Id:            pl.Id,
			Prices:        pl.Prices,
			EulaUrl:       pl.EulaUrl,
			Name:          pl.Name,
			ActivationUrl: pl.ActivationUrl,
		}
	}

	p.Platforms = platforms
	p.CreatedAt, err = ptypes.TimestampProto(decoded.CreatedAt)

	if err != nil {
		return err
	}

	p.UpdatedAt, err = ptypes.TimestampProto(decoded.UpdatedAt)

	if err != nil {
		return err
	}

	if decoded.PublishedAt != nil {
		p.PublishedAt, err = ptypes.TimestampProto(*decoded.PublishedAt)
		if err != nil {
			return err
		}
	}

	p.Name = map[string]string{}
	for _, i := range decoded.Name {
		p.Name[i.Lang] = i.Value
	}

	return nil
}

func (p *KeyProduct) MarshalBSON() ([]byte, error) {
	st := &MgoKeyProduct{
		Object:          p.Object,
		Sku:             p.Sku,
		DefaultCurrency: p.DefaultCurrency,
		Enabled:         p.Enabled,
		Description:     p.Description,
		LongDescription: p.LongDescription,
		Cover:           p.Cover,
		Url:             p.Url,
		Metadata:        p.Metadata,
		Deleted:         p.Deleted,
		Pricing:         p.Pricing,
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

	if len(p.MerchantId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		merchantOid, err := primitive.ObjectIDFromHex(p.MerchantId)

		if err != nil {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.MerchantId = merchantOid
	}

	if len(p.ProjectId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		projectOId, err := primitive.ObjectIDFromHex(p.ProjectId)

		if err != nil {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.ProjectId = projectOId
	}

	if p.CreatedAt != nil {
		t, err := ptypes.Timestamp(p.CreatedAt)

		if err != nil {
			return nil, err
		}

		st.CreatedAt = t
	} else {
		st.CreatedAt = time.Now()
	}

	if p.UpdatedAt != nil {
		t, err := ptypes.Timestamp(p.UpdatedAt)

		if err != nil {
			return nil, err
		}

		st.UpdatedAt = t
	} else {
		st.UpdatedAt = time.Now()
	}

	if p.PublishedAt != nil {
		t, err := ptypes.Timestamp(p.PublishedAt)

		if err != nil {
			return nil, err
		}

		st.PublishedAt = &t
	}

	st.Name = make([]*I18NTextSearchable, len(p.Name))
	index := 0
	for k, v := range p.Name {
		st.Name[index] = &I18NTextSearchable{Lang: k, Value: v}
		index++
	}

	st.Platforms = make([]*MgoPlatformPrice, len(p.Platforms))
	for i, pl := range p.Platforms {
		var prices []*billing.ProductPrice
		prices = make([]*billing.ProductPrice, len(pl.Prices))
		for j, price := range pl.Prices {
			prices[j] = &billing.ProductPrice{
				Currency:          price.Currency,
				Region:            price.Region,
				Amount:            tools.FormatAmount(price.Amount),
				IsVirtualCurrency: price.IsVirtualCurrency,
			}
		}
		st.Platforms[i] = &MgoPlatformPrice{
			Prices:        prices,
			Id:            pl.Id,
			Name:          pl.Name,
			EulaUrl:       pl.EulaUrl,
			ActivationUrl: pl.ActivationUrl,
		}
	}

	return bson.Marshal(st)
}

func (m *DashboardAmountItemWithChart) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoDashboardAmountItemWithChart)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.AmountCurrent = tools.FormatAmount(decoded.Amount)
	m.Currency = decoded.Currency

	for _, v := range decoded.Chart {
		item := &DashboardChartItemFloat{
			Label: v.Label,
			Value: tools.FormatAmount(v.Value),
		}
		m.Chart = append(m.Chart, item)
	}

	return nil
}

func (m *DashboardRevenueDynamicReportItem) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoDashboardRevenueDynamicReportItem)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Label = decoded.Label
	m.Amount = tools.FormatAmount(decoded.Amount)
	m.Currency = decoded.Currency
	m.Count = decoded.Count

	return nil
}

func (m *DashboardRevenueByCountryReportTop) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoDashboardRevenueByCountryReportTop)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Amount = tools.FormatAmount(decoded.Amount)
	m.Country = decoded.Country

	return nil
}

func (m *DashboardRevenueByCountryReportChartItem) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoDashboardRevenueByCountryReportChartItem)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Amount = tools.FormatAmount(decoded.Amount)
	m.Label = decoded.Label

	return nil
}

func (m *DashboardRevenueByCountryReport) UnmarshalBSON(raw []byte) error {
	decoded := new(MgoDashboardRevenueByCountryReport)
	err := bson.Unmarshal(raw, decoded)

	if err != nil {
		return err
	}

	m.Currency = decoded.Currency
	m.Top = decoded.Top
	m.TotalCurrent = tools.FormatAmount(decoded.TotalCurrent)
	m.TotalPrevious = tools.FormatAmount(decoded.TotalPrevious)
	m.Chart = decoded.Chart

	return nil
}
