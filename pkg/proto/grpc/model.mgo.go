package grpc

import (
	"errors"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"time"
)

const (
	errorInvalidObjectId = "invalid bson object id"
)

type MgoProduct struct {
	Id              bson.ObjectId         `bson:"_id" json:"id"`
	Object          string                `bson:"object" json:"object"`
	Type            string                `bson:"type" json:"type"`
	Sku             string                `bson:"sku" json:"sku"`
	Name            []*I18NTextSearchable `bson:"name" json:"name"`
	DefaultCurrency string                `bson:"default_currency" json:"default_currency"`
	Enabled         bool                  `bson:"enabled" json:"enabled"`
	Prices          []*ProductPrice       `bson:"prices" json:"prices"`
	Description     map[string]string     `bson:"description" json:"description"`
	LongDescription map[string]string     `bson:"long_description,omitempty" json:"long_description"`
	CreatedAt       time.Time             `bson:"created_at" json:"created_at"`
	UpdatedAt       time.Time             `bson:"updated_at" json:"updated_at"`
	Images          []string              `bson:"images,omitempty" json:"images"`
	Url             string                `bson:"url,omitempty" json:"url"`
	Metadata        map[string]string     `bson:"metadata,omitempty" json:"metadata"`
	Deleted         bool                  `bson:"deleted" json:"deleted"`
	MerchantId      bson.ObjectId         `bson:"merchant_id" json:"-"`
	ProjectId       bson.ObjectId         `bson:"project_id" json:"project_id"`
}

type MgoPrimaryOnboarding struct {
	Id        bson.ObjectId              `bson:"_id"`
	UserId    string                     `bson:"user_id"`
	Personal  *PrimaryOnboardingPersonal `bson:"personal"`
	Help      *PrimaryOnboardingHelp     `bson:"help"`
	Company   *PrimaryOnboardingCompany  `bson:"company"`
	LastStep  string                     `bson:"last_step"`
	CreatedAt time.Time                  `bson:"created_at"`
	UpdatedAt time.Time                  `bson:"updated_at"`
}

func (p *Product) SetBSON(raw bson.Raw) error {
	decoded := new(MgoProduct)
	err := raw.Unmarshal(decoded)

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

func (p *Product) GetBSON() (interface{}, error) {
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
	}

	if len(p.Id) <= 0 {
		st.Id = bson.NewObjectId()
	} else {
		if bson.IsObjectIdHex(p.Id) == false {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.Id = bson.ObjectIdHex(p.Id)
	}

	if len(p.MerchantId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		if bson.IsObjectIdHex(p.MerchantId) == false {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.MerchantId = bson.ObjectIdHex(p.MerchantId)
	}

	if len(p.ProjectId) <= 0 {
		return nil, errors.New(errorInvalidObjectId)
	} else {
		if bson.IsObjectIdHex(p.ProjectId) == false {
			return nil, errors.New(errorInvalidObjectId)
		}

		st.ProjectId = bson.ObjectIdHex(p.ProjectId)
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
		st.Prices = append(st.Prices, &ProductPrice{
			Currency: price.Currency,
			Region:   price.Region,
			Amount:   tools.FormatAmount(price.Amount),
		})
	}

	return st, nil
}

func (m *PrimaryOnboarding) GetBSON() (interface{}, error) {
	st := &MgoPrimaryOnboarding{
		Id:       bson.ObjectIdHex(m.Id),
		UserId:   m.UserId,
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

	return st, nil
}

func (m *PrimaryOnboarding) SetBSON(raw bson.Raw) error {
	decoded := new(MgoPrimaryOnboarding)
	err := raw.Unmarshal(decoded)

	if err != nil {
		return err
	}

	m.Id = decoded.Id.Hex()
	m.UserId = decoded.UserId
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

	return nil
}
