package paylink

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

func (pl *Paylink) IsPaylinkExpired() bool {
	if pl.NoExpiryDate {
		return false
	}

	if pl.ExpiresAt == nil {
		return false
	}

	expiresAt, err := ptypes.Timestamp(pl.ExpiresAt)
	if err != nil {
		return true
	}

	return time.Now().After(expiresAt)
}

func (pl *Paylink) UpdateConversion() {
	pl.Conversion = conversion(pl.SalesCount, pl.Visits)
}

func (pl *Paylink) GetMgoPaylink() (result *MgoPaylink, err error) {
	plBson, err := pl.MarshalBSON()
	if err != nil {
		return
	}
	result = &MgoPaylink{}
	err = bson.Unmarshal(plBson, result)
	return
}

func (pl *StatCommon) UpdateConversion() {
	pl.Conversion = conversion(pl.SalesCount, pl.Visits)
}

func conversion(sales, visits int32) float64 {
	if visits == 0 {
		return 0
	}
	return tools.ToPrecise(float64(sales) / float64(visits))
}
