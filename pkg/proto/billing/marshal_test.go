package billing

import (
	"encoding/json"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_OrderMarshalJSON(t *testing.T) {
	createdAt, _ := ptypes.TimestampProto(time.Date(2019, 12, 14, 1, 2, 3, 0, time.UTC))
	order := &Order{Uuid: "some_id", Type: "some_type", CreatedAt: createdAt, RefundedAt: createdAt, CanceledAt: createdAt}
	b, err := json.Marshal(order)
	assert.NoError(t, err)

	s := string(b)
	assert.NotZero(t, s)
}
