package service

import (
	"context"
	"encoding/json"
	"github.com/centrifugal/gocent"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
)

type CentrifugoInterface interface {
	Publish(string, interface{}) error
}

type Centrifugo struct {
	svc              *Service
	centrifugoClient *gocent.Client
}

func newCentrifugo(svc *Service) CentrifugoInterface {
	s := &Centrifugo{svc: svc}
	s.centrifugoClient = gocent.New(
		gocent.Config{
			Addr:       s.svc.cfg.CentrifugoURL,
			Key:        s.svc.cfg.CentrifugoApiSecret,
			HTTPClient: tools.NewLoggedHttpClient(zap.S()),
		},
	)
	return s
}

func (c Centrifugo) Publish(channel string, msg interface{}) error {
	b, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	return c.centrifugoClient.Publish(context.Background(), channel, b)
}
