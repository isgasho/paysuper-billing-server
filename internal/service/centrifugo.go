package service

import (
	"context"
	"encoding/json"
	"github.com/centrifugal/gocent"
	"github.com/dgrijalva/jwt-go"
	"github.com/paysuper/paysuper-recurring-repository/tools"
	"go.uber.org/zap"
)

type CentrifugoInterface interface {
	Publish(context.Context, string, interface{}) error
	GetChannelToken(subject string, expire int64) string
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

func (c *Centrifugo) Publish(ctx context.Context, channel string, msg interface{}) error {
	b, err := json.Marshal(msg)

	if err != nil {
		zap.L().Error(
			"Publish message to centrifugo failed",
			zap.Error(err),
			zap.String("channel", channel),
			zap.Any("message", msg),
		)
		return err
	}

	return c.centrifugoClient.Publish(ctx, channel, b)
}

func (c *Centrifugo) GetChannelToken(subject string, expire int64) string {
	claims := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": subject, "exp": expire})
	token, err := claims.SignedString([]byte(c.svc.cfg.CentrifugoSecret))

	if err != nil {
		zap.L().Error(
			"Generate centrifugo channel token failed",
			zap.Error(err),
			zap.String("subject", subject),
			zap.Any("expire", expire),
		)

		return ""
	}

	return token
}
