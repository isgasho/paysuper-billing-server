package service

import (
	"context"
	"encoding/json"
	"github.com/centrifugal/gocent"
	"github.com/dgrijalva/jwt-go"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"go.uber.org/zap"
	"net/http"
)

type CentrifugoInterface interface {
	Publish(context.Context, string, interface{}) error
	GetChannelToken(secret, subject string, expire int64) string
}

type Centrifugo struct {
	centrifugoClient *gocent.Client
}

func newCentrifugo(cfg *config.Centrifugo, httpClient *http.Client) CentrifugoInterface {
	centrifugo := &Centrifugo{
		centrifugoClient: gocent.New(
			gocent.Config{
				Addr:       cfg.URL,
				Key:        cfg.ApiSecret,
				HTTPClient: httpClient,
			},
		),
	}

	return centrifugo
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

func (c *Centrifugo) GetChannelToken(secret, subject string, expire int64) string {
	claims := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": subject, "exp": expire})
	token, err := claims.SignedString([]byte(secret))

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
