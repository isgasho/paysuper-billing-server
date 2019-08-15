package service

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"go.uber.org/zap"
	"sync"
	"time"
)

type MessageBrokerInterface interface {
	Publish(string, interface{}) error
}

type MessageBroker struct {
	svc    *Service
	client stan.Conn
}

func newMessageBroker(svc *Service) (MessageBrokerInterface, error) {
	var opts []nats.Option

	s := &MessageBroker{svc: svc}

	if s.svc.cfg.NatsUser != "" && s.svc.cfg.NatsPassword != "" {
		opts = append(opts, nats.UserInfo(s.svc.cfg.NatsUser, s.svc.cfg.NatsPassword))
	}

	nc, err := nats.Connect(s.svc.cfg.NatsServerUrls, opts...)
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	s.client, err = stan.Connect(s.svc.cfg.NatsClusterId, s.svc.cfg.NatsClientId, stan.NatsConn(nc))
	if err != nil {
		return nil, err
	}
	defer s.client.Close()

	return s, nil
}

func (c MessageBroker) Publish(subject string, msg interface{}) error {
	var (
		glock sync.Mutex
		guid  string
		ch    = make(chan bool)
	)

	message, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	acb := func(lguid string, err error) {
		glock.Lock()
		defer glock.Unlock()

		if err != nil {
			zap.S().Fatalf("[MessageBroker] Error in server ack for guid", "err", err.Error(), "lguid", lguid)
		}

		if lguid != guid {
			zap.S().Fatalf("[MessageBroker] Expected a matching guid in ack callback", "guid", guid, "lguid", lguid)
		}
		ch <- true
	}

	if !c.svc.cfg.NatsAsync {
		if err = c.client.Publish(subject, message); err != nil {
			return err
		}
	} else {
		glock.Lock()

		if guid, err = c.client.PublishAsync(subject, message, acb); err != nil {
			return err
		}

		glock.Unlock()

		if guid == "" {
			zap.S().Fatal("[MessageBroker] Expected non-empty guid to be returned")
		}

		select {
		case <-ch:
			break
		case <-time.After(5 * time.Second):
			zap.S().Fatal("[MessageBroker] timeout to publish message")
		}

	}

	return nil
}
