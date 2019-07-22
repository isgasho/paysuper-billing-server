package mock

import (
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
	rabbitmq "gopkg.in/ProtocolONE/rabbitmq.v1/pkg"
)

const (
	SomeError = "some error"
)

type BrokerMockError struct{}

func NewBrokerMockError() rabbitmq.BrokerInterface {
	return &BrokerMockError{}
}

func (b *BrokerMockError) RegisterSubscriber(topic string, fn interface{}) error {
	return errors.New(SomeError)
}

func (b *BrokerMockError) Subscribe(exit chan bool) error {
	return errors.New(SomeError)
}

func (b *BrokerMockError) Publish(topic string, msg proto.Message, h amqp.Table) (err error) {
	return errors.New(SomeError)
}
