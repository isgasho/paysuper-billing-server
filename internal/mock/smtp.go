package mock

import (
	"errors"
	"gopkg.in/gomail.v2"
	"io"
)

type mockSender gomail.SendFunc

type SendCloserMock struct {
	mockSender
	close func() error
	Err   error
}

func NewSmtpSenderMockOk() gomail.SendCloser {
	return &SendCloserMock{
		mockSender: func(from string, to []string, msg io.WriterTo) error {
			return nil
		},
		close: func() error {
			return nil
		},
	}
}

func NewSmtpSenderMockError() gomail.SendCloser {
	return &SendCloserMock{
		mockSender: func(from string, to []string, msg io.WriterTo) error {
			return errors.New(SomeError)
		},
		close: func() error {
			return nil
		},
	}
}

func (s mockSender) Send(from string, to []string, msg io.WriterTo) error {
	return s(from, to, msg)
}

func (s *SendCloserMock) Close() error {
	return s.close()
}
