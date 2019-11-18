package mocks

import (
	"context"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-webhook-notifier/pkg/proto/grpc"
)

type NotifierOk struct {

}

func (n NotifierOk) CheckUser(ctx context.Context, in *grpc.CheckUserRequest, opts ...client.CallOption) (*grpc.CheckUserResponse, error) {
	return &grpc.CheckUserResponse{
		Status:               200,
	}, nil
}

func NewNotifierOk() grpc.NotifierService {
	return &NotifierOk{}
}



