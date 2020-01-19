package mocks

import (
	"context"
	"errors"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/recurringpb"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type RepositoryServiceOk struct{}
type RepositoryServiceEmpty struct{}
type RepositoryServiceError struct{}

func NewRepositoryServiceOk() recurringpb.RepositoryService {
	return &RepositoryServiceOk{}
}

func NewRepositoryServiceEmpty() recurringpb.RepositoryService {
	return &RepositoryServiceEmpty{}
}

func NewRepositoryServiceError() recurringpb.RepositoryService {
	return &RepositoryServiceError{}
}

func (r *RepositoryServiceOk) InsertSavedCard(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.Result, error) {
	return &recurringpb.Result{}, nil
}

func (r *RepositoryServiceOk) DeleteSavedCard(
	ctx context.Context,
	in *recurringpb.DeleteSavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.DeleteSavedCardResponse, error) {
	return &recurringpb.DeleteSavedCardResponse{Status: billingpb.ResponseStatusOk}, nil
}

func (r *RepositoryServiceOk) FindSavedCards(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.SavedCardList, error) {
	projectId := primitive.NewObjectID().Hex()

	return &recurringpb.SavedCardList{
		SavedCards: []*recurringpb.SavedCard{
			{
				Id:        primitive.NewObjectID().Hex(),
				Token:     primitive.NewObjectID().Hex(),
				ProjectId: projectId,
				MaskedPan: "555555******4444",
				Expire:    &recurringpb.CardExpire{Month: "12", Year: "2019"},
				IsActive:  true,
			},
			{
				Id:        primitive.NewObjectID().Hex(),
				Token:     primitive.NewObjectID().Hex(),
				ProjectId: projectId,
				MaskedPan: "400000******0002",
				Expire:    &recurringpb.CardExpire{Month: "12", Year: "2019"},
				IsActive:  true,
			},
		},
	}, nil
}

func (r *RepositoryServiceOk) FindSavedCardById(
	ctx context.Context,
	in *recurringpb.FindByStringValue,
	opts ...client.CallOption,
) (*recurringpb.SavedCard, error) {
	return &recurringpb.SavedCard{
		Id:          primitive.NewObjectID().Hex(),
		Token:       primitive.NewObjectID().Hex(),
		ProjectId:   primitive.NewObjectID().Hex(),
		MerchantId:  primitive.NewObjectID().Hex(),
		MaskedPan:   "400000******0002",
		RecurringId: primitive.NewObjectID().Hex(),
		Expire:      &recurringpb.CardExpire{Month: "12", Year: "2019"},
		IsActive:    true,
	}, nil
}

func (r *RepositoryServiceEmpty) InsertSavedCard(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.Result, error) {
	return &recurringpb.Result{}, nil
}

func (r *RepositoryServiceEmpty) DeleteSavedCard(
	ctx context.Context,
	in *recurringpb.DeleteSavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.DeleteSavedCardResponse, error) {
	rsp := &recurringpb.DeleteSavedCardResponse{
		Status:  billingpb.ResponseStatusBadData,
		Message: "some error",
	}

	if in.Token == "ffffffffffffffffffffffff" {
		rsp.Status = billingpb.ResponseStatusSystemError
	}

	return rsp, nil
}

func (r *RepositoryServiceEmpty) FindSavedCards(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.SavedCardList, error) {
	return &recurringpb.SavedCardList{}, nil
}

func (r *RepositoryServiceEmpty) FindSavedCardById(
	ctx context.Context,
	in *recurringpb.FindByStringValue,
	opts ...client.CallOption,
) (*recurringpb.SavedCard, error) {
	return &recurringpb.SavedCard{}, nil
}

func (r *RepositoryServiceError) InsertSavedCard(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.Result, error) {
	return &recurringpb.Result{}, nil
}

func (r *RepositoryServiceError) DeleteSavedCard(
	ctx context.Context,
	in *recurringpb.DeleteSavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.DeleteSavedCardResponse, error) {
	return nil, errors.New("some error")
}

func (r *RepositoryServiceError) FindSavedCards(
	ctx context.Context,
	in *recurringpb.SavedCardRequest,
	opts ...client.CallOption,
) (*recurringpb.SavedCardList, error) {
	return &recurringpb.SavedCardList{}, errors.New("some error")
}

func (r *RepositoryServiceError) FindSavedCardById(
	ctx context.Context,
	in *recurringpb.FindByStringValue,
	opts ...client.CallOption,
) (*recurringpb.SavedCard, error) {
	return &recurringpb.SavedCard{}, nil
}
