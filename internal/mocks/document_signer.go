package mocks

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"github.com/paysuper/paysuper-proto/go/document_signerpb"
	"github.com/paysuper/paysuper-proto/go/document_signerpb/mocks"
	mock2 "github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

var (
	CreateSignatureResponse = &document_signerpb.CreateSignatureResponse{
		Status: billingpb.ResponseStatusOk,
		Item: &document_signerpb.CreateSignatureResponseItem{
			DetailsUrl:          "http:/127.0.0.1",
			FilesUrl:            "http:/127.0.0.1",
			SignatureRequestId:  primitive.NewObjectID().Hex(),
			MerchantSignatureId: primitive.NewObjectID().Hex(),
			PsSignatureId:       primitive.NewObjectID().Hex(),
		},
	}
	GetSignatureUrlResponse = &document_signerpb.GetSignatureUrlResponse{
		Status: billingpb.ResponseStatusOk,
		Item: &document_signerpb.GetSignatureUrlResponseEmbedded{
			SignUrl: "http://127.0.0.1",
		},
	}
)

func NewDocumentSignerMockOk() *mocks.DocumentSignerService {
	GetSignatureUrlResponse.Item.ExpiresAt, _ = ptypes.TimestampProto(time.Now().Add(time.Duration(1 * time.Hour)))

	ds := &mocks.DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything, mock2.Anything).
		Return(CreateSignatureResponse, nil)
	ds.On("GetSignatureUrl", mock2.Anything, mock2.Anything).Return(GetSignatureUrlResponse, nil)

	return ds
}
