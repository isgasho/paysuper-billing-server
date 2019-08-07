package mock

import (
	"context"
	"errors"
	"github.com/micro/go-micro/client"
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/pkg"
)

var (
	HellosignResponse = []byte(`{
        "signature_request_id": "17d163069282df5eb63857d31ff4a3bffa9e46c0",
        "title": "Purchase Order",
        "subject": "Purchase Order",
        "message": "Glad we could come to an agreement.",
        "is_complete": false,
        "is_declined": false,
        "has_error": false,
        "custom_fields": [
            {
                "name": "Cost",
                "value": "$20,000",
                "type": "text",
                "editor": "Client",
                "required": true
            }
        ],
        "response_data": [
        ],
        "signing_url": "https://app.hellosign.com/sign/17d163069282df5eb63857d31ff4a3bffa9e46c0",
        "signing_redirect_url": null,
        "details_url": "https://app.hellosign.com/home/manage?guid=17d163069282df5eb63857d31ff4a3bffa9e46c0",
        "requester_email_address": "me@hellosign.com",
        "signatures": [
            {
                "signature_id": "10ab1cd037d9b6cba7975d61ff428c8d",
                "signer_email_address": "test1@unit.test",
                "signer_name": "George",
                "signer_role": "Merchant",
                "order": null,
                "status_code": "awaiting_signature",
                "signed_at": null,
                "last_viewed_at": null,
                "last_reminded_at": null,
                "has_pin": false
            },
			{
                "signature_id": "10ab1cd037d9b6cba7975d61ff428c8d",
                "signer_email_address": "no-reply@protocol.one",
                "signer_name": "George",
                "signer_role": "Paysuper",
                "order": null,
                "status_code": "awaiting_signature",
                "signed_at": null,
                "last_viewed_at": null,
                "last_reminded_at": null,
                "has_pin": false
            }
        ],
        "cc_email_addresses": [
            "accounting@hellosign.com"
        ]
    }`)
)

type DocumentSignerMockOk struct{}
type DocumentSignerMockError struct{}
type DocumentSignerMockSystemError struct{}
type DocumentSignerMockOkIncorrectJson struct{}

func NewDocumentSignerMockOk() proto.DocumentSignerService {
	return &DocumentSignerMockOk{}
}

func NewDocumentSignerMockError() proto.DocumentSignerService {
	return &DocumentSignerMockError{}
}

func NewDocumentSignerMockSystemError() proto.DocumentSignerService {
	return &DocumentSignerMockSystemError{}
}

func NewDocumentSignerMockOkIncorrectJson() proto.DocumentSignerService {
	return &DocumentSignerMockOkIncorrectJson{}
}

func (m *DocumentSignerMockOk) CreateSignature(
	ctx context.Context,
	in *proto.CreateSignatureRequest,
	opts ...client.CallOption,
) (*proto.Response, error) {
	return &proto.Response{Status: pkg.ResponseStatusOk, HelloSignResponse: HellosignResponse}, nil
}

func (m *DocumentSignerMockError) CreateSignature(
	ctx context.Context,
	in *proto.CreateSignatureRequest,
	opts ...client.CallOption,
) (*proto.Response, error) {
	return &proto.Response{
		Status:  pkg.ResponseStatusBadData,
		Message: &proto.ResponseErrorMessage{Message: SomeError},
	}, nil
}

func (m *DocumentSignerMockSystemError) CreateSignature(
	ctx context.Context,
	in *proto.CreateSignatureRequest,
	opts ...client.CallOption,
) (*proto.Response, error) {
	return nil, errors.New(SomeError)
}

func (m *DocumentSignerMockOkIncorrectJson) CreateSignature(
	ctx context.Context,
	in *proto.CreateSignatureRequest,
	opts ...client.CallOption,
) (*proto.Response, error) {
	return &proto.Response{Status: pkg.ResponseStatusOk, HelloSignResponse: []byte("some_not_json_string")}, nil
}
