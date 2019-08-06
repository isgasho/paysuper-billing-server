package mock

import (
	"github.com/paysuper/document-signer/pkg/proto"
	"github.com/paysuper/paysuper-billing-server/pkg"
	mock2 "github.com/stretchr/testify/mock"
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

func NewDocumentSignerMockOk() proto.DocumentSignerService {
	ds := &DocumentSignerService{}
	ds.On("CreateSignature", mock2.Anything, mock2.Anything).
		Return(&proto.Response{Status: pkg.ResponseStatusOk, HelloSignResponse: HellosignResponse}, nil)

	return ds
}
