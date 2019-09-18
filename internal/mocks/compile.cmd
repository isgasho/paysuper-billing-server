mockery -recursive=true -all -dir=../internal/service -output=.
mockery -name=BillingService -dir=../../pkg/proto/grpc -recursive=true -output=../../pkg/mocks
