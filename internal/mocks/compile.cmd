mockery -recursive=true -all -dir=../internal/service -output=.
mockery -name=BillingService -dir=../../pkg/proto -recursive=true -output=../../pkg/mocks
