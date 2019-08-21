cd ../..
mockery -recursive=true -all -dir=./internal/service -output=./internal/mocks
mockery -name=BillingService -dir=./pkg/proto -recursive=true -output=./internal/mocks
