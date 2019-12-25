mockery -recursive=true -all -dir=../service -output=.
mockery -recursive=true -all -dir=../repository -output=.
mockery -recursive=true -all -dir=../database -output=.
mockery -name=BillingService -dir=../../pkg/proto/grpc -recursive=true -output=../../pkg/mocks
