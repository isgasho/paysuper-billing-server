protoc -I=. --micro_out=./billing/ --go_out=./billing/ ./billing/billing.proto
move /Y .\billing\github.com\paysuper\paysuper-billing-server\pkg\proto\billing\billing.micro.go .\billing\billing.micro.go
move /Y .\billing\github.com\paysuper\paysuper-billing-server\pkg\proto\billing\billing.pb.go .\billing\billing.pb.go
rmdir /Q/S .\billing\github.com
protoc-go-inject-tag -input=./billing/billing.pb.go -XXX_skip=bson,json,structure,validate

protoc -I=. --micro_out=./grpc/ --go_out=./grpc/ ./grpc/grpc.proto
move /Y .\grpc\github.com\paysuper\paysuper-billing-server\pkg\proto\grpc\grpc.micro.go .\grpc\grpc.micro.go
move /Y .\grpc\github.com\paysuper\paysuper-billing-server\pkg\proto\grpc\grpc.pb.go .\grpc\grpc.pb.go
rmdir /Q/S .\grpc\github.com
protoc-go-inject-tag -input=./grpc/grpc.pb.go -XXX_skip=bson,json,structure,validate
