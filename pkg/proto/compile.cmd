protoc --proto_path=. --micro_out=C:\projects\ProtocolONE --go_out=plugins=retag:C:\projects\ProtocolONE billing/billing.proto
protoc-go-inject-tag -input=billing/billing.pb.go -XXX_skip=bson,json,structure,validate
protoc --proto_path=. --micro_out=C:\projects\ProtocolONE --go_out=plugins=retag:C:\projects\ProtocolONE grpc/grpc.proto
protoc-go-inject-tag -input=grpc/grpc.pb.go -XXX_skip=bson,json,structure,validate