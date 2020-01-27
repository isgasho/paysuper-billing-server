module github.com/paysuper/paysuper-billing-server

require (
	github.com/InVisionApp/go-health v2.1.0+incompatible
	github.com/ProtocolONE/geoip-service v0.0.0-20190903084234-1d5ae6b96679
	github.com/ProtocolONE/go-micro-plugins v0.3.0
	github.com/PuerkitoBio/purell v1.0.0
	github.com/alicebob/gopher-json v0.0.0-20180125190556-5a6b3ba71ee6 // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/centrifugal/gocent v2.0.2+incompatible
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/divan/num2words v0.0.0-20170904212200-57dba452f942
	github.com/elliotchance/redismock v1.5.1
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/gogo/protobuf v1.3.0
	github.com/golang-migrate/migrate/v4 v4.6.2
	github.com/golang/protobuf v1.3.2
	github.com/google/go-querystring v1.0.0
	github.com/google/uuid v1.1.1
	github.com/jinzhu/copier v0.0.0-20190924061706-b57f9002281a
	github.com/jinzhu/now v1.1.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/micro/cli v0.2.0
	github.com/micro/go-micro v1.18.0
	github.com/micro/go-plugins v1.2.0
	github.com/paysuper/paysuper-i18n v0.0.0-20190926113224-7eaca4563c7b
	github.com/paysuper/paysuper-proto/go/billingpb v0.0.0-20200127133012-ba84dc9f4db3
	github.com/paysuper/paysuper-proto/go/casbinpb v0.0.0-20200119221447-498828dffe30
	github.com/paysuper/paysuper-proto/go/currenciespb v0.0.0-20200119003232-91615911efec
	github.com/paysuper/paysuper-proto/go/document_signerpb v0.0.0-20200117170849-2e388108ebcc
	github.com/paysuper/paysuper-proto/go/postmarkpb v0.0.0-20200117180736-12adca1f8860
	github.com/paysuper/paysuper-proto/go/recurringpb v0.0.0-20200123200131-df93e6644cbd
	github.com/paysuper/paysuper-proto/go/reporterpb v0.0.0-20200117172130-df1a443c1fe8
	github.com/paysuper/paysuper-proto/go/taxpb v0.0.0-20200118235449-5c93300b7a1f
	github.com/paysuper/paysuper-tools v0.0.0-20200117101901-522574ce4d1c
	github.com/prometheus/client_golang v1.2.1
	github.com/stoewer/go-strcase v1.1.0
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.4.0
	github.com/ttacon/builder v0.0.0-20170518171403-c099f663e1c2 // indirect
	github.com/ttacon/libphonenumber v1.0.1
	github.com/yuin/gopher-lua v0.0.0-20191128022950-c6266f4fe8d7 // indirect
	go.mongodb.org/mongo-driver v1.2.1
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20191112222119-e1110fd1c708
	gopkg.in/ProtocolONE/rabbitmq.v1 v1.0.0-20190719062839-9858d727f3ef
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/paysuper/paysuper-database-mongo.v2 v2.0.0-20200116095540-a477bfd0ce4c
)

replace (
	github.com/gogo/protobuf => github.com/gogo/protobuf v1.3.0
	github.com/gogo/protobuf v0.0.0-20190410021324-65acae22fc9 => github.com/gogo/protobuf v1.2.2-0.20190723190241-65acae22fc9d
	github.com/hashicorp/consul => github.com/hashicorp/consul v1.5.2
	github.com/hashicorp/consul/api => github.com/hashicorp/consul/api v1.1.0
	github.com/micro/go-micro => github.com/micro/go-micro v1.8.0
	golang.org/x/sys => golang.org/x/sys v0.0.0-20190927073244-c990c680b611
)

go 1.13
