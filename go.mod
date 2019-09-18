module github.com/paysuper/paysuper-billing-server

require (
	github.com/InVisionApp/go-health v2.1.0+incompatible
	github.com/Jeffail/gabs v1.1.1 // indirect
	github.com/ProtocolONE/geoip-service v0.0.0-20190903084234-1d5ae6b96679
	github.com/ProtocolONE/go-micro-plugins v0.3.0
	github.com/SAP/go-hdb v0.13.2 // indirect
	github.com/SermoDigital/jose v0.9.2-0.20161205224733-f6df55f235c2 // indirect
	github.com/alicebob/gopher-json v0.0.0-20180125190556-5a6b3ba71ee6 // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/asaskevich/govalidator v0.0.0-20180720115003-f9ffefc3facf // indirect
	github.com/centrifugal/gocent v2.0.2+incompatible
	github.com/denisenkom/go-mssqldb v0.0.0-20190121005146-b04fd42d9952 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/elazarl/go-bindata-assetfs v1.0.0 // indirect
	github.com/elliotchance/redismock v1.5.1
	github.com/fatih/structs v1.1.0 // indirect
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/gogo/protobuf v1.2.1
	github.com/golang-migrate/migrate/v4 v4.3.1
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1
	github.com/hashicorp/go-memdb v0.0.0-20181108192425-032f93b25bec // indirect
	github.com/hashicorp/go-plugin v0.0.0-20181212150838-f444068e8f5a // indirect
	github.com/jinzhu/copier v0.0.0-20190625015134-976e0346caa8
	github.com/jinzhu/now v1.0.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/keybase/go-crypto v0.0.0-20181127160227-255a5089e85a // indirect
	github.com/micro/cli v0.2.0
	github.com/micro/go-micro v1.8.0
	github.com/micro/go-plugins v1.2.0
	github.com/micro/go-rcache v0.2.1 // indirect
	github.com/micro/util v0.2.0 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/paysuper/document-signer v0.0.0-20190916111907-052775c62716
	github.com/paysuper/paysuper-currencies v0.0.0-20190903083641-668b8b2b997d
	github.com/paysuper/paysuper-database-mongo v0.1.0
	github.com/paysuper/paysuper-recurring-repository v1.0.123
	github.com/paysuper/paysuper-tax-service v0.0.0-20190903084038-7849f394f122
	github.com/paysuper/postmark-sender v0.0.0-20190903075212-a210e35789fc
	github.com/prometheus/client_golang v1.0.0
	github.com/stoewer/go-strcase v1.0.2
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/stretchr/testify v1.4.0
	github.com/ttacon/builder v0.0.0-20170518171403-c099f663e1c2 // indirect
	github.com/ttacon/libphonenumber v1.0.1
	github.com/yuin/gopher-lua v0.0.0-20190514113301-1cd887cd7036 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4
	gopkg.in/ProtocolONE/rabbitmq.v1 v1.0.0-20190719062839-9858d727f3ef
	gopkg.in/gomail.v2 v2.0.0-20160411212932-81ebce5c23df
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce
)

replace github.com/hashicorp/consul => github.com/hashicorp/consul v1.5.1

go 1.13
