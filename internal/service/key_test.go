package service

import (
	"context"
	"errors"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/mock"
	"github.com/paysuper/paysuper-billing-server/pkg"
	errors2 "github.com/paysuper/paysuper-billing-server/pkg/errors"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
	"time"
)

type KeyTestSuite struct {
	suite.Suite
	service *Service
	log     *zap.Logger
	cache   CacheInterface
}

func Test_Key(t *testing.T) {
	suite.Run(t, new(KeyTestSuite))
}

func (suite *KeyTestSuite) SetupTest() {
	cfg, err := config.NewConfig()
	if err != nil {
		suite.FailNow("Config load failed", "%v", err)
	}
	cfg.AccountingCurrency = "RUB"

	db, err := mongodb.NewDatabase()
	if err != nil {
		suite.FailNow("Database connection failed", "%v", err)
	}

	suite.log, err = zap.NewProduction()

	if err != nil {
		suite.FailNow("Logger initialization failed", "%v", err)
	}

	redisdb := mock.NewTestRedis()
	suite.cache = NewCacheRedis(redisdb)
	suite.service = NewBillingService(
		db,
		cfg,
		mock.NewGeoIpServiceTestOk(),
		mock.NewRepositoryServiceOk(),
		mock.NewTaxServiceOkMock(),
		nil,
		nil,
		suite.cache,
		mock.NewCurrencyServiceMockOk(),
	)

	if err := suite.service.Init(); err != nil {
		suite.FailNow("Billing service initialization failed", "%v", err)
	}

	idx := mgo.Index{
		Unique: true,
		Name:   "udx_key_platform_code",
		Key:    []string{"platform_id", "code"},
	}
	_ = suite.service.db.Collection(collectionKey).EnsureIndex(idx)
}

func (suite *KeyTestSuite) TearDownTest() {
	if err := suite.service.db.Drop(); err != nil {
		suite.FailNow("Database deletion failed", "%v", err)
	}

	suite.service.db.Close()
}

func (suite *KeyTestSuite) TestKey_Insert_Ok() {
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(&billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code",
	}))
}

func (suite *KeyTestSuite) TestKey_Insert_Error_Duplicate() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		OrderId:      bson.NewObjectId().Hex(),
		Code:         "code",
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	key.Id = bson.NewObjectId().Hex()
	assert.Errorf(suite.T(), suite.service.keyRepository.Insert(key), "duplicate key error collection")
}

func (suite *KeyTestSuite) TestKey_GetById_Ok() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		OrderId:      bson.NewObjectId().Hex(),
		Code:         "code",
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	k, err := suite.service.keyRepository.GetById(key.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), key.Id, k.Id)
	assert.Equal(suite.T(), key.PlatformId, k.PlatformId)
	assert.Equal(suite.T(), key.KeyProductId, k.KeyProductId)
	assert.Equal(suite.T(), key.OrderId, k.OrderId)
	assert.Equal(suite.T(), key.Code, k.Code)
}

func (suite *KeyTestSuite) TestKey_GetById_Error_NotFound() {
	_, err := suite.service.keyRepository.GetById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_ReserveKey_Ok() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	duration := int32(3)
	orderId := bson.NewObjectId().Hex()
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	k, err := suite.service.keyRepository.ReserveKey(key.KeyProductId, key.PlatformId, orderId, duration)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), key.Id, k.Id)
	assert.Equal(suite.T(), key.PlatformId, k.PlatformId)
	assert.Equal(suite.T(), key.KeyProductId, k.KeyProductId)
	assert.Equal(suite.T(), orderId, k.OrderId)
	assert.Equal(suite.T(), key.Code, k.Code)

	redeemedAt, err := ptypes.Timestamp(k.RedeemedAt)
	if err != nil {
		assert.FailNow(suite.T(), "Invalid redeemed at")
	}
	assert.Equal(suite.T(), "0001-01-01 00:00:00 +0000 UTC", redeemedAt.String())

	reservedTo, err := ptypes.Timestamp(k.ReservedTo)
	if err != nil {
		assert.FailNow(suite.T(), "Invalid reserved to")
	}
	assert.Equal(
		suite.T(),
		time.Now().UTC().Add(time.Second*time.Duration(duration)).Format("2006-01-02T15:04:05"),
		reservedTo.Format("2006-01-02T15:04:05"),
	)
}

func (suite *KeyTestSuite) TestKey_ReserveKey_Error_NotFound() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	orderId := bson.NewObjectId().Hex()

	_, err := suite.service.keyRepository.ReserveKey(key.KeyProductId, key.PlatformId, orderId, 3)
	assert.Error(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_ReserveKey_Error_NotFree() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		OrderId:      bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	_, err := suite.service.keyRepository.ReserveKey(key.KeyProductId, key.PlatformId, key.OrderId, 3)
	assert.Error(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_CancelById_Ok() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	orderId := bson.NewObjectId().Hex()
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	_, err := suite.service.keyRepository.ReserveKey(key.KeyProductId, key.PlatformId, orderId, 3)
	assert.NoError(suite.T(), err)

	k, err := suite.service.keyRepository.CancelById(key.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), key.Id, k.Id)
	assert.Equal(suite.T(), key.PlatformId, k.PlatformId)
	assert.Equal(suite.T(), key.KeyProductId, k.KeyProductId)
	assert.Equal(suite.T(), key.Code, k.Code)
	assert.Empty(suite.T(), k.OrderId)

	reservedTo, err := ptypes.Timestamp(k.ReservedTo)
	if err != nil {
		assert.FailNow(suite.T(), "Invalid reserved to")
	}
	assert.Equal(suite.T(), "0001-01-01 00:00:00 +0000 UTC", reservedTo.String())
}

func (suite *KeyTestSuite) TestKey_CancelById_Error_NotFound() {
	_, err := suite.service.keyRepository.CancelById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_FinishRedeemById_Ok() {
	key := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	orderId := bson.NewObjectId().Hex()
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(key))

	_, err := suite.service.keyRepository.ReserveKey(key.KeyProductId, key.PlatformId, orderId, 3)
	assert.NoError(suite.T(), err)

	k, err := suite.service.keyRepository.FinishRedeemById(key.Id)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), key.Id, k.Id)
	assert.Equal(suite.T(), key.PlatformId, k.PlatformId)
	assert.Equal(suite.T(), key.KeyProductId, k.KeyProductId)
	assert.Equal(suite.T(), key.Code, k.Code)
	assert.Equal(suite.T(), orderId, k.OrderId)

	redeemedAt, err := ptypes.Timestamp(k.RedeemedAt)
	if err != nil {
		assert.FailNow(suite.T(), "Invalid redeemed at")
	}
	assert.Equal(
		suite.T(),
		time.Now().UTC().Format("2006-01-02T15:04:05"),
		redeemedAt.Format("2006-01-02T15:04:05"),
	)
}

func (suite *KeyTestSuite) TestKey_FinishRedeemById_Error_NotFound() {
	_, err := suite.service.keyRepository.FinishRedeemById(bson.NewObjectId().Hex())
	assert.Error(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_CountKeysByProductPlatform_Ok() {
	platformId := bson.NewObjectId().Hex()
	keyProductId := bson.NewObjectId().Hex()

	cnt, err := suite.service.keyRepository.CountKeysByProductPlatform(keyProductId, platformId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, cnt)

	assert.NoError(suite.T(), suite.service.keyRepository.Insert(&billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   platformId,
		KeyProductId: keyProductId,
		Code:         "code1",
	}))
	cnt, err = suite.service.keyRepository.CountKeysByProductPlatform(keyProductId, platformId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, cnt)

	assert.NoError(suite.T(), suite.service.keyRepository.Insert(&billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   platformId,
		KeyProductId: keyProductId,
		Code:         "code2",
		OrderId:      bson.NewObjectId().Hex(),
	}))
	cnt, err = suite.service.keyRepository.CountKeysByProductPlatform(keyProductId, platformId)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, cnt)
}

func (suite *KeyTestSuite) TestKey_GetAvailableKeysCount_Ok() {
	req := &grpc.GetPlatformKeyCountRequest{
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetPlatformKeyCountResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CountKeysByProductPlatform", req.KeyProductId, req.PlatformId).Return(1, nil)
	suite.service.keyRepository = kr

	err := suite.service.GetAvailableKeysCount(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(1), res.Count)
}

func (suite *KeyTestSuite) TestKey_GetAvailableKeysCount_Error_NotFound() {
	req := &grpc.GetPlatformKeyCountRequest{
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetPlatformKeyCountResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CountKeysByProductPlatform", req.KeyProductId, req.PlatformId).Return(0, errors.New("not found"))
	suite.service.keyRepository = kr

	err := suite.service.GetAvailableKeysCount(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorNotFound, res.Message)
}

func (suite *KeyTestSuite) TestKey_GetKeyByID_Ok() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetKeyForOrderRequestResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("GetById", req.KeyId).Return(&billing.Key{}, nil)
	suite.service.keyRepository = kr

	err := suite.service.GetKeyByID(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
}

func (suite *KeyTestSuite) TestKey_GetKeyByID_Error_NotFound() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetKeyForOrderRequestResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("GetById", req.KeyId).Return(nil, errors.New("not found"))
	suite.service.keyRepository = kr

	err := suite.service.GetKeyByID(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorNotFound, res.Message)
}

func (suite *KeyTestSuite) TestKey_ReserveKeyForOrder_Ok() {
	req := &grpc.PlatformKeyReserveRequest{
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		OrderId:      bson.NewObjectId().Hex(),
		Ttl:          3,
	}
	res := grpc.PlatformKeyReserveResponse{}
	keyId := bson.NewObjectId().Hex()

	kr := &mock.KeyRepositoryInterface{}
	kr.On("ReserveKey", req.KeyProductId, req.PlatformId, req.OrderId, req.Ttl).Return(&billing.Key{Id: keyId}, nil)
	suite.service.keyRepository = kr

	err := suite.service.ReserveKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), keyId, res.KeyId)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, res.Status)
}

func (suite *KeyTestSuite) TestKey_ReserveKeyForOrder_Error_Reserve() {
	req := &grpc.PlatformKeyReserveRequest{
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		OrderId:      bson.NewObjectId().Hex(),
		Ttl:          3,
	}
	res := grpc.PlatformKeyReserveResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("ReserveKey", req.KeyProductId, req.PlatformId, req.OrderId, req.Ttl).Return(nil, errors.New("error"))
	suite.service.keyRepository = kr

	err := suite.service.ReserveKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorReserve, res.Message)
}

func (suite *KeyTestSuite) TestKey_FinishRedeemKeyForOrder_Ok() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetKeyForOrderRequestResponse{}
	key := &billing.Key{
		Id: bson.NewObjectId().Hex(),
	}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FinishRedeemById", req.KeyId).Return(key, nil)
	suite.service.keyRepository = kr

	err := suite.service.FinishRedeemKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), key.Id, res.Key.Id)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, res.Status)
}

func (suite *KeyTestSuite) TestKey_FinishRedeemKeyForOrder_Error_NotFound() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.GetKeyForOrderRequestResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FinishRedeemById", req.KeyId).Return(nil, errors.New("not found"))
	suite.service.keyRepository = kr

	err := suite.service.FinishRedeemKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorFinish, res.Message)
}

func (suite *KeyTestSuite) TestKey_CancelRedeemKeyForOrder_Ok() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.EmptyResponseWithStatus{}
	key := &billing.Key{
		Id: bson.NewObjectId().Hex(),
	}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CancelById", req.KeyId).Return(key, nil)
	suite.service.keyRepository = kr

	err := suite.service.CancelRedeemKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, res.Status)
}

func (suite *KeyTestSuite) TestKey_CancelRedeemKeyForOrder_Error_NotFound() {
	req := &grpc.KeyForOrderRequest{
		KeyId: bson.NewObjectId().Hex(),
	}
	res := grpc.EmptyResponseWithStatus{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CancelById", req.KeyId).Return(nil, errors.New("not found"))
	suite.service.keyRepository = kr

	err := suite.service.CancelRedeemKeyForOrder(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusSystemError, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorCanceled, res.Message)
}

func (suite *KeyTestSuite) TestKey_UploadKeysFile_Ok() {
	req := &grpc.PlatformKeysFileRequest{
		KeyProductId: bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		File:         []byte{},
	}
	res := grpc.PlatformKeysFileResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CountKeysByProductPlatform", req.KeyProductId, req.PlatformId).Return(1, nil)
	kr.On("Insert", mock2.Anything).Return(nil)
	suite.service.keyRepository = kr

	err := suite.service.UploadKeysFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int32(1), res.TotalCount)
	assert.Equal(suite.T(), int32(0), res.KeysProcessed)
	assert.Equal(suite.T(), pkg.ResponseStatusOk, res.Status)
}

func (suite *KeyTestSuite) TestKey_UploadKeysFile_Error_CountKeysByProductPlatform() {
	req := &grpc.PlatformKeysFileRequest{
		KeyProductId: bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		File:         []byte{},
	}
	res := grpc.PlatformKeysFileResponse{}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("CountKeysByProductPlatform", req.KeyProductId, req.PlatformId).Return(0, errors.New("not found"))
	kr.On("Insert", mock2.Anything).Return(nil)
	suite.service.keyRepository = kr

	err := suite.service.UploadKeysFile(context.TODO(), req, &res)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), pkg.ResponseStatusNotFound, res.Status)
	assert.Equal(suite.T(), errors2.KeyErrorNotFound, res.Message)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Ok_Finished() {
	noTime, err := ptypes.TimestampProto(time.Time{})
	reserveTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))
	keys := []*billing.Key{{
		Id:         bson.NewObjectId().Hex(),
		OrderId:    bson.NewObjectId().Hex(),
		ReservedTo: reserveTime,
	}}
	keyFinished := keys[0]
	keyFinished.ReservedTo = noTime
	keyFinished.RedeemedAt = reserveTime
	orders := []*billing.Order{{
		Id:            bson.NewObjectId().Hex(),
		PrivateStatus: constant.OrderStatusPaymentSystemComplete,
	}}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(keys, nil)
	kr.On("FinishRedeemById", keys[0].Id).Return(keyFinished, nil)
	suite.service.keyRepository = kr

	or := &mock.OrderRepositoryInterface{}
	or.On("GetOrdersById", []string{keys[0].OrderId}).Return(orders, nil)
	suite.service.orderRepository = or

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, finished)
	assert.Equal(suite.T(), 0, cancelled)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Ok_Cancelled() {
	noTime, err := ptypes.TimestampProto(time.Time{})
	reserveTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))
	keys := []*billing.Key{{
		Id:         bson.NewObjectId().Hex(),
		OrderId:    bson.NewObjectId().Hex(),
		ReservedTo: reserveTime,
		RedeemedAt: noTime,
	}}
	orders := []*billing.Order{{
		Id: bson.NewObjectId().Hex(),
	}}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(keys, nil)
	kr.On("CancelById", keys[0].Id).Return(&billing.Key{}, nil)
	suite.service.keyRepository = kr

	or := &mock.OrderRepositoryInterface{}
	or.On("GetOrdersById", []string{keys[0].OrderId}).Return(orders, nil)
	suite.service.orderRepository = or

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, cancelled)
	assert.Equal(suite.T(), 0, finished)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Ok_NoJobs() {
	reserveTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, 1))
	keys := []*billing.Key{{
		Id:         bson.NewObjectId().Hex(),
		OrderId:    bson.NewObjectId().Hex(),
		ReservedTo: reserveTime,
	}}
	orders := []*billing.Order{{
		Id: bson.NewObjectId().Hex(),
	}}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(keys, nil)
	suite.service.keyRepository = kr

	or := &mock.OrderRepositoryInterface{}
	or.On("GetOrdersById", []string{keys[0].OrderId}).Return(orders, nil)
	suite.service.orderRepository = or

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, cancelled)
	assert.Equal(suite.T(), 0, finished)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Error_FindUnfinished() {
	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(nil, errors.New("not found"))
	suite.service.keyRepository = kr

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), 0, cancelled)
	assert.Equal(suite.T(), 0, finished)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Error_GetOrdersById() {
	keys := []*billing.Key{{
		Id:      bson.NewObjectId().Hex(),
		OrderId: bson.NewObjectId().Hex(),
	}}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(keys, nil)
	suite.service.keyRepository = kr

	or := &mock.OrderRepositoryInterface{}
	or.On("GetOrdersById", []string{keys[0].OrderId}).Return(nil, errors.New("not found"))
	suite.service.orderRepository = or

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), 0, cancelled)
	assert.Equal(suite.T(), 0, finished)
}

func (suite *KeyTestSuite) TestKey_KeyDaemonProcess_Error_CancelById() {
	noTime, err := ptypes.TimestampProto(time.Time{})
	reserveTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))
	keys := []*billing.Key{{
		Id:         bson.NewObjectId().Hex(),
		OrderId:    bson.NewObjectId().Hex(),
		ReservedTo: reserveTime,
		RedeemedAt: noTime,
	}}
	orders := []*billing.Order{{
		Id: bson.NewObjectId().Hex(),
	}}

	kr := &mock.KeyRepositoryInterface{}
	kr.On("FindUnfinished").Return(keys, nil)
	kr.On("CancelById", keys[0].Id).Return(nil, errors.New("not found"))
	suite.service.keyRepository = kr

	or := &mock.OrderRepositoryInterface{}
	or.On("GetOrdersById", []string{keys[0].OrderId}).Return(orders, nil)
	suite.service.orderRepository = or

	finished, cancelled, err := suite.service.KeyDaemonProcess()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), 0, cancelled)
	assert.Equal(suite.T(), 0, finished)
}

func (suite *KeyTestSuite) TestKey_FindUnfinished_Ok() {
	reserveTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, 1))
	finishedTime, err := ptypes.TimestampProto(time.Now().AddDate(0, 0, -1))

	keyReserved := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
		ReservedTo:   reserveTime,
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(keyReserved))

	keyCancelled := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(keyCancelled))

	keyFinished := &billing.Key{
		Id:           bson.NewObjectId().Hex(),
		PlatformId:   bson.NewObjectId().Hex(),
		KeyProductId: bson.NewObjectId().Hex(),
		Code:         "code1",
		RedeemedAt:   finishedTime,
	}
	assert.NoError(suite.T(), suite.service.keyRepository.Insert(keyFinished))

	keys, err := suite.service.keyRepository.FindUnfinished()
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), keys, 1)
	assert.Equal(suite.T(), keyReserved.Id, keys[0].Id)
}
