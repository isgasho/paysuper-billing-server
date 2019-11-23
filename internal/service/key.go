package service

import (
	"bufio"
	"bytes"
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/errors"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"time"
)

const collectionKey = "key"

func (s *Service) UploadKeysFile(
	ctx context.Context,
	req *grpc.PlatformKeysFileRequest,
	res *grpc.PlatformKeysFileResponse,
) error {
	scanner := bufio.NewScanner(bytes.NewReader(req.File))
	count, err := s.keyRepository.CountKeysByProductPlatform(ctx, req.KeyProductId, req.PlatformId)

	if err != nil {
		zap.S().Errorf(errors.KeyErrorNotFound.Message, "err", err.Error(), "keyProductId", req.KeyProductId, "platformId", req.PlatformId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errors.KeyErrorNotFound
		return nil
	}

	res.TotalCount = int32(count)

	// Process key by line
	for scanner.Scan() {
		key := &billing.Key{
			Id:           primitive.NewObjectID().Hex(),
			Code:         scanner.Text(),
			KeyProductId: req.KeyProductId,
			PlatformId:   req.PlatformId,
		}

		if err := s.keyRepository.Insert(ctx, key); err != nil {
			zap.S().Errorf(errors.KeyErrorFailedToInsert.Message, "err", err, "key", key)
			continue
		}

		res.TotalCount++
		res.KeysProcessed++
	}

	// tell about errors
	if err = scanner.Err(); err != nil {
		zap.S().Errorf(errors.KeyErrorFileProcess.Message, "err", err.Error())
		res.Message = errors.KeyErrorFileProcess
		res.Status = pkg.ResponseStatusBadData
		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetAvailableKeysCount(
	ctx context.Context,
	req *grpc.GetPlatformKeyCountRequest,
	res *grpc.GetPlatformKeyCountResponse,
) error {
	keyProduct, err := s.keyProductRepository.GetById(ctx, req.KeyProductId)

	if err != nil {
		zap.S().Errorf(keyProductNotFound.Message, "err", err.Error(), "keyProductId", req.KeyProductId, "platformId", req.PlatformId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = keyProductNotFound
		return nil
	}

	if keyProduct.MerchantId != req.MerchantId {
		zap.S().Error(keyProductMerchantMismatch.Message, "keyProductId", req.KeyProductId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = keyProductMerchantMismatch
		return nil
	}

	count, err := s.keyRepository.CountKeysByProductPlatform(ctx, req.KeyProductId, req.PlatformId)

	if err != nil {
		zap.S().Errorf(errors.KeyErrorNotFound.Message, "err", err.Error(), "keyProductId", req.KeyProductId, "platformId", req.PlatformId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errors.KeyErrorNotFound
		return nil
	}

	res.Count = int32(count)
	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) GetKeyByID(
	ctx context.Context,
	req *grpc.KeyForOrderRequest,
	res *grpc.GetKeyForOrderRequestResponse,
) error {
	key, err := s.keyRepository.GetById(ctx, req.KeyId)

	if err != nil {
		zap.S().Errorf(errors.KeyErrorNotFound.Message, "err", err.Error(), "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errors.KeyErrorNotFound
		return nil
	}

	res.Key = key

	return nil
}

func (s *Service) ReserveKeyForOrder(
	ctx context.Context,
	req *grpc.PlatformKeyReserveRequest,
	res *grpc.PlatformKeyReserveResponse,
) error {
	zap.S().Infow("[ReserveKeyForOrder] called", "order_id", req.OrderId, "platform_id", req.PlatformId, "KeyProductId", req.KeyProductId)
	key, err := s.keyRepository.ReserveKey(ctx, req.KeyProductId, req.PlatformId, req.OrderId, req.Ttl)
	if err != nil {
		zap.S().Errorf(
			errors.KeyErrorReserve.Message,
			"err", err,
			"keyProductId", req.KeyProductId,
			"platformId", req.PlatformId,
			"orderId", req.OrderId,
			"ttl", req.Ttl,
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errors.KeyErrorReserve
		return nil
	}

	zap.S().Infow("[ReserveKeyForOrder] reserved key", "req.order_id", req.OrderId, "key.order_id", key.OrderId, "key.id", key.Id, "key.RedeemedAt", key.RedeemedAt, "key.KeyProductId", key.KeyProductId)

	res.KeyId = key.Id
	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) FinishRedeemKeyForOrder(
	ctx context.Context,
	req *grpc.KeyForOrderRequest,
	res *grpc.GetKeyForOrderRequestResponse,
) error {
	key, err := s.keyRepository.FinishRedeemById(ctx, req.KeyId)

	if err != nil {
		zap.S().Errorf(errors.KeyErrorFinish.Message, "err", err, "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errors.KeyErrorFinish
		return nil
	}

	res.Key = key
	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) CancelRedeemKeyForOrder(
	ctx context.Context,
	req *grpc.KeyForOrderRequest,
	res *grpc.EmptyResponseWithStatus,
) error {
	_, err := s.keyRepository.CancelById(ctx, req.KeyId)

	if err != nil {
		zap.S().Errorf(errors.KeyErrorCanceled.Message, "err", err, "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errors.KeyErrorCanceled
		return nil
	}

	res.Status = pkg.ResponseStatusOk

	return nil
}

func (s *Service) KeyDaemonProcess(ctx context.Context) (int, error) {
	counter := 0
	keys, err := s.keyRepository.FindUnfinished(ctx)

	if err != nil {
		return counter, err
	}

	for _, key := range keys {
		_, err = s.keyRepository.CancelById(ctx, key.Id)

		if err != nil {
			zap.S().Errorf(errors.KeyErrorCanceled.Message, "err", err, "keyId", key.Id)
			continue
		}

		counter++
	}

	return counter, nil
}

type KeyRepositoryInterface interface {
	Insert(context.Context, *billing.Key) error
	GetById(context.Context, string) (*billing.Key, error)
	ReserveKey(context.Context, string, string, string, int32) (*billing.Key, error)
	CancelById(context.Context, string) (*billing.Key, error)
	FinishRedeemById(context.Context, string) (*billing.Key, error)
	CountKeysByProductPlatform(context.Context, string, string) (int64, error)
	FindUnfinished(context.Context) ([]*billing.Key, error)
}

func newKeyRepository(svc *Service) *Key {
	s := &Key{svc: svc}
	return s
}

func (h *Key) Insert(ctx context.Context, key *billing.Key) error {
	_, err := h.svc.db.Collection(collectionKey).InsertOne(ctx, key)

	if err != nil {
		return err
	}

	return nil
}

func (h *Key) GetById(ctx context.Context, id string) (*billing.Key, error) {
	key := &billing.Key{}
	oid, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": oid}
	err := h.svc.db.Collection(collectionKey).FindOne(ctx, filter).Decode(key)

	if err != nil {
		return nil, err
	}

	return key, nil
}

func (h *Key) ReserveKey(
	ctx context.Context,
	keyProductId string,
	platformId string,
	orderId string,
	ttl int32,
) (*billing.Key, error) {
	var key *billing.Key
	duration := time.Second * time.Duration(ttl)
	oid, _ := primitive.ObjectIDFromHex(keyProductId)
	orderOid, _ := primitive.ObjectIDFromHex(orderId)

	query := bson.M{
		"key_product_id": oid,
		"platform_id":    platformId,
		"order_id":       nil,
	}
	update := bson.M{
		"$set": bson.M{
			"reserved_to": time.Now().UTC().Add(duration),
			"order_id":    orderOid,
		},
	}

	err := h.svc.db.Collection(collectionKey).FindOneAndUpdate(ctx, query, update).Decode(&key)

	if err == mongo.ErrNoDocuments {
		return nil, errors.KeyErrorNotFound
	}

	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, errors.KeyErrorNotFound
	}

	return key, nil
}

func (h *Key) CancelById(ctx context.Context, id string) (*billing.Key, error) {
	var key *billing.Key
	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid}
	update := bson.M{
		"$set": bson.M{
			"reserved_to": "",
			"order_id":    nil,
		},
	}

	err := h.svc.db.Collection(collectionKey).FindOneAndUpdate(ctx, query, update).Decode(&key)

	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, errors.KeyErrorNotFound
	}

	return key, nil
}

func (h *Key) FinishRedeemById(ctx context.Context, id string) (*billing.Key, error) {
	var key *billing.Key
	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid}
	update := bson.M{
		"$set": bson.M{
			"reserved_to": "",
			"redeemed_at": time.Now().UTC(),
		},
	}

	err := h.svc.db.Collection(collectionKey).FindOneAndUpdate(ctx, query, update).Decode(&key)

	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, errors.KeyErrorNotFound
	}

	return key, nil
}

func (h *Key) CountKeysByProductPlatform(ctx context.Context, keyProductId string, platformId string) (int64, error) {
	oid, _ := primitive.ObjectIDFromHex(keyProductId)
	query := bson.M{
		"key_product_id": oid,
		"platform_id":    platformId,
		"order_id":       nil,
	}

	return h.svc.db.Collection(collectionKey).CountDocuments(ctx, query)
}

func (h *Key) FindUnfinished(ctx context.Context) ([]*billing.Key, error) {
	var keys []*billing.Key

	query := bson.M{
		"reserved_to": bson.M{
			"$gt": time.Time{},
			"$lt": time.Now().UTC(),
		},
	}

	cursor, err := h.svc.db.Collection(collectionKey).Find(ctx, query)

	if err != nil {
		return nil, err
	}

	err = cursor.All(ctx, &keys)

	if err != nil {
		return nil, err
	}

	return keys, nil
}
