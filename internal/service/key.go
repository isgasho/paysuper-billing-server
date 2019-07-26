package service

import (
	"bufio"
	"bytes"
	"context"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"go.uber.org/zap"
	"time"
)

const collectionKey = "key"

func (s *Service) UploadKeysFile(ctx context.Context, req *grpc.PlatformKeysFileRequest, res *grpc.PlatformKeysFileResponse) error {
	scanner := bufio.NewScanner(bytes.NewReader(req.File))
	count, err := s.keyRepository.CountKeysByProductPlatform(req.KeyProductId, req.PlatformId)

	if err != nil {
		zap.S().Errorf(pkg.KeyErrorNotFound.Message, "err", err.Error(), "keyProductId", req.KeyProductId, "platformId", req.PlatformId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = pkg.KeyErrorNotFound
		return nil
	}

	res.TotalCount = int32(count)

	// Process key by line
	for scanner.Scan() {
		key := &grpc.Key{
			Id:           bson.NewObjectId().Hex(),
			Code:         scanner.Text(),
			KeyProductId: req.KeyProductId,
			PlatformId:   req.PlatformId,
		}

		if err := s.keyRepository.Insert(key); err != nil {
			zap.S().Errorf(pkg.KeyErrorFailedToInsert.Message, "err", err, "key", key)
			continue
		}

		res.TotalCount++
		res.KeysProcessed++
	}

	// tell about errors
	if err = scanner.Err(); err != nil {
		zap.S().Errorf(pkg.KeyErrorFileProcess.Message, "err", err.Error())
		res.Message = pkg.KeyErrorFileProcess
		res.Status = pkg.ResponseStatusBadData
		return nil
	}

	return nil
}

func (s *Service) GetAvailableKeysCount(ctx context.Context, req *grpc.GetPlatformKeyCountRequest, res *grpc.GetPlatformKeyCountResponse) error {
	count, err := s.keyRepository.CountKeysByProductPlatform(req.KeyProductId, req.PlatformId)

	if err != nil {
		zap.S().Errorf(pkg.KeyErrorNotFound.Message, "err", err.Error(), "keyProductId", req.KeyProductId, "platformId", req.PlatformId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = pkg.KeyErrorNotFound
		return nil
	}

	res.Count = int32(count)
	return nil
}

func (s *Service) GetKeyByID(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.GetKeyForOrderRequestResponse) error {
	key, err := s.keyRepository.GetById(req.KeyId)

	if err != nil {
		zap.S().Errorf(pkg.KeyErrorNotFound.Message, "err", err.Error(), "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = pkg.KeyErrorNotFound
		return nil
	}

	res.Key = key

	return nil
}

func (s *Service) ReserveKeyForOrder(ctx context.Context, req *grpc.PlatformKeyReserveRequest, res *grpc.PlatformKeyReserveResponse) error {
	key, err := s.keyRepository.ReserveKey(req.KeyProductId, req.PlatformId, req.OrderId, req.Ttl)

	if err != nil {
		zap.S().Errorf(
			pkg.KeyErrorReserve.Message,
			"err", err,
			"keyProductId", req.KeyProductId,
			"platformId", req.PlatformId,
			"orderId", req.OrderId,
			"ttl", req.Ttl,
		)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = pkg.KeyErrorReserve
	}

	res.KeyId = key.Id

	return nil
}

func (s *Service) FinishRedeemKeyForOrder(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.GetKeyForOrderRequestResponse) error {
	key, err := s.keyRepository.FinishRedeemById(req.KeyId)

	if err != nil {
		zap.S().Errorf(pkg.KeyErrorFinish.Message, "err", err, "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = pkg.KeyErrorFinish
	}

	res.Key = key

	return nil
}

func (s *Service) CancelRedeemKeyForOrder(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.EmptyResponseWithStatus) error {
	if err := s.keyRepository.CancelById(req.KeyId); err != nil {
		zap.S().Errorf(pkg.KeyErrorCanceled.Message, "err", err, "keyId", req.KeyId)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = pkg.KeyErrorCanceled
	}

	return nil
}

type KeyRepositoryInterface interface {
	Insert(*grpc.Key) error
	CountKeysByProductPlatform(string, string) (int, error)
	GetById(string) (*grpc.Key, error)
	CancelById(string) error
	FinishRedeemById(string) (*grpc.Key, error)
	ReserveKey(string, string, string, int32) (*grpc.Key, error)
}

func newKeyRepository(svc *Service) *Key {
	s := &Key{svc: svc}
	return s
}

func (h *Key) Insert(key *grpc.Key) error {
	err := h.svc.db.Collection(collectionPriceGroup).Insert(key)

	if err != nil {
		return err
	}

	return nil
}

func (h *Key) CountKeysByProductPlatform(keyProductId string, platformId string) (int, error) {
	query := bson.M{
		"key_product_id": bson.ObjectIdHex(keyProductId),
		"platform_id":    bson.ObjectIdHex(platformId),
		"order_id":       bson.ObjectIdHex(""),
	}

	return h.svc.db.Collection(collectionKey).Find(query).Count()
}

func (h *Key) GetById(id string) (*grpc.Key, error) {
	key := &grpc.Key{}
	err := h.svc.db.Collection(collectionKey).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(key)

	if err != nil {
		return nil, err
	}

	return key, nil
}

func (h *Key) CancelById(id string) error {
	query := bson.M{"_id": bson.ObjectIdHex(id)}
	change := mgo.Change{
		Update: bson.M{
			"reserved_to": "",
			"order_id":    bson.ObjectIdHex(""),
		},
	}

	info, err := h.svc.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, &grpc.Key{})

	if err != nil {
		return err
	}

	if info.Updated == 0 {
		return pkg.KeyErrorNotFound
	}

	return nil
}

func (h *Key) FinishRedeemById(id string) (*grpc.Key, error) {
	key := &grpc.Key{}
	query := bson.M{"_id": bson.ObjectIdHex(id)}
	change := mgo.Change{
		Update: bson.M{
			"reserved_to": "",
			"redeemed_at": time.Now().UTC(),
		},
	}

	info, err := h.svc.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, key)

	if err != nil {
		return nil, err
	}

	if info.Updated == 0 {
		return nil, pkg.KeyErrorNotFound
	}

	return key, nil
}

func (h *Key) ReserveKey(keyProductId string, platformId string, orderId string, ttl int32) (*grpc.Key, error) {
	key := &grpc.Key{}
	duration := time.Second * time.Duration(ttl)
	query := bson.M{
		"key_product_id": bson.ObjectIdHex(keyProductId),
		"platform_id":    bson.ObjectIdHex(platformId),
		"order_id":       bson.ObjectIdHex(""),
	}
	change := mgo.Change{
		Update: bson.M{
			"reserved_to": time.Now().UTC().Add(duration),
			"order_id":    bson.ObjectIdHex(orderId),
		},
	}

	info, err := h.svc.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, key)

	if err != nil {
		return nil, err
	}

	if info.Updated == 0 {
		return nil, pkg.KeyErrorNotFound
	}

	return key, nil
}
