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

var (
	KeyFileProcessFailed = newBillingServerErrorMsg("ks000001", "failed to process file")
	KeyDbError           = newBillingServerErrorMsg("ks000002", "failed to retrieve data from db")
	KeyNotFound          = newBillingServerErrorMsg("ks000003", "key not found")
)

func (s *Service) UploadKeysFile(ctx context.Context, req *grpc.PlatformKeysFileRequest, res *grpc.PlatformKeysFileResponse) error {
	scanner := bufio.NewScanner(bytes.NewReader(req.File))
	var err error
	var count int

	// Get total available count of keys
	query := bson.M{
		"key_product_id": bson.ObjectIdHex(req.KeyProductId),
		"platform_id":    bson.ObjectIdHex(req.PlatformId),
		"order_id":       bson.ObjectIdHex(""),
	}
	count, err = s.db.Collection(collectionKey).Find(query).Count()

	if err != nil {
		zap.S().Error("err", err)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = KeyDbError
		return nil
	}

	res.TotalCount = int32(count)

	// Process key by line
	for scanner.Scan() {
		code := scanner.Text()

		// Check code is unique for platform
		err := s.db.Collection(collectionKey).Find(bson.M{"code": code, "platform_id": bson.ObjectIdHex(req.PlatformId)}).One(&grpc.Key{})
		if err == nil {
			continue
		}

		if err != mgo.ErrNotFound {
			res.Status = pkg.ResponseStatusSystemError
			res.Message = KeyDbError
			return nil
		}

		key := &grpc.Key{
			Id:           bson.NewObjectId().Hex(),
			Code:         code,
			KeyProductId: req.KeyProductId,
			PlatformId:   req.PlatformId,
		}

		if err = s.db.Collection(collectionKey).Insert(key); err != nil {
			zap.S().Error("err", err)
		} else {
			res.TotalCount++
			res.KeysProcessed++
		}
	}

	// tell about errors
	if err := scanner.Err(); err != nil {
		zap.S().Error("err", err)
		res.Message = KeyFileProcessFailed
		res.Status = pkg.ResponseStatusBadData
		return nil
	}

	return nil
}

func (s *Service) GetAvailableKeysCount(ctx context.Context, req *grpc.GetPlatformKeyCountRequest, res *grpc.GetPlatformKeyCountResponse) error {
	var err error
	var count int

	// Get total available count of keys
	query := bson.M{
		"key_product_id": bson.ObjectIdHex(req.KeyProductId),
		"platform_id":    bson.ObjectIdHex(req.PlatformId),
		"order_id":       bson.ObjectIdHex(""),
	}
	count, err = s.db.Collection(collectionKey).Find(query).Count()

	if err != nil {
		zap.S().Error("err", err)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = KeyDbError
		return nil
	}

	res.Count = int32(count)
	return nil
}

func (s *Service) GetKeyByID(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.GetKeyForOrderRequestResponse) error {
	query := bson.M{
		"_id": bson.ObjectIdHex(req.KeyId),
	}

	var err *grpc.ResponseErrorMessage
	if res.Key, err = s.getKeyBy(query); err != nil {
		res.Message = err
		if err == KeyNotFound {
			res.Status = pkg.ResponseStatusNotFound
			return nil
		}
		res.Message = KeyDbError
		return nil
	}

	return nil
}

func (s *Service) getKeyBy(query bson.M) (*grpc.Key, *grpc.ResponseErrorMessage) {
	key := &grpc.Key{}
	var err error
	if err = s.db.Collection(collectionKey).Find(query).One(&key); err != nil {
		zap.S().Error("err", err)
		if err == mgo.ErrNotFound {
			return nil, KeyNotFound
		}
		return nil, KeyDbError
	}

	return key, nil
}

func (s *Service) ReserveKeyForOrder(ctx context.Context, req *grpc.PlatformKeyReserveRequest, res *grpc.PlatformKeyReserveResponse) error {
	query := bson.M{
		"key_product_id": bson.ObjectIdHex(req.KeyProductId),
		"platform_id":    bson.ObjectIdHex(req.PlatformId),
		"order_id":       bson.ObjectIdHex(""),
	}


	duration := time.Second * time.Duration(req.Ttl)
	reservedTo := time.Now().UTC().Add(duration)

	change := mgo.Change{
		Update: bson.M{
			"reserved_to": reservedTo,
			"order_id":    req.OrderId,
		},
		ReturnNew: false,
	}

	key := &grpc.Key{}

	// Reserving first available key
	if info, err := s.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, key); err != nil || info.Updated == 0 {
		if err != mgo.ErrNotFound {
			zap.S().Error("err", err)
			res.Status = pkg.ResponseStatusSystemError
			res.Message = KeyDbError
			return nil
		}

		zap.S().Error("err", err)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = KeyNotFound
		return nil
	}

	res.KeyId = key.Id

	return nil
}

func (s *Service) FinishRedeemKeyForOrder(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.GetKeyForOrderRequestResponse) error {
	query := bson.M{
		"_id": bson.ObjectIdHex(req.KeyId),
	}

	change := mgo.Change{
		Update: bson.M{
			"reserved_to": "",
			"redeemed_at": time.Now().UTC(),
		},
		ReturnNew: false,
	}

	key := &grpc.Key{}

	// Reserving first available key
	if info, err := s.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, key); err != nil || info.Updated == 0 {
		if err != mgo.ErrNotFound {
			zap.S().Error("err", err)
			res.Status = pkg.ResponseStatusSystemError
			res.Message = KeyDbError
			return nil
		}

		zap.S().Error("err", err)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = KeyNotFound
		return nil
	}

	res.Key = key

	return nil
}

func (s *Service) CancelRedeemKeyForOrder(ctx context.Context, req *grpc.KeyForOrderRequest, res *grpc.EmptyResponseWithStatus) error {
	query := bson.M{
		"_id": bson.ObjectIdHex(req.KeyId),
	}

	change := mgo.Change{
		Update: bson.M{
			"reserved_to": "",
			"order_id": bson.ObjectIdHex(""),
		},
		ReturnNew: false,
	}

	key := &grpc.Key{}

	// Reserving first available key
	if info, err := s.db.Collection(collectionKey).Find(query).Limit(1).Apply(change, key); err != nil || info.Updated == 0 {
		if err != mgo.ErrNotFound {
			zap.S().Error("err", err)
			res.Status = pkg.ResponseStatusSystemError
			res.Message = KeyDbError
			return nil
		}

		zap.S().Error("err", err)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = KeyNotFound
		return nil
	}

	return nil
}
