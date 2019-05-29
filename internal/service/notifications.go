package service

import (
	"container/list"
	"github.com/globalsign/mgo/bson"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2"
	"time"
)

const (
	errorInitNotificationsFailed            = "notifications init failed"
	errorRetryPublishNotificationFailed     = "retry publish order notification to queue failed"
	errorTooManyFailuresPublishNotification = "too many failures while retry publish order notification to queue"
	errorCentrifugoNotificationFailure      = "cannot send centrifugo message about order notification failure"
	errorDbStoreNotificationsFailed         = "queued notifications store to db failed"
	errorDbCleanNotificationsFailed         = "clean queued notifications from DB failed"
	errorCentrifugoStoreNotificationFailure = "cannot send centrifugo message about queued notifications store to db failed"
	errorDbGetNotificationsFailed           = "cannot get saved notifications from db"
)

var (
	notificationsStoreCleaned = false
)

func (s *Service) initNotifications() error {
	s.notificationsRetryCount = int32(0)
	s.notificationsQueue = list.New()

	err := s.getSavedNotifications()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go s.processNotifications()

	s.wg.Add(1)
	go s.processRetryNotifications()

	return nil
}

func (s *Service) processRetryNotifications() {
	defer s.wg.Done()
	s.notificationsRetryTicker = time.NewTicker(time.Second * time.Duration(s.cfg.NotificationsRetryTimeout))
	for {
		select {
		case <-s.notificationsRetryTicker.C:
			s.notificationsWg.Wait()
			s.notificationsWg.Add(1)
			go s.publishNotifications()

		case <-s.exitNotificationsPublish:
			zap.S().Info("Notifications sending retry loop stopped")
			return
		}
	}

}

func (s *Service) processNotifications() {
	defer s.wg.Done()
	for {
		select {
		case order, ok := <-s.notifications:
			if !ok {
				zap.L().Error("Process notifications channel closed")
				continue
			}
			// add new order to the end of queue
			s.notificationsQueue.PushBack(order)

		case <-s.exitNotifications:
			zap.S().Info("Saving queued notifications do DB...")
			s.notificationsRetryTicker.Stop()
			s.notificationsWg.Wait()
			s.saveQueuedNotifications()
			zap.S().Info("Saving queued notifications finished, see log to details")
			return
		}
	}
}

func (s *Service) getSavedNotifications() error {

	var savedNotifications []*billing.Order

	err := s.db.Collection(pkg.CollectionFailedOrderNotifications).Find(bson.M{}).All(&savedNotifications)
	if err != nil && err != mgo.ErrNotFound {
		zap.S().Errorw(errorDbGetNotificationsFailed, "error", err)
		return err
	}

	for _, order := range savedNotifications {
		// restoring original order id
		if order.OriginalOrderId != "" {
			order.Id = order.OriginalOrderId
		}
		s.notificationsQueue.PushBack(order)
	}

	return nil
}

func (s *Service) saveQueuedNotifications() {
	if s.notificationsQueue.Len() == 0 {
		return
	}

	var queuedNotifications []interface{}

	for s.notificationsQueue.Len() > 0 {
		e := s.notificationsQueue.Front()
		order := e.Value.(*billing.Order)

		// prevent replace orders with the same id but different status
		order.OriginalOrderId = order.Id
		order.Id = bson.NewObjectId().Hex()

		queuedNotifications = append(queuedNotifications, order)
		s.notificationsQueue.Remove(e)
	}

	s.cleanNotificationsStore()

	err := s.db.Collection(pkg.CollectionFailedOrderNotifications).Insert(queuedNotifications...)

	if err != nil {
		zap.S().Errorw(errorDbStoreNotificationsFailed, "error", err, "data", queuedNotifications)

		msg := map[string]interface{}{
			"event":   "error",
			"message": errorDbStoreNotificationsFailed,
			"data":    queuedNotifications,
		}
		sErr := s.sendCentrifugoMessage(msg)
		if sErr != nil {
			s.logError(errorCentrifugoStoreNotificationFailure, []interface{}{
				"topic", constant.PayOneTopicNotifyMerchantName, "attempt", s.notificationsRetryCount,
			})
		}
	}
}

func (s *Service) cleanNotificationsStore() {
	if notificationsStoreCleaned {
		return
	}
	var selector interface{}
	_, err := s.db.Collection(pkg.CollectionFailedOrderNotifications).RemoveAll(selector)
	if err != nil {
		zap.S().Errorw(errorDbCleanNotificationsFailed, "error", err)
	}
	notificationsStoreCleaned = true
}

func (s *Service) publishNotifications() {
	zap.S().Info("Publishing notifications")
	defer s.notificationsWg.Done()

	for s.notificationsQueue.Len() > 0 {
		e := s.notificationsQueue.Front()
		order := e.Value.(*billing.Order)

		err := s.broker.Publish(constant.PayOneTopicNotifyMerchantName, order, amqp.Table{"x-retry-count": int32(0)})
		if err == nil {
			s.notificationsQueue.Remove(e)
		} else {
			s.logError(errorRetryPublishNotificationFailed, []interface{}{
				"err", err.Error(), "order", order, "topic", constant.PayOneTopicNotifyMerchantName,
				"attempt", s.notificationsRetryCount,
			})
			// stop publish to prevent infinity loop when broker is down
			break
		}
	}

	if s.notificationsQueue.Len() == 0 {
		s.notificationsRetryCount = 0
		s.cleanNotificationsStore()
	} else {
		s.notificationsRetryCount++
	}

	if s.notificationsRetryCount < s.cfg.NotificationsRetryAlarm {
		return
	}

	s.logError(errorTooManyFailuresPublishNotification, []interface{}{
		"topic", constant.PayOneTopicNotifyMerchantName, "attempt", s.notificationsRetryCount,
	})

	msg := map[string]interface{}{
		"event":       "error",
		"message":     errorTooManyFailuresPublishNotification,
		"retry_count": s.notificationsRetryCount,
	}
	sErr := s.sendCentrifugoMessage(msg)
	if sErr != nil {
		s.logError(errorCentrifugoNotificationFailure, []interface{}{
			"topic", constant.PayOneTopicNotifyMerchantName, "attempt", s.notificationsRetryCount,
		})
	}
}

func (s *Service) orderNotifyMerchant(order billing.Order) {
	s.notifications <- &order
}
