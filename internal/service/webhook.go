package service

import (
	"context"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

var (
	webhookTypeIncorrect = newBillingServerErrorMsg("wh000001", "type for webhook is invalid")
)

const (
	orderRequestType = "type"
)

func (s *Service) SendWebhookToMerchant(ctx context.Context, req *billing.OrderCreateRequest, res *grpc.SendWebhookToMerchantResponse) error {
	res.Status = pkg.ResponseStatusOk

	processor := &OrderCreateRequestProcessor{
		Service: s,
		request: req,
		checked: &orderCreateRequestProcessorChecked{},
	}

	if req.Signature != "" || processor.checked.project.SignatureRequired == true {
		if err := processor.processSignature(); err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}
	}

	switch req.Type {
	case billing.OrderType_key:
		if err := processor.processPaylinkKeyProducts(); err != nil {
			if pid := req.PrivateMetadata["PaylinkId"]; pid != "" {
				s.notifyPaylinkError(ctx, pid, err, req, nil)
			}
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}
		break
	case billing.OrderType_product:
		if err := processor.processPaylinkProducts(ctx); err != nil {
			if pid := req.PrivateMetadata["PaylinkId"]; pid != "" {
				s.notifyPaylinkError(ctx, pid, err, req, nil)
			}
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}
		break
	case billing.OrderTypeVirtualCurrency:
		err := processor.processVirtualCurrency(ctx)
		if err != nil {
			zap.L().Error(
				pkg.MethodFinishedWithError,
				zap.Error(err),
			)

			res.Status = pkg.ResponseStatusBadData
			res.Message = err.(*grpc.ResponseErrorMessage)
			return nil
		}
		break
	default:
		zap.L().Error(
			webhookTypeIncorrect.Message,
			zap.String(orderRequestType, req.Type),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = webhookTypeIncorrect
		res.Message.Details = req.Type
		return nil
	}

	if req.User != nil {
		err := processor.processUserData()

		if err != nil {
			zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
			if e, ok := err.(*grpc.ResponseErrorMessage); ok {
				res.Status = pkg.ResponseStatusBadData
				res.Message = e
				return nil
			}
			return err
		}
	}

	if processor.checked.currency == "" {
		res.Status = pkg.ResponseStatusBadData
		res.Message = orderErrorCurrencyIsRequired
		return nil
	}

	processor.processMetadata()
	processor.processPrivateMetadata()

	order, err := processor.prepareOrder()
	if err != nil {
		zap.S().Errorw(pkg.MethodFinishedWithError, "err", err.Error())
		if e, ok := err.(*grpc.ResponseErrorMessage); ok {
			res.Status = pkg.ResponseStatusBadData
			res.Message = e
			return nil
		}
		return err
	}

	zap.S().Debug("[orderNotifyMerchant] send notify merchant to rmq failed", "order_id", order.Id)
	err = s.broker.Publish(constant.PayOneTopicNotifyPaymentName, order, amqp.Table{"x-retry-count": int32(0)})
	if err != nil {
		zap.S().Debug("[orderNotifyMerchant] send notify merchant to rmq failed", "order_id", order.Id)
		s.logError(orderErrorPublishNotificationFailed, []interface{}{
			"err", err.Error(), "order", order, "topic", constant.PayOneTopicNotifyPaymentName,
		})
	} else {
		zap.S().Debug("[orderNotifyMerchant] send notify merchant to rmq failed", "order_id", order.Id)
	}

	res.OrderId = order.GetMerchantId()

	return nil
}

func (s *Service) NotifyWebhookTestResults(ctx context.Context, req *grpc.NotifyWebhookTestResultsRequest, res *grpc.EmptyResponseWithStatus) error {
	res.Status = pkg.ResponseStatusOk

	project, err := s.project.GetById(req.ProjectId)
	if err != nil {
		zap.L().Error(
			pkg.ErrorGrpcServiceCallFailed,
			zap.Error(err),
			zap.String(errorFieldService, "Project"),
			zap.String(errorFieldMethod, "GetById"),
		)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = projectErrorUnknown
		return nil
	}

	if project.WebhookTesting == nil {
		project.WebhookTesting = &billing.WebHookTesting {
		}
	}

	switch req.Type {
	case billing.OrderType_product:
		s.processTestingProducts(project, req)
		break
	case billing.OrderType_key:
		s.processTestingKeys(project, req)
		break
	case billing.OrderTypeVirtualCurrency:
		s.processTestingVirtualCurrency(project, req)
		break
	default:
		zap.L().Error(
			pkg.UnknownTypeError,
			zap.Error(err),
			zap.String(errorFieldService, "Project"),
			zap.String(errorFieldMethod, "GetById"),
		)
		res.Status = pkg.ResponseStatusBadData
		res.Message = webhookTypeIncorrect
		res.Message.Details = req.Type
		return nil
	}

	return nil
}


func (s *Service) processTestingVirtualCurrency(project *billing.Project, req *grpc.NotifyWebhookTestResultsRequest) {
	if project.WebhookTesting.VirtualCurrency == nil {
		project.WebhookTesting.VirtualCurrency = &billing.VirtualCurrencyTesting{}
	}
	switch req.TestCase {
	case pkg.TestCaseNonExistingUser:
		project.WebhookTesting.VirtualCurrency.NonExistingUser = req.IsPassed
		break
	case pkg.TestCaseExistingUser:
		project.WebhookTesting.VirtualCurrency.ExistingUser = req.IsPassed
		break
	case pkg.TestCaseCorrectPayment:
		project.WebhookTesting.VirtualCurrency.CorrectPayment = req.IsPassed
		break
	case pkg.TestCaseIncorrectPayment:
		project.WebhookTesting.VirtualCurrency.IncorrectPayment = req.IsPassed
		break
	}
}

func (s *Service) processTestingKeys(project *billing.Project, req *grpc.NotifyWebhookTestResultsRequest) {
	if project.WebhookTesting.Keys == nil {
		project.WebhookTesting.Keys = &billing.KeysTesting{}
	}
	project.WebhookTesting.Keys.IsPassed = req.IsPassed
}

func (s *Service) processTestingProducts(project *billing.Project, req *grpc.NotifyWebhookTestResultsRequest) {
	if project.WebhookTesting.Products == nil {
		project.WebhookTesting.Products = &billing.ProductsTesting{}
	}
	switch req.TestCase {
	case pkg.TestCaseNonExistingUser:
		project.WebhookTesting.Products.NonExistingUser = req.IsPassed
		break
	case pkg.TestCaseExistingUser:
		project.WebhookTesting.Products.ExistingUser = req.IsPassed
		break
	case pkg.TestCaseCorrectPayment:
		project.WebhookTesting.Products.CorrectPayment = req.IsPassed
		break
	case pkg.TestCaseIncorrectPayment:
		project.WebhookTesting.Products.IncorrectPayment = req.IsPassed
		break
	}
}
