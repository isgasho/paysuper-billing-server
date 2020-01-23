package service

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/internal/helper"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"net/http"
)

const (
	cacheProjectId = "project:id:%s"

	collectionProject = "project"
)

var (
	projectErrorUnknown                                          = newBillingServerErrorMsg("pr000001", "project unknown error")
	projectErrorNotFound                                         = newBillingServerErrorMsg("pr000002", "project with specified identifier not found")
	projectErrorNameDefaultLangRequired                          = newBillingServerErrorMsg("pr000003", "project name in \""+DefaultLanguage+"\" locale is required")
	projectErrorCallbackCurrencyIncorrect                        = newBillingServerErrorMsg("pr000004", "project callback currency is incorrect")
	projectErrorLimitCurrencyIncorrect                           = newBillingServerErrorMsg("pr000005", "project limit currency is incorrect")
	projectErrorLimitCurrencyRequired                            = newBillingServerErrorMsg("pr000006", "project limit currency can't be empty if you send min or max payment amount")
	projectErrorCurrencyIsNotSupport                             = newBillingServerErrorMsg("pr000007", `project currency is not supported`)
	projectErrorVirtualCurrencyNameDefaultLangRequired           = newBillingServerErrorMsg("pr000008", "project virtual currency name in \""+DefaultLanguage+"\" locale is required")
	projectErrorVirtualCurrencySuccessMessageDefaultLangRequired = newBillingServerErrorMsg("pr000009", "project virtual currency success message in \""+DefaultLanguage+"\" locale is required")
	projectErrorVirtualCurrencyPriceCurrencyIsNotSupport         = newBillingServerErrorMsg("pr000010", `project virtual currency price currency is not support`)
	projectErrorVirtualCurrencyLimitsIncorrect                   = newBillingServerErrorMsg("pr000011", `project virtual currency purchase limits is incorrect`)
	projectErrorShortDescriptionDefaultLangRequired              = newBillingServerErrorMsg("pr000012", "project short description in \""+DefaultLanguage+"\" locale is required")
	projectErrorFullDescriptionDefaultLangRequired               = newBillingServerErrorMsg("pr000013", "project full description in \""+DefaultLanguage+"\" locale is required")
	projectErrorVatPayerUnknown                                  = newBillingServerErrorMsg("pr000014", "project vat payer unknown")
	projectErrorRedirectModeSuccessfulUrlIsRequired              = newBillingServerErrorMsg("pr000015", "redirect url for user's redirect after payment successful ending for selected redirect mode is required")
	projectErrorRedirectModeFailUrlIsRequired                    = newBillingServerErrorMsg("pr000016", "redirect url for user's redirect after failed payment for selected redirect mode is required")
	projectErrorRedirectModeBothRedirectUrlsIsRequired           = newBillingServerErrorMsg("pr000017", "redirect urls for user's redirect after payment completed for selected redirect mode is required")
	projectErrorButtonCaptionAllowedOnlyForAfterRedirect         = newBillingServerErrorMsg("pr000018", "caption for redirect button can't be set with non zero delay for auto redirect")
	projectErrorRedirectModeIsRequired                           = newBillingServerErrorMsg("pr000019", "redirect mode must be selected")
	projectErrorRedirectUsageIsRequired                          = newBillingServerErrorMsg("pr000020", "type of redirect usage must be selected")
	projectErrorRedirectSettingsIsRequired                       = newBillingServerErrorMsg("pr000021", "redirect settings is required")
)

func (s *Service) ChangeProject(
	ctx context.Context,
	req *billingpb.Project,
	rsp *billingpb.ChangeProjectResponse,
) error {
	var project *billingpb.Project
	var err error

	var merchant = &billingpb.Merchant{}
	if merchant, err = s.merchantRepository.GetById(ctx, req.MerchantId); err != nil {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = merchantErrorNotFound

		return nil
	}

	if req.Id != "" {
		oid, _ := primitive.ObjectIDFromHex(req.Id)
		merchantOid, _ := primitive.ObjectIDFromHex(req.MerchantId)
		filter := bson.M{"_id": oid, "merchant_id": merchantOid}
		project, err = s.getProjectBy(ctx, filter)

		if err != nil || project.MerchantId != req.MerchantId {
			rsp.Status = billingpb.ResponseStatusNotFound
			rsp.Message = projectErrorNotFound

			return nil
		}
	}

	if _, ok := req.Name[DefaultLanguage]; !ok {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = projectErrorNameDefaultLangRequired

		return nil
	}

	if req.CallbackCurrency != "" {
		if !helper.Contains(s.supportedCurrencies, req.CallbackCurrency) {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorCallbackCurrencyIncorrect

			return nil
		}
	}

	if req.LimitsCurrency != "" {
		if !helper.Contains(s.supportedCurrencies, req.LimitsCurrency) {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorLimitCurrencyIncorrect

			return nil
		}
	}

	if len(req.Currencies) > 0 {
		for _, v := range req.Currencies {
			if !helper.Contains(s.supportedCurrencies, v.Currency) {
				rsp.Status = billingpb.ResponseStatusBadData
				rsp.Message = projectErrorCurrencyIsNotSupport
				rsp.Message.Details = v.Currency

				return nil
			}
		}
	}

	if req.VirtualCurrency != nil {
		payoutCurrency := merchant.GetPayoutCurrency()
		err = s.validateProjectVirtualCurrency(req.VirtualCurrency, payoutCurrency)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = err.(*billingpb.ResponseErrorMessage)

			return nil
		}

		if req.VirtualCurrency.SellCountType == "" {
			req.VirtualCurrency.SellCountType = pkg.ProjectSellCountTypeFractional
		}
	}

	if len(req.ShortDescription) > 0 {
		if _, ok := req.ShortDescription[DefaultLanguage]; !ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorShortDescriptionDefaultLangRequired

			return nil
		}
	}

	if len(req.FullDescription) > 0 {
		if _, ok := req.FullDescription[DefaultLanguage]; !ok {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorFullDescriptionDefaultLangRequired

			return nil
		}
	}

	if (req.MinPaymentAmount > 0 || req.MaxPaymentAmount > 0) && req.LimitsCurrency == "" {
		rsp.Status = billingpb.ResponseStatusBadData
		rsp.Message = projectErrorLimitCurrencyRequired

		return nil
	}

	if merchant.DontChargeVat == true {
		req.VatPayer = billingpb.VatPayerNobody
	} else {
		if req.VatPayer != billingpb.VatPayerBuyer && req.VatPayer != billingpb.VatPayerSeller {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorVatPayerUnknown

			return nil
		}
	}

	if project == nil {
		if req.RedirectSettings == nil {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = projectErrorRedirectSettingsIsRequired
			return nil
		}

		err = s.validateRedirectSettings(req)

		if err != nil {
			rsp.Status = billingpb.ResponseStatusBadData
			rsp.Message = err.(*billingpb.ResponseErrorMessage)
			return nil
		}

		project, err = s.createProject(ctx, req)
	} else {
		if req.RedirectSettings != nil {
			err = s.validateRedirectSettings(req)

			if err != nil {
				rsp.Status = billingpb.ResponseStatusBadData
				rsp.Message = err.(*billingpb.ResponseErrorMessage)
				return nil
			}
		}

		err = s.updateProject(ctx, req, project)
	}

	if err != nil {
		rsp.Status = billingpb.ResponseStatusSystemError
		zap.S().Errorw("create or update project error", "err", err, "req", req)
		rsp.Message = projectErrorUnknown

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = project

	return nil
}

func (s *Service) GetProject(
	ctx context.Context,
	req *billingpb.GetProjectRequest,
	rsp *billingpb.ChangeProjectResponse,
) error {
	oid, _ := primitive.ObjectIDFromHex(req.ProjectId)
	query := bson.M{"_id": oid}

	if req.MerchantId != "" {
		query["merchant_id"], _ = primitive.ObjectIDFromHex(req.MerchantId)
	}

	project, err := s.getProjectBy(ctx, query)

	if err != nil || project.MerchantId != req.MerchantId {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = projectErrorNotFound

		return nil
	}

	project.ProductsCount = s.getProductsCountByProject(ctx, project.Id)

	rsp.Status = billingpb.ResponseStatusOk
	rsp.Item = project

	return nil
}

func (s *Service) ListProjects(
	ctx context.Context,
	req *billingpb.ListProjectsRequest,
	rsp *billingpb.ListProjectsResponse,
) error {
	var projects []*billingpb.Project
	query := make(bson.M)

	if req.MerchantId != "" {
		query["merchant_id"], _ = primitive.ObjectIDFromHex(req.MerchantId)
	}

	if req.QuickSearch != "" {
		query["$or"] = []bson.M{
			{"name": bson.M{"$elemMatch": bson.M{"value": primitive.Regex{Pattern: req.QuickSearch, Options: "i"}}}},
			{"id_string": primitive.Regex{Pattern: req.QuickSearch, Options: "i"}},
		}
	}

	if len(req.Statuses) > 0 {
		query["status"] = bson.M{"$in": req.Statuses}
	}

	count, err := s.db.Collection(collectionProject).CountDocuments(ctx, query)

	if err != nil {
		zap.S().Errorf("Query to count projects failed", "err", err.Error(), "query", query)
		return projectErrorUnknown
	}

	afQuery := []bson.M{
		{"$match": query},
		{
			"$lookup": bson.M{
				"from":         collectionProduct,
				"localField":   "_id",
				"foreignField": "project_id",
				"as":           "products",
			},
		},
		{
			"$project": bson.M{
				"_id":                         "$_id",
				"merchant_id":                 "$merchant_id",
				"name":                        "$name",
				"callback_protocol":           "$callback_protocol",
				"callback_currency":           "$callback_currency",
				"create_order_allowed_urls":   "$create_order_allowed_urls",
				"allow_dynamic_notify_urls":   "$allow_dynamic_notify_urls",
				"allow_dynamic_redirect_urls": "$allow_dynamic_redirect_urls",
				"limits_currency":             "$limits_currency",
				"min_payment_amount":          "$min_payment_amount",
				"max_payment_amount":          "$max_payment_amount",
				"notify_emails":               "$notify_emails",
				"is_products_checkout":        "$is_products_checkout",
				"secret_key":                  "$secret_key",
				"signature_required":          "$signature_required",
				"send_notify_email":           "$send_notify_email",
				"url_check_account":           "$url_check_account",
				"url_process_payment":         "$url_process_payment",
				"url_redirect_fail":           "$url_redirect_fail",
				"url_redirect_success":        "$url_redirect_success",
				"status":                      "$status",
				"created_at":                  "$created_at",
				"updated_at":                  "$updated_at",
				"products_count":              bson.M{"$size": "$products"},
				"cover":                       "$cover",
				"currencies":                  "$currencies",
				"short_description":           "$short_description",
				"full_description":            "$full_description",
				"localizations":               "$localizations",
				"virtual_currency":            "$virtual_currency",
				"vat_payer":                   "$vat_payer",
				"redirect_settings":           "$redirect_settings",
			},
		},
		{"$skip": req.Offset},
		{"$limit": req.Limit},
	}

	if len(req.Sort) > 0 {
		afQuery = s.mgoPipeSort(afQuery, req.Sort)
	}

	cursor, err := s.db.Collection(collectionProject).Aggregate(ctx, afQuery)

	if err != nil {
		zap.S().Errorf("Query to find projects failed", "err", err.Error(), "query", afQuery)
		return projectErrorUnknown
	}

	err = cursor.All(ctx, &projects)

	if err != nil {
		zap.L().Error(
			pkg.ErrorQueryCursorExecutionFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return projectErrorUnknown
	}

	rsp.Count = count
	rsp.Items = []*billingpb.Project{}

	if count > 0 {
		rsp.Items = projects
	}

	return nil
}

func (s *Service) DeleteProject(
	ctx context.Context,
	req *billingpb.GetProjectRequest,
	rsp *billingpb.ChangeProjectResponse,
) error {
	oid, _ := primitive.ObjectIDFromHex(req.ProjectId)
	query := bson.M{"_id": oid}

	project, err := s.getProjectBy(ctx, query)

	if err != nil || req.MerchantId != project.MerchantId {
		rsp.Status = billingpb.ResponseStatusNotFound
		rsp.Message = projectErrorNotFound

		return nil
	}

	rsp.Status = billingpb.ResponseStatusOk

	if project.IsDeleted() == true {
		return nil
	}

	project.Status = billingpb.ProjectStatusDeleted

	if err := s.project.Update(ctx, project); err != nil {
		zap.S().Errorf("Query to delete project failed", "err", err.Error(), "data", project)

		rsp.Status = billingpb.ResponseStatusSystemError
		rsp.Message = projectErrorUnknown

		return nil
	}

	return nil
}

func (s *Service) getProjectBy(ctx context.Context, query bson.M) (project *billingpb.Project, err error) {
	err = s.db.Collection(collectionProject).FindOne(ctx, query).Decode(&project)

	if err != nil {
		if err != mongo.ErrNoDocuments {
			zap.S().Errorf("Query to find project failed", "err", err.Error(), "query", query)
		}

		return project, projectErrorNotFound
	}

	return
}

func (s *Service) createProject(ctx context.Context, req *billingpb.Project) (*billingpb.Project, error) {
	project := &billingpb.Project{
		Id:                       primitive.NewObjectID().Hex(),
		MerchantId:               req.MerchantId,
		Cover:                    req.Cover,
		Name:                     req.Name,
		CallbackCurrency:         req.CallbackCurrency,
		CallbackProtocol:         req.CallbackProtocol,
		CreateOrderAllowedUrls:   req.CreateOrderAllowedUrls,
		AllowDynamicNotifyUrls:   req.AllowDynamicNotifyUrls,
		AllowDynamicRedirectUrls: req.AllowDynamicRedirectUrls,
		LimitsCurrency:           req.LimitsCurrency,
		MinPaymentAmount:         req.MinPaymentAmount,
		MaxPaymentAmount:         req.MaxPaymentAmount,
		NotifyEmails:             req.NotifyEmails,
		IsProductsCheckout:       req.IsProductsCheckout,
		SecretKey:                req.SecretKey,
		SignatureRequired:        req.SignatureRequired,
		SendNotifyEmail:          req.SendNotifyEmail,
		UrlCheckAccount:          req.UrlCheckAccount,
		UrlProcessPayment:        req.UrlProcessPayment,
		UrlRedirectFail:          req.UrlRedirectFail,
		UrlRedirectSuccess:       req.UrlRedirectSuccess,
		UrlChargebackPayment:     req.UrlChargebackPayment,
		UrlCancelPayment:         req.UrlCancelPayment,
		UrlFraudPayment:          req.UrlFraudPayment,
		UrlRefundPayment:         req.UrlRefundPayment,
		Status:                   billingpb.ProjectStatusDraft,
		Localizations:            req.Localizations,
		FullDescription:          req.FullDescription,
		ShortDescription:         req.ShortDescription,
		Currencies:               req.Currencies,
		VirtualCurrency:          req.VirtualCurrency,
		VatPayer:                 req.VatPayer,
		RedirectSettings:         req.RedirectSettings,
		CreatedAt:                ptypes.TimestampNow(),
		UpdatedAt:                ptypes.TimestampNow(),
	}

	if err := s.project.Insert(ctx, project); err != nil {
		zap.S().Errorf("Query to create project failed", "err", err.Error(), "data", project)
		return nil, projectErrorUnknown
	}

	return project, nil
}

func (s *Service) updateProject(ctx context.Context, req *billingpb.Project, project *billingpb.Project) error {
	project.Name = req.Name
	project.CallbackCurrency = req.CallbackCurrency
	project.CreateOrderAllowedUrls = req.CreateOrderAllowedUrls
	project.AllowDynamicNotifyUrls = req.AllowDynamicNotifyUrls
	project.AllowDynamicRedirectUrls = req.AllowDynamicRedirectUrls
	project.LimitsCurrency = req.LimitsCurrency
	project.MinPaymentAmount = req.MinPaymentAmount
	project.MaxPaymentAmount = req.MaxPaymentAmount
	project.NotifyEmails = req.NotifyEmails
	project.IsProductsCheckout = req.IsProductsCheckout
	project.SecretKey = req.SecretKey
	project.SignatureRequired = req.SignatureRequired
	project.SendNotifyEmail = req.SendNotifyEmail
	project.UrlRedirectFail = req.UrlRedirectFail
	project.UrlRedirectSuccess = req.UrlRedirectSuccess
	project.Status = req.Status
	project.UpdatedAt = ptypes.TimestampNow()

	if project.NeedChangeStatusToDraft(req) == true {
		project.Status = billingpb.ProjectStatusDraft
	}

	project.CallbackProtocol = req.CallbackProtocol
	project.UrlCheckAccount = req.UrlCheckAccount
	project.UrlProcessPayment = req.UrlProcessPayment
	project.UrlChargebackPayment = req.UrlChargebackPayment
	project.UrlCancelPayment = req.UrlCancelPayment
	project.UrlFraudPayment = req.UrlFraudPayment
	project.UrlRefundPayment = req.UrlRefundPayment
	project.Localizations = req.Localizations
	project.FullDescription = req.FullDescription
	project.ShortDescription = req.ShortDescription
	project.Currencies = req.Currencies
	project.VirtualCurrency = req.VirtualCurrency
	project.VatPayer = req.VatPayer
	project.Cover = req.Cover

	if req.RedirectSettings != nil {
		project.RedirectSettings = req.RedirectSettings
	}

	if err := s.project.Update(ctx, project); err != nil {
		return projectErrorUnknown
	}

	project.ProductsCount = s.getProductsCountByProject(ctx, project.Id)

	return nil
}

func (s *Service) getProjectsCountByMerchant(ctx context.Context, merchantId string) int32 {
	oid, _ := primitive.ObjectIDFromHex(merchantId)
	query := bson.M{"merchant_id": oid}
	count, err := s.db.Collection(collectionProject).CountDocuments(ctx, query)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)

		return 0
	}

	return int32(count)
}

func (s *Service) validateProjectVirtualCurrency(virtualCurrency *billingpb.ProjectVirtualCurrency, payoutCurrency string) error {
	if _, ok := virtualCurrency.Name[DefaultLanguage]; !ok {
		return projectErrorVirtualCurrencyNameDefaultLangRequired
	}

	if _, ok := virtualCurrency.SuccessMessage[DefaultLanguage]; !ok {
		return projectErrorVirtualCurrencySuccessMessageDefaultLangRequired
	}

	if len(virtualCurrency.Prices) > 0 {
		currencies := make([]string, len(virtualCurrency.Prices))

		for _, v := range virtualCurrency.Prices {
			if !helper.Contains(s.supportedCurrencies, v.Currency) {
				err := projectErrorVirtualCurrencyPriceCurrencyIsNotSupport
				err.Details = v.Currency

				return err
			}
			currencies = append(currencies, v.Currency)
		}

		if !helper.Contains(currencies, payoutCurrency) {
			err := projectErrorVirtualCurrencyPriceCurrencyIsNotSupport
			err.Details = payoutCurrency

			return err
		}
	}

	if virtualCurrency.MinPurchaseValue > 0 && virtualCurrency.MaxPurchaseValue > 0 &&
		virtualCurrency.MinPurchaseValue > virtualCurrency.MaxPurchaseValue {
		return projectErrorVirtualCurrencyLimitsIncorrect
	}

	return nil
}

// Validate redirect settings for project in request to project create or update
func (s *Service) validateRedirectSettings(req *billingpb.Project) error {
	settings := req.RedirectSettings

	if settings.Mode == "" {
		return projectErrorRedirectModeIsRequired
	}

	if settings.Usage == "" {
		return projectErrorRedirectUsageIsRequired
	}

	if settings.Mode == pkg.ProjectRedirectModeSuccessful && req.UrlRedirectSuccess == "" {
		return projectErrorRedirectModeSuccessfulUrlIsRequired
	}

	if settings.Mode == pkg.ProjectRedirectModeFail && req.UrlRedirectFail == "" {
		return projectErrorRedirectModeFailUrlIsRequired
	}

	if settings.Mode == pkg.ProjectRedirectModeAny && (req.UrlRedirectFail == "" || req.UrlRedirectSuccess == "") {
		return projectErrorRedirectModeBothRedirectUrlsIsRequired
	}

	if settings.Delay > 0 && settings.ButtonCaption != "" {
		return projectErrorButtonCaptionAllowedOnlyForAfterRedirect
	}

	return nil
}

func newProjectService(svc *Service) *Project {
	s := &Project{svc: svc}
	return s
}

func (h *Project) Insert(ctx context.Context, project *billingpb.Project) error {
	_, err := h.svc.db.Collection(collectionProject).InsertOne(ctx, project)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	key := fmt.Sprintf(cacheProjectId, project.Id)
	err = h.svc.cacher.Set(key, project, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	return nil
}

func (h *Project) MultipleInsert(ctx context.Context, projects []*billingpb.Project) error {
	p := make([]interface{}, len(projects))
	for i, v := range projects {
		p[i] = v
	}

	_, err := h.svc.db.Collection(collectionProject).InsertMany(ctx, p)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationInsert),
			zap.Any(pkg.ErrorDatabaseFieldQuery, p),
		)
		return err
	}

	return nil
}

func (h *Project) Update(ctx context.Context, project *billingpb.Project) error {
	oid, _ := primitive.ObjectIDFromHex(project.Id)
	filter := bson.M{"_id": oid}
	_, err := h.svc.db.Collection(collectionProject).ReplaceOne(ctx, filter, project)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.String(pkg.ErrorDatabaseFieldOperation, pkg.ErrorDatabaseFieldOperationUpdate),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	key := fmt.Sprintf(cacheProjectId, project.Id)
	err = h.svc.cacher.Set(key, project, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, project),
		)
		return err
	}

	return nil
}

func (h Project) GetById(ctx context.Context, id string) (*billingpb.Project, error) {
	var c billingpb.Project
	key := fmt.Sprintf(cacheProjectId, id)
	err := h.svc.cacher.Get(key, c)

	if err == nil {
		return &c, nil
	}

	oid, _ := primitive.ObjectIDFromHex(id)
	query := bson.M{"_id": oid}
	err = h.svc.db.Collection(collectionProject).FindOne(ctx, query).Decode(&c)

	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorDatabaseFieldCollection, collectionProject),
			zap.Any(pkg.ErrorDatabaseFieldQuery, query),
		)
		return nil, fmt.Errorf(errorNotFound, collectionProject)
	}

	err = h.svc.cacher.Set(key, c, 0)

	if err != nil {
		zap.L().Error(
			pkg.ErrorCacheQueryFailed,
			zap.Error(err),
			zap.String(pkg.ErrorCacheFieldCmd, "SET"),
			zap.String(pkg.ErrorCacheFieldKey, key),
			zap.Any(pkg.ErrorDatabaseFieldQuery, c),
		)
	}

	return &c, nil
}

func (s *Service) CheckSkuAndKeyProject(ctx context.Context, req *billingpb.CheckSkuAndKeyProjectRequest, rsp *billingpb.EmptyResponseWithStatus) error {
	rsp.Status = billingpb.ResponseStatusOk

	oid, _ := primitive.ObjectIDFromHex(req.ProjectId)
	dupQuery := bson.M{"project_id": oid, "sku": req.Sku, "deleted": false}
	found, err := s.db.Collection(collectionKeyProduct).CountDocuments(ctx, dupQuery)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, dupQuery),
		)
		rsp.Status = http.StatusBadRequest
		rsp.Message = keyProductRetrieveError
		return nil
	}

	if found > 0 {
		rsp.Status = http.StatusBadRequest
		rsp.Message = keyProductDuplicate
		return nil
	}

	oid, _ = primitive.ObjectIDFromHex(req.ProjectId)
	dupQuery = bson.M{"project_id": oid, "sku": req.Sku, "deleted": false}
	found, err = s.db.Collection(collectionProduct).CountDocuments(ctx, dupQuery)
	if err != nil {
		zap.L().Error(
			pkg.ErrorDatabaseQueryFailed,
			zap.Error(err),
			zap.Any(pkg.ErrorDatabaseFieldQuery, dupQuery),
		)
		rsp.Status = http.StatusBadRequest
		rsp.Message = keyProductRetrieveError
		return nil
	}

	if found > 0 {
		rsp.Status = http.StatusBadRequest
		rsp.Message = keyProductDuplicate
		return nil
	}

	return nil
}
