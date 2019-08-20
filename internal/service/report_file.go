package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/golang/protobuf/ptypes"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/billing"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	mbPkg "github.com/paysuper/paysuper-reporter/pkg"
	"go.uber.org/zap"
	"sort"
	"time"
)

const (
	collectionReportFiles = "report_files"
)

var (
	errorReportFileTemplateNotFound             = newBillingServerErrorMsg("rf000001", "could not find a template for the report.")
	errorReportFileType                         = newBillingServerErrorMsg("rf000002", "invalid file type.")
	errorReportFileUnableToCreate               = newBillingServerErrorMsg("rf000003", "unable to create report file.")
	errorReportFileUnableToUpdate               = newBillingServerErrorMsg("rf000004", "unable to update report file.")
	errorReportFileNotFound                     = newBillingServerErrorMsg("rf000005", "report file not found.")
	errorReportFileCentrifugoNotificationFailed = newBillingServerErrorMsg("rf000006", "unable to send report file to centrifugo.")
	errorReportFileDeleteOldest                 = newBillingServerErrorMsg("rf000007", "unable to delete oldest reports.")
	errorReportFileMessageBrokerFailed          = newBillingServerErrorMsg("rf000008", "unable to publish report file message to the message broker.")
	errorReportFileMarshalMatch                 = newBillingServerErrorMsg("rf000009", "unable to marshal match data.")
	errorReportFileMarshalGroup                 = newBillingServerErrorMsg("rf000010", "unable to marshal group data.")

	reportTemplates = map[string]*ReportFileTemplate{
		pkg.ReportTypeVat: {
			TemplateId: pkg.ReportTypeVatTemplate,
			Table:      collectionPaymentMethod,
			Fields:     []string{"payment_system_id", "name", "min_payment_amount", "max_payment_amount"},
			Match: `{
				"is_active": true,
			}`,
			Group: "",
		},
		pkg.ReportTypeTax: {
			TemplateId: pkg.ReportTypeTaxTemplate,
			Table:      "",
			Fields:     []string{},
			Match:      "",
			Group:      "",
		},
		pkg.ReportTypeRoyalty: {
			TemplateId: pkg.ReportTypeRoyaltyTemplate,
			Table:      "",
			Fields:     []string{},
			Match:      "",
			Group:      "",
		},
	}

	reportFileTypes = []string{
		mbPkg.OutputXslx,
		mbPkg.OutputCsv,
		mbPkg.OutputPdf,
	}
)

type ReportFileTemplate struct {
	TemplateId string
	Table      string
	Fields     []string
	Match      string
	Group      string
}

func (s *Service) CreateReportFile(
	ctx context.Context,
	req *grpc.CreateReportFileRequest,
	res *grpc.CreateReportFileResponse,
) error {
	template, ok := reportTemplates[req.ReportType]
	if !ok {
		zap.S().Errorf(errorReportFileTemplateNotFound.Message, "data", req)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorReportFileTemplateNotFound

		return nil
	}

	i := sort.SearchStrings(reportFileTypes, req.FileType)
	if i == len(reportFileTypes) {
		zap.S().Errorf(errorReportFileType.Message, "data", req)
		res.Status = pkg.ResponseStatusBadData
		res.Message = errorReportFileType

		return nil
	}

	file := &billing.ReportFile{
		Id:         bson.NewObjectId().Hex(),
		MerchantId: req.MerchantId,
		Type:       req.ReportType,
	}
	file.DateFrom, _ = ptypes.TimestampProto(time.Unix(req.PeriodFrom, 0))
	file.DateTo, _ = ptypes.TimestampProto(time.Unix(req.PeriodTo, 0))

	if err := s.reportFileRepository.Insert(file); err != nil {
		zap.S().Errorf(errorReportFileUnableToCreate.Message, "data", req)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileUnableToCreate
		return nil
	}

	match, err := json.Marshal(template.Match)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileMarshalMatch
		return err
	}

	group, err := json.Marshal(template.Group)
	if err != nil {
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileMarshalGroup
		return err
	}

	msg := &mbPkg.ReportRequest{
		FileId:       file.Id,
		TemplateId:   template.TemplateId,
		OutputFormat: req.FileType,
		TableName:    template.Table,
		Fields:       template.Fields,
		Match:        match,
		Group:        group,
	}
	if err := s.messageBroker.Publish(mbPkg.SubjectRequestReportFileCreate, msg); err != nil {
		zap.S().Errorf(errorReportFileMessageBrokerFailed.Message, "data", req)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileMessageBrokerFailed
		return nil
	}

	res.Status = pkg.StatusOK
	res.FileId = file.Id

	return nil
}

func (s *Service) UpdateReportFile(
	ctx context.Context,
	req *grpc.UpdateReportFileRequest,
	res *grpc.ResponseError,
) error {
	file, err := s.reportFileRepository.GetById(req.Id)

	if err != nil {
		zap.S().Errorf(errorReportFileNotFound.Message, "data", req)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorReportFileNotFound
		return nil
	}

	file.FilePath = req.FilePath
	if err = s.reportFileRepository.Update(file); err != nil {
		zap.S().Errorf(errorReportFileUnableToUpdate.Message, "data", req)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileUnableToUpdate
		return nil
	}

	err = s.centrifugo.Publish(fmt.Sprintf(s.cfg.CentrifugoMerchantChannel, file.MerchantId), file)

	if err != nil {
		zap.S().Error(errorReportFileCentrifugoNotificationFailed, zap.Error(err), zap.Any("report_file", file))
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileCentrifugoNotificationFailed

		return nil
	}

	res.Status = pkg.StatusOK

	return nil
}

func (s *Service) DeleteOldestReportFiles(
	ctx context.Context,
	req *grpc.DeleteOldestReportFilesRequest,
	res *grpc.ResponseError,
) error {
	_, err := s.reportFileRepository.DeleteOldestByDays(req.Days)

	if err != nil {
		zap.S().Errorf(errorReportFileDeleteOldest.Message, "data", req)
		res.Status = pkg.ResponseStatusSystemError
		res.Message = errorReportFileDeleteOldest
		return nil
	}

	res.Status = pkg.StatusOK

	return nil
}

func (s *Service) GetReportFile(
	ctx context.Context,
	req *grpc.GetReportFileRequest,
	res *grpc.GetReportFileResponse,
) error {
	file, err := s.reportFileRepository.GetById(req.Id)

	if err != nil {
		zap.S().Errorf(errorReportFileNotFound.Message, "data", req)
		res.Status = pkg.ResponseStatusNotFound
		res.Message = errorReportFileNotFound
		return nil
	}

	res.Status = pkg.StatusOK
	res.File = file

	return nil
}

type ReportFileRepositoryInterface interface {
	Insert(*billing.ReportFile) error
	Update(*billing.ReportFile) error
	GetById(string) (*billing.ReportFile, error)
	Delete(*billing.ReportFile) error
	DeleteOldestByDays(int32) (int, error)
}

func newReportFileRepository(svc *Service) ReportFileRepositoryInterface {
	s := &ReportFileRepository{svc: svc}
	return s
}

func (h *ReportFileRepository) Insert(rf *billing.ReportFile) error {
	if err := h.svc.db.Collection(collectionReportFiles).Insert(rf); err != nil {
		return err
	}

	return nil
}

func (h *ReportFileRepository) Update(rf *billing.ReportFile) error {
	if err := h.svc.db.Collection(collectionReportFiles).UpdateId(bson.ObjectIdHex(rf.Id), rf); err != nil {
		return err
	}

	return nil
}

func (h *ReportFileRepository) Delete(rf *billing.ReportFile) error {
	if err := h.svc.db.Collection(collectionReportFiles).RemoveId(bson.ObjectIdHex(rf.Id)); err != nil {
		return err
	}

	return nil
}

func (h *ReportFileRepository) DeleteOldestByDays(days int32) (int, error) {
	t := time.Now().AddDate(0, 0, int(days)*-1)
	change, err := h.svc.db.Collection(collectionReportFiles).RemoveAll(bson.M{"created_at": bson.M{"$lte": t}})

	if err != nil {
		return 0, err
	}

	return change.Removed, nil
}

func (h *ReportFileRepository) GetById(id string) (*billing.ReportFile, error) {
	var file *billing.ReportFile

	if err := h.svc.db.Collection(collectionReportFiles).Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&file); err != nil {
		return nil, err
	}

	return file, nil
}
