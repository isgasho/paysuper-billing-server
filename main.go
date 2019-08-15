package main

import (
	_ "github.com/micro/go-plugins/registry/kubernetes"
	"github.com/paysuper/paysuper-billing-server/internal"
	"go.uber.org/zap"
)

func main() {
	app := internal.NewApplication()
	app.Init()

	task := app.CliArgs.Get("task").String("")
	date := app.CliArgs.Get("date").String("")
	days := int32(app.CliArgs.Get("days").Int(7))

	if task != "" {

		defer app.Stop()

		var err error

		switch task {
		case "vat_reports":
			err = app.TaskProcessVatReports(date)

		case "royalty_reports":
			err = app.TaskCreateRoyaltyReport()

		case "royalty_reports_accept":
			err = app.TaskAutoAcceptRoyaltyReports()

		case "report_file_remover":
			err = app.TaskReportFileRemove(days)
		}

		if err != nil {
			zap.S().Fatalf("task error", "err", err, "task", task, "date", date)
		}

		return
	}

	app.Run()
}
