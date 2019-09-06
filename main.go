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
		}

		if err != nil {
			zap.S().Fatalf("task error", "err", err, "task", task, "date", date)
		}

		return
	}

	app.KeyDaemonStart()

	app.Run()
}
