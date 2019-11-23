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

		case "create_payouts":
			err = app.TaskAutoCreatePayouts()

		case "rebuild_order_view":
			err = app.TaskRebuildOrderView()

		case "merchants_migrate":
			err = app.TaskMerchantsMigrate()
		}

		if err != nil {
			zap.L().Fatal("task error",
				zap.Error(err),
				zap.String("task", task),
				zap.String("date", date),
			)
		}

		return
	}

	app.KeyDaemonStart()

	app.Run()
}
