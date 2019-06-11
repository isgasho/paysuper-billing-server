package main

import (
	_ "github.com/micro/go-plugins/registry/kubernetes"
	"github.com/paysuper/paysuper-billing-server/internal"
)

func main() {
	app := internal.NewApplication()
	app.Init()
	app.Run()
}
