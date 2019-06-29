package internal

import (
	"context"
	"crypto/tls"
	"github.com/InVisionApp/go-health"
	"github.com/InVisionApp/go-health/handlers"
	"github.com/ProtocolONE/geoip-service/pkg"
	"github.com/ProtocolONE/geoip-service/pkg/proto"
	metrics "github.com/ProtocolONE/go-micro-plugins/wrapper/monitoring/prometheus"
	"github.com/ProtocolONE/rabbitmq/pkg"
	"github.com/go-redis/redis"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/micro/go-micro"
	"github.com/paysuper/paysuper-billing-server/internal/config"
	"github.com/paysuper/paysuper-billing-server/internal/database"
	"github.com/paysuper/paysuper-billing-server/internal/service"
	"github.com/paysuper/paysuper-billing-server/pkg"
	"github.com/paysuper/paysuper-billing-server/pkg/proto/grpc"
	curPkg "github.com/paysuper/paysuper-currencies/pkg"
	"github.com/paysuper/paysuper-currencies/pkg/proto/currencies"
	mongodb "github.com/paysuper/paysuper-database-mongo"
	"github.com/paysuper/paysuper-recurring-repository/pkg/constant"
	"github.com/paysuper/paysuper-recurring-repository/pkg/proto/repository"
	taxPkg "github.com/paysuper/paysuper-tax-service/pkg"
	"github.com/paysuper/paysuper-tax-service/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
	"log"
	"net/http"
	"time"
)

type Application struct {
	cfg        *config.Config
	database   *mongodb.Source
	redis      *redis.Client
	service    micro.Service
	httpServer *http.Server
	router     *http.ServeMux
	logger     *zap.Logger
	svc        *service.Service
}

type appHealthCheck struct{}

func NewApplication() *Application {
	return &Application{}
}

func (app *Application) Init() {
	app.initLogger()

	cfg, err := config.NewConfig()

	if err != nil {
		app.logger.Fatal("Config load failed", zap.Error(err))
	}

	app.cfg = cfg

	app.logger.Info("db migrations started")

	migrations, err := migrate.New(pkg.MigrationSource, app.cfg.MongoDsn)

	if err != nil {
		app.logger.Fatal("Migrations initialization failed", zap.Error(err))
	}

	err = migrations.Up()

	if err != nil && err != migrate.ErrNoChange && err != migrate.ErrNilVersion {
		app.logger.Fatal("Migrations processing failed", zap.Error(err))
	}

	app.logger.Info("db migrations applied")

	db, err := mongodb.NewDatabase()
	if err != nil {
		app.logger.Fatal("Database connection failed", zap.Error(err))
	}

	app.database = db

	app.redis = database.NewRedis(
		&redis.Options{
			Addr:     cfg.RedisHost,
			Password: cfg.RedisPassword,
		},
	)

	if err != nil {
		app.logger.Fatal("Connection to Redis failed", zap.Error(err), zap.String("broker_address", app.cfg.BrokerAddress))
	}

	broker, err := rabbitmq.NewBroker(app.cfg.BrokerAddress)

	if err != nil {
		app.logger.Fatal("Creating RabbitMQ publisher failed", zap.Error(err), zap.String("broker_address", app.cfg.BrokerAddress))
	}

	options := []micro.Option{
		micro.Name(pkg.ServiceName),
		micro.Version(pkg.ServiceVersion),
		micro.WrapHandler(metrics.NewHandlerWrapper()),
		micro.AfterStop(func() error {
			app.logger.Info("Micro service stopped")
			app.Stop()
			return nil
		}),
	}

	app.logger.Info("Initialize micro service")

	app.service = micro.NewService(options...)
	app.service.Init()

	geoService := proto.NewGeoIpService(geoip.ServiceName, app.service.Client())
	repService := repository.NewRepositoryService(constant.PayOneRepositoryServiceName, app.service.Client())
	taxService := tax_service.NewTaxService(taxPkg.ServiceName, app.service.Client())
	curService := currencies.NewCurrencyratesService(curPkg.ServiceName, app.service.Client())

	redisdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        cfg.CacheRedis.Address,
		Password:     cfg.CacheRedis.Password,
		MaxRetries:   cfg.CacheRedis.MaxRetries,
		MaxRedirects: cfg.CacheRedis.MaxRedirects,
		PoolSize:     cfg.CacheRedis.PoolSize,
	})

	d := gomail.NewDialer(app.cfg.SmtpHost, app.cfg.SmtpPort, app.cfg.SmtpUser, app.cfg.SmtpPassword)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	smtpCl, err := d.Dial()

	if err != nil {
		zap.L().Fatal(
			"Connection to SMTP server failed",
			zap.Error(err),
			zap.Int("port", app.cfg.SmtpPort),
			zap.String("user", app.cfg.SmtpUser),
		)
	}

	zap.L().Info(
		"SMTP server connection started",
		zap.String("host", app.cfg.SmtpHost),
		zap.Int("port", app.cfg.SmtpPort),
	)

	app.svc = service.NewBillingService(
		app.database,
		app.cfg,
		geoService,
		repService,
		taxService,
		broker,
		app.redis,
		service.NewCacheRedis(redisdb),
		curService,
		smtpCl,
	)

	if err := app.svc.Init(); err != nil {
		app.logger.Fatal("Create service instance failed", zap.Error(err))
	}

	err = grpc.RegisterBillingServiceHandler(app.service.Server(), app.svc)

	if err != nil {
		app.logger.Fatal("Service init failed", zap.Error(err))
	}

	app.router = http.NewServeMux()
	app.initHealth()
	app.initMetrics()
}

func (app *Application) initLogger() {
	var err error

	logger, err := zap.NewProduction()

	if err != nil {
		log.Fatalf("Application logger initialization failed with error: %s\n", err)
	}
	app.logger = logger.Named(pkg.LoggerName)
	zap.ReplaceGlobals(app.logger)
}

func (app *Application) initHealth() {
	h := health.New()
	err := h.AddChecks([]*health.Config{
		{
			Name:     "health-check",
			Checker:  &appHealthCheck{},
			Interval: time.Duration(1) * time.Second,
			Fatal:    true,
		},
	})

	if err != nil {
		app.logger.Fatal("Health check register failed", zap.Error(err))
	}

	if err = h.Start(); err != nil {
		app.logger.Fatal("Health check start failed", zap.Error(err))
	}

	app.logger.Info("Health check listener started", zap.String("port", app.cfg.MetricsPort))

	app.router.HandleFunc("/health", handlers.NewJSONHandlerFunc(h, nil))
}

func (app *Application) initMetrics() {
	app.router.Handle("/metrics", promhttp.Handler())
}

func (app *Application) Run() {
	app.httpServer = &http.Server{
		Addr:    ":" + app.cfg.MetricsPort,
		Handler: app.router,
	}

	go func() {
		if err := app.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			app.logger.Fatal("Http server starting failed", zap.Error(err))
		}
	}()

	if err := app.service.Run(); err != nil {
		app.logger.Fatal("Micro service starting failed", zap.Error(err))
	}
}

func (c *appHealthCheck) Status() (interface{}, error) {
	return "ok", nil
}

func (app *Application) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := app.httpServer.Shutdown(ctx); err != nil {
		app.logger.Error("Http server shutdown failed", zap.Error(err))
	}
	app.logger.Info("Http server stopped")

	app.database.Close()
	app.logger.Info("Database connection closed")

	if err := app.redis.Close(); err != nil {
		zap.L().Error("Redis connection close failed", zap.Error(err))
	} else {
		zap.L().Info("Redis connection closed")
	}

	if err := app.logger.Sync(); err != nil {
		app.logger.Error("Logger sync failed", zap.Error(err))
	} else {
		app.logger.Info("Logger synced")
	}
}
