package config

import (
	"crypto/rsa"
	"encoding/base64"
	"github.com/dgrijalva/jwt-go"
	"github.com/kelseyhightower/envconfig"
	"net/url"
	"time"
)

type PaymentSystemConfig struct {
	CardPayApiUrl string `envconfig:"CARD_PAY_API_URL" required:"true"`
}

type CustomerTokenConfig struct {
	Length   int   `envconfig:"CUSTOMER_TOKEN_LENGTH" default:"32"`
	LifeTime int64 `envconfig:"CUSTOMER_TOKEN_LIFETIME" default:"2592000"`

	CookiePublicKeyBase64  string `envconfig:"CUSTOMER_COOKIE_PUBLIC_KEY" required:"true"`
	CookiePrivateKeyBase64 string `envconfig:"CUSTOMER_COOKIE_PRIVATE_KEY" required:"true"`
	CookiePublicKey        *rsa.PublicKey
	CookiePrivateKey       *rsa.PrivateKey
}

// CacheRedis defines the parameters for connecting to the radish server for caching objects.
type CacheRedis struct {
	Address      []string `envconfig:"CACHE_REDIS_ADDRESS" required:"true"`
	Password     string   `envconfig:"CACHE_REDIS_PASSWORD" required:"false"`
	PoolSize     int      `envconfig:"CACHE_REDIS_POOL_SIZE" default:"1"`
	MaxRetries   int      `envconfig:"CACHE_REDIS_MAX_RETRIES" default:"10"`
	MaxRedirects int      `envconfig:"CACHE_REDIS_MAX_REDIRECTS" default:"8"`
}

type Config struct {
	MongoDsn           string `envconfig:"MONGO_DSN" required:"true"`
	MongoDialTimeout   string `envconfig:"MONGO_DIAL_TIMEOUT" required:"false" default:"10"`
	AccountingCurrency string `envconfig:"PSP_ACCOUNTING_CURRENCY" default:"EUR"`
	MetricsPort        string `envconfig:"METRICS_PORT" required:"false" default:"8086"`
	Environment        string `envconfig:"ENVIRONMENT" default:"dev"`
	RedisHost          string `envconfig:"REDIS_HOST" default:"127.0.0.1:6379"`
	RedisPassword      string `envconfig:"REDIS_PASSWORD" default:""`

	CentrifugoApiSecret string `envconfig:"CENTRIFUGO_API_SECRET" required:"true"`
	CentrifugoSecret    string `envconfig:"CENTRIFUGO_SECRET" required:"true"`
	CentrifugoURL       string `envconfig:"CENTRIFUGO_URL" required:"false" default:"http://127.0.0.1:8000"`
	BrokerAddress       string `envconfig:"BROKER_ADDRESS" default:"amqp://127.0.0.1:5672"`

	CentrifugoUserChannel         string `envconfig:"CENTRIFUGO_USER_CHANNEL" default:"paysuper:user#%s"`
	EmailConfirmTokenLifetime     int64  `envconfig:"EMAIL_CONFIRM_TOKEN_LIFETIME" default:"86400"`
	EmailConfirmUrl               string `envconfig:"EMAIL_CONFIRM_URL" default:"https://paysupermgmt.tst.protocol.one/confirm_email"`
	EmailConfirmTemplate          string `envconfig:"EMAIL_CONFIRM_TEMPLATE" default:"p1_verify_letter"`
	EmailNewRoyaltyReportTemplate string `envconfig:"EMAIL_NEW_ROYALTY_REPORT_TEMPLATE" default:"p1_new_royalty_report"`
	EmailVatReportTemplate        string `envconfig:"EMAIL_VAT_REPORT_TEMPLATE" default:"p1_vat_report"`
	EmailGameCodeTemplate         string `envconfig:"EMAIL_ACTIVATION_CODE_TEMPLATE" default:"p1_verify_letter-2"`

	MicroRegistry string `envconfig:"MICRO_REGISTRY" required:"false"`

	RoyaltyReportPeriod        int64  `envconfig:"ROYALTY_REPORT_PERIOD" default:"604800"`
	RoyaltyReportTimeZone      string `envconfig:"ROYALTY_REPORT_TIMEZONE" default:"Europe/Moscow"`
	RoyaltyReportAcceptTimeout int64  `envconfig:"ROYALTY_REPORT_TIMEZONE" default:"432000"`
	// moved to config for testing purposes, to prevent royalty reports tests crash on mondays before 18:00
	// must not be changed on normal app running, because it will broke roalty reports calculations
	RoyaltyReportPeriodEndHour int64 `default:"18"`

	CentrifugoMerchantChannel  string `envconfig:"CENTRIFUGO_MERCHANT_CHANNEL" default:"paysuper:merchant#%s"`
	CentrifugoFinancierChannel string `envconfig:"CENTRIFUGO_FINANCIER_CHANNEL" default:"paysuper:financier"`
	CentrifugoAdminChannel     string `envconfig:"CENTRIFUGO_ADMIN_CHANNEL" default:"paysuper:admin"`

	EmailNotificationFinancierRecipient string `envconfig:"EMAIL_NOTIFICATION_FINANCIER_RECIPIENT" required:"true"`

	OrderViewUpdateBatchSize int `envconfig:"ORDER_VIEW_UPDATE_BATCH_SIZE" default:"200"`

	HelloSignDefaultTemplate    string `envconfig:"HELLO_SIGN_DEFAULT_TEMPLATE" required:"true"`
	HelloSignPayoutTemplate     string `envconfig:"HELLO_SIGN_PAYOUT_TEMPLATE" required:"true"`
	HelloSignClientId           string `envconfig:"HELLO_SIGN_CLIENT_ID" required:"true"`
	PaysuperDocumentSignerEmail string `envconfig:"PAYSUPER_DOCUMENT_SIGNER_EMAIL" required:"true"`
	PaysuperDocumentSignerName  string `envconfig:"PAYSUPER_DOCUMENT_SIGNER_NAME" required:"true"`

	KeyDaemonRestartInterval int64 `envconfig:"KEY_DAEMON_RESTART_INTERVAL" default:"60"`

	*PaymentSystemConfig
	*CustomerTokenConfig
	*CacheRedis

	EmailConfirmUrlParsed *url.URL
}

func NewConfig() (*Config, error) {
	cfg := &Config{}
	err := envconfig.Process("", cfg)
	if err != nil {
		return nil, err
	}

	pem, err := base64.StdEncoding.DecodeString(cfg.CookiePublicKeyBase64)

	if err != nil {
		return nil, err
	}

	cfg.CookiePublicKey, err = jwt.ParseRSAPublicKeyFromPEM(pem)

	if err != nil {
		return nil, err
	}

	pem, err = base64.StdEncoding.DecodeString(cfg.CookiePrivateKeyBase64)

	if err != nil {
		return nil, err
	}

	cfg.CookiePrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM(pem)

	if err != nil {
		return nil, err
	}

	cfg.EmailConfirmUrlParsed, err = url.Parse(cfg.EmailConfirmUrl)

	if err != nil {
		return nil, err
	}

	return cfg, err
}

func (cfg *Config) GetCustomerTokenLength() int {
	return cfg.CustomerTokenConfig.Length
}

func (cfg *Config) GetCustomerTokenExpire() time.Duration {
	return time.Second * time.Duration(cfg.CustomerTokenConfig.LifeTime)
}

func (cfg *Config) GetEmailConfirmTokenLifetime() time.Duration {
	return time.Second * time.Duration(cfg.EmailConfirmTokenLifetime)
}

func (cfg *Config) GetUserConfirmEmailUrl(params map[string]string) string {
	query := cfg.EmailConfirmUrlParsed.Query()

	for k, v := range params {
		query.Set(k, v)
	}

	cfg.EmailConfirmUrlParsed.RawQuery = query.Encode()

	return cfg.EmailConfirmUrlParsed.String()
}
