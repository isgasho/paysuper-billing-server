Billing service
=====
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-brightgreen.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Build Status](https://travis-ci.org/paysuper/paysuper-billing-server.svg?branch=master)](https://travis-ci.org/paysuper/paysuper-billing-server) 
[![codecov](https://codecov.io/gh/paysuper/paysuper-billing-server/branch/master/graph/badge.svg)](https://codecov.io/gh/paysuper/paysuper-billing-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/paysuper/paysuper-billing-server)](https://goreportcard.com/report/github.com/paysuper/paysuper-billing-server) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=paysuper_paysuper-billing-server&metric=alert_status)](https://sonarcloud.io/dashboard?id=paysuper_paysuper-billing-server)

This service contain all business logic for payment processing

## Environment variables:

| Name                                 | Required | Default               | Description                                                                                                                         |
|:-------------------------------------|:--------:|:----------------------|:------------------------------------------------------------------------------------------------------------------------------------|
| MONGO_DSN                            | true     | -                     | MongoBD DSN connection string                                                                                                       |
| MONGO_DIAL_TIMEOUT                   | -        | 10                    | MongoBD dial timeout in seconds                                                                                                     |
| PSP_ACCOUNTING_CURRENCY              | -        | EUR                   | PaySuper accounting currency                                                                                                        |
| METRICS_PORT                         | -        | 8086                  | Http server port for health and metrics request                                                                                     |
| CENTRIFUGO_SECRET                    | true     | -                     | Centrifugo secret key                                                                                                               |
| BROKER_ADDRESS                       | -        | amqp://127.0.0.1:5672 | RabbitMQ url address                                                                                                                |
| CARD_PAY_API_URL                     | true     | -                     | CardPay API url to process payments, more in [documentation](https://integration.cardpay.com/v3/)                                   | 
| CACHE_REDIS_ADDRESS                  | true     |                       | A seed list of host:port addresses of cluster nodes                                                                                 |
| CACHE_REDIS_PASSWORD                 | -        |                       | Password for connection string                                                                                                      |
| CACHE_REDIS_POOL_SIZE                | -        | 1                     | PoolSize applies per cluster node and not for the whole cluster                                                                     |
| CACHE_REDIS_MAX_RETRIES              | -        | 10                    | Maximum retries for connection                                                                                                      |
| CACHE_REDIS_MAX_REDIRECTS            | -        | 8                     | The maximum number of retries before giving up                                                                                      |
| CUSTOMER_COOKIE_PUBLIC_KEY           | true     | -                     | Base64 encoded RSA public key - used for encrypt customer browser cookies content. Minimal length of RSA public key must be 4096    |
| CUSTOMER_COOKIE_PRIVATE_KEY          | true     | -                     | Base64 encoded RSA private key - used for decrypt customer browser cookies content. Minimal length of RSA private key must be 4096  |
| REDIS_HOST                           | -        | 127.0.0.1:6379        | Redis server host                                                                                                                   |
| REDIS_PASSWORD                       | -        | ""                    | Password to access to Redis server                                                                                                  |
| SMTP_HOST                            | true     | -                     | SMTP server address                                                                                                                 |
| SMTP_PORT                            | true     | -                     | SMTP server port                                                                                                                    |
| SMTP_USER                            | true     | -                     | User account to authorize on SMTP server                                                                                            |
| SMTP_PASSWORD                        | true     | -                     | Password to authorize user on SMTP server                                                                                           |
| EMAIL_NOTIFICATION_SENDER            | true     | -                     | Sender email for using to send alert notifications to users                                                                         |
| CENTRIFUGO_MERCHANT_CHANNEL          | -        | paysuper:merchant#%s  | Centrifugo channel name to send notifications to merchant                                                                           |

## Docker Deployment

```bash
docker build -f Dockerfile -t paysuper_billing_service .
docker run -d -e "MONGO_HOST=127.0.0.1:27017" -e "MONGO_DB="paysuper" ... e="CACHE_PROJECT_PAYMENT_METHOD_TIMEOUT=600" paysuper_billing_service
```

## Architecture

Described in docs folder.

## Contributing
We feel that a welcoming community is important and we ask that you follow PaySuper's [Open Source Code of Conduct](https://github.com/paysuper/code-of-conduct/blob/master/README.md) in all interactions with the community.

PaySuper welcomes contributions from anyone and everyone. Please refer to each project's style and contribution guidelines for submitting patches and additions. In general, we follow the "fork-and-pull" Git workflow.

The master branch of this repository contains the latest stable release of this component.

 
