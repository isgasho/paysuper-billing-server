# Changelog
All notable changes to this project will be documented in this file.

## [1.1.0] - 2019-12-24

###Added
- The centrifugo has been split into multiple instances for sending notification to the Dashboard and the payment form.
- Added an API method for VAT calculating in a payment process.

### Changed
- Changed the Project settings for VAT calculation. Added some options: to disable VAT for a customer in a payment process, to include VAT in a total payment amount.
- Changed the card number checking for the China UnionPay card validation.
- Update project's dependencies.

### Removed
- Removed the file `.gitlab-ci.yml`.

### Fixed
- Fix for a customer's country detection if it had not been determined by a user's IP address.

***

## [1.0.0] - 2019-12-19

###Added
- Limiting payments by country depending on the country issuing of the customer's bank card.
- The logic of the rounding method for a payment amount for various currencies considering the presence or absence of a currency's fractional part.

### Changed
- Added new response parameters when changing a language on a payment form.
- Added a project ID to payment form events' responses for sending data to web analytics services.
- Added a VAT parameter to a response for a rendering of a payment form.
- Added a merchant's legal name for onboarding process mails.
- Updated README.

### Fixed
- An order with products will be paid in a product's fallback currency if a customer's selected currency does not exist in a project for this product.
- Corrected minimum payments amounts for various currencies.
- Fix an order initialization for products with outdated project's settings.
- The purchase receipt letter sends only for a completed payment.
- Webhooks notifications send for all payments statuses including CANCEL and DECLINE.
- Fix for a payment form language selection via a user's locale in a token parameter.
- Edited the rounding method for a payment amount.

### Removed
- Deleted the unused source code.