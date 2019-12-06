package service

type TokenRepositoryInterface interface {
	SetToken(token string) error
	GetToken(token string) error
	GetTokenString(n int) string
}
