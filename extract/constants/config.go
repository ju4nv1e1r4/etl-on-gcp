package constants

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var Logger *zap.Logger

func InitLogger() {
	var err error

	Logger, err = zap.NewProduction()
	if err != nil {
		panic("Erro ao utilizar o logger: " + err.Error())
	}
}

func LoadEnvVars(key string) (string, error) {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		Logger.Error("Erro ao ler arquivo de configuração", 
			zap.Error(err), 
		)
		return "", fmt.Errorf("erro ao ler o arquivo de configuração: %v", err)
	}

	apiKey := viper.GetString(key)
	if apiKey == "" { 
		Logger.Warn("Erro: verifique as variáveis de ambiente.")
	}

	return apiKey, nil
}
