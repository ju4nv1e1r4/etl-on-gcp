package constants

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var Logger *zap.Logger

// InitLogger initializes the Logger variable.
func InitLogger() {
	var err error

	Logger, err = zap.NewProduction()
	if err != nil {
		panic("Error using logger: " + err.Error())
	}
}

// LoadEnvVars loads the environment variables. 
// The function receives the variable name as a string.
func LoadEnvVars(key string) (string, error) {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		Logger.Error("Error reading configuration file", 
			zap.Error(err), 
		)
		return "", fmt.Errorf("error reading configuration file: %v", err)
	}

	apiKey := viper.GetString(key)
	if apiKey == "" { 
		Logger.Warn("Error: Check environment variables and try again.")
	}

	return apiKey, nil
}
