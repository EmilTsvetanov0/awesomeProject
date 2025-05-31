package cconfig

import (
	"fmt"
	"github.com/spf13/viper"
	"sync"
)

var once sync.Once

func InitConfig() {
	once.Do(func() {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath("./config")

		if err := viper.ReadInConfig(); err != nil {
			panic(fmt.Errorf("[producer] ошибка чтения конфига: %w", err))
		}
	})
}
