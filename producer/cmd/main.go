package main

import (
	"awesomeProject/internal/kafka"
	"awesomeProject/internal/server"
	"fmt"
	"github.com/spf13/viper"
	"log"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("ошибка чтения конфига: %w", err))
	}

	producers := viper.GetStringSlice("brokers")

	if err := kafka.InitKafka(producers); err != nil {
		log.Fatal("Failed to init Kafka:", err)
	}
	defer func() {
		if err := kafka.CloseKafka(); err != nil {
			log.Fatal("Failed to close Kafka producer:", err)
		}
	}()

	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>
	client := server.New("8080")

	if err := client.Run(); err != nil {
		return
	}
}
