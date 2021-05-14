package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/hello-error/KafkaProducerAPI/pkg/api"
	"github.com/hello-error/KafkaProducerAPI/pkg/config"
	"github.com/hello-error/KafkaProducerAPI/pkg/producer"
)

var (
	// Version of the service. Can be set by -ldflags
	Version = "0.1.0"
	// Commit ID. Can be set by -ldflags
	Commit = ""
	// Branch name. Can be set by -ldflags
	Branch = ""

	// this is used when there are tests
	parseFlags = true
)

var (
	configName, configPath string
)

func init() {
	flag.StringVar(&configName, "config", "config", "config file name without the extension")
	flag.StringVar(&configPath, "configpath", ".", "config file directory path")
}

func main() {
	conf, err := config.LoadConfig(configPath)
	if err != nil {
		log.Printf("failed to load config %v\n", err)
		os.Exit(1)
	}

	// get the kafka config
	kafkaConf, err := conf.Producer.GetConfigMap()
	if err != nil {
		log.Printf("invalid kafka config: %v", err)
		os.Exit(1)
	}

	// create a kafka service
	ks, err := producer.NewKafkaService(conf.Producer.Topic, kafkaConf)
	if err != nil {
		log.Printf("failed to initialize kafka producer: %v\n", err)
		os.Exit(1)
	} else {
		log.Println("Kafka producer is now initialized")
	}
	defer ks.Shutdown()

	// start the api server
	srvr := api.Create(
		fmt.Sprintf("%s:%d", conf.Server.Host, conf.Server.Port),
		ks)
	if err := srvr.Run(); err != nil {
		os.Exit(1)
	}
}
