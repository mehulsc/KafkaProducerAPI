package config

import (
	"errors"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

const (
	configName = "config"
	envPrefix  = "KAFKA_PRODUCER_API"
)

// Config scanner config
type Config struct {
	Server   Server
	Producer Producer
}

// Server http server config
type Server struct {
	Host string
	Port int
}

// Producer kafka producer config
type Producer struct {
	Broker          string `mapstructure:"broker"`
	Protocol        string `mapstructure:"protocol"`
	AutoOffsetReset string `mapstructure:"auto_offset_reset"`
	Mechanism       string `mapstructure:"mechanism"`
	Username        string `mapstructure:"username"`
	Password        string `mapstructure:"password"`
	Topic           string `mapstructure:"topic"`
}

// LoadConfig loads a config file from given path
func LoadConfig(path string) (Config, error) {
	viper.SetConfigName(configName)
	viper.AddConfigPath(path)
	viper.SetConfigType("yaml")
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.ReadInConfig(); err != nil {
		return Config{}, err
	}

	var conf Config
	err := viper.Unmarshal(&conf)
	if err != nil {
		return Config{}, err
	}

	return conf, nil
}

// GetConfigMap returns a kafka config map from producer config
func (p Producer) GetConfigMap() (*kafka.ConfigMap, error) {
	if p.Broker == "" {
		return nil, errors.New("empty broker")
	}
	kafkaConf := kafka.ConfigMap{
		"bootstrap.servers": p.Broker,
	}

	if p.Protocol != "" {
		kafkaConf["security.protocol"] = p.Protocol
	}

	if p.Mechanism != "" {
		kafkaConf["sasl.mechanisms"] = p.Mechanism
	}

	if p.Username != "" {
		kafkaConf["sasl.username"] = p.Username
	}

	if p.Password != "" {
		kafkaConf["sasl.password"] = p.Password
	}

	if p.AutoOffsetReset != "" {
		kafkaConf["auto.offset.reset"] = p.AutoOffsetReset
	}

	return &kafkaConf, nil
}
