package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/service-topic-worker/pkg/configuration"
	"log"
	"strings"
	"time"
)

func Start(ctx context.Context, config configuration.Config) error {
	maxWait, err := time.ParseDuration(config.KafkaConsumerMaxWait)
	if err != nil {
		return errors.New("unable to parse KafkaConsumerMaxWait as duration: " + err.Error())
	}
	return NewConsumer(ctx, ConsumerConfig{
		KafkaUrl:       config.KafkaUrl,
		GroupId:        config.GroupId,
		Topic:          config.DeviceTypeTopic,
		MinBytes:       int(config.KafkaConsumerMinBytes),
		MaxBytes:       int(config.KafkaConsumerMaxBytes),
		MaxWait:        maxWait,
		TopicConfigMap: config.KafkaTopicConfigs,
	}, func(topic string, msg []byte, time time.Time) error {
		command := DeviceTypeCommand{}
		err = json.Unmarshal(msg, &command)
		if err != nil {
			return err
		}
		switch command.Command {
		case "PUT":
			return HandleDeviceTypePut(config, command.DeviceType)
		case "DELETE":
			return nil
		}
		return errors.New("unable to handle command: " + string(msg))
	}, func(err error) {
		log.Fatalln("FATAL-ERROR: kafka consumer", err)
	})
}

func HandleDeviceTypePut(config configuration.Config, deviceType DeviceType) error {
	topics := []string{}
	for _, service := range deviceType.Services {
		topic := ServiceIdToTopic(service.Id)
		log.Println("create service topic", topic)
		topics = append(topics, topic)
	}
	return InitTopic(config.KafkaUrl, config.KafkaTopicConfigs, topics...)
}

type DeviceTypeCommand struct {
	Command    string     `json:"command"`
	Id         string     `json:"id"`
	Owner      string     `json:"owner"`
	DeviceType DeviceType `json:"device_type"`
}

type DeviceType struct {
	Id       string    `json:"id"`
	Services []Service `json:"services"`
}

type Service struct {
	Id string `json:"id"`
}

func ServiceIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}
