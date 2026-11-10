package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/SENERGY-Platform/service-topic-worker/pkg/configuration"
	"github.com/segmentio/kafka-go"
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
		InitTopic:      config.InitDeviceTypesTopics,
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
		case "RIGHTS":
			return nil
		}
		return errors.New("unable to handle command: " + string(msg))
	}, func(err error) {
		slog.Error("FATAL: kafka consumer error", "error", err)
		log.Fatalln("FATAL-ERROR: kafka consumer", err)
	})
}

func HandleDeviceTypePut(config configuration.Config, deviceType DeviceType) error {
	topics := []string{}
	for _, service := range deviceType.Services {
		topic := ServiceIdToTopic(service.Id)
		slog.Info("create service topic", "topic", topic)
		topics = append(topics, topic)
	}
	return CreateTopics(config.KafkaUrl, ServiceTopicConfigs(config, topics...)...)
}

func ServiceTopicConfigs(config configuration.Config, topics ...string) []kafka.TopicConfig {
	partitions := max(int(config.ServiceTopicPartitions), 1)
	replicationFactor := max(int(config.ServiceTopicReplicationFactor), 1)
	result := []kafka.TopicConfig{}
	for _, topic := range topics {
		entries := []kafka.ConfigEntry{}
		for _, entry := range GetTopicConfig(config.KafkaTopicConfigs, topic) {
			if config.ServiceTopicRetentionMs != "" && entry.ConfigName == "retention.ms" {
				continue
			}
			entries = append(entries, entry)
		}
		if config.ServiceTopicRetentionMs != "" {
			entries = append(entries, kafka.ConfigEntry{ConfigName: "retention.ms", ConfigValue: config.ServiceTopicRetentionMs})
		}
		result = append(result, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     entries,
		})
	}
	return result
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
