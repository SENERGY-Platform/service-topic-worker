package pkg

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
	"strings"
)

func InitTopic(kafkaUrl string, configMap map[string][]kafka.ConfigEntry, topics ...string) (err error) {
	return InitTopicWithConfig(kafkaUrl, configMap, 1, 1, topics...)
}

func InitTopicWithConfig(bootstrapUrl string, configMap map[string][]kafka.ConfigEntry, numPartitions int, replicationFactor int, topics ...string) (err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{}

	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     GetTopicConfig(configMap, topic),
		})
	}
	return controllerConn.CreateTopics(topicConfigs...)
}

func GetTopicConfig(configMap map[string][]kafka.ConfigEntry, topic string) []kafka.ConfigEntry {
	if configMap == nil {
		return nil
	}
	result, exists := configMap[topic]
	if exists {
		return result
	}
	for key, conf := range configMap {
		if strings.HasPrefix(topic, key) {
			return conf
		}
	}
	return nil
}
