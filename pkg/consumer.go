package pkg

import (
	"context"
	"github.com/segmentio/kafka-go"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"time"
)

func GetBroker(bootstrapUrl string) (brokers []string, err error) {
	return getBroker(bootstrapUrl)
}

func getBroker(bootstrapUrl string) (result []string, err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	brokers, err := conn.Brokers()
	if err != nil {
		return result, err
	}
	for _, broker := range brokers {
		result = append(result, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	}
	return result, nil
}


type ConsumerConfig struct {
	KafkaUrl       string
	GroupId        string
	Topic          string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	TopicConfigMap map[string][]kafka.ConfigEntry
}

func NewConsumer(ctx context.Context, config ConsumerConfig, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error)) (err error) {
	log.Println("DEBUG: consume topic: \"" + config.Topic + "\"")
	broker, err := GetBroker(config.KafkaUrl)
	if err != nil {
		log.Println("ERROR: unable to get broker list", err)
		return err
	}
	err = InitTopic(config.KafkaUrl, config.TopicConfigMap, config.Topic)
	if err != nil {
		log.Println("ERROR: unable to create topic", err)
		return err
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval: 0, //synchronous commits
		Brokers:        broker,
		GroupID:        config.GroupId,
		Topic:          config.Topic,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		Logger:         log.New(ioutil.Discard, "", 0),
		ErrorLogger:    log.New(ioutil.Discard, "", 0),
	})
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("close kafka reader ", config.Topic)
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					log.Println("close consumer for topic ", config.Topic)
					return
				}
				if err != nil {
					log.Println("ERROR: while consuming topic ", config.Topic, err)
					errorhandler(err)
					return
				}
				if time.Now().Sub(m.Time) > 1*time.Hour { //floodgate to prevent old messages to DOS the consumer
					log.Println("WARNING: kafka message older than 1h: ", config.Topic, time.Now().Sub(m.Time))
					err = r.CommitMessages(ctx, m)
					if err != nil {
						log.Println("ERROR: while committing message ", config.Topic, err)
						errorhandler(err)
						return
					}
				} else {
					err = listener(m.Topic, m.Value, m.Time)
					if err != nil {
						log.Println("ERROR: unable to handle message (no commit)", err, m.Topic, string(m.Value))
					} else {
						err = r.CommitMessages(ctx, m)
						if err != nil {
							log.Println("ERROR: while committing message ", config.Topic, err)
							errorhandler(err)
							return
						}
					}
				}
			}
		}
	}()
	return err
}