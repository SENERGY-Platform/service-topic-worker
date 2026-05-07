package pkg

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
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
	InitTopic      bool
}

func NewConsumer(ctx context.Context, config ConsumerConfig, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error)) (err error) {
	slog.Debug("consume", "topic", config.Topic)
	broker, err := GetBroker(config.KafkaUrl)
	if err != nil {
		slog.Error("unable to get broker list", "error", err)
		return err
	}
	if config.InitTopic {
		err = InitTopic(config.KafkaUrl, config.TopicConfigMap, config.Topic)
		if err != nil {
			slog.Error("unable to create topic", "error", err)
			return err
		}
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
		defer r.Close()
		defer slog.Debug("consumer closed", "topic", config.Topic)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					slog.Error("unable to consume topic", "topic", config.Topic, "error", err)
					errorhandler(err)
					return
				}

				err = retry(func() error {
					return listener(m.Topic, m.Value, m.Time)
				}, func(n int64) time.Duration {
					return time.Duration(n) * time.Second
				}, 10*time.Minute)

				if err != nil {
					slog.Error("unable to handle message (no commit)", "topic", config.Topic, "error", err)
					errorhandler(err)
				} else {
					err = r.CommitMessages(ctx, m)
				}
			}
		}
	}()
	return err
}

func retry(f func() error, waitProvider func(n int64) time.Duration, timeout time.Duration) (err error) {
	err = errors.New("")
	start := time.Now()
	for i := int64(1); err != nil && time.Since(start) < timeout; i++ {
		err = f()
		if err != nil {
			slog.Error("kafka listener error", "error", err)
			wait := waitProvider(i)
			if time.Since(start)+wait < timeout {
				slog.Error("retry after", "wait", wait.String())
				time.Sleep(wait)
			} else {
				return err
			}
		}
	}
	return err
}
