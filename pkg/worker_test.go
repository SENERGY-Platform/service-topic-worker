/*
 * Copyright (c) 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"testing"

	"github.com/SENERGY-Platform/service-topic-worker/pkg/configuration"
	"github.com/segmentio/kafka-go"
)

const testTopic = "urn_infai_ses_service_c40f0614-413c-59d8-aef3-1ee0a87ec136"

func retentionEntries(entries []kafka.ConfigEntry) []string {
	result := []string{}
	for _, entry := range entries {
		if entry.ConfigName == "retention.ms" {
			result = append(result, entry.ConfigValue)
		}
	}
	return result
}

func TestServiceTopicConfigsDefault(t *testing.T) {
	config, err := configuration.Load("../config.json")
	if err != nil {
		t.Fatal(err)
	}
	result := ServiceTopicConfigs(config, testTopic)
	if len(result) != 1 {
		t.Fatalf("expected one topic, got %d", len(result))
	}
	if result[0].NumPartitions != 1 || result[0].ReplicationFactor != 1 {
		t.Errorf("expected 1 partition and replication factor 1, got %d and %d", result[0].NumPartitions, result[0].ReplicationFactor)
	}
	if r := retentionEntries(result[0].ConfigEntries); len(r) != 1 || r[0] != "31536000000" {
		t.Errorf("expected the retention of kafka_topic_configs, got %v", r)
	}
}

func TestServiceTopicConfigsFromEnvironment(t *testing.T) {
	t.Setenv("SERVICE_TOPIC_PARTITIONS", "3")
	t.Setenv("SERVICE_TOPIC_REPLICATION_FACTOR", "2")
	t.Setenv("SERVICE_TOPIC_RETENTION_MS", "604800000")
	config, err := configuration.Load("../config.json")
	if err != nil {
		t.Fatal(err)
	}
	result := ServiceTopicConfigs(config, testTopic)
	if len(result) != 1 {
		t.Fatalf("expected one topic, got %d", len(result))
	}
	if result[0].NumPartitions != 3 || result[0].ReplicationFactor != 2 {
		t.Errorf("expected 3 partitions and replication factor 2, got %d and %d", result[0].NumPartitions, result[0].ReplicationFactor)
	}
	if r := retentionEntries(result[0].ConfigEntries); len(r) != 1 || r[0] != "604800000" {
		t.Errorf("expected exactly the retention of the environment, got %v", r)
	}
}

func TestServiceTopicConfigsBelowOne(t *testing.T) {
	result := ServiceTopicConfigs(configuration.Config{}, testTopic)
	if result[0].NumPartitions != 1 || result[0].ReplicationFactor != 1 {
		t.Errorf("expected values below 1 to count as 1, got %d and %d", result[0].NumPartitions, result[0].ReplicationFactor)
	}
}
