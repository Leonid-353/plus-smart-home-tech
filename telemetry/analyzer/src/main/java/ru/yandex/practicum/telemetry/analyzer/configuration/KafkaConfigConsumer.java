package ru.yandex.practicum.telemetry.analyzer.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import ru.yandex.practicum.telemetry.analyzer.kafka.HubsEventConsumer;
import ru.yandex.practicum.telemetry.analyzer.kafka.SnapshotsEventConsumer;

import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@ToString
@ConfigurationProperties("analyzer.kafka")
public class KafkaConfigConsumer {
    private ConsumerSnapshotsConfig consumerSnapshots;
    private ConsumerHubsConfig consumerHubs;

    @Bean
    public SnapshotsEventConsumer snapshotsEventConsumer() {
        return new SnapshotsEventConsumer(consumerSnapshots);
    }

    @Bean
    public HubsEventConsumer hubsEventConsumer() {
        return new HubsEventConsumer(consumerHubs);
    }

    public enum TopicType {
        SNAPSHOTS_EVENTS,
        HUBS_EVENTS;

        public static TopicType from(String type) {
            for (TopicType value : values()) {
                if (value.name().equalsIgnoreCase(type.replace('-', '_'))) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Неизвестный тип топика: " + type);
        }
    }

    @Getter
    @Setter
    @ToString
    public static class ConsumerSnapshotsConfig {
        private Properties properties;
        private Map<String, String> topics;

        public String getTopicName() {
            return topics.get("snapshots-events");
        }
    }

    @Getter
    @Setter
    @ToString
    public static class ConsumerHubsConfig {
        private Properties properties;
        private Map<String, String> topics;

        public String getTopicName() {
            return topics.get("hubs-events");
        }
    }

    @Getter
    @ToString
    public static class ConsumerConfig {
        private final Properties properties;
        private final Map<TopicType, String> topics = new EnumMap<>(TopicType.class);

        public ConsumerConfig(Properties properties, Map<String, String> topics) {
            this.properties = properties;
            topics.forEach((key, value) ->
                    this.topics.put(TopicType.from(key), value)
            );
        }

        public String getTopicName(TopicType topicType) {
            return topics.get(topicType);
        }
    }
}
