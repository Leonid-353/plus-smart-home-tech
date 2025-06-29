package ru.yandex.practicum.telemetry.aggregator.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import ru.yandex.practicum.telemetry.aggregator.kafka.KafkaEventConsumer;

import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@ToString
@ConfigurationProperties("aggregator.kafka")
public class KafkaConfigConsumer {
    private ConsumerConfig consumer;

    @Bean
    public KafkaEventConsumer kafkaEventConsumer() {
        return new KafkaEventConsumer(this);
    }

    public enum TopicType {
        SENSORS_EVENTS;

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
    public static class ConsumerConfig {
        private final Properties properties;
        private final Map<TopicType, String> topics = new EnumMap<>(TopicType.class);

        public ConsumerConfig(Properties properties, Map<String, String> topics) {
            this.properties = properties;
            for (Map.Entry<String, String> entry : topics.entrySet()) {
                this.topics.put(TopicType.from(entry.getKey()), entry.getValue());
            }
        }
    }
}
