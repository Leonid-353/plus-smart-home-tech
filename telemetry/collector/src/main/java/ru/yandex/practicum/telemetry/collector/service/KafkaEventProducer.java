package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final KafkaConfig kafkaConfig;

    @Autowired
    public KafkaEventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.producer = new KafkaProducer<>(kafkaConfig.getProducer().getProperties());
    }

    public void send(KafkaConfig.TopicType topicType, String key, SpecificRecordBase message) {
        String topicName = kafkaConfig.getProducer().getTopics().get(topicType);
        if (topicName == null || topicName.isEmpty()) {
            throw new RuntimeException("Нет такого топика: " + topicType);
        }
        producer.send(new ProducerRecord<>(topicName, key, message));
        producer.flush();
    }

    public void close() {
        producer.flush();
        producer.close(Duration.ofSeconds(15));
    }
}
