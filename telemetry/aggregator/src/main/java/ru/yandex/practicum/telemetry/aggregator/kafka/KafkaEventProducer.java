package ru.yandex.practicum.telemetry.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import ru.yandex.practicum.telemetry.aggregator.configuration.KafkaConfigProducer;

@Slf4j
@RequiredArgsConstructor
public class KafkaEventProducer {
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final KafkaConfigProducer kafkaConfig;

    @Autowired
    public KafkaEventProducer(KafkaConfigProducer kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.producer = new KafkaProducer<>(kafkaConfig.getProducer().getProperties());
    }

    public void send(KafkaConfigProducer.TopicType topicType, String key, SpecificRecordBase message) {
        String topicName = kafkaConfig.getProducer().getTopics().get(topicType);
        if (topicName == null || topicName.isEmpty()) {
            throw new RuntimeException(String.format("Нет такого топика: %s", topicType));
        }
        producer.send(new ProducerRecord<>(topicName, key, message));
        producer.flush();
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
