package ru.yandex.practicum.telemetry.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import ru.yandex.practicum.telemetry.aggregator.configuration.KafkaConfigConsumer;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@RequiredArgsConstructor
public class KafkaEventConsumer {
    private final KafkaConsumer<String, SpecificRecordBase> consumer;
    private final KafkaConfigConsumer kafkaConfig;

    @Autowired
    public KafkaEventConsumer(KafkaConfigConsumer kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.consumer = new KafkaConsumer<>(kafkaConfig.getConsumer().getProperties());

        kafkaConfig.getConsumer().getTopics().values()
                .forEach(topic -> consumer.subscribe(Collections.singletonList(topic)));
    }

    public ConsumerRecords<String, SpecificRecordBase> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    public void commitSync() {
        consumer.commitSync();
    }

    public void close() {
        consumer.close();
    }

    public void wakeup() {
        consumer.wakeup();
    }
}
