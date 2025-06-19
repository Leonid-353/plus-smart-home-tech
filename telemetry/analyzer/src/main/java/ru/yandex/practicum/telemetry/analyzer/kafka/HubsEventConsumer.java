package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.analyzer.configuration.KafkaConfigConsumer;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubsEventConsumer {
    private final KafkaConsumer<String, SpecificRecordBase> consumer;
    private final KafkaConfigConsumer.ConsumerConfig config;

    @Autowired
    public HubsEventConsumer(KafkaConfigConsumer.ConsumerHubsConfig config) {
        this.config = new KafkaConfigConsumer.ConsumerConfig(
                config.getProperties(),
                config.getTopics()
        );
        this.consumer = new KafkaConsumer<>(config.getProperties());
        subscribeToTopics();
    }

    private void subscribeToTopics() {
        consumer.subscribe(Collections.singletonList(
                config.getTopicName(KafkaConfigConsumer.TopicType.HUBS_EVENTS)
        ));
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
