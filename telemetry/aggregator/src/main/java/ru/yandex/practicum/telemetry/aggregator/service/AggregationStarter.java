package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.configuration.KafkaConfigProducer;
import ru.yandex.practicum.telemetry.aggregator.kafka.KafkaEventConsumer;
import ru.yandex.practicum.telemetry.aggregator.kafka.KafkaEventProducer;

import java.time.Duration;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final KafkaEventConsumer consumer;
    private final KafkaEventProducer producer;
    private final SensorSnapshotService snapshotService;
    private volatile boolean running = true;

    /**
     * Метод для начала процесса агрегации данных.
     * Подписывается на топики для получения событий от датчиков,
     * формирует снимок их состояния и записывает в кафку.
     */
    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            while (running) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(5000));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        try {
                            processSensorEvent((SensorEventAvro) record.value());
                        } catch (ClassCastException e) {
                            log.error("Получен неверный тип сообщения: {}", record.value().getClass(), e);
                        }
                    }
                    consumer.commitSync();
                }
            }

        } catch (WakeupException ignored) {
            log.info("Принят сигнал на выключение");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                log.info("Сброс данных в буффере продюсера");
                producer.flush();
                log.info("Фиксиция смещений");
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private void processSensorEvent(SensorEventAvro event) {
        try {
            Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(event);

            updatedSnapshot.ifPresent(snapshot -> {
                producer.send(
                        KafkaConfigProducer.TopicType.SNAPSHOTS_EVENTS,
                        event.getHubId(),
                        snapshot
                );
            });
        } catch (Exception e) {
            log.error("Ошибка обработки события датчика: {}", event, e);
        }
    }

    public void stop() {
        log.info("Остановка процесса агрегации");
        running = false;
        consumer.wakeup();
    }
}
