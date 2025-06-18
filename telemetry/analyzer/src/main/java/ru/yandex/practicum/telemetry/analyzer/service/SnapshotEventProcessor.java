package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.kafka.SnapshotsEventConsumer;
import ru.yandex.practicum.telemetry.analyzer.service.handler.snapshot.SnapshotEventHandler;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotEventProcessor {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000);

    private final SnapshotsEventConsumer consumer;
    private final SnapshotEventHandler handler;

    public void start() {
        try {
            registerShutdownHook();
            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(POLL_TIMEOUT);
                log.debug("Количество прочитанных сообщений: {}", records.count());

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        SensorsSnapshotAvro sensorsSnapshot = (SensorsSnapshotAvro) record.value();
                        handler.handle(sensorsSnapshot);
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.error("Принят сигнал на выключение SnapshotEventProcessor");
        } catch (Exception e) {
            log.error("Ошибка обработки сообщений в SnapshotEventProcessor", e);
        } finally {
            try {
                log.info("Фиксиция смещений");
                consumer.commitSync();
            } finally {
                log.info("Закрываем SnapshotsEventConsumer");
                consumer.close();
            }
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Сработал ShutdownHook в SnapshotEventProcessor");
            consumer.wakeup();
        }));
    }
}
