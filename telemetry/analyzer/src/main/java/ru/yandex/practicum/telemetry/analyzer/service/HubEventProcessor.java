package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.kafka.HubsEventConsumer;
import ru.yandex.practicum.telemetry.analyzer.service.handler.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.service.handler.hub.HubEventHandlers;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000);

    private final HubsEventConsumer consumer;
    private final HubEventHandlers handlers;

    @Override
    public void run() {
        try {
            registerShutdownHook();
            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(POLL_TIMEOUT);
                log.debug("Количество прочитанных сообщений: {}", records.count());

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        HubEventAvro event = (HubEventAvro) record.value();
                        String messageType = event.getPayload().getClass().getSimpleName();

                        HubEventHandler handler = handlers.getHandler(messageType);
                        if (handler != null) {
                            handler.handle(event);
                        } else {
                            throw new IllegalArgumentException("Нет обработчика для события " + event);
                        }
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.error("Принят сигнал на выключение HubEventProcessor");
        } catch (Exception e) {
            log.error("Ошибка обработки сообщений в HubEventProcessor", e);
        } finally {
            try {
                log.info("Фиксиция смещений");
                consumer.commitSync();
            } finally {
                log.info("Закрываем HubsEventConsumer");
                consumer.close();
            }
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Сработал ShutdownHook в HubEventProcessor");
            consumer.wakeup();
        }));
    }
}
