package ru.yandex.practicum.telemetry.collector.service.handler.avro.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerAvro;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubEventHandlerAvro<T extends SpecificRecordBase> implements HubEventHandler {
    protected final KafkaEventProducerAvro producer;

    protected abstract T mapToAvro(HubEventProto event);

    @Override
    public void handle(HubEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            throw new IllegalArgumentException(
                    String.format("Неизвестный тип события: %s", event.getPayloadCase().name())
            );
        }

        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        T payload = mapToAvro(event);
        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        producer.send(
                KafkaConfig.TopicType.HUBS_EVENTS,
                eventAvro.getHubId(),
                eventAvro
        );
    }
}
