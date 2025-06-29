package ru.yandex.practicum.telemetry.collector.service.handler.avro.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerAvro;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorEventHandlerAvro<T extends SpecificRecordBase> implements SensorEventHandler {
    protected final KafkaEventProducerAvro producer;

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public void handle(SensorEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getPayloadCase().name());
        }

        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        T payload = mapToAvro(event);
        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();

        producer.send(
                KafkaConfig.TopicType.SENSORS_EVENTS,
                eventAvro.getId(),
                eventAvro
        );
    }
}
