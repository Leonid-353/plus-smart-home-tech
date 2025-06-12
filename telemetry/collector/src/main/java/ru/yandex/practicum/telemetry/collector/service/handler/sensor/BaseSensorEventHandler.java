package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import com.google.protobuf.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends Message> implements SensorEventHandler {
    protected final KafkaEventProducer producer;

    protected abstract T mapToProto(SensorEventProto event);

    protected abstract void setPayload(SensorEventProto.Builder builder, T payload);

    @Override
    public void handle(SensorEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            throw new IllegalArgumentException(
                    String.format("Неизвестный тип события: %s", event.getPayloadCase().name())
            );
        }

        T payload = mapToProto(event);
        SensorEventProto.Builder eventBuilder = SensorEventProto.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(event.getTimestamp());

        setPayload(eventBuilder, payload);

        SensorEventProto eventProto = eventBuilder.build();

        producer.send(
                KafkaConfig.TopicType.SENSORS_EVENTS,
                eventProto.getId(),
                eventProto
        );
    }
}
