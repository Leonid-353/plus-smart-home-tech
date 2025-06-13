package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.hub;

import com.google.protobuf.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaConfig;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T extends Message> implements HubEventHandler {
    protected final KafkaEventProducerProto producer;

    protected abstract T mapToProto(HubEventProto event);

    protected abstract void setPayload(HubEventProto.Builder builder, T payload);

    @Override
    public void handle(HubEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            throw new IllegalArgumentException(
                    String.format("Неизвестный тип события: %s", event.getPayloadCase().name())
            );
        }

        T payload = mapToProto(event);
        HubEventProto.Builder eventBuilder = HubEventProto.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp());

        setPayload(eventBuilder, payload);

        HubEventProto eventProto = eventBuilder.build();

        producer.send(
                KafkaConfig.TopicType.HUBS_EVENTS,
                eventProto.getHubId(),
                eventProto
        );
    }
}
