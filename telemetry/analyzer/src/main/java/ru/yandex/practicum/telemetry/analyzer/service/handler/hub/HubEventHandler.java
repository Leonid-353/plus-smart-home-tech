package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {
    String getMessageType();

    default boolean canHandle(HubEventAvro event) {
        return event != null
                && event.getPayload() != null
                && getMessageType().equals(event.getPayload().getClass().getSimpleName());
    }

    void handle(HubEventAvro event);
}
