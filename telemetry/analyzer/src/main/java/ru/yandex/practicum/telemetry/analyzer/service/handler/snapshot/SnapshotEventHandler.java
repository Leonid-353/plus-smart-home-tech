package ru.yandex.practicum.telemetry.analyzer.service.handler.snapshot;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public interface SnapshotEventHandler {
    String getMessageType();

    default boolean canHandle(SensorsSnapshotAvro snapshot) {
        return snapshot != null
                && snapshot.getSensorsState() != null
                && getMessageType().equals(snapshot.getClass().getSimpleName());
    }

    void handle(SensorsSnapshotAvro snapshot);
}
