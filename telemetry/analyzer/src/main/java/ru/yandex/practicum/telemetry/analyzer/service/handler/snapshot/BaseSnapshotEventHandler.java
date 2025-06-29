package ru.yandex.practicum.telemetry.analyzer.service.handler.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSnapshotEventHandler implements SnapshotEventHandler {

    protected abstract void processEntity(SensorsSnapshotAvro snapshot);

    @Override
    public void handle(SensorsSnapshotAvro snapshot) {
        if (!canHandle(snapshot)) {
            throw new IllegalArgumentException("Невозможно обработать данный тип снапшота");
        }

        processEntity(snapshot);
    }
}
