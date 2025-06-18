package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T> implements HubEventHandler {

    protected abstract T mapToEntity(HubEventAvro event);

    protected abstract void processEntity(T entity, HubEventAvro event);

    @Override
    public void handle(HubEventAvro event) {
        if (!canHandle(event)) {
            throw new IllegalArgumentException("Невозможно обработать данный тип события");
        }

        T entity = mapToEntity(event);
        processEntity(entity, event);
    }
}
