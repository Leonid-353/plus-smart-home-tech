package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Component
@RequiredArgsConstructor
public class DeviceRemovedHandler extends BaseHubEventHandler<Sensor> {
    private final SensorRepository sensorRepository;

    @Override
    public String getMessageType() {
        return DeviceRemovedEventAvro.class.getSimpleName();
    }

    @Override
    protected Sensor mapToEntity(HubEventAvro event) {
        DeviceRemovedEventAvro payload = (DeviceRemovedEventAvro) event.getPayload();
        return Sensor.builder()
                .id(payload.getId())
                .hubId(event.getHubId())
                .build();
    }

    @Override
    @Transactional
    protected void processEntity(Sensor sensor, HubEventAvro event) {
        sensorRepository.deleteByIdAndHubId(sensor.getId(), sensor.getHubId());
    }
}
