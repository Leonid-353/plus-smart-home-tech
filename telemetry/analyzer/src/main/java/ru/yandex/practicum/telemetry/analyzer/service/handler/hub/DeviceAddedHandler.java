package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Component
@RequiredArgsConstructor
public class DeviceAddedHandler extends BaseHubEventHandler<Sensor> {
    private final SensorRepository sensorRepository;

    @Override
    public String getMessageType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    @Override
    protected Sensor mapToEntity(HubEventAvro event) {
        DeviceAddedEventAvro payload = (DeviceAddedEventAvro) event.getPayload();
        return Sensor.builder()
                .id(payload.getId())
                .hubId(event.getHubId())
                .build();
    }

    @Override
    @Transactional
    protected void processEntity(Sensor sensor, HubEventAvro event) {
        sensorRepository.save(sensor);
    }
}
