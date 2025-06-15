package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class SensorSnapshotService {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        if (event == null || event.getHubId() == null || event.getId() == null) {
            return Optional.empty();
        }

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(
                event.getHubId(),
                hubId -> createNewSnapshot(hubId, event.getTimestamp())
        );

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());

        if (oldState != null && shouldSkipUpdate(oldState, event)) {
            return Optional.empty();
        }

        updateSnapshot(snapshot, event);

        return Optional.of(snapshot);
    }

    private SensorsSnapshotAvro createNewSnapshot(String hubId, Instant timestamp) {
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(new HashMap<>())
                .build();
    }

    private boolean shouldSkipUpdate(SensorStateAvro oldState, SensorEventAvro event) {
        if (oldState.getTimestamp().isAfter(event.getTimestamp())) {
            return true;
        }

        return isUnchanged(oldState.getData(), event.getPayload());
    }

    private void updateSnapshot(SensorsSnapshotAvro snapshot, SensorEventAvro event) {
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);

        if (event.getTimestamp().isAfter(snapshot.getTimestamp())) {
            snapshot.setTimestamp(event.getTimestamp());
        }
    }

    private Boolean isUnchanged(Object oldPayload, Object newPayload) {
        if (oldPayload == null || newPayload == null) {
            log.warn("Один из payload равен null. old: {}, new: {}", oldPayload, newPayload);
            return false;
        }

        if (!oldPayload.getClass().equals(newPayload.getClass())) {
            log.warn("Типы payload не совпадают:  {} != {}",
                    oldPayload.getClass().getSimpleName(),
                    newPayload.getClass().getSimpleName());
            return false;
        }

        switch (oldPayload) {
            case ClimateSensorAvro oldClimate -> {
                ClimateSensorAvro newClimate = (ClimateSensorAvro) newPayload;
                return compareClimateSensors(oldClimate, newClimate);
            }
            case LightSensorAvro oldLight -> {
                LightSensorAvro newLight = (LightSensorAvro) newPayload;
                return compareLightSensors(oldLight, newLight);
            }
            case MotionSensorAvro oldMotion -> {
                MotionSensorAvro newMotion = (MotionSensorAvro) newPayload;
                return compareMotionSensors(oldMotion, newMotion);
            }
            case SwitchSensorAvro oldSwitch -> {
                SwitchSensorAvro newSwitch = (SwitchSensorAvro) newPayload;
                return compareSwitchSensors(oldSwitch, newSwitch);
            }
            case TemperatureSensorAvro oldTemperature -> {
                TemperatureSensorAvro newTemperature = (TemperatureSensorAvro) newPayload;
                return compareTemperatureSensors(oldTemperature, newTemperature);
            }
            default -> {
            }
        }

        log.warn("Неизвестный тип датчика: {}", oldPayload.getClass().getSimpleName());
        return false;
    }

    private boolean compareClimateSensors(ClimateSensorAvro oldClimate, ClimateSensorAvro newClimate) {
        log.debug("Сравнение ClimateSensor");
        return oldClimate.getTemperatureC() == newClimate.getTemperatureC()
                && oldClimate.getHumidity() == newClimate.getHumidity()
                && oldClimate.getCo2Level() == newClimate.getCo2Level();
    }

    private boolean compareLightSensors(LightSensorAvro oldLight, LightSensorAvro newLight) {
        log.debug("Сравнение LightSensor");
        return oldLight.getLinkQuality() == newLight.getLinkQuality()
                && oldLight.getLuminosity() == newLight.getLuminosity();
    }

    private boolean compareMotionSensors(MotionSensorAvro oldMotion, MotionSensorAvro newMotion) {
        log.debug("Сравнение MotionSensor");
        return oldMotion.getLinkQuality() == newMotion.getLinkQuality()
                && oldMotion.getMotion() == newMotion.getMotion()
                && oldMotion.getVoltage() == newMotion.getVoltage();
    }

    private boolean compareSwitchSensors(SwitchSensorAvro oldSwitch, SwitchSensorAvro newSwitch) {
        log.debug("Сравнение SwitchSensor");
        return oldSwitch.getState() == newSwitch.getState();
    }

    private boolean compareTemperatureSensors(TemperatureSensorAvro oldTemp, TemperatureSensorAvro newTemp) {
        log.debug("Сравнение TemperatureSensor");
        return oldTemp.getTemperatureC() == newTemp.getTemperatureC()
                && oldTemp.getTemperatureF() == newTemp.getTemperatureF();
    }

    public Optional<SensorsSnapshotAvro> getSnapshot(String hubId) {
        return Optional.ofNullable(snapshots.get(hubId));
    }
}
