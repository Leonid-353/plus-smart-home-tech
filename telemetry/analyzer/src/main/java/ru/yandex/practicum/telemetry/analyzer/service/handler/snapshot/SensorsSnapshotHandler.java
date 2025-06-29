package ru.yandex.practicum.telemetry.analyzer.service.handler.snapshot;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.service.HubRouterClientGrpc;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorsSnapshotHandler extends BaseSnapshotEventHandler {
    private final HubRouterClientGrpc hubRouterClient;
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public String getMessageType() {
        return SensorsSnapshotAvro.class.getSimpleName();
    }

    @Override
    protected void processEntity(SensorsSnapshotAvro snapshot) {
        log.debug("Snapshot: {}, в обработке", snapshot);
        var sensorState = snapshot.getSensorsState();

        scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> matchesConditions(scenario, sensorState))
                .forEach(scenario -> {
                    actionRepository.findAllByScenario(scenario).stream()
                            .map(this::convertToDeviceActionRequest)
                            .forEach(hubRouterClient::sendRequest);
                });
    }

    private boolean matchesConditions(Scenario scenario, Map<String, SensorStateAvro> sensorState) {
        return conditionRepository.findAllByScenario(scenario).stream()
                .allMatch(condition -> learnsCondition(condition, sensorState.get(condition.getSensor().getId())));
    }

    private boolean learnsCondition(Condition condition, SensorStateAvro state) {
        if (state == null || state.getData() == null) return false;

        return switch (condition.getType()) {
            case MOTION -> verifyCondition(condition, ((MotionSensorAvro) state.getData()).getMotion() ? 1 : 0);
            case LUMINOSITY -> verifyCondition(condition, ((LightSensorAvro) state.getData()).getLuminosity());
            case SWITCH -> verifyCondition(condition, ((SwitchSensorAvro) state.getData()).getState() ? 1 : 0);
            case TEMPERATURE -> verifyCondition(condition, ((ClimateSensorAvro) state.getData()).getTemperatureC());
            case CO2LEVEL -> verifyCondition(condition, ((ClimateSensorAvro) state.getData()).getCo2Level());
            case HUMIDITY -> verifyCondition(condition, ((ClimateSensorAvro) state.getData()).getHumidity());
            default -> false;
        };
    }

    private boolean verifyCondition(Condition condition, Integer value) {
        var expected = condition.getValue();
        return switch (condition.getOperation()) {
            case EQUALS -> Objects.equals(value, expected);
            case GREATER_THAN -> value > expected;
            case LOWER_THAN -> value < expected;
            default -> false;
        };
    }

    private DeviceActionRequest convertToDeviceActionRequest(Action action) {
        Objects.requireNonNull(action, "Действие не может быть null");
        Objects.requireNonNull(action.getScenario(), "Значение сценария у действия не может быть null");
        Objects.requireNonNull(action.getSensor(), "Значение датчика у действия не может быть null");

        return DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(convertToDeviceActionProto(action))
                .setTimestamp(createCurrentTimestamp())
                .build();
    }

    private DeviceActionProto convertToDeviceActionProto(Action action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensor().getId())
                .setType(mapActionType(action));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    private Timestamp createCurrentTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    private ActionTypeProto mapActionType(Action action) {
        Objects.requireNonNull(action.getType(), "Тип действия не может быть null");

        return switch (action.getType()) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }
}
