package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler extends BaseHubEventHandler<Scenario> {
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    public String getMessageType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    @Override
    protected Scenario mapToEntity(HubEventAvro event) {
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();
        return scenarioRepository
                .findByHubIdAndName(event.getHubId(), payload.getName())
                .orElseGet(() -> Scenario.builder()
                        .name(payload.getName())
                        .hubId(event.getHubId())
                        .build());
    }

    @Override
    @Transactional
    protected void processEntity(Scenario scenario, HubEventAvro event) {
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();

        if (scenario.getId() == null) {
            scenario = scenarioRepository.save(scenario);
        }

        processActions(payload, scenario);

        processConditions(payload, scenario);
    }

    private void processActions(ScenarioAddedEventAvro event, Scenario scenario) {
        List<String> sensorIds = event.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();

        if (hasSensors(sensorIds, scenario.getHubId())) {
            actionRepository.saveAll(mapActions(event, scenario));
        }
    }

    private void processConditions(ScenarioAddedEventAvro event, Scenario scenario) {
        List<String> sensorIds = event.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList();

        if (hasSensors(sensorIds, scenario.getHubId())) {
            conditionRepository.saveAll(mapConditions(event, scenario));
        }
    }

    private boolean hasSensors(List<String> sensorsIds, String hubId) {
        return sensorRepository.existsByIdInAndHubId(sensorsIds, hubId);
    }

    private Set<Action> mapActions(ScenarioAddedEventAvro event, Scenario scenario) {
        return event.getActions().stream()
                .map(action -> Action.builder()
                        .sensor(sensorRepository.findById(action.getSensorId()).orElseThrow())
                        .scenario(scenario)
                        .type(action.getType())
                        .value(action.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    private Set<Condition> mapConditions(ScenarioAddedEventAvro event, Scenario scenario) {
        return event.getConditions().stream()
                .map(condition -> Condition.builder()
                        .sensor(sensorRepository.findById(condition.getSensorId()).orElseThrow())
                        .scenario(scenario)
                        .type(condition.getType())
                        .operation(condition.getOperation())
                        .value(convertValue(condition.getValue()))
                        .build())
                .collect(Collectors.toSet());
    }

    private Integer convertValue(Object value) {
        return switch (value) {
            case null -> null;
            case Integer i -> i;
            case Boolean b -> b ? 1 : 0;
            default -> throw new IllegalArgumentException("Неподдерживаемый тип значения: " + value.getClass());
        };
    }
}
