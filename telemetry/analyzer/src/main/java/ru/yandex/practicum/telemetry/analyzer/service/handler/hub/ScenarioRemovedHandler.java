package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedHandler extends BaseHubEventHandler<Scenario> {
    private final ActionRepository actionRepository;
    private final ConditionRepository conditionRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public String getMessageType() {
        return ScenarioRemovedEventAvro.class.getSimpleName();
    }

    @Override
    protected Scenario mapToEntity(HubEventAvro event) {
        ScenarioRemovedEventAvro payload = (ScenarioRemovedEventAvro) event.getPayload();
        return scenarioRepository
                .findByHubIdAndName(event.getHubId(), payload.getName())
                .orElseThrow(() -> new EntityNotFoundException(
                        String.format("Сценарий %s не найден для хаба %s",
                                payload.getName(), event.getHubId())
                ));
    }

    @Override
    @Transactional
    protected void processEntity(Scenario scenario, HubEventAvro event) {
        actionRepository.deleteByScenario(scenario);
        conditionRepository.deleteByScenario(scenario);

        scenarioRepository.delete(scenario);
    }
}
