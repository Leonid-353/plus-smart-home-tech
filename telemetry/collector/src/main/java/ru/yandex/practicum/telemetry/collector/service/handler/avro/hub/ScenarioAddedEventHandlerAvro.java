package ru.yandex.practicum.telemetry.collector.service.handler.avro.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerAvro;

import java.util.List;

@Component
public class ScenarioAddedEventHandlerAvro extends BaseHubEventHandlerAvro<ScenarioAddedEventAvro> {
    public ScenarioAddedEventHandlerAvro(KafkaEventProducerAvro producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEventProto event) {
        ScenarioAddedEventProto scenarioAddedEvent = event.getScenarioAdded();

        List<ScenarioConditionAvro> conditions = scenarioAddedEvent.getConditionsList().stream()
                .map(this::convertCondition)
                .toList();

        List<DeviceActionAvro> actions = scenarioAddedEvent.getActionsList().stream()
                .map(this::convertAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioConditionAvro convertCondition(ScenarioConditionProto condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));
        switch (condition.getValueCase()) {
            case BOOL_VALUE -> builder.setValue(condition.getBoolValue());
            case INT_VALUE -> builder.setValue(condition.getIntValue());
            case VALUE_NOT_SET -> {
            }
        }

        return builder.build();
    }

    private DeviceActionAvro convertAction(DeviceActionProto action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}
