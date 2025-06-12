package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import java.util.List;

@Component
public class ScenarioAddedEventHandler extends BaseHubEventHandler<ScenarioAddedEventProto> {
    public ScenarioAddedEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventProto mapToProto(HubEventProto event) {
        ScenarioAddedEventProto scenarioAddedEvent = event.getScenarioAdded();

        List<ScenarioConditionProto> conditions = scenarioAddedEvent.getConditionsList().stream()
                .map(this::convertCondition)
                .toList();

        List<DeviceActionProto> actions = scenarioAddedEvent.getActionsList().stream()
                .map(this::convertAction)
                .toList();

        return ScenarioAddedEventProto.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .addAllConditions(conditions)
                .addAllActions(actions)
                .build();
    }

    @Override
    protected void setPayload(HubEventProto.Builder builder, ScenarioAddedEventProto payload) {
        builder.setScenarioAdded(payload);
    }

    private ScenarioConditionProto convertCondition(ScenarioConditionProto condition) {
        ScenarioConditionProto.Builder builder = ScenarioConditionProto.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(condition.getType())
                .setOperation(condition.getOperation());
        switch (condition.getValueCase()) {
            case BOOL_VALUE -> builder.setBoolValue(condition.getBoolValue());
            case INT_VALUE -> builder.setIntValue(condition.getIntValue());
            case VALUE_NOT_SET -> {
            }
        }

        return builder.build();
    }

    private DeviceActionProto convertAction(DeviceActionProto action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(action.getType());

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}
